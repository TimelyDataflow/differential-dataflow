//! Move invariant statements to the outermost scope where their inputs are available.
//! Inside an iterative scope an invariant statement does the same work with longer
//! timestamps. Outside, it carries outer times only and is computed once per outer
//! step; entering its result reuses the same data (and the same trace for arrangements).
//! This also holds for non-iterative child scopes.
//!
//! A scope's variables, time operators, half joins, and inspections stop motion.
//! Exports and binds stay in their scope, referring to an import when their value moves.

use std::collections::BTreeMap;
use crate::ir::LinearOp;
use crate::scope_ir::{Import, Item, Node, Ref, Scope, Source};
use super::structural::compact;

use super::{Observe, Pass, Timings, item_ordinals};

fn key(r: &Ref) -> (u8, usize, usize) {
    match r {
        Ref::Local(i) => (0, *i, 0), Ref::Import(i) => (1, *i, 0),
        Ref::Var(i) => (2, *i, 0), Ref::ChildExport(i, e) => (3, *i, *e),
    }
}

fn arranged(scope: &Scope, imports: &[bool], r: &Ref) -> bool {
    match r {
        Ref::Local(i) => matches!(scope.items[*i], Item::Op(Node::Arrange(_) | Node::Reduce { .. })),
        Ref::Import(i) => imports[*i],
        // Leaving a child flattens its exports. Feedback is also a collection.
        Ref::Var(_) | Ref::ChildExport(_, _) => false,
    }
}

fn child_imports(scope: &Scope, imports: &[bool], child: &Scope) -> Vec<bool> {
    child.imports.iter().map(|i| match &i.from {
        Source::Parent(r) => arranged(scope, imports, r),
        _ => false,
    }).collect()
}

pub(super) fn run(scope: &mut Scope, collapse: bool, hoist: bool, timings: &mut Timings, observe: &mut Observe<'_>) -> bool {
    let imports = scope.imports.iter().map(|i| matches!(i.from, Source::Trace(_))).collect::<Vec<_>>();
    let mut changed = false;
    if collapse {
        changed |= timings.pass_mut(Pass::CollapseParentArranges).observed(observe, |observe| collapse_parent_arranges(scope, &imports, "root", observe));
    }
    if hoist {
        changed |= timings.pass_mut(Pass::HoistInvariant).observed(observe, |observe| {
            hoist_statements(scope, "root", observe)
                | hoist_arranges(scope, &imports, "root", observe)
                | prune_imports(scope)
        });
    }
    changed
}

/// Moving a reader can leave its old parent edge unused. Remove these edges even
/// when dead-binding cleanup is off: DDP reconstructs imports from their readers.
/// Names count as readers here, so an explicit source alias is still preserved.
fn prune_imports(scope: &mut Scope) -> bool {
    let mut changed = false;
    for item in &mut scope.items {
        if let Item::Sub(child) = item { changed |= prune_imports(child); }
    }
    let mut read = vec![false; scope.imports.len()];
    scope.for_each_ref(|r| if let Ref::Import(i) = r { read[*i] = true; });
    let mut remap = vec![0; read.len()];
    let imports = std::mem::take(&mut scope.imports);
    for (i, import) in imports.into_iter().enumerate() {
        if read[i] || !matches!(import.from, Source::Parent(_)) {
            remap[i] = scope.imports.len();
            scope.imports.push(import);
        }
    }
    changed |= scope.imports.len() != read.len();
    scope.for_each_ref(|r| if let Ref::Import(i) = r { *i = remap[*i]; });
    changed
}

fn binding(scope: &Scope, r: &Ref) -> String {
    scope.names.iter().find(|(_, value)| value == r).map(|(name, _)| name.clone()).unwrap_or_else(|| match r {
        Ref::Local(i) => scope.labels.get(*i).cloned().unwrap_or_else(|| "rows".into()),
        Ref::Import(i) => scope.imports[*i].name.rsplit("::").next().unwrap().into(),
        Ref::Var(i) => scope.vars[*i].name.clone(),
        Ref::ChildExport(i, e) => if let Item::Sub(child) = &scope.items[*i] { child.exports[*e].name.clone() } else { unreachable!() },
    })
}

struct MovedNames {
    used: std::collections::BTreeSet<String>,
    next: BTreeMap<String, usize>,
    bindings: std::collections::HashMap<Ref, String>,
}

impl MovedNames {
    fn new(scope: &Scope) -> Self {
        let used = scope.names.iter().map(|(n, _)| n.clone())
            .chain(scope.vars.iter().map(|v| v.name.clone()))
            .chain(scope.items.iter().filter_map(|i| if let Item::Sub(c) = i { Some(c.name.clone()) } else { None })).collect();
        let mut bindings = std::collections::HashMap::new();
        for (name, r) in &scope.names { bindings.entry(r.clone()).or_insert_with(|| name.clone()); }
        Self { used, bindings, next: BTreeMap::new() }
    }
}

fn moved_name(scope: &Scope, names: &mut MovedNames, preferred: Option<&str>, node: &Node) -> String {
    let used = &mut names.used;
    if let Some(name) = preferred { if used.insert(name.into()) { return name.into(); } }
    let mut inputs = Vec::new();
    node.clone().for_each_ref(|r| inputs.push(r.clone()));
    let source = inputs.first().map(|r| names.bindings.get(r).cloned().unwrap_or_else(|| binding(scope, r))).unwrap_or_else(|| "rows".into());
    let role = match node {
        Node::Arrange(_) => "idx", Node::Join { .. } => "join", Node::Reduce { .. } => "reduce",
        Node::Linear { ops, .. } => match ops.as_slice() {
            [LinearOp::Project(_)] => "map", [LinearOp::Filter(_)] => "filter", [LinearOp::FlatMap(_)] => "flatmap",
            [LinearOp::Negate] => "neg", _ => "rows",
        },
        _ => "rows",
    };
    let mut base: String = format!("{source}_{role}").chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if base.as_bytes()[0].is_ascii_digit() { base.insert_str(0, "x_"); }
    let mut name = base.clone();
    let n = names.next.entry(base.clone()).or_insert(2);
    while !used.insert(name.clone()) { name = format!("{base}{n}"); *n += 1; }
    name
}

/// Arrangements use the sharing walk below. The preparation fixed point alternates
/// the two walks, so hoisting a map exposes its arrange, which can expose a join.
fn hoist_statements(scope: &mut Scope, path: &str, observe: &mut Observe<'_>) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < scope.items.len() {
        let count = scope.items.len();
        let labelled = scope.labels.len() == count;
        let Item::Sub(child) = &mut scope.items[i] else { i += 1; continue };
        let child_path = format!("{path}/{}", child.name);
        changed |= hoist_statements(child, &child_path, observe);
        let mut moved = Vec::new();
        let mut labels = Vec::new();
        let mut authored = Vec::new();
        let mut dead = vec![false; child.items.len()];
        let ordinals = item_ordinals(child);
        let mut remap: Vec<_> = (0..child.items.len()).map(Ref::Local).collect();
        let mut child_names = std::collections::HashMap::new();
        for (name, r) in &child.names { child_names.entry(r.clone()).or_insert_with(|| name.clone()); }
        for j in 0..child.items.len() {
            let Item::Op(node) = &child.items[j] else { continue };
            let movable = match node {
                Node::Linear { ops, .. } => ops.iter().all(|op| matches!(op,
                    LinearOp::Project(_) | LinearOp::Filter(_) | LinearOp::FlatMap(_) | LinearOp::Negate)),
                Node::Concat(_) | Node::Join { .. } | Node::Reduce { .. } => true,
                Node::Arrange(_) | Node::HalfJoin { .. } | Node::Inspect { .. } => false,
            };
            if !movable { continue; }
            let mut node = node.clone();
            let mut invariant = true;
            node.for_each_ref(|r| {
                if let Ref::Local(k) = r { *r = remap[*k].clone(); }
                if let Ref::Import(k) = r {
                    if let Source::Parent(parent) = &child.imports[*k].from {
                        *r = parent.clone();
                        return;
                    }
                }
                invariant = false;
            });
            if !invariant { continue; }
            observe(&child_path, child, &ordinals, "hoisted invariant statement", j, j);
            let label = child.labels.get(j).cloned().unwrap_or_else(|| "invariant".into());
            let import = child.imports.len();
            child.imports.push(Import {
                name: format!("::{label}"), from: Source::Parent(Ref::Local(count + moved.len())), shape: None,
            });
            authored.push(child_names.get(&Ref::Local(j)).cloned());
            // Readers use the entered result directly; exports still supply any child alias needed.
            remap[j] = Ref::Import(import);
            dead[j] = true;
            moved.push(Item::Op(node));
            labels.push(label);
        }
        child.names.retain(|(_, r)| !matches!(r, Ref::Local(j) if dead[*j]));
        child.for_each_ref(|r| if let Ref::Local(j) = r { *r = remap[*j].clone(); });
        compact(child, &dead);
        let n = moved.len();
        if n > 0 {
            // All sources precede this child. Insert the moved statements before it,
            // preserving their dependency order and remapping every parent edge.
            let mut names = MovedNames::new(scope);
            for (k, item) in moved.into_iter().enumerate() {
                let Item::Op(node) = &item else { unreachable!() };
                let name = moved_name(scope, &mut names, authored[k].as_deref(), node);
                names.bindings.entry(Ref::Local(count + k)).or_insert_with(|| name.clone());
                scope.names.push((name, Ref::Local(count + k)));
                scope.items.push(item);
            }
            scope.items[i..].rotate_right(n);
            if labelled {
                scope.labels.extend(labels);
                scope.labels[i..].rotate_right(n);
            }
            scope.for_each_ref(|r| match r {
                Ref::Local(k) | Ref::ChildExport(k, _) if *k >= count => *k = i + (*k - count),
                Ref::Local(k) | Ref::ChildExport(k, _) if *k >= i => *k += n,
                _ => {},
            });
            let order = scope.clustered_order(&(i..i + n).collect::<Vec<_>>());
            let child_index = order.iter().position(|&old| old == i + n).unwrap();
            scope.reorder(&order);
            i = child_index;
            changed = true;
        }
        i += 1;
    }
    changed
}

/// An arrangement remains one on entry, including entry into an iterative scope.
/// Only parent imports are cleaned up here; the existing structural rule owns local ones.
fn collapse_parent_arranges(scope: &mut Scope, imports: &[bool], path: &str, observe: &mut Observe<'_>) -> bool {
    let mut changed = false;
    let mut dead = vec![false; scope.items.len()];
    let ordinals = item_ordinals(scope);
    let mut remap: Vec<_> = (0..scope.items.len()).map(Ref::Local).collect();
    for i in 0..scope.items.len() {
        if let Item::Op(Node::Arrange(Ref::Import(k))) = &scope.items[i] {
            if imports[*k] && matches!(scope.imports[*k].from, Source::Parent(_)) {
                let to = Ref::Import(*k);
                observe(path, scope, &ordinals, "removed arrange of parent arrangement", i, i);
                remap[i] = to;
                dead[i] = true;
                changed = true;
            }
        }
    }
    scope.for_each_ref(|r| if let Ref::Local(i) = r { *r = remap[*i].clone(); });
    compact(scope, &dead);
    for i in 0..scope.items.len() {
        if let Item::Sub(child) = &scope.items[i] {
            let imports = child_imports(scope, imports, child);
            let Item::Sub(child) = &mut scope.items[i] else { unreachable!() };
            changed |= collapse_parent_arranges(child, &imports, &format!("{path}/{}", child.name), observe);
        }
    }
    changed
}

#[derive(Clone)]
struct Site {
    path: Vec<usize>,
    item: usize,
}

/// Gather arrangements of this scope's bindings, following only unchanged imports
/// through descendants. A map, delay, feedback, or child export breaks that chain.
fn sites(scope: &Scope, imports: &[Option<Ref>], path: &[usize], groups: &mut BTreeMap<(u8, usize, usize), (Ref, Vec<Site>)>) {
    let lift = |r: &Ref| if path.is_empty() { Some(r.clone()) }
        else if let Ref::Import(i) = r { imports[*i].clone() } else { None };
    for (i, item) in scope.items.iter().enumerate() {
        match item {
            Item::Op(Node::Arrange(r)) => if let Some(r) = lift(r) {
                groups.entry(key(&r)).or_insert_with(|| (r, Vec::new())).1.push(Site { path: path.to_vec(), item: i });
            },
            Item::Sub(child) => {
                let imports = child.imports.iter().map(|i| match &i.from {
                    Source::Parent(r) => lift(r), _ => None,
                }).collect::<Vec<_>>();
                let mut nested = path.to_vec();
                nested.push(i);
                sites(child, &imports, &nested, groups);
            }
            _ => {},
        }
    }
}

/// Redirect a group's uses, adding separate imports so existing row readers keep
/// their collection edge. Child items are compacted before returning; the caller
/// compacts this scope after moving the surviving arrangement into dependency order.
fn replace(scope: &mut Scope, sites: &[Site], to: Ref, name: &str, path: &str, observe: &mut Observe<'_>) -> Vec<bool> {
    let mut dead = vec![false; scope.items.len()];
    let ordinals = item_ordinals(scope);
    let mut remap: Vec<_> = (0..scope.items.len()).map(Ref::Local).collect();
    let mut nested: BTreeMap<usize, Vec<Site>> = BTreeMap::new();
    for site in sites {
        if let Some((&i, path)) = site.path.split_first() {
            nested.entry(i).or_default().push(Site { path: path.to_vec(), item: site.item });
            continue;
        }
        if key(&Ref::Local(site.item)) != key(&to) {
            let action = if matches!(to, Ref::Import(_)) { "shared arrangement from ancestor" } else { "shared local arrangement" };
            observe(path, scope, &ordinals, action, site.item, site.item);
            remap[site.item] = to.clone();
            dead[site.item] = true;
        }
    }
    if matches!(to, Ref::Import(_)) {
        scope.names.retain(|(_, r)| !matches!(r, Ref::Local(i) if dead[*i]));
    }
    scope.for_each_ref(|r| if let Ref::Local(i) = r { *r = remap[*i].clone(); });
    for (i, sites) in nested {
        let item = &mut scope.items[i];
        let Item::Sub(child) = item else { continue };
        let import = child.imports.iter().position(|i| matches!(&i.from, Source::Parent(r) if key(r) == key(&to)))
            .unwrap_or_else(|| {
                let i = child.imports.len();
                child.imports.push(Import { name: format!("::{name}"), from: Source::Parent(to.clone()), shape: None });
                i
            });
        let dead = replace(child, &sites, Ref::Import(import), name, &format!("{path}/{}", child.name), observe);
        compact(child, &dead);
    }
    dead
}

fn hoist_arranges(scope: &mut Scope, imports: &[bool], path: &str, observe: &mut Observe<'_>) -> bool {
    let mut changed = false;
    for i in 0..scope.items.len() {
        if let Item::Sub(child) = &scope.items[i] {
            let imports = child_imports(scope, imports, child);
            let Item::Sub(child) = &mut scope.items[i] else { unreachable!() };
            changed |= hoist_arranges(child, &imports, &format!("{path}/{}", child.name), observe);
        }
    }
    loop {
        let mut groups = BTreeMap::new();
        sites(scope, &[], &[], &mut groups);
        let group = groups.into_values().find(|(_, sites)| sites.iter().any(|s| !s.path.is_empty()));
        let Some((source, sites)) = group else { break };
        // Reuse an arrangement already available here. In particular, lowering a
        // printed child join may have inserted an arrange on its parent import.
        if arranged(scope, imports, &source) {
            let name = match &source {
                Ref::Local(i) => scope.labels.get(*i).cloned().unwrap_or_else(|| "arrangement".into()),
                Ref::Import(i) => scope.imports[*i].name.clone(),
                _ => unreachable!(),
            };
            // Only descendant sites move. Leave redundant local arrangements to
            // structural cleanup; a re-lowered child join's implicit arrange must
            // not make an explicit local arrange disappear on the second plan.
            let descendants = sites.into_iter().filter(|s| !s.path.is_empty()).collect::<Vec<_>>();
            let dead = replace(scope, &descendants, source, &name, path, observe);
            compact(scope, &dead);
            changed = true;
            continue;
        }
        // Even a lone descendant arrangement belongs beside the binding it arranges.
        let first = sites.iter().map(|s| s.path.first().copied().unwrap_or(s.item)).min().unwrap();
        let existing = sites.iter().find(|s| s.path.is_empty()).map(|s| s.item);
        let labels = scope.labels.len() == scope.items.len();
        let index = existing.unwrap_or_else(|| {
            let site = &sites[0];
            let mut child = &*scope;
            for &i in &site.path {
                let Item::Sub(next) = &child.items[i] else { unreachable!() };
                child = next;
            }
            let label = child.labels.get(site.item).cloned().unwrap_or_else(|| "arrangement".into());
            let preferred = child.names.iter().find(|(_, r)| *r == Ref::Local(site.item)).map(|(n, _)| n.clone());
            let node = Node::Arrange(source.clone());
            let name = moved_name(scope, &mut MovedNames::new(scope), preferred.as_deref(), &node);
            let index = scope.items.len();
            scope.names.push((name, Ref::Local(index)));
            scope.items.push(Item::Op(node));
            if labels { scope.labels.push(label); }
            index
        });
        let name = scope.labels.get(index).cloned().unwrap_or_else(|| "arrangement".into());
        let mut dead = replace(scope, &sites, Ref::Local(index), &name, path, observe);
        // The source precedes every original user, so inserting before the earliest
        // user keeps all dependencies backward, even when the parent peer was later.
        if index > first {
            let item = scope.items.remove(index);
            scope.items.insert(first, item);
            if labels {
                let label = scope.labels.remove(index);
                scope.labels.insert(first, label);
            }
            let removed = dead.remove(index);
            dead.insert(first, removed);
            scope.for_each_ref(|r| match r {
                Ref::Local(i) | Ref::ChildExport(i, _) => {
                    if *i == index { *i = first; }
                    else if (first..index).contains(i) { *i += 1; }
                }
                _ => {},
            });
        }
        let survivor = if index > first { first } else { index };
        let survivor = (0..survivor).filter(|&i| !dead[i]).count();
        compact(scope, &dead);
        let order = scope.clustered_order(&[survivor]);
        scope.reorder(&order);
        changed = true;
    }
    changed
}

#[cfg(test)]
#[path = "arrangements_tests.rs"]
mod tests;
