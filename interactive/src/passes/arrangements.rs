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
        });
    }
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
    let source = node.refs().next().map(|r| names.bindings.get(r).cloned().unwrap_or_else(|| binding(scope, r))).unwrap_or_else(|| "rows".into());
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
            let parent = |r: &Ref| {
                let r = if let Ref::Local(k) = r { &remap[*k] } else { r };
                if let Ref::Import(k) = r {
                    if let Source::Parent(parent) = &child.imports[*k].from { return Some(parent.clone()); }
                }
                None
            };
            if !node.refs().all(|r| parent(r).is_some()) { continue; }
            observe(&child_path, child, &ordinals, "hoisted invariant statement", j, j);
            let Item::Op(mut node) = std::mem::replace(&mut child.items[j], Item::Op(Node::Concat(Vec::new()))) else { unreachable!() };
            node.for_each_ref(|r| *r = parent(r).unwrap());
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

/// All groups use the original item indices until the whole scope has been rewritten.
#[derive(Default)]
struct Replacements {
    items: Vec<(usize, usize)>,
    children: BTreeMap<usize, Replacements>,
    groups: std::collections::BTreeSet<usize>,
}

impl Replacements {
    fn add(&mut self, site: &Site, group: usize) {
        let mut at = self;
        for &child in &site.path {
            at = at.children.entry(child).or_default();
            at.groups.insert(group);
        }
        at.items.push((site.item, group));
    }
}

/// Redirect every group in one walk, adding imports once and compacting each child once.
fn replace(scope: &mut Scope, sites: Replacements, targets: &std::collections::HashMap<usize, Ref>, names: &[String], path: &str, observe: &mut Observe<'_>) -> Vec<bool> {
    let mut dead = vec![false; scope.items.len()];
    let mut unnamed = vec![false; scope.items.len()];
    let ordinals = item_ordinals(scope);
    let mut remap: Vec<_> = (0..scope.items.len()).map(Ref::Local).collect();
    for (item, group) in sites.items {
        let to = &targets[&group];
        if *to != Ref::Local(item) {
            let action = if matches!(to, Ref::Import(_)) { "shared arrangement from ancestor" } else { "shared local arrangement" };
            observe(path, scope, &ordinals, action, item, item);
            remap[item] = to.clone();
            dead[item] = true;
            unnamed[item] = matches!(to, Ref::Import(_));
        }
    }
    // One group's source can itself be another group's redundant arrangement.
    for i in 0..remap.len() {
        let mut to = remap[i].clone();
        while let Ref::Local(j) = to {
            if remap[j] == to { break; }
            to = remap[j].clone();
        }
        remap[i] = to;
    }
    scope.names.retain(|(_, r)| !matches!(r, Ref::Local(i) if unnamed[*i]));
    let redirect = |r: &mut Ref| if let Ref::Local(i) = r { *r = remap[*i].clone(); };
    scope.for_each_ref(redirect);
    for (i, sites) in sites.children {
        let Item::Sub(child) = &mut scope.items[i] else { unreachable!() };
        let mut imports = std::collections::HashMap::new();
        for (i, import) in child.imports.iter().enumerate() {
            if let Source::Parent(r) = &import.from { imports.entry(r.clone()).or_insert(i); }
        }
        let mut nested = std::collections::HashMap::new();
        for &group in &sites.groups {
            let mut to = targets[&group].clone();
            redirect(&mut to);
            let import = *imports.entry(to.clone()).or_insert_with(|| {
                let i = child.imports.len();
                child.imports.push(Import { name: format!("::{}", names[group]), from: Source::Parent(to), shape: None });
                i
            });
            nested.insert(group, Ref::Import(import));
        }
        let dead = replace(child, sites, &nested, names, &format!("{path}/{}", child.name), observe);
        compact(child, &dead);
    }
    dead
}

/// Cluster arrangements without shifting the item vector for each group. Their
/// sources are not selected arrangements: those were reused above. The other
/// items therefore retain their relative order throughout this walk.
fn cluster_arranges(scope: &Scope, selected: &[usize]) -> Vec<usize> {
    let count = scope.items.len();
    let mut next: Vec<_> = (1..=count).collect();
    let mut prev: Vec<_> = (0..count).map(|i| i.checked_sub(1).unwrap_or(count)).collect();
    let mut head = if count == 0 { count } else { 0 };
    let mut rank: Vec<_> = (0..=count).collect();
    loop {
        let mut changed = false;
        let mut previous_entry = None;
        for &moving in selected {
            let Item::Op(Node::Arrange(anchor)) = &scope.items[moving] else { unreachable!() };
            let mut target = match anchor {
                Ref::Local(i) | Ref::ChildExport(i, _) => next[*i],
                _ => head,
            };
            if matches!(anchor, Ref::Import(_) | Ref::Var(_)) {
                if let Some(previous) = previous_entry {
                    target = next[previous];
                    let mut memo = std::collections::HashMap::new();
                    while rank[target] <= rank[moving] && target != moving
                        && derives(scope, target, &Ref::Local(previous), &mut memo) {
                        target = next[target];
                    }
                }
                previous_entry = Some(moving);
            }
            let mut memo = std::collections::HashMap::new();
            while rank[target] <= rank[moving] && target != moving
                && derives(scope, target, anchor, &mut memo) {
                target = next[target];
            }
            if target == moving || rank[target] > rank[moving] { continue; }
            // Unlink and insert before target; no other item's position needs updating.
            if prev[moving] == count { head = next[moving]; }
            else { next[prev[moving]] = next[moving]; }
            if next[moving] != count { prev[next[moving]] = prev[moving]; }
            prev[moving] = prev[target];
            next[moving] = target;
            if prev[target] == count { head = moving; }
            else { next[prev[target]] = moving; }
            prev[target] = moving;
            rank[moving] = rank[target];
            changed = true;
        }
        let mut order = Vec::with_capacity(count);
        let mut at = head;
        while at != count {
            rank[at] = order.len();
            order.push(at);
            at = next[at];
        }
        if !changed { return order; }
    }
}

/// Only operators extend a cluster. A child scope is a boundary, even if its
/// imports depend on the anchor. Cache the earlier operators visited for a group.
fn derives(scope: &Scope, item: usize, anchor: &Ref, memo: &mut std::collections::HashMap<usize, bool>) -> bool {
    if let Some(&answer) = memo.get(&item) { return answer; }
    let answer = if let Some(Item::Op(node)) = scope.items.get(item) {
        node.refs().any(|r| r == anchor || matches!(r, Ref::Local(i) if derives(scope, *i, anchor, memo)))
    } else { false };
    memo.insert(item, answer);
    answer
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
    let mut groups = BTreeMap::new();
    sites(scope, &[], &[], &mut groups);
    let mut replacements = Replacements::default();
    let mut targets = std::collections::HashMap::new();
    let mut names = Vec::new();
    let mut moved_names = MovedNames::new(scope);
    let count = scope.items.len();
    let labelled = scope.labels.len() == count;
    let mut before = vec![Vec::new(); count];
    let mut moved = vec![false; count];
    let mut selected = Vec::new();
    for (source, sites) in groups.into_values() {
        if !sites.iter().any(|s| !s.path.is_empty()) { continue; }
        let group = names.len();
        // Reuse an arrangement already available here. Local redundant arrangements
        // remain for structural cleanup, just as they do without descendant readers.
        let (target, name) = if arranged(scope, imports, &source) {
            let name = match &source {
                Ref::Local(i) => scope.labels.get(*i).cloned().unwrap_or_else(|| "arrangement".into()),
                Ref::Import(i) => scope.imports[*i].name.clone(),
                _ => unreachable!(),
            };
            for site in sites.iter().filter(|s| !s.path.is_empty()) { replacements.add(site, group); }
            (source, name)
        } else {
            let first = sites.iter().map(|s| s.path.first().copied().unwrap_or(s.item)).min().unwrap();
            let existing = sites.iter().find(|s| s.path.is_empty()).map(|s| s.item);
            let index = existing.unwrap_or_else(|| {
                let site = &sites[0];
                let mut child = &*scope;
                for &i in &site.path {
                    let Item::Sub(next) = &child.items[i] else { unreachable!() };
                    child = next;
                }
                let label = child.labels.get(site.item).cloned().unwrap_or_else(|| "arrangement".into());
                let preferred = child.names.iter().find(|(_, r)| *r == Ref::Local(site.item)).map(|(n, _)| n.as_str());
                let node = Node::Arrange(source);
                let name = moved_name(scope, &mut moved_names, preferred, &node);
                let index = scope.items.len();
                moved_names.bindings.insert(Ref::Local(index), name.clone());
                scope.names.push((name, Ref::Local(index)));
                scope.items.push(Item::Op(node));
                if labelled { scope.labels.push(label); }
                index
            });
            if index > first {
                before[first].push(index);
                if index < count { moved[index] = true; }
            }
            selected.push(index);
            for site in &sites { replacements.add(site, group); }
            (Ref::Local(index), scope.labels.get(index).cloned().unwrap_or_else(|| "arrangement".into()))
        };
        targets.insert(group, target);
        names.push(name);
    }
    if names.is_empty() { return changed; }
    let dead = replace(scope, replacements, &targets, &names, path, observe);
    let mut remap = vec![0; dead.len()];
    let mut next = 0;
    for (i, &dead) in dead.iter().enumerate() {
        remap[i] = next;
        if !dead { next += 1; }
    }
    let mut order = Vec::with_capacity(next);
    for i in 0..count {
        for &j in &before[i] { if !dead[j] { order.push(remap[j]); } }
        if !moved[i] && !dead[i] { order.push(remap[i]); }
    }
    selected.retain(|&i| !dead[i]);
    for i in &mut selected { *i = remap[*i]; }
    compact(scope, &dead);
    let mut positions = vec![0; order.len()];
    for (new, &old) in order.iter().enumerate() { positions[old] = new; }
    for i in &mut selected { *i = positions[*i]; }
    scope.reorder(&order);
    let order = cluster_arranges(scope, &selected);
    scope.reorder(&order);
    true
}

#[cfg(test)]
#[path = "arrangements_tests.rs"]
mod tests;
