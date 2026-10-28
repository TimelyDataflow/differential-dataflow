//! Share explicit arrangements through unchanged parent imports.

use std::collections::BTreeMap;
use crate::scope_ir::{Import, Item, Node, Ref, Scope, Source};
use super::structural::compact;

use super::{Observe, item_ordinals};

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

pub(super) fn run(scope: &mut Scope, collapse: bool, hoist: bool, observe: &mut Observe<'_>) -> bool {
    let imports = scope.imports.iter().map(|i| matches!(i.from, Source::Trace(_))).collect::<Vec<_>>();
    let mut changed = false;
    if collapse { changed |= collapse_parent_arranges(scope, &imports, "root", observe); }
    if hoist { changed |= hoist_arranges(scope, &imports, "root", observe); }
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
        let group = groups.into_values().find(|(r, sites)| {
            !arranged(scope, imports, r) && sites.iter().any(|s| s.path != sites[0].path)
        });
        let Some((source, sites)) = group else { break };
        // At least two scopes arrange this exact binding. Never relocate a lone use.
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
            let index = scope.items.len();
            scope.items.push(Item::Op(Node::Arrange(source)));
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
        compact(scope, &dead);
        changed = true;
    }
    changed
}

#[cfg(test)]
#[path = "arrangements_tests.rs"]
mod tests;
