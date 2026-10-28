//! Liveness facts and removal of computations and bindings with no live reader.

use crate::scope_ir::{Item, Node, Ref, Scope, Source};

/// References are indexed in the original tree, before any removal or remapping.
struct Live {
    items: Vec<bool>,
    imports: Vec<bool>,
    exports: Vec<Vec<bool>>,
    children: Vec<Option<Box<Live>>>,
    inspect: bool,
}

impl Live {
    fn mark(&mut self, r: &Ref) {
        match r {
            Ref::Local(i) => self.items[*i] = true,
            Ref::Import(i) => self.imports[*i] = true,
            Ref::ChildExport(i, e) => { self.items[*i] = true; self.exports[*i][*e] = true; }
            Ref::Var(_) => {} // All feedback bindings in a retained scope are roots.
        }
    }
}

/// One backward walk per scope. A child's live imports feed its parent, and an
/// Inspect keeps all enclosing scopes alive. Analyze children at their position
/// in this walk, when every reader has contributed its demanded exports.
/// Feedback roots matter only if the enclosing scope is retained.
fn analyze(scope: &Scope, exports: Option<&[bool]>) -> Live {
    let mut live = Live {
        items: vec![false; scope.items.len()],
        // Root positional inputs remain part of the client's interface.
        imports: scope.imports.iter().map(|i| matches!(i.from, Source::Input(_))).collect(),
        exports: scope.items.iter().map(|item| match item {
            Item::Sub(child) => vec![false; child.exports.len()],
            Item::Op(_) => Vec::new(),
        }).collect(),
        children: (0..scope.items.len()).map(|_| None).collect(),
        inspect: false,
    };
    for (i, export) in scope.exports.iter().enumerate() {
        if exports.is_none_or(|exports| exports[i]) { live.mark(&export.value); }
    }
    for bind in &scope.binds { live.mark(&bind.value); }
    for (i, item) in scope.items.iter().enumerate().rev() {
        match item {
            Item::Sub(child) => {
                let child_live = analyze(child, Some(&live.exports[i]));
                live.inspect |= child_live.inspect;
                live.items[i] |= child_live.inspect;
                if live.items[i] {
                    for (import, &keep) in child.imports.iter().zip(&child_live.imports) {
                        if keep {
                            if let Source::Parent(r) = &import.from { live.mark(r); }
                        }
                    }
                    live.children[i] = Some(Box::new(child_live));
                }
            }
            Item::Op(node) => {
                if matches!(node, Node::Inspect { .. }) { live.items[i] = true; live.inspect = true; }
                if !live.items[i] { continue; }
                match node {
                    Node::Linear { input, .. } | Node::Arrange(input)
                    | Node::Reduce { input, .. } | Node::Inspect { input, .. } => live.mark(input),
                    Node::Join { left, right, .. } => { live.mark(left); live.mark(right); }
                    Node::HalfJoin { stream, arrangement, .. } => { live.mark(stream); live.mark(arrangement); }
                    Node::Concat(refs) => for r in refs { live.mark(r); },
                }
            }
        }
    }
    live
}

fn remap(keep: &[bool]) -> Vec<usize> {
    let mut next = 0;
    keep.iter().map(|&keep| {
        if keep { let slot = next; next += 1; slot } else { usize::MAX }
    }).collect()
}

/// Apply the analysis without revisiting liveness or optimizing children. Names
/// describe surviving values; they never keep a computation alive.
fn prune(scope: &mut Scope, live: &Live) -> usize {
    let mut changed = 0;
    let mut exports = vec![Vec::new(); scope.items.len()];
    for (i, item) in scope.items.iter_mut().enumerate() {
        if !live.items[i] { changed += 1; continue; }
        if let Item::Sub(child) = item {
            exports[i] = remap(&live.exports[i]);
            let before = child.exports.len();
            let mut keep = live.exports[i].iter();
            child.exports.retain(|_| *keep.next().unwrap());
            changed += before - child.exports.len();
            changed += prune(child, live.children[i].as_ref().unwrap());
        }
    }
    let mut seen = std::collections::HashSet::new();
    scope.names.retain(|(_, r)| {
        let keep = match r {
            Ref::Local(i) => live.items[*i],
            Ref::Import(i) => live.imports[*i],
            Ref::ChildExport(i, e) => live.items[*i] && live.exports[*i][*e],
            Ref::Var(_) => false, // The variable already has its declared name.
        };
        let keep = keep && seen.insert(r.clone());
        if !keep { changed += 1; }
        keep
    });
    let imports = remap(&live.imports);
    let before = scope.imports.len();
    let mut keep = live.imports.iter();
    scope.imports.retain(|_| *keep.next().unwrap());
    changed += before - scope.imports.len();
    let mut redirect = |r: &mut Ref| match r {
        Ref::Import(i) => *i = imports[*i],
        Ref::ChildExport(i, e) => *e = exports[*i][*e],
        _ => {}
    };
    // Dead holders can still refer to imports and exports that were removed.
    for (item, &keep) in scope.items.iter_mut().zip(&live.items) {
        if keep { item.for_each_ref(&mut redirect); }
    }
    for bind in &mut scope.binds { redirect(&mut bind.value); }
    for export in &mut scope.exports { redirect(&mut export.value); }
    for (_, r) in &mut scope.names { redirect(r); }
    let dead: Vec<_> = live.items.iter().map(|live| !live).collect();
    super::structural::compact(scope, &dead);
    changed
}

pub(super) fn run(scope: &mut Scope) -> usize {
    let live = analyze(scope, None);
    prune(scope, &live)
}
