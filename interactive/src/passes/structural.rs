//! Structural rewrites and item compaction within each scope.

use crate::scope_ir::{Item, Node, Ref, Scope, Source};

/// Optimize each scope to a fixed point: prune unread items and imports, collapse
/// redundant arrangements, deduplicate, and fuse linear chains. Keep root inputs.
/// Pruning precedes fusion so dead readers do not inflate use counts.
/// Observations precede removal and renumbering; the result says whether this
/// scope or any descendant contains an Inspect.
pub(super) fn optimize_scope(scope: &mut Scope, path: &str, observe: &mut super::Observe<'_>) -> bool {
    let inspect: Vec<bool> = scope.items.iter_mut().map(|item| match item {
        Item::Op(Node::Inspect { .. }) => true,
        Item::Sub(child) => optimize_scope(child, &format!("{path}/{}", child.name), observe),
        _ => false,
    }).collect();
    let ordinals = super::item_ordinals(scope);
    let mut dead: Vec<bool> = vec![false; scope.items.len()];
    loop {
        let pruned = prune(scope, &mut dead, &inspect, path, &ordinals, observe);
        let mut note = |scope: &Scope, action: &'static str, from, to| observe(path, scope, &ordinals, action, from, to);
        let changed = pruned | collapse_arranges(scope, &mut dead, &mut note)
            | dedup(scope, &mut dead, &mut note)
            | fuse_linear(scope, &mut dead, &mut note);
        if !changed {
            compact(scope, &dead);
            return inspect.iter().any(|&i| i);
        }
    }
}

/// Walk backward once: all readers of an item precede it in this walk.
/// Retained scopes keep their feedback bindings as roots, since a variable
/// can read a later item. Inspect also keeps its enclosing scopes alive.
fn prune(scope: &mut Scope, dead: &mut [bool], inspect: &[bool], path: &str, ordinals: &[usize], observe: &mut super::Observe<'_>) -> bool {
    let mut live = inspect.to_vec();
    // Positional inputs only occur at the root. Clients can still feed an
    // unread input, so retain its slot even when no operator reads it.
    let mut imports: Vec<bool> = scope.imports.iter().map(|i| matches!(i.from, Source::Input(_))).collect();
    let mut exports: Vec<Vec<bool>> = scope.items.iter().map(|item| match item {
        Item::Sub(child) => vec![false; child.exports.len()],
        Item::Op(_) => Vec::new(),
    }).collect();
    let mark = |r: &Ref, live: &mut [bool], imports: &mut [bool], exports: &mut [Vec<bool>]| match r {
        Ref::Local(i) => live[*i] = true,
        Ref::Import(i) => imports[*i] = true,
        Ref::ChildExport(i, e) => { live[*i] = true; exports[*i][*e] = true; },
        Ref::Var(_) => {}, // The retained scope's bindings are roots below.
    };
    for e in &scope.exports { mark(&e.value, &mut live, &mut imports, &mut exports); }
    for b in &scope.binds { mark(&b.value, &mut live, &mut imports, &mut exports); }
    let mut changed = false;
    let mut export_remap = vec![Vec::new(); scope.items.len()];
    for i in (0..scope.items.len()).rev() {
        if dead[i] { continue; }
        let item = &mut scope.items[i];
        if let Item::Sub(child) = item {
            let mut next = 0;
            export_remap[i] = exports[i].iter().map(|&keep| {
                if keep { let slot = next; next += 1; slot } else { usize::MAX }
            }).collect();
            if next != child.exports.len() {
                let mut keep = exports[i].iter();
                child.exports.retain(|_| *keep.next().unwrap());
                // Only a changed set of roots needs another child pass, and a
                // child about to be removed needs none. Inspect survives pruning,
                // so its cached flag stays valid.
                if live[i] { optimize_scope(child, &format!("{path}/{}", child.name), observe); }
                changed = true;
            }
        }
        if live[i] {
            item.for_each_ref(|r| mark(r, &mut live, &mut imports, &mut exports));
        } else {
            observe(path, scope, ordinals, "removed dead", i, i);
            dead[i] = true;
            changed = true;
        }
    }
    let mut next = 0;
    let import_remap: Vec<_> = imports.iter().map(|&keep| {
        if keep { let slot = next; next += 1; slot } else { usize::MAX }
    }).collect();
    changed |= next != scope.imports.len();
    let mut keep = imports.iter();
    scope.imports.retain(|_| *keep.next().unwrap());
    let mut remap = |r: &mut Ref| match r {
        Ref::Import(i) => *i = import_remap[*i],
        Ref::ChildExport(i, e) => *e = export_remap[*i][*e],
        _ => {},
    };
    // Dead holders may still refer to exports or imports already removed.
    for (i, item) in scope.items.iter_mut().enumerate() {
        if !dead[i] { item.for_each_ref(&mut remap); }
    }
    for b in &mut scope.binds { remap(&mut b.value); }
    for e in &mut scope.exports { remap(&mut e.value); }
    // Source names are provenance, not roots. Drop names for removed values
    // before remapping the surviving imports and child exports.
    scope.names.retain(|(_, r)| match r {
        Ref::Local(i) => !dead[*i],
        Ref::Import(i) => imports[*i],
        Ref::ChildExport(i, e) => !dead[*i] && exports[*i][*e],
        Ref::Var(_) => true,
    });
    for (_, r) in &mut scope.names { remap(r); }
    changed
}

/// Count, for each item, the references to it (`Local` or `ChildExport`)
/// from LIVE holders only. Dead items keep their (stale, already-redirected-
/// around) input refs until `compact`, and counting those would make a
/// single-consumer item look shared — blocking fusion after its consumer
/// chain has partially fused (e.g. the middle of a three-op linear chain
/// dies into its consumer, and its stale input ref still counts against
/// the head, so the chain never finishes fusing).
pub(super) fn use_counts(scope: &Scope, dead: &[bool]) -> Vec<usize> {
    let mut counts = vec![0usize; scope.items.len()];
    let mut tally = |r: &Ref| match r {
        Ref::Local(i) | Ref::ChildExport(i, _) => counts[*i] += 1,
        Ref::Import(_) | Ref::Var(_) => {},
    };
    for (i, item) in scope.items.iter().enumerate() {
        if dead[i] { continue; }
        match item {
            Item::Op(node) => match node {
                Node::Linear { input, .. } | Node::Arrange(input)
                | Node::Reduce { input, .. } | Node::Inspect { input, .. } => tally(input),
                Node::Join { left, right, .. } => { tally(left); tally(right); },
                Node::HalfJoin { stream, arrangement, .. } => { tally(stream); tally(arrangement); },
                Node::Concat(refs) => for r in refs { tally(r); },
            },
            Item::Sub(child) => for imp in &child.imports {
                if let Source::Parent(r) = &imp.from { tally(r); }
            },
        }
    }
    for b in &scope.binds { tally(&b.value); }
    for e in &scope.exports { tally(&e.value); }
    counts
}

/// `Arrange` of an arrangement (an `Arrange` or `Reduce` item) is redundant.
fn collapse_arranges(scope: &mut Scope, dead: &mut [bool], note: &mut dyn FnMut(&Scope, &'static str, usize, usize)) -> bool {
    let mut changed = false;
    let mut remap: Vec<_> = (0..scope.items.len()).collect();
    for i in 0..scope.items.len() {
        if dead[i] { continue; }
        let Item::Op(Node::Arrange(Ref::Local(j))) = &scope.items[i] else { continue };
        let j = remap[*j];
        if !dead[j] && matches!(&scope.items[j], Item::Op(Node::Arrange(_) | Node::Reduce { .. })) {
            note(scope, "removed redundant arrange", i, j);
            remap[i] = j;
            dead[i] = true;
            changed = true;
        }
    }
    if changed { scope.for_each_ref(|r| if let Ref::Local(i) = r { *i = remap[*i]; }); }
    changed
}

/// Fuse a `Linear` into its `Linear` input when it is the only consumer.
fn fuse_linear(scope: &mut Scope, dead: &mut [bool], note: &mut dyn FnMut(&Scope, &'static str, usize, usize)) -> bool {
    let counts = use_counts(scope, dead);
    let mut changed = false;
    for i in 0..scope.items.len() {
        if dead[i] { continue; }
        let Item::Op(Node::Linear { input: Ref::Local(j), .. }) = &scope.items[i] else { continue };
        let j = *j;
        if dead[j] || counts[j] != 1 { continue; }
        if !matches!(scope.items[j], Item::Op(Node::Linear { .. })) { continue; }
        note(scope, "fused", j, i);
        let Item::Op(Node::Linear { input: inner_input, ops: mut fused }) =
            std::mem::replace(&mut scope.items[j], Item::Op(Node::Concat(Vec::new()))) else { unreachable!() };
        let Item::Op(Node::Linear { ops, .. }) = &mut scope.items[i] else { unreachable!() };
        fused.append(ops);
        scope.items[i] = Item::Op(Node::Linear { input: inner_input, ops: fused });
        dead[j] = true;
        changed = true;
    }
    scope.names.retain(|(_, r)| !matches!(r, Ref::Local(k) if dead[*k]));
    changed
}

/// Rewrite inputs before keying each item, so duplicate chains merge in one walk.
/// Children are already optimized. Keep the first item's text and labels.
fn dedup(scope: &mut Scope, dead: &mut [bool], note: &mut dyn FnMut(&Scope, &'static str, usize, usize)) -> bool {
    let mut seen = std::collections::HashMap::new();
    let mut redirects: Vec<usize> = (0..scope.items.len()).collect();
    let rewrite = |r: &mut Ref, redirects: &[usize]| {
        if let Ref::Local(i) | Ref::ChildExport(i, _) = r { *i = redirects[*i]; }
    };
    for (i, item) in scope.items.iter_mut().enumerate() {
        if dead[i] { continue; }
        item.for_each_ref(|r| rewrite(r, &redirects));
        redirects[i] = *seen.entry(item.key()).or_insert(i);
        dead[i] = redirects[i] != i;
    }
    for b in &mut scope.binds { rewrite(&mut b.value, &redirects); }
    for e in &mut scope.exports { rewrite(&mut e.value, &redirects); }
    for (_, r) in &mut scope.names { rewrite(r, &redirects); }
    let mut changed = false;
    for (i, &first) in redirects.iter().enumerate() {
        if i != first { note(scope, "merged", i, first); changed = true; }
    }
    changed
}

/// Drop dead items and remap every item-indexed `Ref` to the compacted
/// positions. Dead items have no remaining live readers.
pub(super) fn compact(scope: &mut Scope, dead: &[bool]) {
    let mut remap = vec![usize::MAX; scope.items.len()];
    let mut next = 0;
    for (i, &d) in dead.iter().enumerate() {
        if !d { remap[i] = next; next += 1; }
    }
    if scope.labels.len() == scope.items.len() {
        let mut keep = dead.iter().map(|d| !d);
        scope.labels.retain(|_| keep.next().unwrap());
    }
    let mut keep = dead.iter().map(|d| !d);
    scope.items.retain(|_| keep.next().unwrap());
    scope.for_each_ref(|r| match r {
        Ref::Local(i) | Ref::ChildExport(i, _) => {
            assert!(remap[*i] != usize::MAX, "compact: a reference points at a dead item");
            *i = remap[*i];
        },
        Ref::Import(_) | Ref::Var(_) => {},
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ir::LinearOp, lambda::Lambda, scope_ir::{Bind, Export}};

    #[test]
    fn dedup_chains_in_one_walk() {
        let mut scope = Scope::default();
        let mut inputs = [Ref::Import(0), Ref::Import(0)];
        for _ in 0..128 {
            for (input, names) in inputs.iter_mut().zip(["(k, v)", "(p, q)"]) {
                let lambda = Lambda::new(format!("let {names} = input in {names}"), &corgi::Scope::default());
                scope.items.push(Item::Op(Node::Linear { input: input.clone(), ops: vec![LinearOp::Project(lambda)] }));
                *input = Ref::Local(scope.items.len() - 1);
            }
        }
        scope.binds.push(Bind { var: 0, value: inputs[1].clone() });
        scope.exports.push(Export { name: "out".into(), value: inputs[1].clone(), shape: None });
        let mut dead = vec![false; scope.items.len()];
        assert!(dedup(&mut scope, &mut dead, &mut |_, _, _, _| {}));
        assert_eq!(dead.iter().filter(|&&d| d).count(), 128);
        assert_eq!(scope.binds[0].value, inputs[0]);
        assert_eq!(scope.exports[0].value, inputs[0]);
        assert!(!dedup(&mut scope, &mut dead, &mut |_, _, _, _| {}));
        compact(&mut scope, &dead);
        assert_eq!(scope.items.len(), 128);
    }
}
