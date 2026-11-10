//! Reduce a concatenation through its leaves' arrangements.
//!
//! `reduce(arrange(concat(±x1, .., ±xn)))`, where the arrange, the concat, and any negation of a
//! part have no other reader, becomes a reduce over `Merged([(x1, ±1), .., (xn, ±1)])`. Nested
//! single-use concats flatten, and single-use negations fold into the signs, so two of them cancel.
//! Any other operator ends a leaf. The rewrite fires only when a leaf is statically arranged: an
//! `Arrange` or `Reduce` item, or an import of an arranged value. The reduce reads that leaf's
//! arrangement as it is. The other leaves go into one new signed `Concat` under one new `Arrange`,
//! which holds what the old arrangement held of them, so the arranged leaves are no longer copied.
//! A single unarranged leaf gets its own `Arrange` and keeps its sign as the input's sign.
//!
//! The pass must run before code motion: hoisting can move a loop-invariant negation out of an
//! iterative scope, where its import is no longer one this pass can fold.

use crate::scope_ir::{Item, Node, ReduceInput, Ref, Scope, Source};
use crate::ir::{Diff, LinearOp};
use super::structural::{compact, use_counts};

/// Rewrite every scope of the tree rooted at `scope`, whose imports are arranged as `imports`
/// says. Returns how many reduces were rewritten.
pub(super) fn run(scope: &mut Scope, imports: &[bool]) -> usize {
    let mut count = rewrite_scope(scope, imports);
    for i in 0..scope.items.len() {
        let Item::Sub(child) = &scope.items[i] else { continue };
        let child_imports: Vec<bool> = child.imports.iter().map(|import| match &import.from {
            Source::Parent(r) => arranged(scope, imports, r),
            _ => false,
        }).collect();
        let Item::Sub(child) = &mut scope.items[i] else { unreachable!() };
        count += run(child, &child_imports);
    }
    count
}

/// Whether `r` is statically arranged in `scope`.
fn arranged(scope: &Scope, imports: &[bool], r: &Ref) -> bool {
    match r {
        Ref::Local(i) => matches!(scope.items[*i], Item::Op(Node::Arrange(_) | Node::Reduce { .. })),
        Ref::Import(i) => imports[*i],
        // Leaving a child flattens its exports. Feedback is also a collection.
        Ref::Var(_) | Ref::ChildExport(_, _) => false,
    }
}

/// The signed leaves of `r` read with `sign`, through single-use concats and negations, which are
/// added to `absorbed`.
fn leaves(scope: &Scope, uses: &[usize], r: &Ref, sign: Diff, absorbed: &mut Vec<usize>, out: &mut Vec<(Ref, Diff)>) {
    if let Ref::Local(n) = r {
        if uses[*n] == 1 {
            match &scope.items[*n] {
                Item::Op(Node::Concat(refs)) => {
                    absorbed.push(*n);
                    for r in refs { leaves(scope, uses, r, sign, absorbed, out); }
                    return;
                }
                Item::Op(Node::Linear { input, ops }) if !ops.is_empty() && ops.iter().all(|op| matches!(op, LinearOp::Negate)) => {
                    absorbed.push(*n);
                    let sign = if ops.len() % 2 == 1 { -sign } else { sign };
                    leaves(scope, uses, input, sign, absorbed, out);
                    return;
                }
                _ => {}
            }
        }
    }
    out.push((r.clone(), sign));
}

fn rewrite_scope(scope: &mut Scope, imports: &[bool]) -> usize {
    let count = scope.items.len();
    let uses = use_counts(scope, &vec![false; count]);
    let mut dead = vec![false; count];
    // Per rewritten reduce, the items it needs, in dependency order, inserted just before it.
    let mut inserted: Vec<Vec<usize>> = vec![Vec::new(); count];
    let mut rewrites = 0;
    for i in 0..count {
        let Item::Op(Node::Reduce { input, .. }) = &scope.items[i] else { continue };
        let Some(Ref::Local(j)) = input.as_single() else { continue };
        let j = *j;
        let Item::Op(Node::Arrange(source)) = &scope.items[j] else { continue };
        if uses[j] != 1 { continue; }
        let (mut absorbed, mut found) = (vec![j], Vec::new());
        leaves(scope, &uses, source, 1, &mut absorbed, &mut found);
        // Only a concat or a negation makes a merged reduce, and only an arranged leaf saves.
        if absorbed.len() == 1 || !found.iter().any(|(r, _)| arranged(scope, imports, r)) { continue; }
        let mut merged = Vec::with_capacity(found.len());
        let mut rest = Vec::new();
        for (leaf, sign) in found {
            if arranged(scope, imports, &leaf) { merged.push((leaf, sign)); } else { rest.push((leaf, sign)); }
        }
        let mut add = |scope: &mut Scope, node: Node| {
            let at = scope.items.len();
            scope.items.push(Item::Op(node));
            if scope.labels.len() == at { scope.labels.push(scope.labels[i].clone()); }
            inserted[i].push(at);
            Ref::Local(at)
        };
        if rest.is_empty() {
            // Every leaf is arranged already.
        } else if let [(leaf, sign)] = &rest[..] {
            let arr = add(scope, Node::Arrange(leaf.clone()));
            merged.push((arr, *sign));
        } else {
            // A negated part is a negate node feeding the concat, as `a - b` lowers.
            let parts = rest.into_iter().map(|(leaf, sign)| {
                if sign < 0 { add(scope, Node::Linear { input: leaf, ops: vec![LinearOp::Negate] }) } else { leaf }
            }).collect();
            let concat = add(scope, Node::Concat(parts));
            let arr = add(scope, Node::Arrange(concat));
            merged.push((arr, 1));
        }
        for n in absorbed { dead[n] = true; }
        let Item::Op(Node::Reduce { input, .. }) = &mut scope.items[i] else { unreachable!() };
        *input = ReduceInput::Merged(merged);
        rewrites += 1;
    }
    if rewrites == 0 { return 0; }
    // The new arranges go just before their reduces, and the absorbed items go.
    let mut order = Vec::with_capacity(scope.items.len());
    for i in 0..count {
        order.extend(inserted[i].iter().copied());
        order.push(i);
    }
    let dead: Vec<bool> = order.iter().map(|&i| i < count && dead[i]).collect();
    scope.reorder(&order);
    scope.names.retain(|(_, r)| !matches!(r, Ref::Local(k) if dead[*k]));
    compact(scope, &dead);
    rewrites
}

#[cfg(test)]
mod tests {
    use crate::scope_ir::{Item, Node, ReduceInput, Ref, Scope};
    use crate::ir::LinearOp;

    fn optimized(src: &str, config: &crate::passes::Config) -> crate::scope_ir::Program {
        let mut p = crate::lower::lower_tree(crate::parse::pipe::parse(src));
        crate::passes::run(&mut p, &|_| Some((corgi::Shape::Int, corgi::Shape::Int)), config, &crate::server::host_time(0), 1, &mut |_, _, _| {});
        p
    }

    fn reduces(scope: &Scope) -> Vec<ReduceInput> {
        scope.items.iter().flat_map(|item| match item {
            Item::Op(Node::Reduce { input, .. }) => vec![input.clone()],
            Item::Sub(child) => reduces(child),
            _ => Vec::new(),
        }).collect()
    }

    /// The only reduce's merged inputs.
    fn merged(scope: &Scope) -> Vec<(Ref, i64)> {
        let [ReduceInput::Merged(inputs)] = &reduces(scope)[..] else { panic!("{:?}", reduces(scope)) };
        inputs.clone()
    }

    fn default() -> crate::passes::Config { crate::passes::Config::default() }

    /// The unarranged leaves share one arrangement of a signed concat, and an arranged leaf is read as it is.
    #[test]
    fn shares_one_arrangement_for_unarranged_leaves() {
        let src = r#"
            let a = input 0 : (int, ()) | arrange;
            let b = input 1 : (int, ());
            let c = input 2 : (int, ());
            let d = input 3 : (int, ());
            output "out" = a + b + c - d | distinct;
        "#;
        let p = optimized(src, &default());
        let inputs = merged(&p.root);
        assert_eq!(inputs.iter().map(|(_, s)| *s).collect::<Vec<_>>(), vec![1, 1], "{inputs:?}");
        assert!(matches!(p.root.items[match &inputs[0].0 { Ref::Local(i) => *i, r => panic!("{r:?}") }], Item::Op(Node::Arrange(_))));
        let Ref::Local(arr) = inputs[1].0 else { panic!("{:?}", inputs[1]) };
        let Item::Op(Node::Arrange(Ref::Local(concat))) = &p.root.items[arr] else { panic!("{:?}", p.root.items[arr]) };
        let Item::Op(Node::Concat(parts)) = &p.root.items[*concat] else { panic!("{:?}", p.root.items[*concat]) };
        assert_eq!(parts.len(), 3);
        let negated = parts.iter().filter(|part| matches!(part, Ref::Local(n) if matches!(&p.root.items[*n], Item::Op(Node::Linear { ops, .. }) if ops == &[LinearOp::Negate]))).count();
        assert_eq!(negated, 1, "{parts:?}");
        assert_eq!(p.root.items.iter().filter(|item| matches!(item, Item::Op(Node::Concat(_)))).count(), 1);
    }

    /// One unarranged leaf gets its own arrangement and keeps its sign.
    #[test]
    fn single_unarranged_leaf_keeps_its_sign() {
        let src = r#"
            let a = input 0 : (int, int) | arrange;
            let b = input 1 : (int, int);
            output "out" = a - b | count;
        "#;
        let p = optimized(src, &default());
        let inputs = merged(&p.root);
        assert_eq!(inputs.iter().map(|(_, s)| *s).collect::<Vec<_>>(), vec![1, -1]);
        let Ref::Local(arr) = inputs[1].0 else { panic!("{:?}", inputs[1]) };
        assert!(matches!(p.root.items[arr], Item::Op(Node::Arrange(Ref::Import(_)))), "{:?}", p.root.items[arr]);
        assert!(!p.root.items.iter().any(|item| matches!(item, Item::Op(Node::Concat(_)))));
    }

    /// With no arranged leaf, or a concat read elsewhere, the reduce stays.
    #[test]
    fn no_arranged_leaf_no_rewrite() {
        let src = r#"let b = input 0 : (int, int); let c = input 1 : (int, int); output "out" = b + c | count;"#;
        let p = optimized(src, &default());
        assert!(merged(&p.root).len() == 1 && reduces(&p.root)[0].as_single().is_some());
        assert!(p.root.items.iter().any(|item| matches!(item, Item::Op(Node::Concat(_)))));
    }

    #[test]
    fn shared_concat_no_rewrite() {
        let src = r#"
            let a = input 0 : (int, int) | arrange;
            let b = input 1 : (int, int);
            let u = a + b;
            output "out" = u | count;
            output "u" = u;
        "#;
        let p = optimized(src, &default());
        assert!(reduces(&p.root)[0].as_single().is_some(), "{:?}", reduces(&p.root));
    }

    /// A loop-invariant negation of an outer arrangement folds into a sign before hoisting can
    /// move it out of the loop.
    #[test]
    fn loop_negation_folds_before_hoisting() {
        let src = r#"
            let seed = input 0 : (int, ()) | arrange;
            let blocked = input 1 : (int, ()) | arrange;
            s: { set x = (::seed - ::blocked + x) | distinct; }
            output "o" = s::x;
        "#;
        let p = optimized(src, &crate::passes::Config::all());
        let Some(Item::Sub(child)) = p.root.items.iter().find(|item| matches!(item, Item::Sub(_))) else { panic!("{:?}", p.root.items) };
        let folded = reduces(child).iter().any(|input| matches!(input, ReduceInput::Merged(v) if v.iter().any(|(r, s)| *s == -1 && matches!(r, Ref::Import(_)))));
        assert!(folded, "{:?}", reduces(child));
    }
}
