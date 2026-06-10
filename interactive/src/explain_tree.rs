//! Explanation rewrite on the scope-tree IR (in progress).
//!
//! Current contents: the clone-with-lifts, the foundation the rewrite's
//! witness and forward copies are built from. `clone_into` clones an original
//! program's scopes into an output scope under construction; every nested
//! scope additionally `lift_iter`s and exports each internal collection, so
//! every value-producing site in the subtree has a "host-visible" form — its
//! user-iter coordinates folded into the value, innermost first — at the
//! embedding level.
//!
//! This is the tree form of the flat rewrite's `host` map. There it required
//! positional scope tracking, a pending pile per scope, depth-offset
//! arithmetic for each `leave`, and a fix-up pass for `Leave` aliasing; here
//! it is "a scope exports its lifted internals", and the cascade through
//! enclosing scopes is the recursion. The embedding depth is not a parameter:
//! the renderer derives depth structurally, so a clone needn't know where it
//! will sit.

use crate::ir::LinearOp;
use crate::scope_ir::{Bind, Export, Import, Item, Node, Program, Ref, Scope, Source, Var};

/// A value-producing site in an original tree: the `path` of `Sub` item
/// indices from the root, then the site within that scope. Sites are ops and
/// feedback variables; imports are substituted, not sites.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Addr {
    pub path: Vec<usize>,
    pub site: Site,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Site {
    Op(usize),
    Var(usize),
}

/// Clone `orig`'s contents into `out`, splicing (no extra scope around the
/// root level). `import_map[k]` is the ref in `out` standing in for `orig`'s
/// import `k`. Original exports are cloned in order (so `ChildExport` indices
/// keep meaning); nested scopes gain lifted `$host:` exports after them.
/// Returns the host-visible ref at the `out` level for every site in the
/// subtree.
pub fn clone_into(orig: &Scope, out: &mut Scope, import_map: &[Ref]) -> Vec<(Addr, Ref)> {
    clone_rec(orig, out, import_map, &[])
}

/// The identity check: a program cloned into a fresh root computes the same
/// named exports as the original (the extra `$host:` exports ride along,
/// unconsumed). Backends hook this behind `CLONE_RT=1` for A/B verification.
pub fn clone_identity(p: &Program) -> Program {
    let mut out = Scope {
        name: p.root.name.clone(),
        imports: p.root.imports.clone(),
        ..Scope::default()
    };
    let import_map: Vec<Ref> = (0..out.imports.len()).map(Ref::Import).collect();
    let _visible = clone_into(&p.root, &mut out, &import_map);
    Program { root: out }
}

fn map_ref(r: &Ref, locals: &[Option<Ref>], subs: &[Option<usize>], import_map: &[Ref], var_base: usize) -> Ref {
    match r {
        Ref::Local(i) => locals[*i].clone().expect("clone: reference to a later item"),
        Ref::Import(k) => import_map[*k].clone(),
        Ref::Var(v) => Ref::Var(var_base + v),
        Ref::ChildExport(i, j) => Ref::ChildExport(subs[*i].expect("clone: reference to a later sub"), *j),
    }
}

fn clone_node(node: &Node, m: impl Fn(&Ref) -> Ref) -> Node {
    match node {
        Node::Linear { input, ops } => Node::Linear { input: m(input), ops: ops.clone() },
        Node::Concat(refs) => Node::Concat(refs.iter().map(&m).collect()),
        Node::Arrange(r) => Node::Arrange(m(r)),
        Node::Join { left, right, projection } => Node::Join { left: m(left), right: m(right), projection: projection.clone() },
        Node::Reduce { input, reducer } => Node::Reduce { input: m(input), reducer: reducer.clone() },
        Node::Inspect { input, label } => Node::Inspect { input: m(input), label: label.clone() },
    }
}

fn clone_rec(orig: &Scope, out: &mut Scope, import_map: &[Ref], path: &[usize]) -> Vec<(Addr, Ref)> {
    let addr = |site: Site| Addr { path: path.to_vec(), site };
    let mut visible: Vec<(Addr, Ref)> = Vec::new();

    // Feedback variables first: anything in the scope may reference them.
    let var_base = out.vars.len();
    for (vi, v) in orig.vars.iter().enumerate() {
        out.vars.push(Var { name: v.name.clone() });
        visible.push((addr(Site::Var(vi)), Ref::Var(var_base + vi)));
    }

    // orig-ref -> out-ref tables for this scope's content.
    let mut locals: Vec<Option<Ref>> = vec![None; orig.items.len()];
    let mut subs: Vec<Option<usize>> = vec![None; orig.items.len()];

    for (i, item) in orig.items.iter().enumerate() {
        match item {
            Item::Op(node) => {
                let cloned = clone_node(node, |r| map_ref(r, &locals, &subs, import_map, var_base));
                let out_idx = out.items.len();
                out.items.push(Item::Op(cloned));
                locals[i] = Some(Ref::Local(out_idx));
                // The host form of an Arrange is its underlying collection
                // (reverse rules build pair tables from collections).
                let vis = match node {
                    Node::Arrange(input) => map_ref(input, &locals, &subs, import_map, var_base),
                    _ => Ref::Local(out_idx),
                };
                visible.push((addr(Site::Op(i)), vis));
            }
            Item::Sub(child) => {
                // The cloned child declares the same imports, with the parent-
                // side refs mapped into the output parent.
                let cloned_imports: Vec<Import> = child.imports.iter().map(|imp| Import {
                    name: imp.name.clone(),
                    from: match &imp.from {
                        Source::Parent(r) => Source::Parent(map_ref(r, &locals, &subs, import_map, var_base)),
                        other => panic!("clone: nested scope with external source {:?}", other),
                    },
                }).collect();
                let mut child_out = Scope {
                    name: child.name.clone(),
                    imports: cloned_imports,
                    ..Scope::default()
                };
                // Inside the child, its imports map to themselves (same order).
                let ident: Vec<Ref> = (0..child.imports.len()).map(Ref::Import).collect();
                let mut child_path = path.to_vec();
                child_path.push(i);
                let child_visible = clone_rec(child, &mut child_out, &ident, &child_path);

                // Lift each subtree site at this scope's exit and export it:
                // the lift folds this scope's iteration coordinate into the
                // value (innermost coordinates were folded by deeper exits),
                // and the export is the host-visible edge upward.
                let sub_idx = out.items.len();
                for (a, vref) in child_visible {
                    let lift_idx = child_out.items.len();
                    child_out.items.push(Item::Op(Node::Linear { input: vref, ops: vec![LinearOp::LiftIter] }));
                    let export_idx = child_out.exports.len();
                    child_out.exports.push(Export {
                        name: format!("$host:{}", export_idx),
                        value: Ref::Local(lift_idx),
                    });
                    visible.push((a, Ref::ChildExport(sub_idx, export_idx)));
                }

                out.items.push(Item::Sub(child_out));
                subs[i] = Some(sub_idx);
            }
        }
    }

    // Loop closures, with this scope's variable indices offset.
    for b in &orig.binds {
        out.binds.push(Bind {
            var: var_base + b.var,
            value: map_ref(&b.value, &locals, &subs, import_map, var_base),
        });
    }

    // Original exports, in order (ChildExport indices upward stay valid).
    for e in &orig.exports {
        out.exports.push(Export {
            name: e.name.clone(),
            value: map_ref(&e.value, &locals, &subs, import_map, var_base),
        });
    }

    visible
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lower::lower_tree;

    fn parse(src: &str) -> Vec<crate::parse::Stmt> { crate::parse::pipe::parse(src) }

    const SCC: &str = r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        let trans = edges | key($1 ; $0);
        outer: {
            let scc = edges + trim;
            fwd: {
                let nodes = edges | key($1 ; $1) | enter_at($1[0]);
                let labels = proposals + nodes | min;
                var proposals = labels | join(scc, ($2 ; $1));
            }
            let trim_fwd = edges
                | join(fwd::labels, ($1 ; $0, $2))
                | join(fwd::labels, ($0 ; $1, $2))
                | filter($1[1] == $1[2])
                | key($0 ; $1[0]);
            bwd: {
                let nodes = trans | key($1 ; $1) | enter_at($1[0]);
                let labels = proposals + nodes | min;
                var proposals = labels | join(trim_fwd, ($2 ; $1));
            }
            let trim_bwd = trans
                | join(bwd::labels, ($1 ; $0, $2))
                | join(bwd::labels, ($0 ; $1, $2))
                | filter($1[1] == $1[2])
                | key($0 ; $1[0]);
            var trim = trim_bwd - edges;
        }
        export "result" = outer::scc | map(;) | arrange | inspect(total);
    "#;

    fn vars_total(s: &Scope) -> usize {
        s.vars.len() + s.items.iter().map(|i| match i { Item::Sub(c) => vars_total(c), _ => 0 }).sum::<usize>()
    }

    #[test]
    fn every_site_is_host_visible() {
        let p = lower_tree(parse(SCC));
        let mut out = Scope { imports: p.root.imports.clone(), ..Scope::default() };
        let import_map: Vec<Ref> = (0..out.imports.len()).map(Ref::Import).collect();
        let visible = clone_into(&p.root, &mut out, &import_map);
        let sites = p.op_count() + vars_total(&p.root);
        assert_eq!(visible.len(), sites, "one host-visible ref per op and var");
        // A depth-2 site (inside fwd) surfaces as a ChildExport at the root.
        assert!(visible.iter().any(|(a, r)| a.path.len() == 2 && matches!(r, Ref::ChildExport(..))),
            "depth-2 sites surface via child exports");
    }

    #[test]
    fn nested_exports_carry_one_lift_per_level() {
        let p = lower_tree(parse(SCC));
        let mut out = Scope { imports: p.root.imports.clone(), ..Scope::default() };
        let import_map: Vec<Ref> = (0..out.imports.len()).map(Ref::Import).collect();
        clone_into(&p.root, &mut out, &import_map);
        // outer's clone: every $host: export is a LiftIter Linear; and the
        // ones re-exporting fwd/bwd internals chain TWO lifts (one per level):
        // the value behind the lift is itself a ChildExport of a lift.
        let Item::Sub(outer) = out.items.iter().find(|i| matches!(i, Item::Sub(_))).unwrap() else { unreachable!() };
        let mut depth2_chains = 0;
        for e in outer.exports.iter().filter(|e| e.name.starts_with("$host:")) {
            let Ref::Local(li) = &e.value else { panic!("$host export should be a fresh lift") };
            let Item::Op(Node::Linear { input, ops }) = &outer.items[*li] else { panic!("expected a lift") };
            assert_eq!(ops.as_slice().len(), 1);
            assert!(matches!(ops[0], LinearOp::LiftIter));
            if matches!(input, Ref::ChildExport(..)) { depth2_chains += 1; }
        }
        assert!(depth2_chains > 0, "fwd/bwd internals re-lift at outer's exit");
    }

    #[test]
    fn identity_clone_preserves_structure() {
        let p = lower_tree(parse(SCC));
        let c = clone_identity(&p);
        assert_eq!(c.root.exports.len(), p.root.exports.len(), "root exports preserved (no $host at root)");
        assert_eq!(c.root.exports[0].name, "result");
        assert_eq!(vars_total(&c.root), vars_total(&p.root), "feedback variables preserved");
        assert!(c.op_count() > p.op_count(), "clone adds the lift chains");
    }
}
