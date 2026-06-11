//! The explanation rewrite on the scope-tree IR.
//!
//! `explain(p)` transforms a program into one whose execution produces
//! per-source demand-set explanations for queries against `p`'s first
//! export: root { sources, query input, witness clone } plus an iterative
//! `explain` scope { demand-set vars, forward clone on demanded rows,
//! reverse-tracing ops, demand exports }. See the section comments below.
//!
//! The clone-with-lifts is the foundation the rewrite's witness and
//! forward copies are built from. `clone_into` clones an original
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

// ===== The explanation transform =====
//
// `explain(p)` produces a Program whose execution yields per-source
// demand-set explanations for queries against `p`'s first export. Output
// shape: root { sources, query input, witness clone } and an iterative
// `explain` scope { demand-set vars, forward clone on demanded rows,
// reverse-tracing ops, demand exports }.
//
// The reverse dataflow is *flat inside the explain scope* by design: demand
// rows carry the user-iteration chain folded into the value (the `folded`
// layout), so no nesting is needed. The per-op reverse rules port from the
// flat rewrite nearly unchanged; what the tree changes is the boundary
// bookkeeping. Flat `Leave` had a special backward rule injecting the inner
// user-chain coordinate; here a reference is *resolved* through explicit
// import/export edges to the value site it names, and the ordinary
// shape-preserving lookup against that site's host form injects or strips
// coordinates as the depths dictate. No op needs to know about boundaries.

use std::collections::BTreeMap;
use crate::parse::{Condition, FieldExpr, Projection, Reducer};
use crate::ir::{apply_ops_arity, proj_arity};

/// What a reference ultimately names: a value-producing site, or one of the
/// root's external sources.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Target {
    Site(Addr),
    /// Index into the original root's imports.
    Source(usize),
}

fn scope_at<'a>(root: &'a Scope, path: &[usize]) -> &'a Scope {
    let mut s = root;
    for &i in path {
        let Item::Sub(c) = &s.items[i] else { panic!("scope_at: path step is not a Sub") };
        s = c;
    }
    s
}

/// Resolve `r`, a reference within the scope at `path`, to its target,
/// chasing import edges upward and child-export edges downward.
fn resolve(root: &Scope, path: &[usize], r: &Ref) -> Target {
    match r {
        Ref::Local(i) => Target::Site(Addr { path: path.to_vec(), site: Site::Op(*i) }),
        Ref::Var(v) => Target::Site(Addr { path: path.to_vec(), site: Site::Var(*v) }),
        Ref::Import(k) => {
            if path.is_empty() {
                Target::Source(*k)
            } else {
                let parent_path = &path[..path.len() - 1];
                let scope = scope_at(root, path);
                match &scope.imports[*k].from {
                    Source::Parent(pr) => resolve(root, parent_path, pr),
                    _ => unreachable!("non-root scope with external source"),
                }
            }
        }
        Ref::ChildExport(c, j) => {
            let mut child_path = path.to_vec();
            child_path.push(*c);
            let child = scope_at(root, &child_path);
            resolve(root, &child_path, &child.exports[*j].value.clone())
        }
    }
}

/// `(k, v)` per site, forward-propagated from source arities to a fixed
/// point (feedback variables converge through their binds; a program with an
/// unconstrained var would simply leave it absent, and the rewrite panics on
/// lookup — the standalone shape pass is the place for a polite error).
fn site_shapes(
    p: &Program,
    source_shapes: &[(usize, usize)],
) -> BTreeMap<Addr, (usize, usize)> {
    let mut shapes: BTreeMap<Addr, (usize, usize)> = BTreeMap::new();
    loop {
        let before = shapes.len();
        walk_shapes(&p.root, &p.root, &[], source_shapes, &mut shapes);
        if shapes.len() == before { break; }
    }
    shapes
}

fn shape_of_ref(
    root: &Scope,
    path: &[usize],
    r: &Ref,
    source_shapes: &[(usize, usize)],
    shapes: &BTreeMap<Addr, (usize, usize)>,
) -> Option<(usize, usize)> {
    match resolve(root, path, r) {
        Target::Source(k) => Some(source_shapes[k]),
        Target::Site(a) => shapes.get(&a).copied(),
    }
}

fn walk_shapes(
    root: &Scope,
    s: &Scope,
    path: &[usize],
    source_shapes: &[(usize, usize)],
    shapes: &mut BTreeMap<Addr, (usize, usize)>,
) {
    let addr = |site: Site| Addr { path: path.to_vec(), site };
    // Var shapes from their binds.
    for b in &s.binds {
        if !shapes.contains_key(&addr(Site::Var(b.var))) {
            if let Some(sh) = shape_of_ref(root, path, &b.value, source_shapes, shapes) {
                shapes.insert(addr(Site::Var(b.var)), sh);
            }
        }
    }
    for (i, item) in s.items.iter().enumerate() {
        match item {
            Item::Op(node) => {
                if shapes.contains_key(&addr(Site::Op(i))) { continue; }
                let of = |r: &Ref| shape_of_ref(root, path, r, source_shapes, shapes);
                let sh = match node {
                    Node::Linear { input, ops } => of(input).map(|s| apply_ops_arity(s, ops)),
                    Node::Concat(refs) => refs.iter().find_map(|r| of(r)),
                    Node::Arrange(r) | Node::Inspect { input: r, .. } => of(r),
                    Node::Join { left, right, projection } => match (of(left), of(right)) {
                        (Some((kl, vl)), Some((_, vr))) => {
                            let rows = [kl, vl, vr];
                            Some((proj_arity(&projection.key, &rows), proj_arity(&projection.val, &rows)))
                        }
                        _ => None,
                    },
                    Node::Reduce { input, reducer } => of(input).map(|(k, v)| match reducer {
                        Reducer::Distinct => (k, 0),
                        Reducer::Min => (k, v),
                        Reducer::Count => (k, 1),
                    }),
                };
                if let Some(sh) = sh { shapes.insert(addr(Site::Op(i)), sh); }
            }
            Item::Sub(child) => {
                let mut cp = path.to_vec();
                cp.push(i);
                walk_shapes(root, child, &cp, source_shapes, shapes);
            }
        }
    }
}

/// Minimal builder over a `Scope` under construction: push an op, get a Ref.
struct Sb {
    s: Scope,
}

impl Sb {
    fn new(name: &str) -> Self { Sb { s: Scope { name: name.into(), ..Scope::default() } } }
    fn op(&mut self, n: Node) -> Ref {
        let i = self.s.items.len();
        self.s.items.push(Item::Op(n));
        Ref::Local(i)
    }
    fn linear(&mut self, input: Ref, ops: Vec<LinearOp>) -> Ref { self.op(Node::Linear { input, ops }) }
    fn project(&mut self, input: Ref, p: Projection) -> Ref { self.linear(input, vec![LinearOp::Project(p)]) }
    fn filter(&mut self, input: Ref, c: Condition) -> Ref { self.linear(input, vec![LinearOp::Filter(c)]) }
    fn concat(&mut self, refs: Vec<Ref>) -> Ref {
        if refs.len() == 1 { return refs.into_iter().next().unwrap(); }
        self.op(Node::Concat(refs))
    }
    fn join(&mut self, l: Ref, r: Ref, projection: Projection) -> Ref {
        let la = self.op(Node::Arrange(l));
        let ra = self.op(Node::Arrange(r));
        self.op(Node::Join { left: la, right: ra, projection })
    }
    fn reduce(&mut self, input: Ref, reducer: Reducer) -> Ref {
        let a = self.op(Node::Arrange(input));
        self.op(Node::Reduce { input: a, reducer })
    }
    fn variable(&mut self, name: &str) -> Ref {
        let v = self.s.vars.len();
        self.s.vars.push(Var { name: name.into() });
        Ref::Var(v)
    }
    fn bind(&mut self, var: Ref, value: Ref) {
        let Ref::Var(v) = var else { panic!("bind: not a variable ref") };
        self.s.binds.push(Bind { var: v, value });
    }
    fn import(&mut self, name: String, from: Source) -> Ref {
        let k = self.s.imports.len();
        self.s.imports.push(Import { name, from });
        Ref::Import(k)
    }
    fn export(&mut self, name: String, value: Ref) -> usize {
        let j = self.s.exports.len();
        self.s.exports.push(Export { name, value });
        j
    }
    fn debug_inspect(&mut self, input: Ref, label: String) {
        if std::env::var("EXPLAIN_DEBUG_DEP").is_ok() {
            self.op(Node::Inspect { input, label });
        }
    }
    /// Semijoin `left (K; V)` with `right (K; V)` by `(K)`, keep left's rows.
    fn semijoin_data(&mut self, left: Ref, right: Ref, k_arity: usize, v_arity: usize) -> Ref {
        let key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        let val: Vec<FieldExpr> = (0..v_arity).map(|i| FieldExpr::Index(1, i)).collect();
        self.join(left, right, Projection { key, val })
    }
    /// Set-level distinct on `(K; V)` rows (pack-distinct-unpack).
    fn distinct_full(&mut self, input: Ref, k_arity: usize, v_arity: usize) -> Ref {
        let mut pack_key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        for i in 0..v_arity { pack_key.push(FieldExpr::Index(1, i)); }
        let packed = self.project(input, Projection { key: pack_key, val: vec![] });
        let dist = self.reduce(packed, Reducer::Distinct);
        let unpack_key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        let unpack_val: Vec<FieldExpr> = (0..v_arity).map(|i| FieldExpr::Index(0, k_arity + i)).collect();
        self.project(dist, Projection { key: unpack_key, val: unpack_val })
    }
    /// Soundness filter + strip, via the shared `folded` layout algebra.
    fn filter_time_and_strip(&mut self, coll: Ref, k_out: usize, v_pre: usize, in_len: usize, out_len: usize, keep_in_len: usize) -> Ref {
        let layout = crate::folded::Joined { v_pre, in_len, out_len };
        let mut cur = coll;
        if let Some(cond) = layout.time_le() { cur = self.filter(cur, cond); }
        self.project(cur, layout.strip(k_out, keep_in_len))
    }
}

/// One upstream edge into a backward rule: the target's host-side `(data,
/// user)` collections from both clones, its shape, and its user-chain length.
/// (The flat version carried two lengths that diverged at `Leave`; with
/// per-site hosts there is one.)
struct Side {
    witness: Ref,
    forward: Ref,
    shape: (usize, usize),
    user_len: usize,
}

#[allow(clippy::too_many_arguments)]
fn strip_user_and_q(k_arity: usize, v_arity: usize) -> Projection {
    let key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
    let val: Vec<FieldExpr> = (0..v_arity).map(|i| FieldExpr::Index(1, i)).collect();
    Projection { key, val }
}

impl Sb {
    /// Shape-preserving lookup; also the universal depth adapter — when a
    /// contribution's chain length differs from its target's, the join against
    /// the target's host form injects (deeper target) or strips (shallower)
    /// the difference, outer-aligned by `folded`.
    fn emit_lookup_shape_preserving(&mut self, dep_y: Ref, side: &Side, output_depth: usize) -> Ref {
        let (k, v) = side.shape;
        let user_len = side.user_len;
        let pack_kv = |k: usize, v: usize| -> Vec<FieldExpr> {
            let mut out: Vec<FieldExpr> = Vec::with_capacity(k + v);
            for i in 0..k { out.push(FieldExpr::Index(0, i)); }
            for i in 0..v { out.push(FieldExpr::Index(1, i)); }
            out
        };
        let pair = self.concat(vec![side.witness.clone(), side.forward.clone()]);
        let pair_keyed = self.project(pair, Projection {
            key: pack_kv(k, v),
            val: (0..user_len).map(|i| FieldExpr::Index(1, v + i)).collect(),
        });
        let dep_keyed = self.project(dep_y, Projection {
            key: pack_kv(k, v),
            val: (0..output_depth + 1).map(|i| FieldExpr::Index(1, v + i)).collect(),
        });
        let key: Vec<FieldExpr> = (0..k).map(|i| FieldExpr::Index(0, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v { val.push(FieldExpr::Index(0, k + i)); }
        for i in 0..user_len { val.push(FieldExpr::Index(2, i)); }
        for i in 0..output_depth { val.push(FieldExpr::Index(1, i)); }
        val.push(FieldExpr::Index(1, output_depth)); // q
        let joined = self.join(dep_keyed, pair_keyed, Projection { key, val });
        self.filter_time_and_strip(joined, k, v, user_len, output_depth, user_len)
    }

    /// Keyed lookup (Reduce-style), with Min's value-narrowing.
    fn emit_lookup_keyed(&mut self, dep_y: Ref, side: &Side, output_shape: (usize, usize), out_user_len: usize, reducer: &Reducer) -> Ref {
        let (k_in, v_in) = side.shape;
        let in_user_len = side.user_len;
        let (_, v_out) = output_shape;
        let pair = self.concat(vec![side.witness.clone(), side.forward.clone()]);
        let include_v_out = matches!(reducer, Reducer::Min) && v_in == v_out && v_in > 0;
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v_in { val.push(FieldExpr::Index(2, i)); }
        if include_v_out {
            for i in 0..v_out { val.push(FieldExpr::Index(1, i)); }
        }
        for i in 0..in_user_len { val.push(FieldExpr::Index(2, v_in + i)); }
        for i in 0..out_user_len { val.push(FieldExpr::Index(1, v_out + i)); }
        val.push(FieldExpr::Index(1, v_out + out_user_len));
        let proj = Projection { key: (0..k_in).map(|i| FieldExpr::Index(0, i)).collect(), val };
        let joined = self.join(dep_y, pair, proj);
        let after_min = if include_v_out {
            let mut acc: Option<Condition> = None;
            for i in 0..v_in {
                let cond = Condition::Eq(FieldExpr::Index(1, i), FieldExpr::Index(1, v_in + i));
                acc = Some(match acc { None => cond, Some(prev) => Condition::And(Box::new(prev), Box::new(cond)) });
            }
            let filtered = self.filter(joined, acc.unwrap());
            let key: Vec<FieldExpr> = (0..k_in).map(|i| FieldExpr::Index(0, i)).collect();
            let mut new_val: Vec<FieldExpr> = Vec::new();
            for i in 0..v_in { new_val.push(FieldExpr::Index(1, i)); }
            let after_vout = v_in + v_out;
            for i in 0..in_user_len { new_val.push(FieldExpr::Index(1, after_vout + i)); }
            for i in 0..out_user_len { new_val.push(FieldExpr::Index(1, after_vout + in_user_len + i)); }
            new_val.push(FieldExpr::Index(1, after_vout + in_user_len + out_user_len));
            self.project(filtered, Projection { key, val: new_val })
        } else {
            joined
        };
        self.filter_time_and_strip(after_min, k_in, v_in, in_user_len, out_user_len, in_user_len)
    }

    /// Lossy lookup (Linear[Project]): pure-map shortcut when invertible and
    /// same-scope, pair-table fallback otherwise.
    fn emit_lookup_lossy(&mut self, dep_y: Ref, side: &Side, output_shape: (usize, usize), out_user_len: usize, proj: &Projection) -> Ref {
        let (k_in, v_in) = side.shape;
        let in_user_len = side.user_len;
        let (k_out, v_out) = output_shape;
        let known = analyze_lossy_invertibility(proj, k_in, v_in);
        let total = (0..k_in).all(|c| known.contains_key(&(0, c)))
            && (0..v_in).all(|c| known.contains_key(&(1, c)));
        if total && in_user_len == out_user_len {
            let access = |p: usize| -> FieldExpr {
                if p < k_out { FieldExpr::Index(0, p) } else { FieldExpr::Index(1, p - k_out) }
            };
            let key: Vec<FieldExpr> = (0..k_in).map(|c| access(known[&(0, c)])).collect();
            let mut val: Vec<FieldExpr> = Vec::with_capacity(v_in + in_user_len + 1);
            for c in 0..v_in { val.push(access(known[&(1, c)])); }
            for i in 0..out_user_len { val.push(FieldExpr::Index(1, v_out + i)); }
            val.push(FieldExpr::Index(1, v_out + out_user_len));
            return self.project(dep_y, Projection { key, val });
        }
        let pair_src = self.concat(vec![side.witness.clone(), side.forward.clone()]);
        let mut pair_val: Vec<FieldExpr> = Vec::with_capacity(k_in + v_in + in_user_len);
        for i in 0..k_in { pair_val.push(FieldExpr::Index(0, i)); }
        for i in 0..v_in + in_user_len { pair_val.push(FieldExpr::Index(1, i)); }
        let pair = self.project(pair_src, Projection { key: proj.key.clone(), val: pair_val });
        let key: Vec<FieldExpr> = (0..k_in).map(|i| FieldExpr::Index(2, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v_in { val.push(FieldExpr::Index(2, k_in + i)); }
        for i in 0..in_user_len { val.push(FieldExpr::Index(2, k_in + v_in + i)); }
        for i in 0..out_user_len { val.push(FieldExpr::Index(1, v_out + i)); }
        val.push(FieldExpr::Index(1, v_out + out_user_len));
        let joined = self.join(dep_y, pair, Projection { key, val });
        self.filter_time_and_strip(joined, k_in, v_in, in_user_len, out_user_len, in_user_len)
    }

    /// Join's backward rule: two contribs (left, right).
    fn emit_lookup_join(&mut self, dep_y: Ref, left: &Side, right: &Side, output_shape: (usize, usize), out_user_len: usize, projection: &Projection) -> (Ref, Ref) {
        let (k_arity, v_l) = left.shape;
        let (_, v_r) = right.shape;
        let (_, v_out) = output_shape;
        let left_user_len = left.user_len;
        let right_user_len = right.user_len;
        let left_pair_src = self.concat(vec![left.witness.clone(), left.forward.clone()]);
        let right_pair_src = self.concat(vec![right.witness.clone(), right.forward.clone()]);
        let mut pair_val: Vec<FieldExpr> = Vec::new();
        for i in 0..k_arity { pair_val.push(FieldExpr::Index(0, i)); }
        for i in 0..v_l { pair_val.push(FieldExpr::Index(1, i)); }
        for i in 0..v_r { pair_val.push(FieldExpr::Index(2, i)); }
        for i in 0..left_user_len { pair_val.push(FieldExpr::Index(1, v_l + i)); }
        for i in 0..right_user_len { pair_val.push(FieldExpr::Index(2, v_r + i)); }
        let pos_arities = [k_arity, v_l, v_r];
        let key_expanded = expand_pos_bounded(&projection.key, &pos_arities);
        let pair = self.join(left_pair_src, right_pair_src, Projection { key: key_expanded, val: pair_val });
        let q_pair_pos = v_out + out_user_len;
        let vl_pair_start = k_arity;
        let vr_pair_start = vl_pair_start + v_l;
        let ul_pair_start = vr_pair_start + v_r;
        let ur_pair_start = ul_pair_start + left_user_len;
        // One dep ⋈ pair join carrying BOTH sides' user chains, so that both
        // time filters apply before either side's coords are projected away.
        // Projecting away the partner's user chain first is unsound: pair rows
        // that differ only in the partner's time — e.g. a `+1` while the
        // partner held the row and a `-1` from the partner's later retraction
        // — merge into the same contribution row and cancel, annihilating
        // demand that the surviving (time-valid) configuration justifies.
        // A (left, right) pair can only explain output demanded at `u_out` if
        // BOTH inputs were present at times ≤ `u_out`.
        let key: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(2, i)).collect();
        let mut val: Vec<FieldExpr> = Vec::new();
        for i in 0..v_l { val.push(FieldExpr::Index(2, vl_pair_start + i)); }
        for i in 0..v_r { val.push(FieldExpr::Index(2, vr_pair_start + i)); }
        for i in 0..left_user_len { val.push(FieldExpr::Index(2, ul_pair_start + i)); }
        for i in 0..right_user_len { val.push(FieldExpr::Index(2, ur_pair_start + i)); }
        for i in 0..out_user_len { val.push(FieldExpr::Index(1, v_out + i)); }
        val.push(FieldExpr::Index(1, q_pair_pos));
        let joined = self.join(dep_y, pair, Projection { key, val });
        // Joined val layout: V_L ++ V_R ++ U_L ++ U_R ++ user_out ++ [q].
        let ul_j = v_l + v_r;
        let ur_j = ul_j + left_user_len;
        let uo_j = ur_j + right_user_len;
        let q_j = uo_j + out_user_len;
        // Outer-aligned `u_in ≤ u_out` for one side's chain at offset `off`.
        let time_cond = |off: usize, in_len: usize| -> Option<Condition> {
            let n = in_len.min(out_user_len);
            let in_skip = in_len - n;
            let out_skip = out_user_len - n;
            let mut acc: Option<Condition> = None;
            for i in 0..n {
                let cond = Condition::Le(
                    FieldExpr::Index(1, off + in_skip + i),
                    FieldExpr::Index(1, uo_j + out_skip + i),
                );
                acc = Some(match acc {
                    None => cond,
                    Some(prev) => Condition::And(Box::new(prev), Box::new(cond)),
                });
            }
            acc
        };
        let both = match (time_cond(ul_j, left_user_len), time_cond(ur_j, right_user_len)) {
            (Some(a), Some(b)) => Some(Condition::And(Box::new(a), Box::new(b))),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        let timed = match both {
            Some(cond) => self.filter(joined, cond),
            None => joined,
        };
        // Per-side contributions: (K; V_side ++ U_side ++ [q]).
        let key_left: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        let mut val_left: Vec<FieldExpr> = Vec::new();
        for i in 0..v_l { val_left.push(FieldExpr::Index(1, i)); }
        for i in 0..left_user_len { val_left.push(FieldExpr::Index(1, ul_j + i)); }
        val_left.push(FieldExpr::Index(1, q_j));
        let left_contrib = self.project(timed.clone(), Projection { key: key_left, val: val_left });
        let key_right: Vec<FieldExpr> = (0..k_arity).map(|i| FieldExpr::Index(0, i)).collect();
        let mut val_right: Vec<FieldExpr> = Vec::new();
        for i in 0..v_r { val_right.push(FieldExpr::Index(1, v_l + i)); }
        for i in 0..right_user_len { val_right.push(FieldExpr::Index(1, ur_j + i)); }
        val_right.push(FieldExpr::Index(1, q_j));
        let right_contrib = self.project(timed, Projection { key: key_right, val: val_right });
        (left_contrib, right_contrib)
    }
}

fn expand_pos_bounded(fields: &[FieldExpr], arities: &[usize]) -> Vec<FieldExpr> {
    let mut out = Vec::with_capacity(fields.len());
    for f in fields { expand_pos_one(f, arities, &mut out); }
    out
}

fn expand_pos_one(f: &FieldExpr, arities: &[usize], out: &mut Vec<FieldExpr>) {
    match f {
        FieldExpr::Pos(i) => {
            for c in 0..arities[*i] { out.push(FieldExpr::Index(*i, c)); }
        }
        FieldExpr::Index(_, _) | FieldExpr::Const(_) => out.push(f.clone()),
        FieldExpr::Neg(inner) => {
            let mut tmp = Vec::new();
            expand_pos_one(inner, arities, &mut tmp);
            for t in tmp { out.push(FieldExpr::Neg(Box::new(t))); }
        }
        FieldExpr::Sub(a, b) => {
            let (mut ta, mut tb) = (Vec::new(), Vec::new());
            expand_pos_one(a, arities, &mut ta);
            expand_pos_one(b, arities, &mut tb);
            for (x, y) in ta.into_iter().zip(tb) { out.push(FieldExpr::Sub(Box::new(x), Box::new(y))); }
        }
    }
}

fn analyze_lossy_invertibility(proj: &Projection, k_in: usize, v_in: usize) -> BTreeMap<(usize, usize), usize> {
    let mut known: BTreeMap<(usize, usize), usize> = BTreeMap::new();
    let mut p: usize = 0;
    for fe in proj.key.iter().chain(proj.val.iter()) {
        match fe {
            FieldExpr::Index(r, c) => { known.entry((*r, *c)).or_insert(p); p += 1; }
            FieldExpr::Pos(r) => {
                let arity = if *r == 0 { k_in } else { v_in };
                for c in 0..arity { known.entry((*r, c)).or_insert(p + c); }
                p += arity;
            }
            FieldExpr::Const(_) | FieldExpr::Neg(_) | FieldExpr::Sub(_, _) => { p += 1; }
        }
    }
    known
}

/// The `(k, v)` shape of the program's first export — what a query against it
/// must match. A query row is `(key[k]; val[v] ++ [q])`; a shape-mismatched
/// query addresses nothing and yields junk demand, so harnesses should check
/// loudly at seeding time.
pub fn export_shape(p: &Program, source_shapes: &[(usize, usize)]) -> (usize, usize) {
    let shapes = site_shapes(p, source_shapes);
    let first = p.root.exports.first().expect("export_shape: program has no export").value.clone();
    match resolve(&p.root, &[], &first) {
        Target::Site(a) => shapes[&a],
        Target::Source(k) => source_shapes[k],
    }
}

/// The transform. `source_shapes[k]` is the `(k, v)` of the original root's
/// import `k` (positional inputs and named traces alike). The query arrives
/// as one extra positional input appended after the original inputs.
pub fn explain(p: &Program, source_shapes: &[(usize, usize)]) -> Program {
    let n_sources = p.root.imports.len();
    assert_eq!(source_shapes.len(), n_sources, "one shape per root import");
    let shapes = site_shapes(p, source_shapes);
    // The largest positional-input index, for placing the query input after.
    let max_input = p.root.imports.iter().filter_map(|imp| match &imp.from {
        Source::Input(i) => Some(*i + 1),
        _ => None,
    }).max().unwrap_or(0);

    // ---- output root: original sources + query input + witness clone ----
    let mut root = Sb::new(&p.root.name);
    root.s.imports = p.root.imports.clone();
    let src_refs: Vec<Ref> = (0..n_sources).map(Ref::Import).collect();
    let query_ref = root.import("query".into(), Source::Input(max_input));
    let witness: BTreeMap<Addr, Ref> = clone_into(&p.root, &mut root.s, &src_refs).into_iter().collect();
    // The witness clone re-exported the original program's exports; the
    // explain output's exports are the demand sets only.
    root.s.exports.clear();

    // ---- explain scope ----
    let mut ex = Sb::new("explain");
    let ex_src: Vec<Ref> = (0..n_sources)
        .map(|k| ex.import(format!("$src:{}", k), Source::Parent(src_refs[k].clone())))
        .collect();
    let ex_query = ex.import("$query".into(), Source::Parent(query_ref));
    let wit: BTreeMap<Addr, Ref> = witness.iter()
        .map(|(a, r)| (a.clone(), ex.import(format!("$wit:{:?}:{:?}", a.path, a.site), Source::Parent(r.clone()))))
        .collect();

    // Demand-set variables, one per source; forward inputs are the actual
    // sources restricted to the demand-sets.
    let dsets: Vec<Ref> = (0..n_sources).map(|k| ex.variable(&format!("demand_set_{}", k))).collect();
    let fwd_inputs: Vec<Ref> = (0..n_sources).map(|k| {
        let (ka, va) = source_shapes[k];
        ex.semijoin_data(dsets[k].clone(), ex_src[k].clone(), ka, va)
    }).collect();
    let forward: BTreeMap<Addr, Ref> = clone_into(&p.root, &mut ex.s, &fwd_inputs).into_iter().collect();
    ex.s.exports.clear();

    // Pre-allocated demand variables for the original feedback vars (forward
    // cycles induce backward cycles that need a Variable to close).
    let mut var_addrs: Vec<Addr> = Vec::new();
    collect_var_addrs(&p.root, &[], &mut var_addrs);
    let mut demand: BTreeMap<Addr, Ref> = BTreeMap::new();
    for a in &var_addrs {
        let dv = ex.variable(&format!("demand_var_{:?}_{:?}", a.path, a.site));
        demand.insert(a.clone(), dv);
    }

    // The reverse walk.
    let mut rev = Reverse {
        orig: &p.root,
        shapes: &shapes,
        source_shapes,
        wit: &wit,
        fwd: &forward,
        ex_src: &ex_src,
        fwd_inputs: &fwd_inputs,
        demand,
        contribs: BTreeMap::new(),
    };
    // Seed: the query rows are demand against the first export's target.
    let first = p.root.exports.first().expect("explain: program has no export").value.clone();
    let target = resolve(&p.root, &[], &first);
    let seeded = rev.route(&mut ex, ex_query, 0, &target);
    rev.contribs.entry(target).or_default().push(seeded);
    rev.walk(&mut ex, &p.root, &[]);

    // Demand-set closure per source: strip q, restrict to actual rows,
    // accumulate, and export.
    let mut demand_exports: Vec<(String, usize)> = Vec::new();
    for k in 0..n_sources {
        let (ka, va) = source_shapes[k];
        let cs = rev.contribs.remove(&Target::Source(k)).unwrap_or_default();
        let combined = if cs.is_empty() {
            dsets[k].clone() // no demand: the set stays empty (self-bind below)
        } else {
            let merged = ex.concat(cs);
            let stripped = ex.project(merged, strip_user_and_q(ka, va));
            let semi = ex.semijoin_data(stripped, ex_src[k].clone(), ka, va);
            ex.concat(vec![dsets[k].clone(), semi])
        };
        let dist = ex.distinct_full(combined, ka, va);
        ex.debug_inspect(dist.clone(), format!("demand_set:{}", k));
        ex.bind(dsets[k].clone(), dist.clone());
        let name = match &p.root.imports[k].from {
            Source::Input(i) => format!("demand:input{}", i),
            Source::Trace(nm) => format!("demand:{}", nm),
            Source::Parent(_) => unreachable!(),
        };
        let j = ex.export(name.clone(), dsets[k].clone());
        demand_exports.push((name, j));
    }

    // Close: the explain scope becomes a Sub of the root; its demand exports
    // become the program's exports.
    let ex_idx = root.s.items.len();
    root.s.items.push(Item::Sub(ex.s));
    for (name, j) in demand_exports {
        root.export(name, Ref::ChildExport(ex_idx, j));
    }
    Program { root: root.s }
}

fn collect_var_addrs(s: &Scope, path: &[usize], out: &mut Vec<Addr>) {
    for v in 0..s.vars.len() {
        out.push(Addr { path: path.to_vec(), site: Site::Var(v) });
    }
    for (i, item) in s.items.iter().enumerate() {
        if let Item::Sub(c) = item {
            let mut cp = path.to_vec();
            cp.push(i);
            collect_var_addrs(c, &cp, out);
        }
    }
}

/// State for the reverse walk: demand refs per site, contributions per target.
struct Reverse<'a> {
    orig: &'a Scope,
    shapes: &'a BTreeMap<Addr, (usize, usize)>,
    source_shapes: &'a [(usize, usize)],
    wit: &'a BTreeMap<Addr, Ref>,
    fwd: &'a BTreeMap<Addr, Ref>,
    ex_src: &'a [Ref],
    fwd_inputs: &'a [Ref],
    demand: BTreeMap<Addr, Ref>,
    contribs: BTreeMap<Target, Vec<Ref>>,
}

impl<'a> Reverse<'a> {
    fn side(&self, t: &Target) -> Side {
        match t {
            Target::Site(a) => Side {
                witness: self.wit[a].clone(),
                forward: self.fwd[a].clone(),
                shape: self.shapes[a],
                user_len: a.path.len(),
            },
            Target::Source(k) => Side {
                witness: self.ex_src[*k].clone(),
                forward: self.fwd_inputs[*k].clone(),
                shape: self.source_shapes[*k],
                user_len: 0,
            },
        }
    }

    /// Adapt `contrib` (chain length `from_len`) to `target`'s depth: equal
    /// lengths push through; otherwise the shape-preserving lookup against the
    /// target's host injects or strips the difference.
    fn route(&mut self, ex: &mut Sb, contrib: Ref, from_len: usize, target: &Target) -> Ref {
        let to_len = match target { Target::Site(a) => a.path.len(), Target::Source(_) => 0 };
        if to_len == from_len {
            contrib
        } else {
            let side = self.side(target);
            ex.emit_lookup_shape_preserving(contrib, &side, from_len)
        }
    }

    fn push(&mut self, ex: &mut Sb, path: &[usize], input: &Ref, contrib: Ref, from_len: usize) {
        let target = resolve(self.orig, path, input);
        let routed = self.route(ex, contrib, from_len, &target);
        self.contribs.entry(target).or_default().push(routed);
    }

    fn walk(&mut self, ex: &mut Sb, s: &Scope, path: &[usize]) {
        // Binds first: route each var's demand into its value's contribs,
        // inverting the feedback's iter advance (user_chain[0] -= 1, dropping
        // iter-0 demand, which has no body-side source).
        for b in &s.binds {
            let var_addr = Addr { path: path.to_vec(), site: Site::Var(b.var) };
            let dv = self.demand[&var_addr].clone();
            let (kx, vx) = self.shapes[&var_addr];
            let var_user_len = path.len();
            let chain_pos = vx;
            let filtered = ex.filter(dv, Condition::Gt(FieldExpr::Index(1, chain_pos), FieldExpr::Const(0)));
            let key: Vec<FieldExpr> = (0..kx).map(|i| FieldExpr::Index(0, i)).collect();
            let mut val: Vec<FieldExpr> = Vec::new();
            for i in 0..vx { val.push(FieldExpr::Index(1, i)); }
            val.push(FieldExpr::Sub(Box::new(FieldExpr::Index(1, chain_pos)), Box::new(FieldExpr::Const(1))));
            for i in 1..var_user_len { val.push(FieldExpr::Index(1, chain_pos + i)); }
            val.push(FieldExpr::Index(1, chain_pos + var_user_len));
            let contrib = ex.project(filtered, Projection { key, val });
            self.push(ex, path, &b.value, contrib, var_user_len);
        }
        // Items in reverse: consumers have contributed by the time we arrive.
        for (i, item) in s.items.iter().enumerate().rev() {
            match item {
                Item::Op(node) => self.site(ex, s, path, i, node),
                Item::Sub(child) => {
                    let mut cp = path.to_vec();
                    cp.push(i);
                    self.walk(ex, child, &cp);
                }
            }
        }
        // Close this scope's feedback variables: bind each demand variable to
        // its accumulated (distinct) demand, or to itself if none arrived.
        for v in 0..s.vars.len() {
            let addr = Addr { path: path.to_vec(), site: Site::Var(v) };
            let dv = self.demand[&addr].clone();
            let cs = self.contribs.remove(&Target::Site(addr.clone())).unwrap_or_default();
            if cs.is_empty() {
                ex.bind(dv.clone(), dv);
                continue;
            }
            let combined = ex.concat(cs);
            let (k, vx) = self.shapes[&addr];
            let dist = ex.distinct_full(combined, k, vx + path.len() + 1);
            ex.debug_inspect(dist.clone(), format!("demand_{:?}:{:?}", path, Site::Var(v)));
            ex.bind(dv, dist);
        }
    }

    fn site(&mut self, ex: &mut Sb, _s: &Scope, path: &[usize], i: usize, node: &Node) {
        let addr = Addr { path: path.to_vec(), site: Site::Op(i) };
        let cs = self.contribs.remove(&Target::Site(addr.clone())).unwrap_or_default();
        if cs.is_empty() { return; }
        let combined = ex.concat(cs);
        let (k, v) = self.shapes[&addr];
        let out_user_len = path.len();
        let dist = ex.distinct_full(combined, k, v + out_user_len + 1);
        ex.debug_inspect(dist.clone(), format!("demand_{:?}:{:?}", path, Site::Op(i)));
        self.demand.insert(addr.clone(), dist.clone());
        let out_shape = (k, v);
        let dep_this = dist;

        match node {
            Node::Linear { input, ops } => {
                let op = match ops.as_slice() {
                    [single] => single,
                    _ => panic!("explain: multi-op Linear (run before optimize)"),
                };
                match op {
                    LinearOp::Project(proj) => {
                        let target = resolve(self.orig, path, input);
                        let side = self.side(&target);
                        let contrib = ex.emit_lookup_lossy(dep_this, &side, out_shape, out_user_len, proj);
                        self.contribs.entry(target).or_default().push(contrib);
                    }
                    LinearOp::Filter(cond) => {
                        let contrib = ex.filter(dep_this, cond.clone());
                        self.push(ex, path, input, contrib, out_user_len);
                    }
                    LinearOp::Negate | LinearOp::EnterAt(_) => {
                        // Negate: pure pass-through. EnterAt: sound but
                        // over-broad pass-through (see the flat rule's note);
                        // the routing adapter handles any depth difference.
                        self.push(ex, path, input, dep_this, out_user_len);
                    }
                    LinearOp::LiftIter => panic!("explain: LiftIter in user program"),
                }
            }
            Node::Concat(refs) => {
                for r in refs {
                    let target = resolve(self.orig, path, r);
                    let side = self.side(&target);
                    let contrib = ex.emit_lookup_shape_preserving(dep_this.clone(), &side, out_user_len);
                    self.contribs.entry(target).or_default().push(contrib);
                }
            }
            Node::Arrange(input) | Node::Inspect { input, .. } => {
                self.push(ex, path, input, dep_this, out_user_len);
            }
            Node::Reduce { input, reducer } => {
                let target = resolve(self.orig, path, input);
                let side = self.side(&target);
                let contrib = ex.emit_lookup_keyed(dep_this, &side, out_shape, out_user_len, reducer);
                self.contribs.entry(target).or_default().push(contrib);
            }
            Node::Join { left, right, projection } => {
                let lt = resolve(self.orig, path, left);
                let rt = resolve(self.orig, path, right);
                let ls = self.side(&lt);
                let rs = self.side(&rt);
                let (lc, rc) = ex.emit_lookup_join(dep_this, &ls, &rs, out_shape, out_user_len, projection);
                self.contribs.entry(lt).or_default().push(lc);
                self.contribs.entry(rt).or_default().push(rc);
            }
        }
    }
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
