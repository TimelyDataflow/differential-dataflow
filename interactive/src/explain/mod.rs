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
//! value as `(V, chain…)`, the user's value whole and then its user-iter
//! coordinates, innermost first — at the
//! embedding level.
//!
//! A scope exports its lifted internals, and the cascade through enclosing
//! scopes is the recursion. The embedding depth is not a parameter: the
//! renderer derives depth structurally, so a clone needn't know where it will
//! sit.

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
/// unconsumed). Pinned by the `clone_identity_preserves_*` tests.
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
        out.vars.push(Var { name: v.name.clone(), shape: v.shape.clone() });
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
                    shape: imp.shape.clone(),
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
                // and the export is the host-visible edge upward. A site of
                // this scope first has its value wrapped as `(V)`, so the
                // lifts extend it to `(V, chain…)` with `V` whole.
                let sub_idx = out.items.len();
                for (a, vref) in child_visible {
                    let vref = if a.path == child_path {
                        let wrap_idx = child_out.items.len();
                        let wrap = Projection { key: Term::Var(0), val: Term::Tuple(vec![Term::Var(1)]) };
                        child_out.items.push(Item::Op(Node::Linear { input: vref, ops: vec![LinearOp::Project(wrap)] }));
                        Ref::Local(wrap_idx)
                    } else {
                        vref
                    };
                    let lift_idx = child_out.items.len();
                    child_out.items.push(Item::Op(Node::Linear { input: vref, ops: vec![LinearOp::LiftIter] }));
                    let export_idx = child_out.exports.len();
                    child_out.exports.push(Export {
                        name: format!("$host:{}", export_idx),
                        value: Ref::Local(lift_idx),
                        shape: None,
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
            shape: e.shape.clone(),
        });
    }

    visible
}

// ===== The explanation transform =====
//
// The reverse dataflow is flat inside the explain scope: demand rows carry the
// user-iteration chain in the value, so no nesting is needed. A reference is
// *resolved* through explicit import/export edges to the value site it names,
// and the ordinary shape-preserving lookup against that site's host form
// injects or strips coordinates as the depths dictate. No op needs to know
// about boundaries.

use std::collections::BTreeMap;
use crate::ir::{BinOp, Projection, Reducer, Term};
mod decouple;
use decouple::{Dataflow, RowModel};

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

// ===== User terms over explain's rows =====
//
// Explain reuses the user's terms (a filter's predicate, a projection, a
// flatmap's list) over its own rows, whose values carry the chain (and `q`)
// after the user's value. A term must see the user's row, not that one, so
// each `$r` is replaced by the user's row as it sits in explain's.

/// `t` with each input row `$r` replaced by `rows[r]`.
fn with_rows(t: &Term, rows: &[Term]) -> Term {
    let go = |t: &Term| with_rows(t, rows);
    let bx = |t: &Term| Box::new(with_rows(t, rows));
    match t {
        Term::Var(r) => rows[*r].clone(),
        Term::Bound(_) | Term::Int(_) => t.clone(),
        Term::Tuple(ts) => Term::Tuple(ts.iter().map(go).collect()),
        Term::List(ts) => Term::List(ts.iter().map(go).collect()),
        Term::Hash(ts) => Term::Hash(ts.iter().map(go).collect()),
        Term::Call(name, ts) => Term::Call(name.clone(), ts.iter().map(go).collect()),
        Term::Spread(t) => Term::Spread(bx(t)),
        Term::Proj(t, i) => Term::Proj(bx(t), *i),
        Term::Unary(op, t) => Term::Unary(*op, bx(t)),
        Term::Binary(op, a, b) => Term::Binary(*op, bx(a), bx(b)),
        Term::Inject { tag, payload, sum } => Term::Inject { tag: bx(tag), payload: bx(payload), sum: sum.clone() },
        Term::Case { scrutinee, arms, default } => Term::Case {
            scrutinee: bx(scrutinee),
            arms: arms.iter().map(go).collect(),
            default: default.as_deref().map(bx),
        },
        Term::Fold { list, init, step } => Term::Fold { list: bx(list), init: bx(init), step: bx(step) },
        Term::If { cond, then, els } => Term::If { cond: bx(cond), then: bx(then), els: bx(els) },
    }
}

/// `p` over rows whose input rows are `rows` (see [`with_rows`]).
fn proj_with_rows(p: &Projection, rows: &[Term]) -> Projection {
    Projection { key: with_rows(&p.key, rows), val: with_rows(&p.val, rows) }
}

/// The user's rows in an explain row whose slots after the key are envelope
/// values: the key whole, and each value the envelope's first field.
fn user_rows(slots: usize) -> Vec<Term> {
    std::iter::once(var(0)).chain((1..slots).map(|s| f(s, 0))).collect()
}

/// A user row `(K ; V)` as an envelope row `(K ; (V))`, with an empty chain.
fn envelope() -> Projection {
    Projection { key: var(0), val: tup(vec![var(1)]) }
}

/// Options for the explanation rewrite.
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    /// Insert an `Inspect` tap on every demand collection (`demand_*` /
    /// `demand_set:*` labels), for tracing the reverse dataflow's contents.
    pub debug_inspects: bool,
}

/// Minimal builder over a `Scope` under construction: push an op, get a Ref.
struct Sb {
    s: Scope,
    debug: bool,
}

impl Sb {
    fn new(name: &str) -> Self { Sb { s: Scope { name: name.into(), ..Scope::default() }, debug: false } }
    fn op(&mut self, n: Node) -> Ref {
        let i = self.s.items.len();
        self.s.items.push(Item::Op(n));
        Ref::Local(i)
    }
    fn linear(&mut self, input: Ref, ops: Vec<LinearOp>) -> Ref { self.op(Node::Linear { input, ops }) }
    fn project(&mut self, input: Ref, p: Projection) -> Ref { self.linear(input, vec![LinearOp::Project(p)]) }
    fn filter(&mut self, input: Ref, c: Term) -> Ref { self.linear(input, vec![LinearOp::Filter(c)]) }
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
        self.s.vars.push(Var { name: name.into(), shape: None });
        Ref::Var(v)
    }
    fn bind(&mut self, var: Ref, value: Ref) {
        let Ref::Var(v) = var else { panic!("bind: not a variable ref") };
        self.s.binds.push(Bind { var: v, value });
    }
    fn import(&mut self, name: String, from: Source) -> Ref {
        let k = self.s.imports.len();
        self.s.imports.push(Import { name, from, shape: None });
        Ref::Import(k)
    }
    fn export(&mut self, name: String, value: Ref) -> usize {
        let j = self.s.exports.len();
        self.s.exports.push(Export { name, value, shape: None });
        j
    }
    fn debug_inspect(&mut self, input: Ref, label: String) {
        if self.debug {
            self.op(Node::Inspect { input, label });
        }
    }
    /// Semijoin `left (K ; V)` with `right` by `K`, keeping left's rows.
    fn semijoin(&mut self, left: Ref, right: Ref) -> Ref {
        self.join(left, right, Projection { key: var(0), val: var(1) })
    }
    /// Set-level distinct on whole rows: move the row into the key, distinct,
    /// and move it back.
    fn distinct_full(&mut self, input: Ref) -> Ref {
        let packed = self.project(input, Projection { key: tup(vec![var(0), var(1)]), val: tup(Vec::new()) });
        let dist = self.reduce(packed, Reducer::Distinct);
        self.project(dist, Projection { key: f(0, 0), val: f(0, 1) })
    }
}

/// One upstream edge into a backward rule: the target's host-side `(data,
/// user)` collections from both clones, and its user-chain length.
struct Side {
    witness: Ref,
    forward: Ref,
    user_len: usize,
}

impl Side {
    /// View this edge as the data-model-agnostic `SideInfo` the reverse rules take.
    fn info(&self) -> decouple::SideInfo<Sb> {
        decouple::SideInfo {
            witness: self.witness.clone(),
            forward: self.forward.clone(),
            user_len: self.user_len,
        }
    }
}

impl Sb {
    /// Shape-preserving lookup; also the universal depth adapter — when a
    /// contribution's chain length differs from its target's, the join against
    /// the target's host form injects (deeper target) or strips (shallower)
    /// the difference, outer-aligned by `folded`.
    fn emit_lookup_shape_preserving(&mut self, dep_y: Ref, side: &Side, output_depth: usize) -> Ref {
        decouple::shape_preserving_lookup::<Val, Sb>(self, &dep_y, &side.info(), output_depth)
    }

    /// Keyed lookup (Reduce-style), with Min's value-narrowing.
    fn emit_lookup_keyed(&mut self, dep_y: Ref, side: &Side, out_user_len: usize, reducer: &Reducer) -> Ref {
        let min = matches!(reducer, Reducer::Min);
        decouple::keyed_lookup::<Val, Sb>(self, &dep_y, &side.info(), out_user_len, min)
    }

    /// Lossy lookup (Linear[Project]), through the pair table.
    fn emit_lookup_lossy(&mut self, dep_y: Ref, side: &Side, out_user_len: usize, proj: &Projection) -> Ref {
        decouple::lossy_lookup::<Val, Sb>(self, &dep_y, &side.info(), out_user_len, proj)
    }

    /// Join's backward rule: two contribs (left, right).
    fn emit_lookup_join(&mut self, dep_y: Ref, left: &Side, right: &Side, out_user_len: usize, projection: &Projection) -> (Ref, Ref) {
        decouple::join_lookup::<Val, Sb>(self, &dep_y, &left.info(), &right.info(), out_user_len, projection)
    }

    /// FlatMap's (UNNEST's) backward rule — the universal backstop instantiated
    /// for a cardinality-changing op. FlatMap is *same-depth* (it doesn't touch
    /// iteration time) and its list rides as one opaque value, so this needs no
    /// envelope change: build the `(output -> input)` pair table by running the
    /// op forward on the input side, join the demand on the output, recover the
    /// whole input (the `None` endpoint — `RESIDUAL` is the input value).
    ///
    /// The one wrinkle is that a plain flatmap drops the source row and the key
    /// isn't unique, so we **re-key the input by itself** before exploding (the
    /// source then rides through in the join key) and re-project to the packed
    /// output afterwards. `list_term` reads the re-keyed rows: the input's key
    /// is the key's first field, and its value is the row's value.
    fn emit_lookup_flatmap(&mut self, dep_y: Ref, side: &Side, out_user_len: usize, list_term: &Term) -> Ref {
        let d = out_user_len;                // same depth for input and output
        // Pair table: re-key the input by the whole input row, explode (the
        // source survives in the key), then re-key to (k, (pos, elem)) carrying
        // the source as the value.
        let src = self.concat(vec![side.witness.clone(), side.forward.clone()]);
        let rekeyed = self.project(src, Projection { key: tup(vec![var(0), var(1)]), val: f(1, 0) });
        let exploded = self.linear(rekeyed, vec![LinearOp::FlatMap(list_term.clone())]);
        let pair = self.project(exploded, Projection { key: tup(vec![f(0, 0), var(1)]), val: var(0) });
        // Re-key the demand by the same output (k, (pos, elem)); keep its chain ++ q.
        let dep_keyed = self.project(dep_y, Projection { key: tup(vec![var(0), f(1, 0)]), val: tup(fidx(1, 1, d + 2)) });
        // Join into a wide row [V_in | chain_in | chain_out | q]. Source is `$2`
        // = (k, host_val); host_val = (V_in, chain_in). Dep' is `$1` = [chain_out | q].
        let mut wide = vec![fld(f(2, 1), 0)];                                     // V_in
        wide.extend((0..d).map(|c| fld(f(2, 1), 1 + c)));                         // chain_in
        wide.extend(fidx(1, 0, d));                                               // chain_out
        wide.push(f(1, d));                                                       // q
        let joined = self.join(dep_keyed, pair, Projection { key: f(2, 0), val: tup(wide) });
        // Soundness: chain_in ≤ chain_out (same depth, so coordinate-aligned).
        let cond = and_all((0..d).map(|c| le(f(1, 1 + c), f(1, 1 + d + c))).collect());
        let filtered = match cond { Some(c) => self.filter(joined, c), None => joined };
        // Reform the demanded input (k ; V_in, chain_out, q).
        let mut val = vec![f(1, 0)];
        val.extend(fidx(1, 1 + d, 2 + 2 * d));
        self.project(filtered, Projection { key: var(0), val: tup(val) })
    }
}

/// The `Value` data model for explain: a demand row is `(K ; (V, chain…, q))`
/// — the user's key and value whole, then the loop-iteration chain
/// (innermost-first), then the trailing query id, matching the host lift's
/// `append_iter` on `(V)`. Every builder works in field positions of that tuple.
pub(crate) struct Val;

impl Dataflow for Sb {
    type Handle = Ref;
    type Proj = Projection;
    type Pred = Term;
    fn project(&mut self, c: &Ref, p: Projection) -> Ref { Sb::project(self, c.clone(), p) }
    fn filter(&mut self, c: &Ref, p: Term) -> Ref { Sb::filter(self, c.clone(), p) }
    fn join(&mut self, l: &Ref, r: &Ref, p: Projection) -> Ref { Sb::join(self, l.clone(), r.clone(), p) }
    fn concat(&mut self, cs: Vec<Ref>) -> Ref { Sb::concat(self, cs) }
}

// Term-building helpers for the Value reverse rules. `f(s, c)` selects field c
// of slot s, `fidx(s, lo, hi)` a range.
fn var(s: usize) -> Term { Term::Var(s) }
fn fld(t: Term, i: usize) -> Term { Term::Proj(Box::new(t), i) }
fn f(s: usize, i: usize) -> Term { fld(var(s), i) }
fn tup(xs: Vec<Term>) -> Term { Term::Tuple(xs) }
fn fidx(s: usize, lo: usize, hi: usize) -> Vec<Term> { (lo..hi).map(|c| f(s, c)).collect() }
fn le(a: Term, b: Term) -> Term { Term::Binary(BinOp::Le, Box::new(a), Box::new(b)) }
fn eq(a: Term, b: Term) -> Term { Term::Binary(BinOp::Eq, Box::new(a), Box::new(b)) }
fn gt(a: Term, b: Term) -> Term { Term::Binary(BinOp::Gt, Box::new(a), Box::new(b)) }
fn sub(a: Term, b: Term) -> Term { Term::Binary(BinOp::Sub, Box::new(a), Box::new(b)) }
fn and_all(conds: Vec<Term>) -> Option<Term> {
    conds.into_iter().reduce(|a, b| Term::Binary(BinOp::And, Box::new(a), Box::new(b)))
}

impl RowModel for Val {
    type Proj = Projection;
    type Pred = Term;

    fn pack(chain_len: usize, has_q: bool) -> Projection {
        let tail = chain_len + has_q as usize;
        Projection { key: tup(vec![var(0), f(1, 0)]), val: tup(fidx(1, 1, 1 + tail)) }
    }
    fn strip_all() -> Projection {
        Projection { key: var(0), val: f(1, 0) }
    }
    fn sp_reassemble(in_len: usize, out_len: usize) -> Projection {
        let mut val = vec![f(0, 1)];                  // V (from packed key)
        val.extend(fidx(2, 0, in_len));               // chain_in (pair)
        val.extend(fidx(1, 0, out_len));              // chain_out (dep)
        val.push(f(1, out_len));                      // q
        Projection { key: f(0, 0), val: tup(val) }
    }

    fn ky_join(in_len: usize, out_len: usize, min: bool) -> Projection {
        let mut val = vec![f(2, 0)];                  // V_in
        if min { val.push(f(1, 0)); }                 // V_out (for narrowing)
        val.extend(fidx(2, 1, 1 + in_len));           // chain_in
        val.extend(fidx(1, 1, 1 + out_len));          // chain_out
        val.push(f(1, 1 + out_len));                  // q
        Projection { key: var(0), val: tup(val) }
    }
    fn ky_data_eq() -> Option<Term> {
        Some(eq(f(1, 0), f(1, 1)))
    }
    fn ky_drop_vout(in_len: usize, out_len: usize) -> Projection {
        let mut val = vec![f(1, 0)];
        val.extend(fidx(1, 2, 3 + in_len + out_len)); // chain_in, chain_out, q
        Projection { key: var(0), val: tup(val) }
    }

    fn lossy_pair(proj: &Projection, in_len: usize) -> Projection {
        let mut val = vec![var(0), f(1, 0)];          // K_in, V_in
        val.extend(fidx(1, 1, 1 + in_len));           // chain_in
        Projection { key: proj.key.clone(), val: tup(val) }
    }
    fn lossy_reassemble(in_len: usize, out_len: usize) -> Projection {
        let mut val = vec![f(2, 1)];                  // V_in
        val.extend(fidx(2, 2, 2 + in_len));           // chain_in
        val.extend(fidx(1, 1, 1 + out_len));          // chain_out
        val.push(f(1, 1 + out_len));                  // q
        Projection { key: f(2, 0), val: tup(val) }
    }

    fn join_forward(proj: &Projection, l_len: usize, r_len: usize) -> Projection {
        let mut val = vec![var(0), f(1, 0), f(2, 0)]; // K, V_L, V_R
        val.extend(fidx(1, 1, 1 + l_len));            // chain_L
        val.extend(fidx(2, 1, 1 + r_len));            // chain_R
        val.push(proj.val.clone());                   // V_out (pair-produced)
        Projection { key: proj.key.clone(), val: tup(val) }
    }
    fn join_combined(l_len: usize, r_len: usize, out_len: usize) -> Projection {
        let ul_s = 3;
        let ur_s = ul_s + l_len;
        let vout_s = ur_s + r_len;
        let mut val = vec![f(2, 1), f(2, 2)];         // V_L, V_R
        val.extend(fidx(2, ul_s, ul_s + l_len));      // chain_L
        val.extend(fidx(2, ur_s, ur_s + r_len));      // chain_R
        val.extend(fidx(1, 1, 1 + out_len));          // chain_out
        val.push(f(1, 1 + out_len));                  // q
        val.push(f(1, 0));                            // V_out_dep
        val.push(f(2, vout_s));                       // V_out_pair
        Projection { key: f(2, 0), val: tup(val) }
    }
    fn join_filter(l_len: usize, r_len: usize, out_len: usize) -> Option<Term> {
        let ul_j = 2;
        let ur_j = ul_j + l_len;
        let uo_j = ur_j + r_len;
        let q_j = uo_j + out_len;
        let time_cond = |off: usize, in_len: usize| -> Option<Term> {
            let n = in_len.min(out_len);
            let in_skip = in_len - n;
            let out_skip = out_len - n;
            and_all((0..n).map(|i| le(f(1, off + in_skip + i), f(1, uo_j + out_skip + i))).collect())
        };
        let value_cond = Some(eq(f(1, q_j + 1), f(1, q_j + 2)));
        [time_cond(ul_j, l_len), time_cond(ur_j, r_len), value_cond]
            .into_iter().flatten().reduce(|a, b| Term::Binary(BinOp::And, Box::new(a), Box::new(b)))
    }
    fn join_split(left: bool, l_len: usize, r_len: usize, out_len: usize) -> Projection {
        let ul_j = 2;
        let ur_j = ul_j + l_len;
        let q_j = ur_j + r_len + out_len;
        let mut val = if left { vec![f(1, 0)] } else { vec![f(1, 1)] };   // V_L or V_R
        if left {
            val.extend(fidx(1, ul_j, ul_j + l_len));     // chain_L
        } else {
            val.extend(fidx(1, ur_j, ur_j + r_len));     // chain_R
        }
        val.push(f(1, q_j));                             // q
        Projection { key: var(0), val: tup(val) }
    }

    fn time_le(in_len: usize, out_len: usize) -> Option<Term> {
        // Joined row value: (V, user_in(in_len), user_out(out_len), q), each
        // chain innermost-first; compare outer-aligned (last min(in,out)).
        let n = in_len.min(out_len);
        let in_off = in_len - n;
        let out_off = out_len - n;
        and_all((0..n).map(|i| le(f(1, 1 + in_off + i), f(1, 1 + in_len + out_off + i))).collect())
    }
    fn strip(in_len: usize, out_len: usize, keep: usize) -> Projection {
        let mut val = vec![f(1, 0)];                  // V
        let drop_in = in_len - keep;
        for i in 0..keep { val.push(f(1, 1 + drop_in + i)); }   // outer kept user_in coords
        val.push(f(1, 1 + in_len + out_len));         // q
        Projection { key: var(0), val: tup(val) }
    }

    fn bind_filter() -> Option<Term> {
        Some(gt(f(1, 1), Term::Int(0)))
    }
    fn bind_decrement(user_len: usize) -> Projection {
        let mut val = vec![f(1, 0)];
        val.push(sub(f(1, 1), Term::Int(1)));         // innermost chain coord - 1
        val.extend(fidx(1, 2, 2 + user_len));         // the rest of the chain, then q
        Projection { key: var(0), val: tup(val) }
    }
}

/// The shape of a query row against `p`'s first export, `(key ; (val, q))`,
/// when the export's shape follows from `p`'s declared sources.
fn query_shape(p: &Program) -> Option<(corgi::Shape, corgi::Shape)> {
    let (shapes, _) = crate::shapes::infer(p, &|_| None);
    let export = shapes.exports.first()?.clone()?;
    Some((export.key, corgi::Shape::Prod(vec![export.val, corgi::Shape::Int])))
}

/// The transform. The query arrives as one extra positional input appended
/// after the original inputs, its rows `(key ; (val, q))`: a row of the first
/// export, and a query id.
pub fn explain(p: &Program) -> Program {
    explain_with(p, Options::default())
}

/// [`explain`], with [`Options`].
pub fn explain_with(p: &Program, options: Options) -> Program {
    let n_sources = p.root.imports.len();
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
    if let Ref::Import(q) = query_ref { root.s.imports[q].shape = query_shape(p); }
    let witness: BTreeMap<Addr, Ref> = clone_into(&p.root, &mut root.s, &src_refs).into_iter().collect();
    // The witness clone re-exported the original program's exports; the
    // explain output's exports are the demand sets only.
    root.s.exports.clear();

    // ---- explain scope ----
    // Root-level host forms and the sources are the user's rows, which enter
    // the envelope here; a nested host form was lifted into it on export.
    let mut ex = Sb::new("explain");
    ex.debug = options.debug_inspects;
    let ex_src: Vec<Ref> = (0..n_sources)
        .map(|k| ex.import(format!("$src:{}", k), Source::Parent(src_refs[k].clone())))
        .collect();
    let src_env: Vec<Ref> = ex_src.iter().map(|r| ex.project(r.clone(), envelope())).collect();
    let ex_query = ex.import("$query".into(), Source::Parent(query_ref));
    let wit: BTreeMap<Addr, Ref> = witness.iter()
        .map(|(a, r)| {
            let imported = ex.import(format!("$wit:{:?}:{:?}", a.path, a.site), Source::Parent(r.clone()));
            let host = if a.path.is_empty() { ex.project(imported, envelope()) } else { imported };
            (a.clone(), host)
        })
        .collect();

    // Demand-set variables, one per source, of the user's rows; forward inputs
    // are the actual sources restricted to the demand-sets.
    let dsets: Vec<Ref> = (0..n_sources).map(|k| ex.variable(&format!("demand_set_{}", k))).collect();
    let fwd_inputs: Vec<Ref> = (0..n_sources).map(|k| ex.semijoin(dsets[k].clone(), ex_src[k].clone())).collect();
    let fwd_env: Vec<Ref> = fwd_inputs.iter().map(|r| ex.project(r.clone(), envelope())).collect();
    let forward: BTreeMap<Addr, Ref> = clone_into(&p.root, &mut ex.s, &fwd_inputs).into_iter()
        .map(|(a, r)| { let host = if a.path.is_empty() { ex.project(r, envelope()) } else { r }; (a, host) })
        .collect();
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
        wit: &wit,
        fwd: &forward,
        src_env: &src_env,
        fwd_env: &fwd_env,
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
        let cs = rev.contribs.remove(&Target::Source(k)).unwrap_or_default();
        let combined = if cs.is_empty() {
            dsets[k].clone() // no demand: the set stays empty (self-bind below)
        } else {
            let merged = ex.concat(cs);
            let stripped = ex.project(merged, <Val as RowModel>::strip_all());
            let semi = ex.semijoin(stripped, ex_src[k].clone());
            ex.concat(vec![dsets[k].clone(), semi])
        };
        let dist = ex.distinct_full(combined);
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
    wit: &'a BTreeMap<Addr, Ref>,
    fwd: &'a BTreeMap<Addr, Ref>,
    src_env: &'a [Ref],
    fwd_env: &'a [Ref],
    demand: BTreeMap<Addr, Ref>,
    contribs: BTreeMap<Target, Vec<Ref>>,
}

impl<'a> Reverse<'a> {
    fn side(&self, t: &Target) -> Side {
        match t {
            Target::Site(a) => Side {
                witness: self.wit[a].clone(),
                forward: self.fwd[a].clone(),
                user_len: a.path.len(),
            },
            Target::Source(k) => Side {
                witness: self.src_env[*k].clone(),
                forward: self.fwd_env[*k].clone(),
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
            let var_user_len = path.len();
            let filtered = match <Val as RowModel>::bind_filter() {
                Some(c) => ex.filter(dv, c),
                None => dv,
            };
            let contrib = ex.project(filtered, <Val as RowModel>::bind_decrement(var_user_len));
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
            let dist = ex.distinct_full(combined);
            ex.debug_inspect(dist.clone(), format!("demand_{:?}:{:?}", path, Site::Var(v)));
            ex.bind(dv, dist);
        }
    }

    fn site(&mut self, ex: &mut Sb, _s: &Scope, path: &[usize], i: usize, node: &Node) {
        let addr = Addr { path: path.to_vec(), site: Site::Op(i) };
        let cs = self.contribs.remove(&Target::Site(addr.clone())).unwrap_or_default();
        if cs.is_empty() { return; }
        let combined = ex.concat(cs);
        let out_user_len = path.len();
        let dist = ex.distinct_full(combined);
        ex.debug_inspect(dist.clone(), format!("demand_{:?}:{:?}", path, Site::Op(i)));
        self.demand.insert(addr.clone(), dist.clone());
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
                        let proj = proj_with_rows(proj, &user_rows(2));
                        let contrib = ex.emit_lookup_lossy(dep_this, &side, out_user_len, &proj);
                        self.contribs.entry(target).or_default().push(contrib);
                    }
                    LinearOp::Filter(cond) => {
                        let contrib = ex.filter(dep_this, with_rows(cond, &user_rows(2)));
                        self.push(ex, path, input, contrib, out_user_len);
                    }
                    LinearOp::Negate | LinearOp::EnterAt(_) => {
                        // Negate: pure pass-through. EnterAt: sound but
                        // over-broad pass-through; the routing adapter handles
                        // any depth difference.
                        self.push(ex, path, input, dep_this, out_user_len);
                    }
                    LinearOp::LiftIter => panic!("explain: LiftIter in user program"),
                    LinearOp::FlatMap(list_term) => {
                        let target = resolve(self.orig, path, input);
                        let side = self.side(&target);
                        let list_term = with_rows(list_term, &[f(0, 0), var(1)]);
                        let contrib = ex.emit_lookup_flatmap(dep_this, &side, out_user_len, &list_term);
                        self.contribs.entry(target).or_default().push(contrib);
                    }
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
                let contrib = ex.emit_lookup_keyed(dep_this, &side, out_user_len, reducer);
                self.contribs.entry(target).or_default().push(contrib);
            }
            Node::Join { left, right, projection } => {
                let lt = resolve(self.orig, path, left);
                let rt = resolve(self.orig, path, right);
                let ls = self.side(&lt);
                let rs = self.side(&rt);
                let projection = proj_with_rows(projection, &user_rows(3));
                let (lc, rc) = ex.emit_lookup_join(dep_this, &ls, &rs, out_user_len, &projection);
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

    /// The checked-in example, so the fixture cannot drift from the program
    /// the rest of the tree runs.
    const SCC: &str = include_str!("../../examples/programs/scc.ddp");

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
