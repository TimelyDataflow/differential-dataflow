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
        Node::HalfJoin { stream, arrangement, strict, projection } => Node::HalfJoin { stream: m(stream), arrangement: m(arrangement), strict: *strict, projection: projection.clone() },
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
                        child_out.items.push(Item::Op(Node::Linear { input: vref, ops: vec![LinearOp::Project(envelope())] }));
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
use crate::ir::{Lambda, Reducer};
mod decouple;
use decouple::{Dataflow, RowModel};

/// The scope path of `target`; the root's, for a source.
fn target_path(target: &Target) -> &[usize] {
    match target { Target::Site(a) => &a.path, Target::Source(_) => &[] }
}

/// How many enclosing scopes the scopes at `a` and `b` share.
fn common_len(a: &[usize], b: &[usize]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// How many of the chain coordinates of demand in the scope at `from` a target in the scope at
/// `to` can be compared with. A chain's coordinates are the rounds of the scopes enclosing `from`,
/// innermost first, and a target outside one of them (in a sibling scope, say) has rounds of its
/// own that those do not measure: only the coordinates of the scopes the two share count.
fn shared_len(from: &[usize], to: &[usize]) -> usize {
    let shared = common_len(from, to);
    if shared >= from.len().min(to.len()) { from.len() } else { shared }
}

/// Demand `dep`, in the scope at `from`, keeping only the outermost `keep` chain coordinates.
fn outer(ex: &mut Sb, dep: Ref, from: &[usize], keep: usize) -> (Ref, usize) {
    if keep == from.len() { return (dep, keep); }
    let mut val = vec![f(1, 0)];
    val.extend(fidx(1, 1 + from.len() - keep, 1 + from.len()));
    val.push(f(1, 1 + from.len()));
    (ex.project(dep, projection(var(0), tup(val), 2)), keep)
}

/// Demand `dep`, in the scope at `from`, for a lookup against a target in the scope at `to`, and
/// its chain length: see [`shared_len`].
fn align(ex: &mut Sb, dep: Ref, from: &[usize], to: &[usize]) -> (Ref, usize) {
    outer(ex, dep, from, shared_len(from, to))
}

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

// ===== Lambda slots over explain's rows =====
//
// Explain reuses the user's lambdas (a filter's predicate, a projection, a flatmap's list) over
// its own rows, whose values carry the chain (and `q`) after the user's value. A user lambda must
// see the user's row, not explain's, so explain calls it on the user's row as it sits in explain's.

/// A slot of `arity` rows, `(k, v)` or `(k, v, r)`, whose body is `body` applied to the results
/// of the user's lambdas in `calls`, each called on an argument written over those rows (explain's
/// spelling of the user's row). See [`Lambda::compose`]: the user's text is never parsed again.
fn slot(arity: usize, calls: &[(&Lambda, String)], body: impl FnOnce(&[String]) -> String) -> Lambda {
    let pattern = if arity == 2 { "(k, v)" } else { "(k, v, r)" };
    Lambda::compose(pattern, calls, body)
}
/// A projection of explain's own, from the lambda text of its key and value.
fn projection(key: String, val: String, arity: usize) -> Lambda {
    slot(arity, &[], |_| tup(vec![key, val]))
}
/// A filter of explain's own, from the lambda text of its 0/1 mask.
fn predicate(body: String) -> Lambda { slot(2, &[], |_| body) }

/// A user row `(K ; V)` as an envelope row `(K ; (V))`, with an empty chain.
fn envelope() -> Lambda { projection(var(0), tup(vec![var(1)]), 2) }

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
    fn project(&mut self, input: Ref, p: Lambda) -> Ref { self.linear(input, vec![LinearOp::Project(p)]) }
    fn filter(&mut self, input: Ref, c: Lambda) -> Ref { self.linear(input, vec![LinearOp::Filter(c)]) }
    fn concat(&mut self, refs: Vec<Ref>) -> Ref {
        if refs.len() == 1 { return refs.into_iter().next().unwrap(); }
        self.op(Node::Concat(refs))
    }
    fn join(&mut self, l: Ref, r: Ref, projection: Lambda) -> Ref {
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
    /// The rows of `rows (K ; V)` that the set `set` holds, matched as whole rows, at `rows`' counts.
    fn restrict(&mut self, rows: Ref, set: Ref) -> Ref {
        let rows = self.project(rows, projection(tup(vec![var(0), var(1)]), tup(Vec::new()), 2));
        let set = self.project(set, projection(tup(vec![var(0), var(1)]), tup(Vec::new()), 2));
        let held = self.join(rows, set, projection(var(0), tup(Vec::new()), 3));
        self.project(held, projection(f(0, 0), f(0, 1), 2))
    }
    /// Each row whose count is not zero, once: a collection read as a set.
    ///
    /// Demand is a set. A collection maps data to diffs, and an explanation names the data whose
    /// diffs must be accurate, so demand's counts mean nothing and must never cancel. Nor may the
    /// tables demand is looked up in multiply it by a negative count: their host forms hold
    /// negative counts below a negation or a difference, and inside a loop, whose records hold
    /// each round's changes. So demand and every host form are read through here. The row moves
    /// into the key and back; `distinct` keeps a key whose count is not zero, whatever its sign.
    fn presence(&mut self, input: Ref) -> Ref {
        let packed = self.project(input, projection(tup(vec![var(0), var(1)]), tup(Vec::new()), 2));
        let dist = self.reduce(packed, Reducer::Distinct);
        self.project(dist, projection(f(0, 0), f(0, 1), 2))
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
    fn emit_lookup_lossy(&mut self, dep_y: Ref, side: &Side, out_user_len: usize, proj: &Lambda) -> Ref {
        decouple::lossy_lookup::<Val, Sb>(self, &dep_y, &side.info(), out_user_len, proj)
    }

    /// Join's backward rule: two contribs (left, right).
    fn emit_lookup_join(&mut self, dep_y: Ref, left: &Side, right: &Side, out_user_len: usize, projection: &Lambda) -> (Ref, Ref) {
        decouple::join_lookup::<Val, Sb>(self, &dep_y, &left.info(), &right.info(), out_user_len, projection)
    }

    /// FlatMap's (UNNEST's) backward rule — the universal backstop instantiated
    /// for a cardinality-changing op. FlatMap doesn't touch iteration time and
    /// its list rides as one opaque value, so this needs no envelope change:
    /// build the `(output -> input)` pair table by running the op forward on the
    /// input side, join the demand on the output, recover the whole input (the
    /// `None` endpoint — `RESIDUAL` is the input value).
    ///
    /// A plain flatmap drops the source row and the key isn't unique, so each
    /// exploded element must say which source row it came from. It says so by
    /// the source's key and a hash of the source's value, not by the source row
    /// itself: the row holds the whole list, and carrying it with each of its
    /// elements is quadratic in the list's length. The demanded elements' sources
    /// are then recovered by joining on that `(key, hash)`. Two source rows with
    /// equal hashes are both demanded, which only makes an explanation larger;
    /// no source row can be missed. `list` reads the re-keyed rows: the input's
    /// key is the key's first field, and its value is the row's value.
    ///
    /// The input may sit in an enclosing scope (a flatmap of a parent's
    /// collection inside a loop), so its chain may be shorter than the demand's:
    /// the time filter and the demanded row follow the other lookups
    /// (`time_le`, `strip`), at the input's own chain.
    fn emit_lookup_flatmap(&mut self, dep_y: Ref, side: &Side, out_user_len: usize, list: &Lambda) -> Ref {
        let (in_len, out_len) = (side.user_len, out_user_len);
        let src = self.concat(vec![side.witness.clone(), side.forward.clone()]);
        // Pair table: each source row keyed by (k, hash of its value), exploded, then re-keyed
        // to the output (k, (pos, elem)) carrying (k, hash).
        let hashed = || tup(vec![var(0), format!("{} hash", var(1))]);
        let rekeyed = self.project(src.clone(), projection(hashed(), f(1, 0), 2));
        let exploded = self.linear(rekeyed, vec![LinearOp::FlatMap(list.clone())]);
        let pair = self.project(exploded, projection(tup(vec![f(0, 0), var(1)]), var(0), 2));
        // Re-key the demand by the same output (k, (pos, elem)); keep its chain ++ q.
        let dep_keyed = self.project(dep_y, projection(tup(vec![var(0), f(1, 0)]), tup(fidx(1, 1, out_len + 2)), 2));
        // The demanded elements' sources, as (k, hash) ; chain_out ++ q.
        let sourced = self.join(dep_keyed, pair, projection(tup(vec![f(2, 0), f(2, 1)]), tup(fidx(1, 0, out_len + 1)), 3));
        // The source rows by (k, hash), then the wide row [V_in | chain_in | chain_out | q].
        let by_hash = self.project(src, projection(hashed(), var(1), 2));
        let mut wide = vec![f(2, 0)];                                             // V_in
        wide.extend((0..in_len).map(|c| f(2, 1 + c)));                            // chain_in
        wide.extend(fidx(1, 0, out_len));                                         // chain_out
        wide.push(f(1, out_len));                                                 // q
        let joined = self.join(sourced, by_hash, projection(f(0, 0), tup(wide), 3));
        // Soundness: chain_in ≤ chain_out, on the coordinates they share.
        let filtered = match <Val as RowModel>::time_le(in_len, out_len) { Some(c) => self.filter(joined, c), None => joined };
        // Reform the demanded input (k ; V_in, chain_in, q).
        self.project(filtered, <Val as RowModel>::strip(in_len, out_len, in_len))
    }
}

/// The `Value` data model for explain: a demand row is `(K ; (V, chain…, q))`
/// — the user's key and value whole, then the loop-iteration chain
/// (innermost-first), then the trailing query id, matching the host lift's
/// `append_iter` on `(V)`. Every builder works in field positions of that tuple.
pub(crate) struct Val;

impl Dataflow for Sb {
    type Handle = Ref;
    type Proj = Lambda;
    type Pred = Lambda;
    fn project(&mut self, c: &Ref, p: Lambda) -> Ref { Sb::project(self, c.clone(), p) }
    fn filter(&mut self, c: &Ref, p: Lambda) -> Ref { Sb::filter(self, c.clone(), p) }
    fn join(&mut self, l: &Ref, r: &Ref, p: Lambda) -> Ref { Sb::join(self, l.clone(), r.clone(), p) }
    fn concat(&mut self, cs: Vec<Ref>) -> Ref { Sb::concat(self, cs) }
}

// Lambda text for the Value reverse rules. `v` is the value (left value in joins), `r` the
// right value. Parentheses construct tuples, including the one-field tuple and unit.
fn var(s: usize) -> String { ["k", "v", "r"][s].into() }
fn fld(t: String, i: usize) -> String { format!("{t}.{i}") }
fn f(s: usize, i: usize) -> String { fld(var(s), i) }
fn tup(xs: Vec<String>) -> String { format!("({})", xs.join(", ")) }
fn fidx(s: usize, lo: usize, hi: usize) -> Vec<String> { (lo..hi).map(|c| f(s, c)).collect() }
fn le(a: String, b: String) -> String { format!("({a}, {b}) le") }
fn eq(a: String, b: String) -> String { format!("({a}, {b}) eq") }
fn gt(a: String, b: String) -> String { format!("({a}, {b}) gt") }
fn sub(a: String, b: String) -> String { format!("({a}, {b}) sub") }
fn and_all(conds: Vec<String>) -> Option<String> {
    if conds.is_empty() { None } else { Some(format!("{} all", tup(conds))) }
}

impl RowModel for Val {
    type Proj = Lambda;
    type Pred = Lambda;

    fn pack(chain_len: usize, has_q: bool) -> Lambda {
        let tail = chain_len + has_q as usize;
        projection(tup(vec![var(0), f(1, 0)]), tup(fidx(1, 1, 1 + tail)), 2)
    }
    fn strip_all() -> Lambda {
        projection(var(0), f(1, 0), 2)
    }
    fn sp_reassemble(in_len: usize, out_len: usize) -> Lambda {
        let mut val = vec![f(0, 1)];                  // V (from packed key)
        val.extend(fidx(2, 0, in_len));               // chain_in (pair)
        val.extend(fidx(1, 0, out_len));              // chain_out (dep)
        val.push(f(1, out_len));                      // q
        projection(f(0, 0), tup(val), 3)
    }

    fn ky_join(in_len: usize, out_len: usize, min: bool) -> Lambda {
        let mut val = vec![f(2, 0)];                  // V_in
        if min { val.push(f(1, 0)); }                 // V_out (for narrowing)
        val.extend(fidx(2, 1, 1 + in_len));           // chain_in
        val.extend(fidx(1, 1, 1 + out_len));          // chain_out
        val.push(f(1, 1 + out_len));                  // q
        projection(var(0), tup(val), 3)
    }
    fn ky_data_le() -> Option<Lambda> {
        Some(predicate(le(f(1, 0), f(1, 1))))
    }
    fn blank_v(out_len: usize) -> Lambda {
        let mut val = vec!["()".to_string()];
        val.extend(fidx(1, 1, 2 + out_len));          // chain, q
        projection(var(0), tup(val), 2)
    }
    fn ky_drop_vout(in_len: usize, out_len: usize) -> Lambda {
        let mut val = vec![f(1, 0)];
        val.extend(fidx(1, 2, 3 + in_len + out_len)); // chain_in, chain_out, q
        projection(var(0), tup(val), 2)
    }

    fn lossy_pair(proj: &Lambda, in_len: usize) -> Lambda {
        let mut val = vec![var(0), f(1, 0)];          // K_in, V_in
        val.extend(fidx(1, 1, 1 + in_len));           // chain_in
        slot(2, &[(proj, tup(vec![var(0), var(1)]))], |out| tup(vec![fld(out[0].clone(), 0), tup(val)]))
    }
    fn lossy_reassemble(in_len: usize, out_len: usize) -> Lambda {
        let mut val = vec![f(2, 1)];                  // V_in
        val.extend(fidx(2, 2, 2 + in_len));           // chain_in
        val.extend(fidx(1, 1, 1 + out_len));          // chain_out
        val.push(f(1, 1 + out_len));                  // q
        projection(f(2, 0), tup(val), 3)
    }

    fn join_forward(proj: &Lambda, l_len: usize, r_len: usize) -> Lambda {
        let mut val = vec![var(0), f(1, 0), f(2, 0)]; // K, V_L, V_R
        val.extend(fidx(1, 1, 1 + l_len));            // chain_L
        val.extend(fidx(2, 1, 1 + r_len));            // chain_R
        slot(3, &[(proj, tup(vec![var(0), var(1), var(2)]))], |out| {
            val.push(fld(out[0].clone(), 1));       // V_out (pair-produced)
            tup(vec![fld(out[0].clone(), 0), tup(val)])
        })
    }
    fn join_combined(l_len: usize, r_len: usize, out_len: usize) -> Lambda {
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
        projection(f(2, 0), tup(val), 3)
    }
    fn join_filter(l_len: usize, r_len: usize, out_len: usize) -> Option<Lambda> {
        let ul_j = 2;
        let ur_j = ul_j + l_len;
        let uo_j = ur_j + r_len;
        let q_j = uo_j + out_len;
        let time_cond = |off: usize, in_len: usize| -> Option<String> {
            let n = in_len.min(out_len);
            let in_skip = in_len - n;
            let out_skip = out_len - n;
            and_all((0..n).map(|i| le(f(1, off + in_skip + i), f(1, uo_j + out_skip + i))).collect())
        };
        let value_cond = Some(eq(f(1, q_j + 1), f(1, q_j + 2)));
        and_all([time_cond(ul_j, l_len), time_cond(ur_j, r_len), value_cond]
            .into_iter().flatten().collect()).map(predicate)
    }
    fn join_split(left: bool, l_len: usize, r_len: usize, out_len: usize) -> Lambda {
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
        projection(var(0), tup(val), 2)
    }

    fn time_le(in_len: usize, out_len: usize) -> Option<Lambda> {
        // Joined row value: (V, user_in(in_len), user_out(out_len), q), each
        // chain innermost-first; compare outer-aligned (last min(in,out)).
        let n = in_len.min(out_len);
        let in_off = in_len - n;
        let out_off = out_len - n;
        and_all((0..n).map(|i| le(f(1, 1 + in_off + i), f(1, 1 + in_len + out_off + i))).collect()).map(predicate)
    }
    fn strip(in_len: usize, out_len: usize, keep: usize) -> Lambda {
        let mut val = vec![f(1, 0)];                  // V
        let drop_in = in_len - keep;
        for i in 0..keep { val.push(f(1, 1 + drop_in + i)); }   // outer kept user_in coords
        val.push(f(1, 1 + in_len + out_len));         // q
        projection(var(0), tup(val), 2)
    }

    fn bind_filter() -> Option<Lambda> {
        Some(predicate(gt(f(1, 1), "0".into())))
    }
    fn bind_decrement(user_len: usize) -> Lambda {
        let mut val = vec![f(1, 0)];
        val.push(sub(f(1, 1), "1".into()));           // innermost chain coord - 1
        val.extend(fidx(1, 2, 2 + user_len));         // the rest of the chain, then q
        projection(var(0), tup(val), 2)
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
    // The sources as sets: the rows demand may name, and their host forms (see `Sb::presence`).
    let src_rows: Vec<Ref> = ex_src.iter().map(|r| ex.presence(r.clone())).collect();
    let src_env: Vec<Ref> = src_rows.iter().map(|r| ex.project(r.clone(), envelope())).collect();
    let ex_query = ex.import("$query".into(), Source::Parent(query_ref));
    let wit: BTreeMap<Addr, Ref> = witness.iter()
        .map(|(a, r)| {
            let imported = ex.import(format!("$wit:{:?}:{:?}", a.path, a.site), Source::Parent(r.clone()));
            let host = if a.path.is_empty() { ex.project(imported, envelope()) } else { imported };
            (a.clone(), ex.presence(host))
        })
        .collect();

    // Demand-set variables, one per source, of the user's rows; forward inputs
    // are the actual sources restricted to the demand-sets.
    let dsets: Vec<Ref> = (0..n_sources).map(|k| {
        let v = ex.variable(&format!("demand_set_{}", k));
        // A source no demand reaches binds its set to itself, which says nothing of its shape:
        // the set holds the source's rows, so it takes the source's declared shape.
        if let Ref::Var(i) = v { ex.s.vars[i].shape = p.root.imports[k].shape.clone(); }
        v
    }).collect();
    let fwd_inputs: Vec<Ref> = (0..n_sources).map(|k| ex.restrict(ex_src[k].clone(), dsets[k].clone())).collect();
    let fwd_env: Vec<Ref> = fwd_inputs.iter().map(|r| { let env = ex.project(r.clone(), envelope()); ex.presence(env) }).collect();
    let forward: BTreeMap<Addr, Ref> = clone_into(&p.root, &mut ex.s, &fwd_inputs).into_iter()
        .map(|(a, r)| { let host = if a.path.is_empty() { ex.project(r, envelope()) } else { r }; (a, ex.presence(host)) })
        .collect();
    ex.s.exports.clear();

    // Pre-allocated demand variables for the original feedback vars (forward
    // cycles induce backward cycles that need a Variable to close).
    let mut var_addrs: Vec<Addr> = Vec::new();
    collect_var_addrs(&p.root, &[], &mut var_addrs);
    let mut demand: BTreeMap<Addr, Ref> = BTreeMap::new();
    // A demand variable no query reaches is bound only to demand derived from itself, which says
    // nothing of its shape: it declares the shape of the variable's rows as demand,
    // `(K ; (V, chain…, q))`, with a coordinate per scope enclosing it.
    let (shapes, _) = crate::shapes::infer(p, &|_| None);
    for a in &var_addrs {
        let dv = ex.variable(&format!("demand_var_{:?}_{:?}", a.path, a.site));
        if let (Ref::Var(i), Site::Var(v)) = (&dv, &a.site) {
            if let Some(user) = shapes.at(&a.path).vars[*v].clone() {
                let fields = std::iter::once(user.val).chain(std::iter::repeat_n(corgi::Shape::Int, a.path.len() + 1)).collect();
                ex.s.vars[*i].shape = Some((user.key, corgi::Shape::Prod(fields)));
            }
        }
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
    let seeded = rev.route(&mut ex, ex_query, &[], &target);
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
            let semi = ex.restrict(stripped, src_rows[k].clone());
            ex.concat(vec![dsets[k].clone(), semi])
        };
        let dist = ex.presence(combined);
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

    /// Adapt `contrib`, demand in the scope at `from`, to `target`'s scope: within one scope it
    /// pushes through; otherwise the shape-preserving lookup against the target's host injects or
    /// strips the difference, after [`align`] drops the coordinates the target does not share.
    fn route(&self, ex: &mut Sb, contrib: Ref, from: &[usize], target: &Target) -> Ref {
        if target_path(target) == from {
            contrib
        } else {
            let (contrib, from_len) = align(ex, contrib, from, target_path(target));
            let side = self.side(target);
            ex.emit_lookup_shape_preserving(contrib, &side, from_len)
        }
    }

    fn push(&mut self, ex: &mut Sb, path: &[usize], input: &Ref, contrib: Ref) {
        let target = resolve(self.orig, path, input);
        let routed = self.route(ex, contrib, path, &target);
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
            self.push(ex, path, &b.value, contrib);
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
            let dist = ex.presence(combined);
            ex.debug_inspect(dist.clone(), format!("demand_{:?}:{:?}", path, Site::Var(v)));
            ex.bind(dv, dist);
        }
    }

    fn site(&mut self, ex: &mut Sb, _s: &Scope, path: &[usize], i: usize, node: &Node) {
        let addr = Addr { path: path.to_vec(), site: Site::Op(i) };
        let cs = self.contribs.remove(&Target::Site(addr.clone())).unwrap_or_default();
        if cs.is_empty() { return; }
        let combined = ex.concat(cs);
        let dist = ex.presence(combined);
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
                        let proj = slot(2, &[(proj, tup(vec![var(0), f(1, 0)]))], |out| out[0].clone());
                        let (dep, out_len) = align(ex, dep_this, path, target_path(&target));
                        let contrib = ex.emit_lookup_lossy(dep, &side, out_len, &proj);
                        self.contribs.entry(target).or_default().push(contrib);
                    }
                    LinearOp::Filter(cond) => {
                        let cond = slot(2, &[(cond, tup(vec![var(0), f(1, 0)]))], |out| out[0].clone());
                        let contrib = ex.filter(dep_this, cond);
                        self.push(ex, path, input, contrib);
                    }
                    LinearOp::Negate | LinearOp::EnterAt(_) => {
                        // Negate: pure pass-through. EnterAt: sound but
                        // over-broad pass-through; the routing adapter handles
                        // any depth difference.
                        self.push(ex, path, input, dep_this);
                    }
                    LinearOp::LiftIter => panic!("explain: LiftIter in user program"),
                    // Both read or write the whole time, whose width is the scope depth, and the
                    // forward clone runs one scope deeper than the program it clones.
                    //
                    // Lift's backward rule is trivial, demand `(k, v)` for `(k, (v, t))`; only its
                    // forward clone is the problem, and it would read different times there.
                    //
                    // JoinFunction can follow FlatMap's rule: a pair table that re-keys each input
                    // row by itself and evaluates the lambda as data, not as a delay, so each output
                    // `(key, val)` sits beside its source row, chain, and the lambda's time `t`. The
                    // time width is then just data and the clone's depth does not matter. The time
                    // filter can be exact, `chain_in ∨ t ≤ chain_out`, compared on the chain's
                    // coordinates only; the epoch and coordinates outside the chain are dropped,
                    // which is over-broad and so still sound.
                    LinearOp::Lift => panic!("explain: lift is not supported"),
                    LinearOp::JoinFunction(_) => panic!("explain: join_function is not supported"),
                    LinearOp::FlatMap(list) => {
                        let target = resolve(self.orig, path, input);
                        let side = self.side(&target);
                        let list = slot(2, &[(list, tup(vec![f(0, 0), var(1)]))], |out| out[0].clone());
                        let (dep, out_len) = align(ex, dep_this, path, target_path(&target));
                        let contrib = ex.emit_lookup_flatmap(dep, &side, out_len, &list);
                        self.contribs.entry(target).or_default().push(contrib);
                    }
                }
            }
            Node::Concat(refs) => {
                for r in refs {
                    let target = resolve(self.orig, path, r);
                    let side = self.side(&target);
                    let (dep, out_len) = align(ex, dep_this.clone(), path, target_path(&target));
                    let contrib = ex.emit_lookup_shape_preserving(dep, &side, out_len);
                    self.contribs.entry(target).or_default().push(contrib);
                }
            }
            Node::Arrange(input) | Node::Inspect { input, .. } => {
                self.push(ex, path, input, dep_this);
            }
            Node::Reduce { input, reducer } => {
                let target = resolve(self.orig, path, input);
                let side = self.side(&target);
                let (dep, out_len) = align(ex, dep_this, path, target_path(&target));
                let contrib = ex.emit_lookup_keyed(dep, &side, out_len, reducer);
                self.contribs.entry(target).or_default().push(contrib);
            }
            // It reads its stream's whole times, as `lift` does.
            Node::HalfJoin { .. } => panic!("explain: half_join is not supported"),
            Node::Join { left, right, projection } => {
                let lt = resolve(self.orig, path, left);
                let rt = resolve(self.orig, path, right);
                let ls = self.side(&lt);
                let rs = self.side(&rt);
                let projection = slot(3, &[(projection, tup(vec![var(0), f(1, 0), f(2, 0)]))], |out| out[0].clone());
                // Keep the coordinates of the scopes both sides share with this one: dropping more
                // than one side needs only widens its demand, which is still sound.
                let shared = shared_len(path, target_path(&lt)).min(shared_len(path, target_path(&rt)));
                let (dep, out_len) = outer(ex, dep_this, path, shared);
                let (lc, rc) = ex.emit_lookup_join(dep, &ls, &rs, out_len, &projection);
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
