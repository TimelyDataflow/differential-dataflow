//! Data-model-agnostic surface for explain's reverse-tracing.
//!
//! The reverse rules are written once, over two traits:
//!
//! * [`RowModel`] builds the projections/predicates each rule needs;
//!   [`crate::explain::Val`] is the model the crate uses.
//! * [`Dataflow`] is the orchestration backend (concat/project/filter/join) —
//!   the scope builder `Sb` for real explain.
//!
//! ## The demand envelope
//!
//! Every method speaks in terms of one object — a **demand row**, written
//!
//! ```text
//!     (K ; V, chain, [q])
//! ```
//!
//! where `;` separates the *key* from the *value*. The key `K` is the user's
//! key, whole. The value is a tuple of three logical parts, in order:
//!
//! * **`V`** — the user's value, whole, as one field. The demanded output's
//!   value on the `dep` side, the candidate input's value on the pair-table
//!   side.
//! * **`chain`** — the loop-iteration coordinates, innermost-first, length =
//!   the node's scope depth. Time lives in data here.
//! * **`q`** — a single trailing query id, present on `dep` rows (the thing
//!   being explained), absent on pair-table rows.
//!
//! Neither `K` nor `V` is taken apart, so no rule depends on their shapes.
//!
//! ## Reading the method contracts
//!
//! Each method returns a projection/predicate that the rule applies to a row of
//! a stated shape. Inputs are addressed by **slot**: after a binary `join`, the
//! row exposes `$0` = the join key, `$1` = the left input's value, `$2` = the
//! right input's value; before a join, `$0`/`$1` are key/value. A method's doc
//! gives the *input row shape it expects* and the *output row shape it
//! produces*, both in the `(K ; …)` notation above; an implementer needs only
//! those shapes, not the positions. Sizes arrive as parameters: `*_len`/`*_depth`
//! chain lengths, `min` toggling Min's value-narrowing. Predicate methods return
//! `None` for "no constraint" (the rule then skips the filter).

/// Builds the projections/predicates the reverse rules need, in terms of the
/// demand envelope `(K ; V, chain, [q])` documented at the module level.
pub trait RowModel {
    type Proj;
    type Pred;

    // --- packing data into the join key ---

    /// Move the data `(K, V)` into the join key, keeping the row's `chain` (and
    /// `q` when `has_q`): `(K ; V, chain, [q]) -> ((K, V) ; chain, [q])`. Pair
    /// rows pass `has_q = false`, `dep` rows `has_q = true`.
    fn pack(chain_len: usize, has_q: bool) -> Self::Proj;
    /// Drop a row's chain and `q`, leaving the user's row: `(K ; V, chain, [q]) -> (K ; V)`.
    fn strip_all() -> Self::Proj;

    // --- shape-preserving lookup (Concat/Leave) ---

    /// Reassemble after the SP join on `(K, V)`. Input: the joined row
    /// `[$0 = (K, V), $1 = chain_out ++ q (dep), $2 = chain_in (pair)]`.
    /// Output: `(K ; V, chain_in, chain_out, q)`.
    fn sp_reassemble(in_len: usize, out_len: usize) -> Self::Proj;

    // --- keyed lookup (Reduce) ---

    /// Project the keyed (join-on-`K`) row. Input:
    /// `[$0 = K, $1 = V_out ++ chain_out ++ q (dep), $2 = V_in ++ chain_in (pair)]`.
    /// Output: `(K ; V_in, [V_out,] chain_in, chain_out, q)` — `V_out` is
    /// carried only when `min`, for the narrowing below.
    fn ky_join(in_len: usize, out_len: usize, min: bool) -> Self::Proj;
    /// Min value-narrowing on the `min` row above: keep rows with `V_in == V_out`.
    fn ky_data_eq() -> Option<Self::Pred>;
    /// Drop the carried `V_out`, restoring the SP-shaped row
    /// `(K ; V_in, chain_in, chain_out, q)`.
    fn ky_drop_vout(in_len: usize, out_len: usize) -> Self::Proj;

    // --- lossy lookup (Linear[Project]) ---

    /// Pair re-key: apply the user `proj.key` to a pair row to compute
    /// `K_out`, carrying the input data through.
    /// `(K_in ; V_in, chain_in) -> (K_out ; K_in, V_in, chain_in)`. `proj` reads
    /// the pair row's slots, so the caller has rebuilt any read of the user's row.
    fn lossy_pair(proj: &Self::Proj, in_len: usize) -> Self::Proj;
    /// Reassemble after the join on `K_out`. Input:
    /// `[$0 = K_out, $1 = V_out ++ chain_out ++ q (dep), $2 = K_in ++ V_in ++ chain_in (pair)]`.
    /// Output: `(K_in ; V_in, chain_in, chain_out, q)`.
    fn lossy_reassemble(in_len: usize, out_len: usize) -> Self::Proj;

    // --- join lookup (two inputs; #758 filters both times before splitting) ---

    /// Forward-join the two pair tables on `K`, applying the user `proj`. Input:
    /// `[$0 = K, $1 = V_L ++ chain_L (left), $2 = V_R ++ chain_R (right)]`.
    /// Output: `(K_out ; K, V_L, V_R, chain_L, chain_R, V_out)`, where
    /// `K_out`/`V_out` are `proj.key`/`proj.val` over `(K, V_L, V_R)`, reading the
    /// joined row's slots as for `lossy_pair`.
    fn join_forward(proj: &Self::Proj, l_len: usize, r_len: usize) -> Self::Proj;
    /// Reassemble after joining `dep` against the forward-join pair on `K_out`,
    /// into one wide row carrying everything both sides' filters and splits need.
    /// Input: `[$0 = K_out, $1 = V_out ++ chain_out ++ q (dep), $2 = pair row above]`.
    /// Output: `(K ; V_L, V_R, chain_L, chain_R, chain_out, q, V_out_dep, V_out_pair)`.
    fn join_combined(l_len: usize, r_len: usize, out_len: usize) -> Self::Proj;
    /// Narrowing on the wide row: `chain_L ≤ chain_out` ∧ `chain_R ≤ chain_out`
    /// (both outer-aligned) ∧ `V_out_dep == V_out_pair`. Both sides' times are
    /// filtered here, before either side's chain is projected away (#758).
    fn join_filter(l_len: usize, r_len: usize, out_len: usize) -> Option<Self::Pred>;
    /// Project one side's contrib out of the filtered wide row. `left` selects
    /// `(K ; V_L, chain_L, q)` vs `(K ; V_R, chain_R, q)`.
    fn join_split(left: bool, l_len: usize, r_len: usize, out_len: usize) -> Self::Proj;

    // --- the chain algebra, shared by SP / keyed / lossy ---

    /// Soundness filter on a `(K ; V, chain_in, chain_out, q)` row: keep rows
    /// with `chain_in ≤ chain_out`, compared outer-aligned (an input can only
    /// explain an output that came no earlier). `None` when the chains share no
    /// coordinate.
    fn time_le(in_len: usize, out_len: usize) -> Option<Self::Pred>;
    /// Strip `chain_out` and the inner `chain_in` coords past `keep` from a
    /// `(K ; V, chain_in, chain_out, q)` row, leaving `(K ; V, chain_in[..keep], q)`.
    /// Kept coords are the *outer* ones, matching `time_le`'s alignment.
    fn strip(in_len: usize, out_len: usize, keep: usize) -> Self::Proj;

    // --- bind iter-decrement (inverts a loop variable's feedback) ---

    /// Keep only rows whose innermost chain coordinate is `> 0` (iteration-0
    /// demand has no body-side source).
    fn bind_filter() -> Option<Self::Pred>;
    /// Decrement the innermost chain coordinate by one, leaving everything else
    /// intact: `(K ; V, chain, q) -> (K ; V, chain with chain[0]-1, q)`.
    /// `user_len` is the chain length.
    fn bind_decrement(user_len: usize) -> Self::Proj;
}

/// Orchestration backend: collection handles and the four primitive ops.
pub trait Dataflow {
    type Handle: Clone;
    type Proj;
    type Pred;
    fn project(&mut self, c: &Self::Handle, p: Self::Proj) -> Self::Handle;
    fn filter(&mut self, c: &Self::Handle, p: Self::Pred) -> Self::Handle;
    fn join(&mut self, l: &Self::Handle, r: &Self::Handle, p: Self::Proj) -> Self::Handle;
    fn concat(&mut self, cs: Vec<Self::Handle>) -> Self::Handle;
}

fn opt_filter<D: Dataflow>(df: &mut D, c: D::Handle, p: Option<D::Pred>) -> D::Handle {
    match p { Some(p) => df.filter(&c, p), None => c }
}

/// One side's inputs to a reverse rule: its pair-table halves and chain length.
pub struct SideInfo<D: Dataflow> {
    pub witness: D::Handle,
    pub forward: D::Handle,
    pub user_len: usize,
}

/// Shape-preserving lookup (Concat/Leave). `keep == in_len` here.
pub fn shape_preserving_lookup<M, D>(df: &mut D, dep: &D::Handle, side: &SideInfo<D>, out_depth: usize) -> D::Handle
where M: RowModel, D: Dataflow<Proj = M::Proj, Pred = M::Pred> {
    let in_len = side.user_len;
    let pair = df.concat(vec![side.witness.clone(), side.forward.clone()]);
    let pair_keyed = df.project(&pair, M::pack(in_len, false));
    let dep_keyed = df.project(dep, M::pack(out_depth, true));
    let joined = df.join(&dep_keyed, &pair_keyed, M::sp_reassemble(in_len, out_depth));
    let filtered = opt_filter(df, joined, M::time_le(in_len, out_depth));
    df.project(&filtered, M::strip(in_len, out_depth, in_len))
}

/// Keyed (Reduce) lookup; `min` enables value-narrowing.
pub fn keyed_lookup<M, D>(df: &mut D, dep: &D::Handle, side: &SideInfo<D>, out_len: usize, min: bool) -> D::Handle
where M: RowModel, D: Dataflow<Proj = M::Proj, Pred = M::Pred> {
    let in_len = side.user_len;
    let pair = df.concat(vec![side.witness.clone(), side.forward.clone()]);
    let joined = df.join(dep, &pair, M::ky_join(in_len, out_len, min));
    let after = if min {
        let f = opt_filter(df, joined, M::ky_data_eq());
        df.project(&f, M::ky_drop_vout(in_len, out_len))
    } else {
        joined
    };
    let filtered = opt_filter(df, after, M::time_le(in_len, out_len));
    df.project(&filtered, M::strip(in_len, out_len, in_len))
}

/// Lossy (Project) lookup, through the pair table.
pub fn lossy_lookup<M, D>(df: &mut D, dep: &D::Handle, side: &SideInfo<D>, out_len: usize, proj: &M::Proj) -> D::Handle
where M: RowModel, D: Dataflow<Proj = M::Proj, Pred = M::Pred> {
    let in_len = side.user_len;
    let pair = df.concat(vec![side.witness.clone(), side.forward.clone()]);
    let pair_keyed = df.project(&pair, M::lossy_pair(proj, in_len));
    let joined = df.join(dep, &pair_keyed, M::lossy_reassemble(in_len, out_len));
    let filtered = opt_filter(df, joined, M::time_le(in_len, out_len));
    df.project(&filtered, M::strip(in_len, out_len, in_len))
}

/// Join lookup (#758): forward-join pair tables, one combined dep×pair join
/// carrying both chains, one combined time+value filter, then split per side.
pub fn join_lookup<M, D>(df: &mut D, dep: &D::Handle, left: &SideInfo<D>, right: &SideInfo<D>, out_len: usize, proj: &M::Proj) -> (D::Handle, D::Handle)
where M: RowModel, D: Dataflow<Proj = M::Proj, Pred = M::Pred> {
    let l_len = left.user_len;
    let r_len = right.user_len;
    let lp = df.concat(vec![left.witness.clone(), left.forward.clone()]);
    let rp = df.concat(vec![right.witness.clone(), right.forward.clone()]);
    let pair = df.join(&lp, &rp, M::join_forward(proj, l_len, r_len));
    let joined = df.join(dep, &pair, M::join_combined(l_len, r_len, out_len));
    let timed = opt_filter(df, joined, M::join_filter(l_len, r_len, out_len));
    let lc = df.project(&timed, M::join_split(true, l_len, r_len, out_len));
    let rc = df.project(&timed, M::join_split(false, l_len, r_len, out_len));
    (lc, rc)
}

/// In-memory [`Dataflow`] over `Vec<(Value, Value)>`, for the contract below:
/// projections and predicates run through the `Term` interpreter (`ir::eval`),
/// and `join` is a nested-loop equi-join on the key.
#[cfg(test)]
mod mem {
    use super::Dataflow;
    use crate::ir::{eval, Projection, Term, Value};

    pub type Coll = Vec<(Value, Value)>;
    pub struct Mem;

    impl Dataflow for Mem {
        type Handle = Coll;
        type Proj = Projection;
        type Pred = Term;
        fn project(&mut self, c: &Coll, p: Projection) -> Coll {
            c.iter().map(|(k, v)| {
                let mut e = vec![k.clone(), v.clone()];
                (eval(&p.key, &mut e), eval(&p.val, &mut e))
            }).collect()
        }
        fn filter(&mut self, c: &Coll, p: Term) -> Coll {
            c.iter().filter(|(k, v)| {
                let mut e = vec![k.clone(), v.clone()];
                eval(&p, &mut e).truthy()
            }).cloned().collect()
        }
        fn join(&mut self, l: &Coll, r: &Coll, p: Projection) -> Coll {
            let mut out = Vec::new();
            for (lk, lv) in l {
                for (rk, rv) in r {
                    if lk == rk {
                        let mut e = vec![lk.clone(), lv.clone(), rv.clone()];
                        out.push((eval(&p.key, &mut e), eval(&p.val, &mut e)));
                    }
                }
            }
            out
        }
        fn concat(&mut self, cs: Vec<Coll>) -> Coll { cs.into_iter().flatten().collect() }
    }
}

#[cfg(test)]
mod value_contract {
    //! Executable contract for the reverse rules: by-example specs on `Value`
    //! rows in `Val`'s envelope `(K ; V, chain, [q])`, run through the in-memory
    //! dataflow against `crate::explain::Val`.

    use super::*;
    use crate::explain::Val;
    use super::mem::{Coll, Mem};
    use crate::ir::Value;
    use crate::ir::{Projection, Term};

    type Row = Value;

    /// A one-field tuple `(n)`: the keys and values below.
    fn one(n: i64) -> Row { Value::Tuple(vec![Value::Int(n)]) }
    /// An envelope value: `V`, then the chain and `q` (or the chain alone).
    fn env(v: i64, rest: &[i64]) -> Row {
        let mut fields = vec![one(v)];
        fields.extend(rest.iter().map(|&n| Value::Int(n)));
        Value::Tuple(fields)
    }
    fn side(witness: Coll, user_len: usize) -> SideInfo<Mem> {
        SideInfo { witness, forward: vec![], user_len }
    }
    fn sorted(mut c: Coll) -> Coll { c.sort(); c }
    fn proj(k: Term, v: Term) -> Projection { Projection { key: k, val: v } }
    /// Slot `n`'s user value: the whole key for `0`, else the envelope's first field.
    fn user(n: usize) -> Term {
        if n == 0 { Term::Var(0) } else { Term::Proj(Box::new(Term::Var(n)), 0) }
    }

    #[test]
    fn sp_depth0_keeps_only_value_matching_pair() {
        // Packing (K,V) into the join key means only the V=7 pair can match.
        let pair = vec![(one(5), env(7, &[])), (one(5), env(8, &[]))];
        let dep = vec![(one(5), env(7, &[9]))];             // (K=5 ; V=7, q=9)
        let out = shape_preserving_lookup::<Val, _>(&mut Mem, &dep, &side(pair, 0), 0);
        assert_eq!(out, vec![(one(5), env(7, &[9]))]);
    }

    #[test]
    fn sp_depth1_time_filters_late_inputs() {
        // chain length 1: an input at iter 2 explains an output demanded at
        // iter 3 (kept); one at iter 4 does not (dropped).
        let keep = vec![(one(5), env(7, &[2]))];            // chain_in = 2
        let drop = vec![(one(5), env(7, &[4]))];            // chain_in = 4
        let dep = vec![(one(5), env(7, &[3, 9]))];          // chain_out = 3, q = 9
        let kept = shape_preserving_lookup::<Val, _>(&mut Mem, &dep, &side(keep, 1), 1);
        assert_eq!(kept, vec![(one(5), env(7, &[2, 9]))]);
        let dropped = shape_preserving_lookup::<Val, _>(&mut Mem, &dep, &side(drop, 1), 1);
        assert!(dropped.is_empty());
    }

    #[test]
    fn keyed_min_narrows_to_the_demanded_value() {
        let pair = vec![(one(5), env(7, &[])), (one(5), env(6, &[]))];
        let dep = vec![(one(5), env(7, &[9]))];
        let out = keyed_lookup::<Val, _>(&mut Mem, &dep, &side(pair, 0), 0, true);
        assert_eq!(out, vec![(one(5), env(7, &[9]))]);
    }

    #[test]
    fn keyed_nonmin_demands_all_same_key_inputs() {
        let pair = vec![(one(5), env(7, &[])), (one(5), env(6, &[]))];
        let dep = vec![(one(5), env(1, &[9]))];             // V_out unrelated (a count)
        let out = keyed_lookup::<Val, _>(&mut Mem, &dep, &side(pair, 0), 0, false);
        assert_eq!(sorted(out), vec![(one(5), env(6, &[9])), (one(5), env(7, &[9]))]);
    }

    #[test]
    fn lossy_via_fallback_recovers_input() {
        // proj: K_out = V_in, V_out = K_in.
        let p = proj(user(1), user(0));
        let pair = vec![(one(3), env(8, &[]))];             // (K_in=3 ; V_in=8)
        let dep = vec![(one(8), env(3, &[9]))];             // (K_out=8 ; V_out=3, q=9)
        let out = lossy_lookup::<Val, _>(&mut Mem, &dep, &side(pair, 0), 0, &p);
        assert_eq!(out, vec![(one(3), env(8, &[9]))]);
    }

    #[test]
    fn join_demands_both_inputs() {
        // proj: K_out = K, V_out = V_L.
        let p = proj(user(0), user(1));
        let left = side(vec![(one(5), env(7, &[]))], 0);
        let right = side(vec![(one(5), env(9, &[]))], 0);
        let dep = vec![(one(5), env(7, &[1]))];             // (K_out=5 ; V_out=7, q=1)
        let (lc, rc) = join_lookup::<Val, _>(&mut Mem, &dep, &left, &right, 0, &p);
        assert_eq!(lc, vec![(one(5), env(7, &[1]))]);
        assert_eq!(rc, vec![(one(5), env(9, &[1]))]);
    }
}
