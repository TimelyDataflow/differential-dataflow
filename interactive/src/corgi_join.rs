//! Phase 2 / M2(b)-4 — the cursor-less corgi JOIN tactic (Route B; reduce `retire` is Frank's).
//!
//! `join_with_tactic` needs only `TraceReader` (not `Navigable`), so a cursor-less `CorgiBatch`
//! arrangement drives a custom `JoinTactic`: `defer` queues bilinear units (fresh batch list ×
//! accumulated other side, keyed by `Fresh`); `work` (NEXT ITERATION) merges each side's list to one
//! run, merge-joins by key over corgi columns (`compare_at`), cross-products matched val runs with
//! lattice time-join + diff-multiply, runs the projection corgi program, and emits via the session.
//!
//! THIS ITERATION: struct + `defer` + a stub `work`, compiling as a valid `JoinTactic` (validates the
//! generic-bound wiring). The join COMPUTE is already validated in `examples/corgi_join_mechanism.rs`.

use std::rc::Rc;


use differential_dataflow::operators::join::{Fresh, JoinTactic};

use corgi::arrange::{find_ranges, gather};
use corgi::Value as CValue;

use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};

use crate::corgi_backend::CorgiContainer;
use crate::col_times::ColTime;
use crate::corgi_chunk::{flatten_batches, flatten_restricted, CorgiChunk};
use crate::corgi_logic::compile_join_projection;
use crate::ir::Diff;
use crate::parse::Term;
type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Cursor-less corgi join tactic. Holds the projection `Term`s (`Var(0)=key`, `Var(1)=val0`,
/// `Var(2)=val1`); compiled to a corgi graph per work-unit using the matched columns' shapes (so
/// `Spread` resolves against the actual key/val arities).
pub struct CorgiJoinTactic<T: ColTime> {
    key: Term,
    val: Term,
    _t: std::marker::PhantomData<T>,
}

impl<T: ColTime> CorgiJoinTactic<T> {
    pub fn new(key: Term, val: Term) -> Self {
        CorgiJoinTactic { key, val, _t: std::marker::PhantomData }
    }
}

impl<T> JoinTactic<CBatch<T>, CBatch<T>, CorgiContainer<T, Diff>> for CorgiJoinTactic<T>
where
    T: ColTime,
{
    /// One bilinear unit → its output container. The operator (`join_with_tactic`) drives capabilities
    /// and fuel by draining the returned iterator; `meet` (compaction) is ignored (correctness-first,
    /// like the reference identity backend — output times are just less compact, never wrong).
    fn prep(&mut self, input0: Vec<CBatch<T>>, input1: Vec<CBatch<T>>, fresh: Fresh, _meet: T) -> Box<dyn Iterator<Item = CorgiContainer<T, Diff>>> {
        Box::new(run_unit(input0, input1, fresh, &self.key, &self.val).into_iter())
    }
}

/// Join one deferred bilinear unit: merge-join the two runs by key over corgi columns, cross-product
/// matched val runs (lattice time-join + diff-multiply), project via corgi, emit at the capability.
fn run_unit<T>(left_b: Vec<CBatch<T>>, right_b: Vec<CBatch<T>>, fresh: Fresh, key: &Term, val: &Term) -> Option<CorgiContainer<T, Diff>>
where
    T: ColTime,
{
    // Materialize the FRESH (delta-sized) side whole; then present the ACCUMULATED side. When it is
    // meaningfully larger than the fresh delta (recursive joins against a built-up trace), probe it
    // with the fresh keys and gather only matches (delta-proportional, not O(trace)); otherwise
    // (non-recursive / comparable sizes) a plain flatten is cheaper than the probe + reconsolidate.
    // Roles (left/right) are preserved for the projection.
    let accumulated = |acc: &[CBatch<T>], fresh: &crate::corgi_chunk::SortedRun<T, Diff>| {
        let acc_len: usize = acc.iter().flat_map(|b| b.chunks.iter()).map(|c| c.len()).sum();
        if acc_len > 2 * fresh.times.len() {
            flatten_restricted(acc, &fresh.keys)
        } else {
            flatten_batches(acc)
        }
    };
    let (left, right) = match fresh {
        Fresh::Input0 => {
            let left = flatten_batches(&left_b)?;
            let right = accumulated(&right_b, &left)?;
            (left, right)
        }
        Fresh::Input1 => {
            let right = flatten_batches(&right_b)?;
            let left = accumulated(&left_b, &right)?;
            (left, right)
        }
    };
    let nl = left.times.len();

    // Multi-record merge-join: one batched `find` gives, per left row, the equal-range of its key in
    // the sorted right keys. Cross-product each left row with its matched right rows (lattice
    // time-join + diff-multiply). Replaces the per-pair `compare_at` scan.
    let (lo, hi) = find_ranges(&left.keys, &right.keys);
    let (mut li, mut ri): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
    let (mut ot, mut od): (Vec<T>, Vec<Diff>) = (Vec::new(), Vec::new());
    for a in 0..nl {
        for b in lo[a]..hi[a] {
            li.push(a);
            ri.push(b);
            ot.push(left.times[a].join(&right.times[b]));
            od.push(left.diffs[a] * right.diffs[b]);
        }
    }
    if li.is_empty() {
        return None;
    }

    // gather matched (key, val0, val1) columns, run the projection corgi program, untranscode.
    let kc = gather(&left.keys, &li);
    let v0 = gather(&left.vals, &li);
    let v1 = gather(&right.vals, &ri);
    // Compile the projection against the matched columns' shapes (so `Spread` resolves correctly).
    let proj = compile_join_projection(key, val, &corgi::shape_of_value(&kc), &corgi::shape_of_value(&v0), &corgi::shape_of_value(&v1));
    let projected = corgi::eval_graph(&proj, CValue::Prod(vec![kc, v0, v1]));
    let mut cols = projected.into_prod("corgi join projection");
    let nv = cols.pop().unwrap();
    let nk = cols.pop().unwrap();
    // The projection's corgi columns directly as a column-native CorgiContainer (no untranscode).
    Some(CorgiContainer { keys: nk, vals: nv, times: ot, diffs: od })
}
