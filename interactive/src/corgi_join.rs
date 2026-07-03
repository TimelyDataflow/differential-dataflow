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

use std::collections::VecDeque;
use std::rc::Rc;

use timely::ContainerBuilder;
use timely::dataflow::operators::generic::OutputBuilderSession;
use timely::dataflow::operators::Capability;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::join::{EffortBuilder, Fresh, JoinTactic};

use corgi::arrange::{find_ranges, gather};
use corgi::Value as CValue;

use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};

use crate::corgi_backend::CorgiContainer;
use crate::corgi_chunk::{flatten_batches, flatten_restricted, CorgiChunk};
use crate::corgi_logic::compile_join_projection;
use crate::ir::Diff;
use crate::parse::Term;
type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// A deferred bilinear join unit: a fresh batch list joined against the accumulated other side.
/// (The accumulated-side advance-by-meet — an output-neutral consolidation optimization — is deferred;
/// `merge_one` consolidates each side without advancing, which is correct.)
struct CorgiDeferred<T: Timestamp + Lattice> {
    left: Vec<CBatch<T>>,
    right: Vec<CBatch<T>>,
    /// Which side carried the fresh batch; the other (accumulated) side is restricted to the fresh
    /// side's keys rather than fully flattened.
    fresh: Fresh,
    capability: Capability<T>,
}

/// Cursor-less corgi join tactic. Holds the projection `Term`s (`Var(0)=key`, `Var(1)=val0`,
/// `Var(2)=val1`); compiled to a corgi graph per work-unit using the matched columns' shapes (so
/// `Spread` resolves against the actual key/val arities).
pub struct CorgiJoinTactic<T: Timestamp + Lattice> {
    key: Term,
    val: Term,
    todo0: VecDeque<CorgiDeferred<T>>, // fresh on input0
    todo1: VecDeque<CorgiDeferred<T>>, // fresh on input1
}

impl<T: Timestamp + Lattice> CorgiJoinTactic<T> {
    pub fn new(key: Term, val: Term) -> Self {
        CorgiJoinTactic { key, val, todo0: VecDeque::new(), todo1: VecDeque::new() }
    }
}

impl<T, CB> JoinTactic<CBatch<T>, CBatch<T>, CB> for CorgiJoinTactic<T>
where
    T: Timestamp + Lattice,
    CB: ContainerBuilder<Container = CorgiContainer<T, Diff>>,
{
    fn defer(&mut self, input0: Vec<CBatch<T>>, input1: Vec<CBatch<T>>, fresh: Fresh, capability: Capability<T>) {
        let queue = match &fresh {
            Fresh::Input0 => &mut self.todo0,
            Fresh::Input1 => &mut self.todo1,
        };
        queue.push_back(CorgiDeferred { left: input0, right: input1, fresh, capability });
    }

    fn work(&mut self, fuel: &mut isize, output: &mut OutputBuilderSession<T, EffortBuilder<CB>>) {
        // Eager (correctness-first): run every queued unit fully. Fuel budgeting is a later
        // refinement; leaving `fuel` non-negative signals "complete" to the operator.
        while let Some(unit) = self.todo0.pop_front() {
            run_unit(unit, &self.key, &self.val, output);
        }
        while let Some(unit) = self.todo1.pop_front() {
            run_unit(unit, &self.key, &self.val, output);
        }
        *fuel = 0;
    }
}

/// Join one deferred bilinear unit: merge-join the two runs by key over corgi columns, cross-product
/// matched val runs (lattice time-join + diff-multiply), project via corgi, emit at the capability.
fn run_unit<T, CB>(unit: CorgiDeferred<T>, key: &Term, val: &Term, output: &mut OutputBuilderSession<T, EffortBuilder<CB>>)
where
    T: Timestamp + Lattice,
    CB: ContainerBuilder<Container = CorgiContainer<T, Diff>>,
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
    let (left, right) = match unit.fresh {
        Fresh::Input0 => {
            let Some(left) = flatten_batches(&unit.left) else { return };
            let Some(right) = accumulated(&unit.right, &left) else { return };
            (left, right)
        }
        Fresh::Input1 => {
            let Some(right) = flatten_batches(&unit.right) else { return };
            let Some(left) = accumulated(&unit.left, &right) else { return };
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
        return;
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
    // Emit the projection's corgi columns directly as a CorgiContainer via `give_container` — no
    // untranscode-to-rows + re-transcode. The output stream is column-native.
    let mut container = CorgiContainer { keys: nk, vals: nv, times: ot, diffs: od };
    output.session_with_builder(&unit.capability).give_container(&mut container);
}
