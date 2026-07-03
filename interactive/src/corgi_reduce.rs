//! Phase 2 / M2(b) — the cursor-less corgi REDUCE tactic (Route B), built on Frank's `active_times`.
//!
//! `reduce_with_tactic` needs only `TraceReader` (not `Navigable`), so a cursor-less `CorgiBatch`
//! arrangement drives a custom `ReduceTactic`. The incremental skeleton mirrors DD's `CursorTactic`/
//! `HistoryReplayer` but swaps the lazy interesting-time derivation for the explicit (over-deriving)
//! `active_times`, and the cursor reads for corgi-column reads:
//!
//!   per changed key (input keys ∪ carried pending keys):
//!     read input history (source+input batches) & output history (output batches) to rows
//!     novel = input times in [lower,upper) ∪ carried pending times;  prior = input times < lower
//!     active_times(upper, prior, novel) -> active (∈[lower,upper)) + pended (≥upper)
//!     for t in active (increasing): desired = reduce(Σ input ≤ t); cur = Σ (output ≤ t ⊎ emitted ≤ t)
//!       emit (desired − cur) at t, into the held bucket = largest held time ≤ t
//!     carry pended → next pending
//!   build one output batch per held time (tile [lower,upper)); held frontier = pending times
//!
//! The per-key reducer runs in RUST (the DDIR `Reducer`s, matching `backend::vec`) — at each
//! interesting time it sees a tiny per-key value slice, where corgi's columnar win doesn't apply
//! (corgi pays on wide columns); corgi's role in reduce is the columnar *storage* + merge/advance.

use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::Builder as _;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;

use corgi::{eval_graph, parse_ml, Bounds, Value as CValue};

use crate::corgi_chunk::{batch_to_rows, CorgiChunk, CorgiChunkBuilder};
use crate::ir::{Diff, Value as DValue};
use crate::parse::Reducer;

type Row = DValue;

/// Columnar per-key sum — the monoid `Count`/consolidate fold as the value building block for a
/// column-native reduce, driven entirely by corgi ops (`group → map(fold_add) → filter`), touching
/// no `Value` rows. Diffs are passed as RAW two's-complement `u64` bits so `fold_add`'s wrapping add
/// is the correct `i64` sum and `ne 0` is the sign-agnostic zero-drop (per the corgi note). Returns
/// the surviving `(key column, per-key raw-bit sums)`. (Groundwork: the wave loop will call this
/// across many keys at once; not yet wired into the live reduce.)
#[allow(dead_code)]
fn columnar_sum_by_key(keys: CValue, diffs_raw: CValue) -> (CValue, Vec<u64>) {
    const ML: &str = "let acc = input group map ((k, vs) -> (k, vs fold_add)) in \
                      let keep = acc map ((k, s) -> s ne 0) in \
                      (acc, keep) filter";
    let n = keys.len();
    if n == 0 {
        return (CValue::Unit(0), Vec::new());
    }
    let input = CValue::List(Bounds::Offsets(vec![n]), Box::new(CValue::Prod(vec![keys, diffs_raw])));
    let graph = parse_ml(ML).expect("parse consolidate ML");
    let out = eval_graph(&graph, input);
    let (_bounds, vals) = out.into_list("consolidate output");
    let (keys_out, sums) = vals.into_pair("consolidate (key, sum)");
    (keys_out, sums.into_u64("consolidate sums"))
}

/// Columnar `Distinct`: the keys with at least one **present** value, where "present" = net diff ≠ 0
/// (the nonzero simplification — ignores the signed sign of the accumulation for now). Two-level
/// consolidate over `(key, val)`: group by the `(key,val)` composite, `fold_add` the raw diffs, keep
/// the nonzero, then take the distinct keys of the survivors. Returns the surviving key column.
#[allow(dead_code)]
fn columnar_distinct_keys(keys: CValue, vals: CValue, diffs_raw: CValue) -> CValue {
    // input : List<((key,val), diff)> — group by the (key,val) composite, sum diffs, drop net-zero,
    // re-key to (key, _), group by key → distinct present keys.
    const ML: &str = "let net = input group map ((kv, ds) -> (kv, ds fold_add)) in \
                      let keep = net map ((kv, s) -> s ne 0) in \
                      let present = (net, keep) filter in \
                      let rekey = present map ((kv, s) -> (kv.0, s)) in \
                      rekey group map ((k, ss) -> k)";
    let n = keys.len();
    if n == 0 {
        return CValue::Unit(0);
    }
    let composite = CValue::Prod(vec![keys, vals]);
    let input = CValue::List(Bounds::Offsets(vec![n]), Box::new(CValue::Prod(vec![composite, diffs_raw])));
    let out = eval_graph(&parse_ml(ML).expect("parse distinct ML"), input);
    // out : List<key> — one row of the surviving keys.
    let (_bounds, keys_out) = out.into_list("distinct output");
    keys_out
}

#[cfg(test)]
mod consolidate_test {
    use super::*;

    #[test]
    fn columnar_distinct_keeps_present_keys() {
        // (1,10):+1,-1 → net 0 (absent); (2,20):+1, (2,21):+1 (present); (3,30):+1 (present).
        let keys = CValue::u64(vec![1, 1, 2, 2, 3]);
        let vals = CValue::u64(vec![10, 10, 20, 21, 30]);
        let diffs = CValue::u64(vec![1u64, (-1i64) as u64, 1, 1, 1]);
        let present = columnar_distinct_keys(keys, vals, diffs);
        assert_eq!(present.into_u64("present"), vec![2, 3]);
    }

    #[test]
    fn columnar_consolidate_sums_and_drops_zeros() {
        // keys 1,1,2,3,3 with raw-bit diffs +1,-1,5,9,1 → key 1 nets 0 (dropped), 2→5, 3→10.
        let keys = CValue::u64(vec![1, 1, 2, 3, 3]);
        let diffs = CValue::u64(vec![1u64, (-1i64) as u64, 5, 9, 1]);
        let (kout, sums) = columnar_sum_by_key(keys, diffs);
        let ks = kout.into_u64("k");
        let got: Vec<(u64, i64)> = ks.iter().zip(&sums).map(|(&k, &s)| (k, s as i64)).collect();
        assert_eq!(got, vec![(2, 5), (3, 10)]);
    }
}

/// Determines the active times for an interval `[lower, upper)` (Frank's definition).
///
/// Active times are joins of input times (`novel` + `prior`) that are `>= lower` but not `>= upper`;
/// the over-derivation is sound (a non-changing time yields a zero delta). Times `>= upper` are
/// `pended` for a future interval. `novel ∈ [lower, upper)` (seeds); `prior ∈ ..lower)` (join partners).
fn active_times<T: Lattice + Ord + Clone>(
    upper: AntichainRef<T>,
    prior: &[T],
    novel: &[T],
    active: &mut BTreeSet<T>,
    pended: &mut BTreeSet<T>,
) {
    let mut todo: BTreeSet<T> = novel.iter().cloned().collect();
    while let Some(next) = todo.pop_first() {
        if upper.less_equal(&next) {
            pended.insert(next);
        } else if active.insert(next.clone()) {
            for t in novel.iter().chain(prior.iter()) {
                let join = next.join(t);
                if !active.contains(&join) {
                    todo.insert(join);
                }
            }
        }
    }
}

/// Consolidate `(val, diff)` pairs (sum per val, drop zeros), sorted by val.
fn consolidate_vals(mut v: Vec<(Row, Diff)>) -> Vec<(Row, Diff)> {
    v.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out: Vec<(Row, Diff)> = Vec::with_capacity(v.len());
    for (val, d) in v {
        if let Some(last) = out.last_mut() {
            if last.0 == val {
                last.1 += d;
                continue;
            }
        }
        out.push((val, d));
    }
    out.retain(|(_, d)| *d != 0);
    out
}

/// The DDIR reducers, in Rust — matching `backend::vec`'s closures so output == the vec backend.
fn reduce_logic(reducer: &Reducer, input: &[(Row, Diff)]) -> Vec<(Row, Diff)> {
    match reducer {
        Reducer::Min => input
            .iter()
            .filter(|(_, d)| *d > 0)
            .map(|(v, _)| v.clone())
            .min()
            .map(|m| vec![(m, 1)])
            .unwrap_or_default(),
        Reducer::Distinct => {
            if input.iter().any(|(_, d)| *d > 0) {
                vec![(DValue::unit(), 1)]
            } else {
                Vec::new()
            }
        }
        Reducer::Count => {
            let count: Diff = input.iter().map(|(_, d)| *d).sum();
            if count > 0 {
                vec![(DValue::Tuple(vec![DValue::Int(count)]), 1)]
            } else {
                Vec::new()
            }
        }
        Reducer::Collect => {
            let mut items = Vec::new();
            for (v, d) in input {
                for _ in 0..(*d).max(0) {
                    items.push(v.clone());
                }
            }
            vec![(DValue::List(items), 1)]
        }
    }
}

/// Cursor-less corgi reduce tactic. Stateful: carries per-key pending (deferred ≥upper) times.
pub struct CorgiReduceTactic<T> {
    reducer: Reducer,
    pending: HashMap<Row, Vec<T>>,
}

impl<T> CorgiReduceTactic<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceTactic { reducer, pending: HashMap::new() }
    }
}

impl<T> ReduceTactic<Rc<ChunkBatch<CorgiChunk<T, Diff>>>, Rc<ChunkBatch<CorgiChunk<T, Diff>>>> for CorgiReduceTactic<T>
where
    T: Timestamp + Lattice + Ord,
{
    fn retire(
        &mut self,
        source_batches: Vec<Rc<ChunkBatch<CorgiChunk<T, Diff>>>>,
        output_batches: Vec<Rc<ChunkBatch<CorgiChunk<T, Diff>>>>,
        input_batches: Vec<Rc<ChunkBatch<CorgiChunk<T, Diff>>>>,
        lower: &Antichain<T>,
        upper: &Antichain<T>,
        held: &Antichain<T>,
    ) -> (Vec<(T, Rc<ChunkBatch<CorgiChunk<T, Diff>>>)>, Antichain<T>) {
        // Only work if we hold a time we can actually ship in [.., upper).
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            return (Vec::new(), held.clone());
        }

        // Incremental key selection: only keys touched by the new input delta (`input_batches`) or
        // carrying pending times can change output in [lower, upper). Unchanged accumulated keys
        // cannot — reconsidering them was an O(all keys × rounds) blowup (mirrors DD's reduce, which
        // only recomputes keys the new batch touches). Restricting the per-key histories to `changed`
        // also keeps `group_by_key`'s BTreeMap + `Value::cmp` cost proportional to the delta.
        let mut changed: std::collections::HashSet<Row> = std::collections::HashSet::new();
        for b in &input_batches {
            for ((k, _), _, _) in batch_to_rows(b) { changed.insert(k); }
        }
        for k in self.pending.keys() { changed.insert(k.clone()); }
        if changed.is_empty() {
            // No input delta and no carried pending → no key's output can change this interval, and
            // nothing remains to hold. Downgrade to the EMPTY frontier: this matches what the full
            // flow computes (an empty `pending` → empty frontier). Returning `held` here would keep
            // the capability forever and DEADLOCK a recursive scope (scc), which reach never exercised.
            self.pending.clear();
            return (Vec::new(), Antichain::new());
        }

        // Full per-key input history (source ∪ input) and output history, restricted to `changed`.
        // (A `find`-semijoin read was tried here and measured SLOWER: the source read is only ~7% of
        // reduce cost, and `find`'s per-chunk overhead exceeds the untranscode it saves. The dominant
        // cost is the row-wise per-key compute below, which needs a columnar segmented-fold, not a
        // faster read. See `corgi_chunk::semijoin_history` — kept for when that lands / at larger scale.)
        let mut input_hist: HashMap<Row, Vec<(Row, T, Diff)>> = HashMap::new();
        for b in source_batches.iter().chain(input_batches.iter()) {
            for ((k, v), t, d) in batch_to_rows(b) {
                if changed.contains(&k) { input_hist.entry(k).or_default().push((v, t, d)); }
            }
        }
        let mut output_hist: HashMap<Row, Vec<(Row, T, Diff)>> = HashMap::new();
        for b in &output_batches {
            for ((k, v), t, d) in batch_to_rows(b) {
                if changed.contains(&k) { output_hist.entry(k).or_default().push((v, t, d)); }
            }
        }
        let keys = changed;

        // One output-row buffer per held time (ship-time bucket); a delta at `t` routes to the
        // largest held time `<= t`.
        let held_elems: Vec<T> = held.elements().to_vec();
        let mut builders: Vec<Vec<((Row, Row), T, Diff)>> = vec![Vec::new(); held_elems.len()];

        let mut new_pending: HashMap<Row, Vec<T>> = HashMap::new();

        for key in keys {
            let in_hist = input_hist.get(&key).cloned().unwrap_or_default();
            let out_hist = output_hist.get(&key).cloned().unwrap_or_default();

            // novel (seeds) = input times in [lower,upper) ∪ carried pending; prior = times < lower.
            let mut novel: Vec<T> = in_hist
                .iter()
                .map(|(_, t, _)| t.clone())
                .filter(|t| lower.less_equal(t) && !upper.less_equal(t))
                .collect();
            if let Some(p) = self.pending.get(&key) {
                novel.extend(p.iter().cloned());
            }
            let prior: Vec<T> = in_hist.iter().map(|(_, t, _)| t.clone()).filter(|t| !lower.less_equal(t)).collect();
            novel.sort();
            novel.dedup();

            let mut active = BTreeSet::new();
            let mut pended = BTreeSet::new();
            active_times(upper.borrow(), &prior, &novel, &mut active, &mut pended);
            if !pended.is_empty() {
                new_pending.entry(key.clone()).or_default().extend(pended);
            }
            if active.is_empty() {
                continue;
            }

            // Running output (committed output + deltas emitted this pass), accumulated as-of each t.
            let mut emitted: Vec<(Row, T, Diff)> = Vec::new();
            for t in active {
                // Count = Σ diffs ≤ t: summing the consolidated per-value diffs equals summing the raw
                // diffs, so skip the per-value consolidation (and its Value sort/clones) entirely.
                let desired = if matches!(self.reducer, Reducer::Count) {
                    let c: Diff = in_hist.iter().filter(|(_, ti, _)| ti.less_equal(&t)).map(|(_, _, d)| *d).sum();
                    if c > 0 { vec![(DValue::Tuple(vec![DValue::Int(c)]), 1)] } else { Vec::new() }
                } else {
                    let in_acc = consolidate_vals(
                        in_hist.iter().filter(|(_, ti, _)| ti.less_equal(&t)).map(|(v, _, d)| (v.clone(), *d)).collect(),
                    );
                    reduce_logic(&self.reducer, &in_acc)
                };

                let mut cur: Vec<(Row, Diff)> =
                    out_hist.iter().filter(|(_, ti, _)| ti.less_equal(&t)).map(|(v, _, d)| (v.clone(), *d)).collect();
                cur.extend(emitted.iter().filter(|(_, ti, _)| ti.less_equal(&t)).map(|(v, _, d)| (v.clone(), *d)));
                let cur = consolidate_vals(cur);

                let mut delta: HashMap<Row, Diff> = HashMap::new();
                for (v, d) in desired {
                    *delta.entry(v).or_insert(0) += d;
                }
                for (v, d) in cur {
                    *delta.entry(v).or_insert(0) -= d;
                }
                delta.retain(|_, d| *d != 0);
                if delta.is_empty() {
                    continue;
                }

                let idx = held_elems.iter().rposition(|h| h.less_equal(&t)).expect("no held capability <= active time");
                for (v, d) in delta {
                    emitted.push((v.clone(), t.clone(), d));
                    builders[idx].push(((key.clone(), v), t.clone(), d));
                }
            }
        }

        self.pending = new_pending;

        // Build one output batch per held time, tiling [lower, upper) (mirrors CursorTactic).
        let mut produced = Vec::new();
        let mut out_lower = lower.clone();
        for (index, rows) in builders.into_iter().enumerate() {
            let mut out_upper = upper.clone();
            for t in &held_elems[index + 1..] {
                out_upper.insert(t.clone());
            }
            if out_upper != out_lower {
                let desc = Description::new(out_lower.clone(), out_upper.clone(), Antichain::from_elem(T::minimum()));
                let mut builder = CorgiChunkBuilder::<T, Diff>::with_capacity(0, 0, rows.len());
                let mut rows = rows;
                builder.push(&mut rows);
                let batch = Rc::new(builder.done(desc));
                produced.push((held_elems[index].clone(), batch));
                out_lower = out_upper;
            }
        }

        // New held frontier = all carried pending times.
        let mut frontier = Antichain::new();
        for times in self.pending.values() {
            for t in times {
                frontier.insert(t.clone());
            }
        }
        (produced, frontier)
    }
}
