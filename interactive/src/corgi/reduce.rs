//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_hash, value_id, time, diff)`; this backend supplies only:
//!
//!   * ids — `key_hash`/`value_id` are value-as-id for primitive columns (the value IS the id) and
//!     the canonical native `corgi::hash` for compound columns (columnar, content-addressed, so ids
//!     coincide across the output→input boundary); DD never hashes.
//!   * the value callback — `reduce_many` runs ONE crossing per retire over every `(key, time)`
//!     bracket, building the output value COLUMNS directly (Count → a `u64` prim, Distinct → a
//!     `Unit`, Min → the chosen input rows, Collect → a `List`), never through DDIR rows.
//!   * materialize — resolve proxy ids back to real columns by `gather` from per-retire pools and
//!     seal a `CorgiChunk` batch column-natively.
//!
//! Transcode-free: the real keys/values never leave corgi columns. Ids are resolved to rows by
//! integer index (`key_index`/`val_index` → offsets into the concatenated `key_blocks`/`val_blocks`
//! pools), not by carrying `DValue`s. Min/Collect use corgi's one-pass segmented structural sort.
//! DDIR integers are signed, so Min builds an order-only columnar view with each integer leaf's
//! sign bit swizzled before sorting; the winning row is still gathered from the original columns.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::int_proxy::ProxyBridge;
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{find_ranges, gather, gather_lanes, sort_blocks};
use corgi::{ArithOp, Bounds, NumOp, OpLike, Value as CValue};

use crate::corgi::col_times::ColTime;
use crate::corgi::chunk::{columns_to_batch, key_ids, key_lane, CorgiChunk};
use crate::ir::Diff;
use crate::parse::Reducer;

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Build a sortable view whose integer leaves have signed `i64` order.
///
/// DDIR's only scalar is `Int`, transcoded into a Corgi primitive as its raw
/// bits. Corgi's radix sort is unsigned, so XORing each payload leaf's sign bit
/// turns signed order into unsigned order. Sum discriminants remain untouched;
/// only their payload lanes recurse. This consumes freshly gathered candidate
/// columns, allowing Corgi to swizzle their buffers in place when unshared.
fn signed_order_view(value: CValue) -> CValue {
    match value {
        value @ CValue::Prim(_) => NumOp::from(ArithOp::ToSigned).eval(value).expect("ToSigned on a leaf"),
        CValue::Prod(fields) => {
            CValue::Prod(fields.into_iter().map(signed_order_view).collect())
        }
        CValue::Sum(tags, variants) => {
            // the lane assignment is untouched — only the payload lanes are swizzled.
            CValue::Sum(tags, variants.into_iter().map(signed_order_view).collect())
        }
        CValue::List(bounds, values) => {
            CValue::List(bounds, Box::new(signed_order_view(*values)))
        }
        CValue::Unit(len) => CValue::Unit(len),
    }
}

/// An identity `Hasher` for the id-index maps: their keys are already well-distributed 64-bit
/// content hashes (`hash_rows`), so passing the id straight through avoids re-hashing it (siphash
/// on `register_keys`/lookups was ~7% of the reduce in profiling). Only `write_u64` is used.
#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline]
    fn write_u64(&mut self, i: u64) { self.0 = i; }
    #[inline]
    fn write(&mut self, _: &[u8]) { unreachable!("IdMap keys are u64") }
    #[inline]
    fn finish(&self) -> u64 { self.0 }
}
/// `key_hash`/`value_id` → row index, hashed by identity.
type IdMap = HashMap<u64, usize, BuildHasherDefault<IdHasher>>;

/// A corgi reduce backend for a single `Reducer`. All per-retire scratch is corgi columns + integer
/// id→row-index maps; nothing carries a `DValue`.
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    /// Input value column for the current window, indexed by `in_index` (for Min/Collect resolution).
    in_vals: CValue,
    /// Input `value_id → row` in `in_vals` for the current window (reduce-time resolution; first row
    /// wins, so equal values — which share a content-hash `value_id` — resolve to one representative).
    in_index: IdMap,
    /// Output rows for `begin`/`emit`/`finish`: the accumulated
    /// `(key row, value row, time, diff)` (pool indices, gathered into columns at `finish`).
    rows: (Vec<usize>, Vec<usize>, Vec<T>, Vec<Diff>),
    /// Key-resolution pool for the current retire: `key_hash → row index` into the concatenation of
    /// `key_blocks` (representative keys from the input + output presentations).
    key_index: IdMap,
    key_blocks: Vec<CValue>,
    key_len: usize,
    /// Value-resolution pool for the current retire: `value_id → row index` into the concatenation of
    /// `val_blocks` (output-history values + values minted by `reduce_many`).
    val_index: IdMap,
    val_blocks: Vec<CValue>,
    val_len: usize,
    _t: std::marker::PhantomData<T>,
}

impl<T> CorgiReduceBackend<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceBackend {
            reducer,
            in_vals: CValue::Unit(0),
            in_index: IdMap::default(),
            rows: (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            key_index: IdMap::default(),
            key_blocks: Vec::new(),
            key_len: 0,
            val_index: IdMap::default(),
            val_blocks: Vec::new(),
            val_len: 0,
            _t: std::marker::PhantomData,
        }
    }

    /// Clear the resolution pools at the start of a retire (called from `next_window`'s first call).
    fn reset_pools(&mut self) {
        self.key_index.clear();
        self.key_blocks.clear();
        self.key_len = 0;
        self.val_index.clear();
        self.val_blocks.clear();
        self.val_len = 0;
    }

    /// Add representative key rows (aligned with `ids`) to the key pool; first id wins.
    fn register_keys(&mut self, col: CValue, ids: &[u64]) {
        for (i, &id) in ids.iter().enumerate() {
            self.key_index.entry(id).or_insert(self.key_len + i);
        }
        self.key_len += col.len();
        self.key_blocks.push(col);
    }

    /// Add value rows (aligned with `ids`) to the val pool; first id wins.
    fn register_vals(&mut self, col: CValue, ids: &[u64]) {
        for (i, &id) in ids.iter().enumerate() {
            self.val_index.entry(id).or_insert(self.val_len + i);
        }
        self.val_len += col.len();
        self.val_blocks.push(col);
    }
}

/// Concatenate corgi columns (skipping empties, which contribute no rows and so don't shift the
/// pool offsets accounted at registration). One `gather_lanes` over the non-empty blocks.
fn concat_columns(blocks: &[CValue]) -> CValue {
    let non_empty: Vec<&CValue> = blocks.iter().filter(|b| b.len() > 0).collect();
    match non_empty.len() {
        0 => CValue::Unit(0),
        1 => non_empty[0].clone(),
        _ => {
            let srcs: Vec<Option<&CValue>> = non_empty.iter().map(|b| Some(*b)).collect();
            let (mut tags, mut offs) = (Vec::new(), Vec::new());
            for (ti, b) in non_empty.iter().enumerate() {
                for o in 0..b.len() {
                    tags.push(ti);
                    offs.push(o);
                }
            }
            gather_lanes(&srcs, &tags, &offs)
        }
    }
}

/// Id column for a VALUE column. For a PRIMITIVE column — a bare 64-bit `Prim`, or a 1-field
/// `Prod([Prim(64)])` — the value itself is already a collision-free id (`i64 as u64` is a bijection),
/// so pass it straight through and skip the content hash. Compound shapes (Unit / List / Sum /
/// multi-field `Prod`) hash via the CANONICAL native `corgi::hash` (the designed boundary-id fold,
/// width-blind and consistent-with-equality) — not the branch-local `arrange::hash_rows`; DDIR
/// transcodes every leaf to `u64`, so width-blindness is a no-op for us and there is no cross-path
/// hash comparison (value-as-id and native hash are never used for the same value: shape is uniform
/// per column). Compound ids are used only for identity, but the leaf fast path additionally relies
/// on raw-id order matching corgi's unsigned leaf order: the stored key lane is searched in that order and
/// `merge_present` merges chunk runs in it. Raw two's-complement `u64` therefore remains correct for
/// negative ints (no swizzle); changing the leaf encoding must also revisit those ordered paths.
/// Applied CONSISTENTLY at every id site (both value presentations AND the freshly-produced
/// `reduce_brackets` outputs), else `desired − current` nets across mismatched ids for the same value.
fn ids(col: &CValue) -> Vec<u64> {
    // Value-as-id: borrow the leaf and copy once, rather than `clone().into_u64()` — the
    // clone bumps the `Arc`, so `into_u64`'s try-unwrap always fails and copies anyway,
    // even for a freshly-gathered column with one holder.
    if let Some(sl) = corgi::arrange::leaf_slice(col) {
        return sl.to_vec();
    }
    corgi::hash(col)
}

/// Concatenate the records of the `changed` keys across a run of chunks into parallel
/// `(keys_col, vals_col)` corgi columns plus per-record `(key_hash, time, diff)`. `changed` is the
/// ASCENDING set of changed key ids; a row is kept iff its key id is in it.
///
/// Seek or scan, decided per retire now that the sizes are known: seeking the changed keys wins
/// when the changed set is narrow (the steady incremental case), and a flat membership scan wins
/// for broad churn — loads and label-cascade retires, where most keys change and a gallop per key
/// only adds overhead.
///
/// What the stored identifier changed is that BOTH branches are now available, and cheap, for every
/// key shape. An arrangement's key leads with its identifier and is sorted by it
/// ([`present_key`](crate::corgi::chunk::present_key)), so [`key_lane`] is a sorted `u64` leaf: the
/// seek is `find_ranges` over it (corgi's `u64` fast path) and the scan borrows it outright, with no
/// hashing and no allocation on either side. Previously a structural key could do neither — its
/// identifier was hashed per chunk per retire, and a hash derived on the fly cannot be inverted into
/// a needle, so those keys were forced onto the scan and forced to materialize to take it.
fn collect_present<T>(chunks: &[&CorgiChunk<T, Diff>], changed: &[u64]) -> (CValue, CValue, Vec<u64>, Vec<T>, Vec<Diff>, Vec<usize>)
where
    T: ColTime,
{
    /// Seek only when the changed set is at least this many times narrower than the presented
    /// rows: a `find_ranges` probe costs ~log(rows) compares against the scan's flat membership
    /// test, so marginal seeks LOSE — measured, not modeled; 16 regressed load-shaped retires
    /// before this was widened.
    const SEEK_ADVANTAGE: usize = 64;

    let key_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.keys())).collect();
    let val_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.vals())).collect();
    let (mut tags, mut offs) = (Vec::new(), Vec::new());
    let (mut khs, mut times, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
    let mut run_ends = Vec::new();
    let total: usize = chunks.iter().map(|c| c.diffs().len()).sum();
    let seek = changed.len().saturating_mul(SEEK_ADVANTAGE) < total;
    // Chunks are id-ordered and `changed` ascends, so either branch emits in merged order.
    for (ci, ch) in chunks.iter().enumerate() {
        let before = khs.len();
        if ch.diffs().is_empty() {
            continue;
        }
        let lane = key_lane(ch.keys());
        if seek {
            // Only the needles within this chunk's key range can hit it. A batch's chunks
            // partition its key range, so at scale each chunk is probed with its slice of the
            // change set rather than the whole of it.
            let kh = corgi::arrange::leaf_slice(lane).expect("the identifier lane is a u64 leaf");
            let from = changed.partition_point(|c| *c < kh[0]);
            let to = changed.partition_point(|c| *c <= kh[kh.len() - 1]);
            if from == to {
                continue;
            }
            let needles = CValue::u64(changed[from..to].to_vec());
            let (lo, hi) = find_ranges(&needles, lane);
            for (j, (&l, &h)) in lo.iter().zip(hi.iter()).enumerate() {
                for i in l..h {
                    tags.push(ci);
                    offs.push(i);
                    khs.push(changed[from + j]);
                    times.push(ch.times().get(i));
                    diffs.push(ch.diffs()[i]);
                }
            }
        } else {
            // Borrowed, never materialized: the identifier lane is a `u64` leaf whatever the key.
            let kh = corgi::arrange::leaf_slice(lane).expect("the identifier lane is a u64 leaf");
            for i in 0..kh.len() {
                if changed.binary_search(&kh[i]).is_ok() {
                    tags.push(ci);
                    offs.push(i);
                    khs.push(kh[i]);
                    times.push(ch.times().get(i));
                    diffs.push(ch.diffs()[i]);
                }
            }
        }
        if khs.len() > before { run_ends.push(khs.len()); }
    }
    if tags.is_empty() {
        return (CValue::Unit(0), CValue::Unit(0), khs, times, diffs, run_ends);
    }
    let keys_col = gather_lanes(&key_srcs, &tags, &offs);
    let vals_col = gather_lanes(&val_srcs, &tags, &offs);
    (keys_col, vals_col, khs, times, diffs, run_ends)
}

/// Merge already-ordered selected chunk runs directly into an empty proxy bridge. Leaf values
/// preserve value-id order. Keys may either be identity-id leaves or carried-hash columns, provided
/// no one chunk run contains two real keys under the same hash; in the latter case the real-key
/// tie-break would interrupt proxy `(key_id, value_id, time)` order, so we fall back to ordinary
/// consolidation. A debug assertion audits the inferred order. Returns false when the inference
/// does not hold or the bridge is nonempty.
fn merge_present<T: Ord + Clone>(
    keys_col: &CValue, vals_col: &CValue,
    khs: &[u64], vids: &[u64], times: &[T], diffs: &[Diff], run_ends: &[usize],
    bridge: &mut ProxyBridge<T, Diff>,
) -> bool {
    let ordered_keys = corgi::arrange::leaf_slice(keys_col).is_some() || {
        // One columnar pass over the adjacent pairs, rather than a dispatching structural
        // compare per row: every row of a group shares its id with its predecessor, so the
        // per-row form was a `compare_at` for nearly every row of every retire.
        let adjacent = corgi::arrange::compare_adjacent(keys_col);
        let mut start = 0usize;
        run_ends.iter().all(|&end| {
            let one_real_key_per_id =
                (start + 1..end).all(|index| khs[index - 1] != khs[index] || adjacent[index - 1] == 0);
            start = end;
            one_real_key_per_id
        }) && start == khs.len()
    };
    let ordered_ids = ordered_keys && corgi::arrange::leaf_slice(vals_col).is_some();
    if !ordered_ids || !bridge.is_empty() {
        return false;
    }

    debug_assert!({
        let mut start = 0usize;
        let sorted = run_ends.iter().all(|&end| {
            let sorted = (start + 1..end).all(|i| {
                (khs[i - 1], vids[i - 1], &times[i - 1])
                    <= (khs[i], vids[i], &times[i])
            });
            start = end;
            sorted
        });
        sorted && start == khs.len()
    }, "identity ids do not preserve selected chunk order");

    let mut current: Option<((u64, u64), T, Diff)> = None;
    let mut accumulate = |kv, time: &T, diff| {
        if current.as_ref().is_some_and(|(ckv, ct, _)| ckv == &kv && ct == time) {
            current.as_mut().unwrap().2 += diff;
        } else {
            if let Some(record) = current.take() {
                if record.2 != 0 { bridge.push(record); }
            }
            current = Some((kv, time.clone(), diff));
        }
    };

    if run_ends.len() == 1 {
        for index in 0..run_ends[0] {
            accumulate((khs[index], vids[index]), &times[index], diffs[index]);
        }
        drop(accumulate);
        if let Some(record) = current {
            if record.2 != 0 { bridge.push(record); }
        }
        return true;
    }

    let mut heap: BinaryHeap<Reverse<((u64, u64), &T, usize, usize)>> = BinaryHeap::new();
    let mut lo = 0usize;
    for (run, &hi) in run_ends.iter().enumerate() {
        heap.push(Reverse(((khs[lo], vids[lo]), &times[lo], run, lo)));
        lo = hi;
    }
    while let Some(Reverse((kv, time, run, index))) = heap.pop() {
        accumulate(kv, time, diffs[index]);
        let end = run_ends[run];
        if index + 1 < end {
            let next = index + 1;
            heap.push(Reverse(((khs[next], vids[next]), &times[next], run, next)));
        }
    }
    drop(accumulate);
    if let Some(record) = current {
        if record.2 != 0 { bridge.push(record); }
    }
    true
}

/// All chunks of a batch list, flattened (empty chunks included — `hash_rows` yields nothing for them).
fn chunks_of<T>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>>
where
    T: ColTime,
{
    batches.iter().flat_map(|b| b.chunks.iter()).collect()
}

impl<T> CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    /// Present the merged input run — novel and prior chunks together — restricted to `keys`.
    ///
    /// Fills `bridge`, registers the run's representative keys, and extends the shared value pool
    /// (`blocks`/`len`, concatenated into `in_vals`) with its values, so `in_index` resolves a value
    /// id from EITHER run to a row. The two runs stay apart as presentations and meet only in the
    /// tactic's accumulation; the pool is shared because a value id means the same thing in both.
    fn present_input(
        &mut self,
        chunks: &[&CorgiChunk<T, Diff>],
        keys: &[u64],
        blocks: &mut Vec<CValue>,
        len: &mut usize,
        bridge: &mut ProxyBridge<T, Diff>,
    ) {
        let (p_keys, p_vals, khs, times, diffs, run_ends) = collect_present(chunks, keys);
        if khs.is_empty() {
            return;
        }
        let vids = ids(&p_vals);
        let merged = merge_present(&p_keys, &p_vals, &khs, &vids, &times, &diffs, &run_ends, bridge);
        for (row, &vid) in vids.iter().enumerate() { self.in_index.entry(vid).or_insert(*len + row); }
        *len += p_vals.len();
        blocks.push(p_vals);
        self.register_keys(p_keys, &khs);
        if !merged {
            bridge.extend((0..khs.len()).map(|i| ((khs[i], vids[i]), times[i].clone(), diffs[i])));
            consolidate_updates(bridge);
        }
    }

    /// The one value crossing for a retire: every `(key, time)` bracket at once. Builds the output
    /// value COLUMN directly per reducer, registers it (id → row) into the val pool, and returns the
    /// proxy `(value_id, diff)` deltas with per-bracket ends. `input[k] = (rep index into the input
    /// presentation, accumulated diff)`; the bracket `i` is `input[ends[i-1]..ends[i]]`, non-empty.
    fn reduce_brackets(&mut self, ends: &[usize], input: &[(usize, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        let mut out_diffs: Vec<Diff> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::with_capacity(ends.len());
        let out_ids: Vec<u64>;

        match self.reducer {
            Reducer::Count => {
                // Per-bracket sum of diffs; survivors become a `Tuple([Int(sum)])` = corgi `Prod([u64])`.
                let mut sums: Vec<u64> = Vec::new();
                let mut start = 0;
                for &end in ends {
                    let c: Diff = input[start..end].iter().map(|&(_, d)| d).sum();
                    if c > 0 {
                        sums.push(c as u64);
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if sums.is_empty() {
                    return (Vec::new(), out_ends);
                }
                let col = CValue::Prod(vec![CValue::u64(sums)]);
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
            Reducer::Distinct => {
                // Present iff any value has NON-ZERO net -- the sign does not matter. DD's `reduce`
                // presents every value whose accumulation is non-zero, negatives included, and
                // `backend::vec`'s Distinct then emits `1` without looking at the diffs at all. A
                // `> 0` test here silently drops a key whose values all accumulate negative, which
                // is exactly what a negated collection produces. Output value is unit (a `Unit` column).
                let mut present = 0usize;
                let mut start = 0;
                for &end in ends {
                    if input[start..end].iter().any(|&(_, d)| d != 0) {
                        present += 1;
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if present == 0 {
                    return (Vec::new(), out_ends);
                }
                let col = CValue::Unit(present);
                out_ids = ids(&col); // all equal (unit content hash)
                self.register_vals(col, &out_ids);
            }
            Reducer::Min => {
                // The structural minimum over values with NON-ZERO net. The sign does not select
                // candidates: DD presents every non-zero accumulation. Filtering to `> 0` here both
                // drops all-negative keys and can pick a different minimum when a bracket mixes signs.
                // Gather all candidates across brackets into one column, segment by
                // bracket, and one corgi `sort_blocks` gives every bracket's argmin at once
                // (`perm[block_start]`). The winning ROW is taken columnar and reuses its input value id.
                let mut cand_reps: Vec<usize> = Vec::new(); // input presentation rep index per candidate
                let mut labels: Vec<u64> = Vec::new(); // dense segment id per candidate
                let mut block_starts: Vec<usize> = Vec::new(); // per emitted bracket: start offset in cand_reps
                let mut start = 0;
                for &end in ends {
                    let lo = cand_reps.len();
                    let seg = block_starts.len() as u64;
                    for k in start..end {
                        if input[k].1 != 0 {
                            cand_reps.push(input[k].0);
                            labels.push(seg);
                        }
                    }
                    if cand_reps.len() > lo {
                        block_starts.push(lo);
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if cand_reps.is_empty() {
                    return (Vec::new(), out_ends);
                }
                let cand_col = gather(&self.in_vals, &cand_reps);
                let (perm, _) = sort_blocks(&labels, &signed_order_view(cand_col));
                let min_reps: Vec<usize> = block_starts.iter().map(|&lo| cand_reps[perm[lo]]).collect();
                let col = gather(&self.in_vals, &min_reps);
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
            Reducer::Collect => {
                // One row per bracket: the values sorted in corgi structural order,
                // each repeated by its diff, as a `List`. One `sort_blocks` orders every bracket's
                // entries at once; element rows are then taken columnar. A bracket emits iff some
                // value has NON-ZERO net (as Distinct/Min: DD invokes the reducer only for a key
                // with input, and the row reducer then lists the positive copies — an empty list
                // when every net is negative). A bracket whose values all cancelled is a key with
                // no input: it must emit nothing, or a retracted key keeps a stale (empty) list.
                let mut entry_reps: Vec<usize> = Vec::new();
                let mut entry_diffs: Vec<Diff> = Vec::new();
                let mut labels: Vec<u64> = Vec::new();
                let mut blocks: Vec<(usize, usize)> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for (bi, &end) in ends.iter().enumerate() {
                    if input[start..end].iter().any(|&(_, d)| d != 0) {
                        let lo = entry_reps.len();
                        for k in start..end {
                            entry_reps.push(input[k].0);
                            entry_diffs.push(input[k].1);
                            labels.push(bi as u64);
                        }
                        blocks.push((lo, entry_reps.len()));
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                let perm = if entry_reps.is_empty() {
                    Vec::new()
                } else {
                    sort_blocks(&labels, &gather(&self.in_vals, &entry_reps)).0
                };
                // Expand each bracket's sorted entries by their diff (max(0, ·) copies).
                let mut elem_reps: Vec<usize> = Vec::new();
                let mut bracket_ends: Vec<usize> = Vec::with_capacity(ends.len());
                for (lo, hi) in blocks {
                    for &e in &perm[lo..hi] {
                        for _ in 0..entry_diffs[e].max(0) {
                            elem_reps.push(entry_reps[e]);
                        }
                    }
                    bracket_ends.push(elem_reps.len());
                }
                // A window whose lists are all empty still has an element SHAPE — the input
                // values' — and the column must carry it, or this batch's `List<()>` meets the
                // next batch's `List<T>` where the two are concatenated. `gather` at no indices
                // is the empty column of that shape.
                let elems = gather(&self.in_vals, &elem_reps);
                let col = CValue::List(Bounds::offsets(bracket_ends), Box::new(elems));
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
        }

        let outs = out_ids.into_iter().zip(out_diffs).collect();
        (outs, out_ends)
    }
}

impl<T> ProxyReduceBackend<T, CBatch<T>, CBatch<T>> for CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, _description: Description<T>) {
        // Open the output session for this retire; reset the per-retire resolution pools.
        self.reset_pools();
        self.rows = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>, changed: &[u64], from: &mut Option<u64>, window: &mut ReduceWindow<T, Diff, Diff>) {
        // Single window: present the WHOLE key space at once, and report it covered. This is NOT a
        // deferred refinement — bounded windows were measured and rejected: at WINDOW = 1<<14, scc
        // (100 rounds x batch 100) cost 84.4s against 63.7s, a 33% regression, while peak RSS
        // fell only 356MB -> 340MB. Two reasons: the per-window, per-chunk seek setup is a
        // fixed cost that multiplies by the window count, and the presentation is not the
        // memory peak in the first place (the trace is).
        if from.is_none() {
            return;
        }
        *from = None;

        // The window's keys: the hashes the novel batches touch, merged with the `changed` set the
        // harness supplies. The novel hashes come from the scan the presentation needs anyway — the
        // separate seeding pass this replaced read the delta a second time to derive them.
        let novel_chunks = chunks_of(instance.input_batches);
        let mut keys: Vec<u64> = Vec::new();
        // The seeds are the novel batches' RAW (key_hash, time) support, recorded here — before the
        // merged presentation below, whose consolidation may net a novel record away entirely. The
        // key hashes come from the scan the key list needs anyway.
        let mut seeds: Vec<(u64, T)> = Vec::new();
        for ch in novel_chunks.iter() {
            let khs = key_ids(ch.keys());
            let times = ch.times();
            for (i, kh) in khs.iter().enumerate() {
                seeds.push((*kh, times.get(i)));
            }
            keys.extend(khs);
        }
        seeds.sort_unstable_by(|a, b| a.cmp(b));
        seeds.dedup();
        window.seeds = seeds;
        keys.sort_unstable();
        keys.dedup();
        if !changed.is_empty() {
            // Both sides ascend, so this is a merge.
            let mut merged: Vec<u64> = Vec::with_capacity(keys.len() + changed.len());
            let (mut a, mut b) = (0usize, 0usize);
            while a < keys.len() || b < changed.len() {
                let key = match (keys.get(a), changed.get(b)) {
                    (Some(x), Some(y)) => *x.min(y),
                    (Some(x), None) => *x,
                    (None, Some(y)) => *y,
                    (None, None) => unreachable!("loop condition ensures one is present"),
                };
                if keys.get(a) == Some(&key) { a += 1; }
                if changed.get(b) == Some(&key) { b += 1; }
                merged.push(key);
            }
            keys = merged;
        }
        if keys.is_empty() {
            self.in_vals = CValue::Unit(0);
            self.in_index = IdMap::default();
            return;
        }

        // ONE merged input presentation: novel and prior together, netted by the consolidation —
        // equal values share a content-hash id, so an exactly cancelling pair vanishes here, and
        // its time survives in `window.seeds` above. `in_index` resolves a value id back to a
        // representative row of `in_vals` for `reduce_corrections`.
        self.in_index = IdMap::default();
        let mut in_blocks: Vec<CValue> = Vec::new();
        let mut in_len = 0usize;
        let mut in_chunks = chunks_of(instance.source_batches);
        in_chunks.extend(novel_chunks.iter().copied());
        self.present_input(&in_chunks, &keys, &mut in_blocks, &mut in_len, &mut window.input);
        self.in_vals = concat_columns(&in_blocks);

        // Output-history presentation, same keys (register keys + values for correction resolution).
        let (o_keys, o_vals, o_khs, o_times, o_diffs, o_run_ends) = collect_present(&chunks_of(instance.output_batches), &keys);
        if !o_khs.is_empty() {
            let vids = ids(&o_vals);
            let merged = merge_present(&o_keys, &o_vals, &o_khs, &vids, &o_times, &o_diffs, &o_run_ends, &mut window.output);
            self.register_keys(o_keys, &o_khs);
            self.register_vals(o_vals, &vids);
            if !merged {
                window.output.extend((0..o_khs.len()).map(|i| ((o_khs[i], vids[i]), o_times[i].clone(), o_diffs[i])));
                consolidate_updates(&mut window.output);
            }
        }
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Resolve input value_ids to `in_vals` rows, reduce (desired output), then difference the
        // desired against the presented current output per key: correction = desired − current.
        let in_rows: Vec<(usize, Diff)> = input.iter()
            .map(|&(vid, d)| (*self.in_index.get(&vid).expect("input value_id presented this window"), d))
            .collect();
        let (desired, desired_ends) = self.reduce_brackets(in_ends, &in_rows);

        let mut corr: Vec<(u64, Diff)> = Vec::new();
        let mut corr_ends: Vec<usize> = Vec::with_capacity(keys.len());
        let (mut ds, mut os) = (0usize, 0usize);
        for i in 0..keys.len() {
            let (de, oe) = (desired_ends[i], out_ends[i]);
            // Net by value_id: desired (+) minus current output (−); keep non-zero, in first-seen order.
            let mut net: HashMap<u64, Diff, BuildHasherDefault<IdHasher>> = Default::default();
            let mut order: Vec<u64> = Vec::new();
            for &(vid, d) in &desired[ds..de] {
                if let Some(x) = net.get_mut(&vid) { *x += d; } else { net.insert(vid, d); order.push(vid); }
            }
            for &(vid, d) in &output[os..oe] {
                if let Some(x) = net.get_mut(&vid) { *x -= d; } else { net.insert(vid, -d); order.push(vid); }
            }
            for vid in order {
                let d = net[&vid];
                if d != 0 { corr.push((vid, d)); }
            }
            corr_ends.push(corr.len());
            ds = de;
            os = oe;
        }
        (corr, corr_ends)
    }

    fn emit(&mut self, records: &[((u64, u64), T, Diff)]) {
        // Resolve each correction's key/value proxies to pool rows and accumulate.
        for rec in records {
            let ((kh, vid), t, d) = (rec.0, &rec.1, rec.2);
            let kr = *self.key_index.get(&kh).expect("key resolvable this retire");
            let vr = *self.val_index.get(&vid).expect("value resolvable this retire");
            let (krows, vrows, times, diffs) = &mut self.rows;
            krows.push(kr);
            vrows.push(vr);
            times.push(t.clone());
            diffs.push(d);
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        // Seal the batch: gather the accumulated (key, val) pool rows into columns, one CorgiChunk batch.
        let key_pool = concat_columns(&self.key_blocks);
        let val_pool = concat_columns(&self.val_blocks);
        let (krows, vrows, times, diffs) = std::mem::take(&mut self.rows);
        if times.is_empty() { return None; }
        let keys = gather(&key_pool, &krows);
        let vals = gather(&val_pool, &vrows);
        Some(Rc::new(columns_to_batch(keys, vals, times, diffs)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compound_keys(hashes: Vec<u64>, real: Vec<u64>) -> CValue {
        CValue::Prod(vec![CValue::u64(hashes), CValue::Prod(vec![CValue::u64(real), CValue::u64(vec![0, 0])])])
    }

    #[test]
    fn merge_present_accepts_ordered_compound_keys() {
        let keys = compound_keys(vec![1, 2], vec![7, 8]);
        let vals = CValue::u64(vec![10, 20]);
        let mut bridge = Vec::new();
        assert!(merge_present(
            &keys, &vals, &[1, 2], &[10, 20], &[0u64, 0], &[1, 1], &[2], &mut bridge,
        ));
        assert_eq!(bridge.len(), 2);
    }

    #[test]
    fn merge_present_rejects_a_compound_hash_collision_within_a_run() {
        let keys = compound_keys(vec![1, 1], vec![7, 8]);
        let vals = CValue::u64(vec![10, 20]);
        assert!(!merge_present(
            &keys,
            &vals,
            &[1, 1],
            &[10, 20],
            &[0u64, 0],
            &[1, 1],
            &[2],
            &mut Vec::new(),
        ));
    }
}
