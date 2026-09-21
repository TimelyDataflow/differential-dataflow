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
//! Transcode-free: primitive columns are reconstructed from their IDs; compound IDs resolve
//! through columnar representative pools, without carrying `DValue`s. Primitive Min scans signed
//! IDs directly; other Min/Collect values use a segmented structural sort over an
//! order-only columnar view: signed integer leaves are swizzled, and lists become lexicographic
//! ranks. The winning rows are still gathered from the original columns.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;

use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::int_proxy::diffs::{consolidate, Records};
use differential_dataflow::operators::int_proxy::KeyPosition;
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{compare_at, gather, gather_lanes, sort_blocks};
use corgi::{ArithOp, Bounds, NumOp, OpLike, Value as CValue};

use crate::corgi::col_times::{ColTime, ColTimes};
use crate::corgi::search::MatchingRanges;
use crate::corgi::chunk::{columns_to_batch, key_ids, key_lane, CorgiChunk};
use crate::ir::{Diff, Reducer};

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Build a sortable view matching DDIR's signed leaves and lexicographic lists.
///
/// DDIR's leaf scalar is `Int`, transcoded into a Corgi primitive as its raw
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
        CValue::List(bounds, values) => lexicographic_list_ranks(bounds, signed_order_view(*values)),
        CValue::Unit(len) => CValue::Unit(len),
    }
}

/// An order-only integer rank for each list, using DDIR's lexicographic order.
/// Corgi's general structural order is intentionally length-first. Preserve
/// that contract and adapt here: rank the element columns, then refine tied
/// list prefixes in column batches, with end-of-list preceding every element.
/// No DDIR rows or per-comparison interpreter calls are materialized.
///
/// This is a correctness adapter, not a performance-neutral view: it eagerly
/// ranks all elements, including unused tails, and each prefix round scans all
/// lists and allocates fresh scratch even when most prefixes are resolved.
/// Cost therefore grows with total element count and unresolved prefix depth.
/// Only list-valued subcolumns pay this ranking cost (including strings encoded
/// as lists); physical arrangement-key ordering is unchanged.
fn lexicographic_list_ranks(bounds: Bounds, ordered_elements: CValue) -> CValue {
    let ends = bounds.to_vec();
    let rows = ends.len();
    let (element_perm, element_labels) = sort_blocks(&vec![0; ordered_elements.len()], &ordered_elements);
    let mut element_ranks = vec![0; element_perm.len()];
    for (i, &row) in element_perm.iter().enumerate() { element_ranks[row] = element_labels[i]; }
    let starts: Vec<_> = std::iter::once(0).chain(ends.iter().copied()).take(rows).collect();
    let mut perm: Vec<_> = (0..rows).collect();
    let mut labels = vec![0; rows];
    let mut position = 0;
    loop {
        let mut present = vec![0; rows];
        let mut keys = vec![0; rows];
        let mut active = false;
        for i in 0..rows {
            let tied = (i > 0 && labels[i] == labels[i-1]) || (i+1 < rows && labels[i] == labels[i+1]);
            let row = perm[i];
            if tied && position < ends[row]-starts[row] {
                present[i] = 1;
                keys[i] = element_ranks[starts[row]+position];
                active = true;
            }
        }
        if !active { break; }
        let (order, refined) = sort_blocks(&labels, &CValue::Prod(vec![CValue::u64(present), CValue::u64(keys)]));
        perm = order.into_iter().map(|i| perm[i]).collect();
        labels = refined;
        position += 1;
    }
    let mut ranks = vec![0; rows];
    for (i, &row) in perm.iter().enumerate() { ranks[row] = labels[i]; }
    CValue::u64(ranks)
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

/// A per-retire ID resolver. Primitive IDs already contain their payload; retain only
/// the singleton-product nesting depth. Compound columns keep representatives.
/// All nonempty registrations in a pool have the same shape, as required by its operator.
#[derive(Default)]
struct IdPool {
    depth: Option<usize>,
    blocks: Vec<CValue>,
    index: IdMap,
    len: usize,
}
impl IdPool {
    fn clear(&mut self) {
        self.depth = None;
        self.blocks.clear();
        self.index.clear();
        self.len = 0;
    }
    fn register(&mut self, col: CValue, ids: &[u64]) {
        if col.len() == 0 { return; }
        if corgi::arrange::leaf_slice(&col).is_some() {
            let (mut depth, mut leaf) = (0, &col);
            while let CValue::Prod(fields) = leaf { depth += 1; leaf = &fields[0]; }
            self.depth = Some(depth);
        } else {
            for (i, &id) in ids.iter().enumerate() { self.index.entry(id).or_insert(self.len + i); }
            self.len += col.len();
            self.blocks.push(col);
        }
    }
    fn gather(&self, ids: &[u64]) -> CValue {
        if let Some(depth) = self.depth {
            let mut col = CValue::u64(ids.to_vec());
            for _ in 0..depth { col = CValue::Prod(vec![col]); }
            col
        } else {
            let rows: Vec<_> = ids.iter().map(|id| self.index[id]).collect();
            if self.blocks.len() == 1 { gather(&self.blocks[0], &rows) }
            else { gather(&concat_columns(&self.blocks), &rows) }
        }
    }
}

/// A corgi reduce backend for a single `Reducer`. Resolution uses primitive IDs
/// directly and columnar representative pools for compound values.
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    input: IdPool,
    keys: IdPool,
    vals: IdPool,
    /// Output IDs, times, and diffs accumulated until `finish`.
    rows: (Vec<u64>, Vec<u64>, ColTimes<T>, Vec<Diff>),
}

impl<T> CorgiReduceBackend<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceBackend {
            reducer,
            input: IdPool::default(),
            keys: IdPool::default(),
            vals: IdPool::default(),
            rows: (Vec::new(), Vec::new(), ColTimes::default(), Vec::new()),
        }
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
            let total: usize = non_empty.iter().map(|b| b.len()).sum();
            let (mut tags, mut offs) = (Vec::with_capacity(total), Vec::with_capacity(total));
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
/// Both the changed set and stored identifier lane are sorted. Match them with
/// monotone positions, galloping over long gaps and stepping through adjacent
/// keys. The same compiled search covers narrow updates and broad cascades.
fn collect_present<T>(chunks: &[&CorgiChunk<T, Diff>], changed: &[u64]) -> (CValue, CValue, Vec<u64>, Vec<T>, Vec<Diff>, Vec<usize>)
where
    T: ColTime,
{
    let key_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.keys())).collect();
    let val_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.vals())).collect();
    let (mut tags, mut offs) = (Vec::new(), Vec::new());
    let (mut khs, mut times, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
    let mut run_ends = Vec::new();
    for (ci, ch) in chunks.iter().enumerate() {
        let before = khs.len();
        if ch.diffs().is_empty() {
            continue;
        }
        let lane = key_lane(ch.keys());
        let kh = corgi::arrange::leaf_slice(lane).expect("the identifier lane is a u64 leaf");
        for (j, range) in MatchingRanges::new(changed, kh) {
            for i in range {
                tags.push(ci);
                offs.push(i);
                khs.push(changed[j]);
                times.push(ch.times().get(i));
                diffs.push(ch.diffs()[i]);
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
fn merge_present<T: timely::progress::Timestamp>(
    keys_col: &CValue, vals_col: &CValue,
    khs: &[u64], vids: &[u64], times: &mut [T], diffs: &[Diff], run_ends: &[usize],
    bridge: &mut Records<((u64, u64), T), Vec<Diff>>,
) -> bool {
    let ordered_keys = corgi::arrange::leaf_slice(keys_col).is_some() || {
        let mut start = 0usize;
        run_ends.iter().all(|&end| {
            let one_real_key_per_id = (start + 1..end).all(|index| {
                khs[index - 1] != khs[index]
                    || compare_at(keys_col, index - 1, keys_col, index) == std::cmp::Ordering::Equal
            });
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
    let mut accumulate = |kv, time: T, diff| {
        if current.as_ref().is_some_and(|(ckv, ct, _)| ckv == &kv && ct == &time) {
            current.as_mut().unwrap().2 += diff;
        } else {
            if let Some(record) = current.take() {
                if record.2 != 0 { bridge.data.push((record.0, record.1)); bridge.diffs.push(record.2); }
            }
            current = Some((kv, time, diff));
        }
    };

    if run_ends.len() == 1 {
        for index in 0..run_ends[0] {
            accumulate((khs[index], vids[index]), std::mem::replace(&mut times[index], T::minimum()), diffs[index]);
        }
        drop(accumulate);
        if let Some(record) = current {
            if record.2 != 0 { bridge.data.push((record.0, record.1)); bridge.diffs.push(record.2); }
        }
        return true;
    }

    let mut heap: BinaryHeap<Reverse<((u64, u64), T, usize, usize)>> = BinaryHeap::new();
    let mut lo = 0usize;
    for (run, &hi) in run_ends.iter().enumerate() {
        heap.push(Reverse(((khs[lo], vids[lo]), std::mem::replace(&mut times[lo], T::minimum()), run, lo)));
        lo = hi;
    }
    while let Some(mut head) = heap.peek_mut() {
        let Reverse((kv, _, run, index)) = *head;
        let time = std::mem::replace(&mut head.0.1, T::minimum());
        accumulate(kv, time, diffs[index]);
        let end = run_ends[run];
        if index + 1 < end {
            let next = index + 1;
            *head = Reverse(((khs[next], vids[next]), std::mem::replace(&mut times[next], T::minimum()), run, next));
        } else {
            std::collections::binary_heap::PeekMut::pop(head);
        }
    }
    drop(accumulate);
    if let Some(record) = current {
        if record.2 != 0 { bridge.data.push((record.0, record.1)); bridge.diffs.push(record.2); }
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
    /// with its values, so the input pool resolves a value
    /// id from EITHER run to a row. The two runs stay apart as presentations and meet only in the
    /// tactic's accumulation; the pool is shared because a value id means the same thing in both.
    fn present_input(
        &mut self,
        chunks: &[&CorgiChunk<T, Diff>],
        keys: &[u64],
        bridge: &mut Records<((u64, u64), T), Vec<Diff>>,
    ) {
        let (p_keys, p_vals, khs, mut times, diffs, run_ends) = collect_present(chunks, keys);
        if khs.is_empty() {
            return;
        }
        let vids = ids(&p_vals);
        let merged = merge_present(&p_keys, &p_vals, &khs, &vids, &mut times, &diffs, &run_ends, bridge);
        self.input.register(p_vals, &vids);
        self.keys.register(p_keys, &khs);
        if !merged {
            bridge.data.extend(times.into_iter().enumerate().map(|(i, time)| ((khs[i], vids[i]), time)));
            bridge.diffs.extend(diffs);
            consolidate(&mut bridge.data, &mut bridge.diffs);
        }
    }

    /// The one value crossing for a retire: every `(key, time)` bracket at once. Builds the output
    /// value COLUMN directly per reducer, registers it (id → row) into the val pool, and returns the
    /// proxy `(value_id, diff)` deltas with per-bracket ends.
    /// Input value IDs and accumulated diffs are aligned; ends delimit the brackets.
    fn reduce_brackets(&mut self, ends: &[usize], input: &Records<u64, Vec<Diff>>) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Primitive IDs contain the signed integer itself; a segmented minimum
        // needs neither payload resolution nor a structural sort.
        if matches!(self.reducer, Reducer::Min) && self.input.depth.is_some() {
            let (mut values, mut output_ends) = (Vec::new(), Vec::with_capacity(ends.len()));
            let mut start = 0;
            for &end in ends {
                if let Some((&id, _)) = input.data[start..end].iter().zip(&input.diffs[start..end]).filter(|r| *r.1 != 0).min_by_key(|r| *r.0 as i64) {
                    values.push((id, 1));
                }
                output_ends.push(values.len());
                start = end;
            }
            self.vals.depth = self.input.depth;
            return (values, output_ends);
        }
        let mut out_diffs: Vec<Diff> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::with_capacity(ends.len());
        let out_ids: Vec<u64>;

        match self.reducer {
            Reducer::Count => {
                // Per-bracket sum of diffs; survivors become a `Tuple([Int(sum)])` = corgi `Prod([u64])`.
                let mut sums: Vec<u64> = Vec::new();
                let mut start = 0;
                for &end in ends {
                    let c: Diff = input.diffs[start..end].iter().sum();
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
                self.vals.register(col, &out_ids);
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
                    if input.diffs[start..end].iter().any(|&d| d != 0) {
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
                self.vals.register(col, &out_ids);
            }
            Reducer::Min => {
                // The structural minimum over values with NON-ZERO net. The sign does not select
                // candidates: DD presents every non-zero accumulation. Filtering to `> 0` here both
                // drops all-negative keys and can pick a different minimum when a bracket mixes signs.
                // Gather all candidates across brackets into one column, segment by
                // bracket, and one corgi `sort_blocks` gives every bracket's argmin at once
                // (`perm[block_start]`). The winning ROW is taken columnar and reuses its input value id.
                let mut cand_reps: Vec<u64> = Vec::new(); // input value ID per candidate
                let mut labels: Vec<u64> = Vec::new(); // dense segment id per candidate
                let mut block_starts: Vec<usize> = Vec::new(); // per emitted bracket: start offset in cand_reps
                let mut start = 0;
                for &end in ends {
                    let lo = cand_reps.len();
                    let seg = block_starts.len() as u64;
                    for k in start..end {
                        if input.diffs[k] != 0 {
                            cand_reps.push(input.data[k]);
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
                let cand_col = self.input.gather(&cand_reps);
                let (perm, _) = sort_blocks(&labels, &signed_order_view(cand_col));
                let min_reps: Vec<u64> = block_starts.iter().map(|&lo| cand_reps[perm[lo]]).collect();
                let col = self.input.gather(&min_reps);
                out_ids = ids(&col);
                self.vals.register(col, &out_ids);
            }
            Reducer::Collect => {
                // One row per bracket: the values sorted in DDIR observable order,
                // each repeated by its diff, as a `List`. One `sort_blocks` orders every bracket's
                // entries at once; element rows are then taken columnar. A bracket emits iff some
                // value has NON-ZERO net (as Distinct/Min: DD invokes the reducer only for a key
                // with input, and the row reducer then lists the positive copies — an empty list
                // when every net is negative). A bracket whose values all cancelled is a key with
                // no input: it must emit nothing, or a retracted key keeps a stale (empty) list.
                let mut entry_reps: Vec<u64> = Vec::new();
                let mut entry_diffs: Vec<Diff> = Vec::new();
                let mut labels: Vec<u64> = Vec::new();
                let mut blocks: Vec<(usize, usize)> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for (bi, &end) in ends.iter().enumerate() {
                    if input.diffs[start..end].iter().any(|&d| d != 0) {
                        let lo = entry_reps.len();
                        for k in start..end {
                            entry_reps.push(input.data[k]);
                            entry_diffs.push(input.diffs[k]);
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
                    sort_blocks(&labels, &signed_order_view(self.input.gather(&entry_reps))).0
                };
                // Expand each bracket's sorted entries by their diff (max(0, ·) copies).
                let mut elem_reps: Vec<u64> = Vec::new();
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
                let elems = self.input.gather(&elem_reps);
                let col = CValue::List(Bounds::offsets(bracket_ends), Box::new(elems));
                out_ids = ids(&col);
                self.vals.register(col, &out_ids);
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
    type Key = u64;
    type VIn = u64;
    type VOut = u64;
    type RIn = Vec<Diff>;
    type ROut = Vec<Diff>;

    fn new_diffs(&self) -> (Vec<Diff>, Vec<Diff>) { (Vec::new(), Vec::new()) }

    fn begin(&mut self, _description: Description<T>) {
        // Open the output session for this retire; reset the per-retire resolution pools.
        self.input.clear();
        self.keys.clear();
        self.vals.clear();
        self.rows = (Vec::new(), Vec::new(), ColTimes::default(), Vec::new());
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>, changed: &[u64], from: &mut KeyPosition<u64>, window: &mut ReduceWindow<T, Vec<Diff>, Vec<Diff>>) {
        // Single window: present the WHOLE key space at once, and report it covered. This is NOT a
        // deferred refinement — bounded windows were measured and rejected: at WINDOW = 1<<14, scc
        // (100 rounds x batch 100) cost 84.4s against 63.7s, a 33% regression, while peak RSS
        // fell only 356MB -> 340MB. Two reasons: the per-window, per-chunk seek setup is a
        // fixed cost that multiplies by the window count, and the presentation is not the
        // memory peak in the first place (the trace is).
        if *from == KeyPosition::End {
            return;
        }
        *from = KeyPosition::End;

        // The window's keys: the hashes the novel batches touch, merged with the `changed` set the
        // harness supplies. The novel hashes come from the scan the presentation needs anyway — the
        // separate seeding pass this replaced read the delta a second time to derive them.
        let novel_chunks = chunks_of(instance.input_batches);
        let mut keys: Vec<u64> = Vec::new();
        // The seeds are the novel batches' RAW (key_hash, time) support, recorded here — before the
        // merged presentation below, whose consolidation may net a novel record away entirely. The
        // key hashes come from the scan the key list needs anyway.
        let mut seeds: Vec<(u64, T)> = Vec::with_capacity(novel_chunks.iter().map(|c| c.diffs().len()).sum());
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
            return;
        }

        // ONE merged input presentation: novel and prior together, netted by the consolidation —
        // equal values share a content-hash id, so an exactly cancelling pair vanishes here, and
        // its time survives in `window.seeds` above. The input pool resolves values
        // needed by Min and Collect.
        let mut in_chunks = chunks_of(instance.source_batches);
        in_chunks.extend(novel_chunks.iter().copied());
        self.present_input(&in_chunks, &keys, &mut window.input);

        // Output-history presentation, same keys (register keys + values for correction resolution).
        let (o_keys, o_vals, o_khs, mut o_times, o_diffs, o_run_ends) = collect_present(&chunks_of(instance.output_batches), &keys);
        if !o_khs.is_empty() {
            let vids = ids(&o_vals);
            let merged = merge_present(&o_keys, &o_vals, &o_khs, &vids, &mut o_times, &o_diffs, &o_run_ends, &mut window.output);
            self.keys.register(o_keys, &o_khs);
            self.vals.register(o_vals, &vids);
            if !merged {
                window.output.data.extend(o_times.into_iter().enumerate().map(|(i, time)| ((o_khs[i], vids[i]), time)));
                window.output.diffs.extend(o_diffs);
                consolidate(&mut window.output.data, &mut window.output.diffs);
            }
        }
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &Records<u64, Vec<Diff>>, out_ends: &[usize], output: &Records<u64, Vec<Diff>>) -> (Records<u64, Vec<Diff>>, Vec<usize>) {
        let (desired, desired_ends) = self.reduce_brackets(in_ends, input);

        let mut corr = Records::new(Vec::new());
        let mut corr_ends: Vec<usize> = Vec::with_capacity(keys.len());
        let (mut ds, mut os) = (0usize, 0usize);
        // Scratch for netting, cleared per key rather than allocated per key.
        let mut net: HashMap<u64, Diff, BuildHasherDefault<IdHasher>> = Default::default();
        let mut order: Vec<u64> = Vec::new();
        for i in 0..keys.len() {
            let (de, oe) = (desired_ends[i], out_ends[i]);
            // Net by value_id: desired (+) minus current output (−); keep non-zero, in first-seen order.
            net.clear();
            order.clear();
            for &(vid, d) in &desired[ds..de] {
                if let Some(x) = net.get_mut(&vid) { *x += d; } else { net.insert(vid, d); order.push(vid); }
            }
            for (&vid, &d) in output.data[os..oe].iter().zip(&output.diffs[os..oe]) {
                if let Some(x) = net.get_mut(&vid) { *x -= d; } else { net.insert(vid, -d); order.push(vid); }
            }
            for &vid in &order {
                let d = net[&vid];
                if d != 0 { corr.data.push(vid); corr.diffs.push(d); }
            }
            corr_ends.push(corr.len());
            ds = de;
            os = oe;
        }
        (corr, corr_ends)
    }

    fn emit(&mut self, records: &Records<((u64, u64), T), Vec<Diff>>) {
        // Accumulate IDs; resolve columns once at the output boundary.
        for (((kh, vid), t), d) in records.data.iter().zip(&records.diffs) {
            let (krows, vrows, times, diffs) = &mut self.rows;
            krows.push(*kh);
            vrows.push(*vid);
            times.push(t);
            diffs.push(*d);
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        // Seal the batch: gather the accumulated (key, val) pool rows into columns, one CorgiChunk batch.
        let (krows, vrows, times, diffs) = std::mem::take(&mut self.rows);
        if times.is_empty() { return None; }
        let keys = self.keys.gather(&krows);
        let vals = self.vals.gather(&vrows);
        Some(Rc::new(columns_to_batch(keys, vals, times, diffs)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_min_uses_signed_ids_and_nonzero_support() {
        for depth in 0..3 {
            let mut backend = CorgiReduceBackend::<u64>::new(Reducer::Min);
            let mut col = CValue::u64(vec![0, u64::MAX, i64::MIN as u64, i64::MAX as u64]);
            for _ in 0..depth { col = CValue::Prod(vec![col]); }
            let input_ids = ids(&col);
            backend.input.register(col, &input_ids);
            let mut input = Records::new(Vec::new());
            input.data = vec![0, u64::MAX, i64::MIN as u64, i64::MAX as u64, i64::MIN as u64];
            input.diffs = vec![1, -1, 0, -1, -2];
            let (values, ends) = backend.reduce_brackets(&[0, 3, 5], &input);
            assert_eq!(values, vec![(u64::MAX, 1), (i64::MIN as u64, 1)]);
            assert_eq!(ends, vec![0, 1, 2]);
            assert_eq!(backend.vals.depth, Some(depth));
        }
    }

    #[test]
    fn id_pool_preserves_primitive_shapes_and_compound_fallback() {
        let mut pool = IdPool::default();
        for depth in 0..4 {
            pool.clear();
            pool.register(CValue::Unit(0), &[]); // no shape information yet
            let mut col = CValue::u64(vec![i64::MIN as u64, u64::MAX, 0, i64::MAX as u64]);
            for _ in 0..depth { col = CValue::Prod(vec![col]); }
            pool.register(col.clone(), &ids(&col));
            assert!(pool.index.is_empty() && pool.blocks.is_empty());
            assert_eq!(pool.gather(&[u64::MAX, 0, u64::MAX]), gather(&col, &[1, 2, 1]));
            assert_eq!(pool.gather(&[]), gather(&col, &[]));
        }
        pool.clear();
        let col = CValue::Prod(vec![CValue::u64(vec![7, 7]), CValue::u64(vec![9, 8])]);
        let id = ids(&col);
        pool.register(col.clone(), &id);
        pool.register(col.clone(), &id); // first representative continues to resolve
        assert_eq!(pool.gather(&[id[1], id[0], id[1]]), gather(&col, &[1, 0, 1]));
        assert_eq!(pool.gather(&[]), gather(&col, &[]));
    }

    #[test]
    fn order_view_matches_ddir_for_ragged_lists_and_signed_products() {
        use crate::ir::Value as V;
        use crate::corgi::logic::{transcode, shape_of_row};
        let mut rows = Vec::new();
        for n in [-3, 0, 7] {
            for bytes in [vec![], vec![0], vec![1], vec![1, -1], vec![1, 0], vec![2], vec![2, -2, 0]] {
                rows.push(V::Tuple(vec![V::Int(n), V::List(bytes.into_iter().map(V::Int).collect())]));
            }
        }
        rows.reverse();
        // Infer from a non-empty representative; the first row need not carry
        // data for every nested-list position once the schema is declared.
        let shape = shape_of_row(&rows[0]).unwrap();
        let columns = transcode(&rows, &shape);
        let (perm, _) = sort_blocks(&vec![0; rows.len()], &signed_order_view(columns));
        let actual: Vec<_> = perm.into_iter().map(|i| rows[i].clone()).collect();
        rows.sort();
        assert_eq!(actual, rows);
    }

    fn compound_keys(hashes: Vec<u64>, real: Vec<u64>) -> CValue {
        CValue::Prod(vec![CValue::u64(hashes), CValue::Prod(vec![CValue::u64(real), CValue::u64(vec![0, 0])])])
    }

    #[test]
    fn merge_present_accepts_ordered_compound_keys() {
        let keys = compound_keys(vec![1, 2], vec![7, 8]);
        let vals = CValue::u64(vec![10, 20]);
        let mut bridge = Records::new(Vec::new());
        assert!(merge_present(
            &keys, &vals, &[1, 2], &[10, 20], &mut [0u64, 0], &[1, 1], &[2], &mut bridge,
        ));
        assert_eq!(bridge.len(), 2);
    }

    #[test]
    fn merge_present_combines_runs_with_ties_cancellation_and_different_lengths() {
        use std::collections::BTreeMap;
        use timely::order::Product;
        let runs = [
            vec![((1, 10), Product::new(0u64, 2u64), 1),
                 ((1, 10), Product::new(1, 0), -1),
                 ((3, 30), Product::new(0, 0), 2)],
            vec![((1, 10), Product::new(0, 2), -1),
                 ((1, 11), Product::new(0, 0), 5),
                 ((3, 30), Product::new(0, 0), -2),
                 ((4, 40), Product::new(0, 0), -1)],
            vec![((1, 10), Product::new(1, 0), 2)],
        ];
        for count in 1..=runs.len() {
            let (mut rows, mut ends) = (Vec::new(), Vec::new());
            let mut expected = BTreeMap::new();
            for run in &runs[..count] {
                rows.extend_from_slice(run);
                ends.push(rows.len());
                for &(kv, time, diff) in run {
                    *expected.entry((kv, time)).or_insert(0) += diff;
                }
            }
            let khs: Vec<_> = rows.iter().map(|r| r.0.0).collect();
            let vids: Vec<_> = rows.iter().map(|r| r.0.1).collect();
            let mut times: Vec<_> = rows.iter().map(|r| r.1).collect();
            let diffs: Vec<_> = rows.iter().map(|r| r.2).collect();
            let mut bridge = Records::new(Vec::new());
            assert!(merge_present(&CValue::u64(khs.clone()), &CValue::u64(vids.clone()),
                &khs, &vids, &mut times, &diffs, &ends, &mut bridge));
            let expected: Vec<_> = expected.into_iter().filter(|(_, d)| *d != 0)
                .map(|((kv, time), diff)| (kv, time, diff)).collect();
            let actual: Vec<_> = bridge.data.into_iter().zip(bridge.diffs).map(|((kv, t), d)| (kv, t, d)).collect();
            assert_eq!(actual, expected, "run count: {count}");
        }
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
            &mut [0u64, 0],
            &[1, 1],
            &[2],
            &mut Records::new(Vec::new()),
        ));
    }
}
