//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_hash, value_id, time, diff)`; this backend supplies only:
//!
//!   * ids — `key_hash`/`value_id` are value-as-id for primitive columns (the value IS the id) and
//!     the canonical native `corgi::hash` for compound columns (columnar, content-addressed, so ids
//!     coincide across the output→input boundary); DD never hashes. A structured value's hash is
//!     read off the lane the arrangement stores it under ([`present_val`]), computed once at
//!     ingest; only a product of leaves or a unit is hashed here, at presentation.
//!   * the value callback — `reduce_many` runs ONE crossing per retire over every `(key, time)`
//!     bracket, building the output value COLUMNS directly (Count → a `u64` prim, Distinct → a
//!     `Unit`, Min → the chosen input rows, Collect → a `List`), never through DDIR rows.
//!   * materialize — resolve proxy ids back to real columns by `gather` from per-retire pools and
//!     seal a `CorgiChunk` batch column-natively.
//!
//! Transcode-free: the real keys/values never leave corgi columns. Ids are resolved to rows by
//! integer index (`key_index`/`val_index` → offsets into the concatenated `key_blocks`/`val_blocks`
//! pools), not by carrying `DValue`s. Min/Collect use a segmented structural sort over an
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
use differential_dataflow::operators::int_proxy::Bridge;
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{equal_idx, gather, gather_lanes, sort_blocks};
use corgi::{ArithOp, Bounds, NumOp, OpLike, Value as CValue};

use crate::corgi::col_times::{ColTime, ColTimes};
use crate::corgi::search::MatchingRanges;
use crate::corgi::chunk::{columns_to_batch, key_ids, key_lane, present_val, recover_val, val_is_hashed, val_lane, CorgiChunk};
use crate::ir::Diff;
use crate::parse::Reducer;

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

/// A corgi reduce backend for a single `Reducer`. All per-retire scratch is corgi columns + integer
/// id→row-index maps; nothing carries a `DValue`.
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    /// Input value column for the current window, in its stored form (a structured value under
    /// its hash lane), indexed by `in_index` (for Min/Collect resolution).
    in_vals: CValue,
    /// Input `value_id → row` in `in_vals` for the current window (reduce-time resolution; first row
    /// wins, so equal values — which share a content-hash `value_id` — resolve to one representative).
    in_index: IdMap,
    /// Output rows for `begin`/`emit`/`finish`: the accumulated
    /// `(key row, value row, time, diff)` (pool indices, gathered into columns at `finish`;
    /// the times a lane column copied from the tactic's).
    rows: (Vec<usize>, Vec<usize>, ColTimes<T>, Vec<Diff>),
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
    /// The leaf fast path for the current retire: when keys and values are both leaves (their
    /// own identifiers) and the reducer builds its output from identifiers alone, presentations
    /// read the id lanes directly and the output is built from the emitted ids — no column
    /// gathers, no pools, no id maps. Holds the key and value templates (empty columns of the
    /// stored shapes) the output is built in.
    leaf: Option<(CValue, CValue)>,
    /// The emitted `(key id, value id)` pairs on the leaf fast path, aligned with `rows`' times.
    leaf_ids: Vec<(u64, u64)>,
    _t: std::marker::PhantomData<T>,
}

impl<T> CorgiReduceBackend<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceBackend {
            reducer,
            in_vals: CValue::Unit(0),
            in_index: IdMap::default(),
            rows: (Vec::new(), Vec::new(), ColTimes::new(), Vec::new()),
            key_index: IdMap::default(),
            key_blocks: Vec::new(),
            key_len: 0,
            val_index: IdMap::default(),
            val_blocks: Vec::new(),
            val_len: 0,
            leaf: None,
            leaf_ids: Vec::new(),
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
        self.key_index.reserve(ids.len());
        for (i, &id) in ids.iter().enumerate() {
            self.key_index.entry(id).or_insert(self.key_len + i);
        }
        self.key_len += col.len();
        self.key_blocks.push(col);
    }

    /// Add value rows (aligned with `ids`) to the val pool; first id wins.
    fn register_vals(&mut self, col: CValue, ids: &[u64]) {
        self.val_index.reserve(ids.len());
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

/// Id column for a VALUE column in its stored form. For a PRIMITIVE column — a bare 64-bit
/// `Prim`, or a 1-field `Prod([Prim(64)])` — the value itself is already a collision-free id
/// (`i64 as u64` is a bijection), so pass it straight through. A structured column carries its
/// hash as a lane ([`present_val`]), computed once at ingest: read it. What remains — a product
/// of leaves, a unit — hashes via the CANONICAL native `corgi::hash` (the designed boundary-id
/// fold, width-blind and consistent-with-equality), the same function the lane holds, so an id
/// means one thing whichever way it was obtained. Compound ids are used only for identity, but
/// the leaf fast path additionally relies on raw-id order matching corgi's unsigned leaf order:
/// the stored key lane is searched in that order and `merge_present` merges chunk runs in it. Raw
/// two's-complement `u64` therefore remains correct for negative ints (no swizzle); changing the
/// leaf encoding must also revisit those ordered paths. Applied CONSISTENTLY at every id site
/// (both value presentations AND the freshly-produced `reduce_brackets` outputs), else
/// `desired − current` nets across mismatched ids for the same value.
fn val_ids(col: &CValue) -> Vec<u64> {
    // Value-as-id: borrow the leaf and copy once, rather than `clone().into_u64()` — the
    // clone bumps the `Arc`, so `into_u64`'s try-unwrap always fails and copies anyway,
    // even for a freshly-gathered column with one holder.
    if let Some(sl) = corgi::arrange::leaf_slice(col) {
        return sl.to_vec();
    }
    if let Some(lane) = val_lane(col) {
        return corgi::arrange::leaf_slice(lane).expect("the value lane is a u64 leaf").to_vec();
    }
    corgi::hash(col)
}

/// The records of the `changed` keys across a run of chunks: parallel `(keys_col, vals_col)`
/// corgi columns, the records' times as one lane column, and per record its key id and diff.
/// `changed` is the ASCENDING set of changed key ids; a row is kept iff its key id is in it.
///
/// Both the changed set and the stored identifier lane are sorted, so the match is a walk of
/// monotone positions, galloping over long gaps and stepping through adjacent keys — and every
/// match is a contiguous RANGE of a chunk, which is the unit every column here is filled in:
/// the times by range copy, the key ids by fill, the diffs and value ids by slice copy. Nothing
/// is done per record.
fn collect_present<T>(chunks: &[&CorgiChunk<T, Diff>], changed: &[u64], leaf: bool) -> Presented<T>
where
    T: ColTime,
{
    let (mut tags, mut offs) = (Vec::new(), Vec::new());
    let (mut khs, mut vids, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
    let mut times = ColTimes::new();
    let mut run_ends = Vec::new();
    for (ci, ch) in chunks.iter().enumerate() {
        let before = khs.len();
        if ch.diffs().is_empty() {
            continue;
        }
        let kh = corgi::arrange::leaf_slice(key_lane(ch.keys())).expect("the identifier lane is a u64 leaf");
        // On the leaf path the value ids are the value lane — or, for an output history whose
        // values are not leaves (Distinct's units), the ids its output was emitted under.
        let owned;
        let vs: &[u64] = if !leaf { &[] } else {
            match corgi::arrange::leaf_slice(ch.vals()) {
                Some(lane) => lane,
                None => { owned = val_ids(ch.vals()); &owned }
            }
        };
        for (j, range) in MatchingRanges::new(changed, kh) {
            khs.resize(khs.len() + range.len(), changed[j]);
            times.push_range(ch.times(), range.start, range.end);
            diffs.extend_from_slice(&ch.diffs()[range.clone()]);
            if leaf {
                vids.extend_from_slice(&vs[range]);
            } else {
                // The key and value columns are gathered, so these rows are named by index.
                tags.resize(tags.len() + range.len(), ci);
                offs.extend(range);
            }
        }
        if khs.len() > before { run_ends.push(khs.len()); }
    }
    let (keys_col, vals_col) = if leaf || tags.is_empty() {
        (CValue::Unit(0), CValue::Unit(0))
    } else {
        let key_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.keys())).collect();
        let val_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.vals())).collect();
        (gather_lanes(&key_srcs, &tags, &offs), gather_lanes(&val_srcs, &tags, &offs))
    };
    Presented { keys_col, vals_col, khs, vids, leaf, times, diffs, run_ends }
}

/// A presentation: the selected records' columns, their times as one lane column, and per
/// record its key id and diff; `run_ends` delimits the records of each chunk, each run in that
/// chunk's (stored) order. On the leaf fast path the key/value columns are not gathered at all
/// (`leaf`) and `vids` holds the value lane read directly.
struct Presented<T> {
    keys_col: CValue,
    vals_col: CValue,
    khs: Vec<u64>,
    vids: Vec<u64>,
    leaf: bool,
    times: ColTimes<T>,
    diffs: Vec<Diff>,
    run_ends: Vec<usize>,
}

/// A leaf column of `ids` in the shape of `template` (a bare 64-bit leaf, or the 1-field
/// product DDIR wraps a scalar in).
fn leaf_like(template: &CValue, ids: Vec<u64>) -> CValue {
    match template {
        CValue::Prod(cols) if cols.len() == 1 => CValue::Prod(vec![CValue::u64(ids)]),
        _ => CValue::u64(ids),
    }
}

impl<T: ColTime> Presented<T> {
    fn is_empty(&self) -> bool { self.khs.is_empty() }
    /// The order of records `i` and `j`'s times, read in place.
    #[inline]
    fn cmp_times(&self, i: usize, j: usize) -> std::cmp::Ordering {
        self.times.cmp(i, j)
    }
    /// Append record `i` to the bridge with `diff`: its ids, a row copy of its time, the diff.
    #[inline]
    fn push_into(&self, vids: &[u64], i: usize, diff: Diff, bridge: &mut Bridge<ColTimes<T>, Diff>) {
        bridge.push_from((self.khs[i], vids[i]), &self.times, i, diff);
    }
    /// The records as bridge entries in presentation order, for the consolidation fallback.
    fn extend_into(&self, vids: &[u64], bridge: &mut Bridge<ColTimes<T>, Diff>) {
        for i in 0..self.khs.len() {
            self.push_into(vids, i, self.diffs[i], bridge);
        }
    }
}

/// Merge already-ordered selected chunk runs directly into an empty proxy bridge. A run is in
/// proxy `(key_id, value_id, time)` order when its identifiers name one real key and, under it,
/// one real value each: a leaf key or value is its own identifier; a carried-hash key or value
/// is, provided no run holds two real keys under one hash or two real values under one
/// `(key, hash)`, which one batched equality over the run's adjacent equal-identifier rows
/// confirms. A unit value has one identifier for all rows. Otherwise the real tie-break would
/// interrupt proxy order, and we fall back to ordinary consolidation. A debug assertion audits
/// the inferred order. Returns false when the inference does not hold or the bridge is nonempty.
fn merge_present<T: ColTime>(
    p: &Presented<T>,
    vids: &[u64],
    bridge: &mut Bridge<ColTimes<T>, Diff>,
) -> bool {
    let Presented { keys_col, vals_col, khs, diffs, run_ends, .. } = p;
    let keys_hashed = !p.leaf && corgi::arrange::leaf_slice(keys_col).is_none();
    let vals_hashed = !p.leaf && val_is_hashed(vals_col);
    let (mut ka, mut kb, mut va, mut vb) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut start = 0usize;
    for &end in run_ends {
        for index in start + 1..end {
            if khs[index - 1] == khs[index] {
                if keys_hashed {
                    ka.push(index - 1);
                    kb.push(index);
                }
                if vals_hashed && vids[index - 1] == vids[index] {
                    va.push(index - 1);
                    vb.push(index);
                }
            }
        }
        start = end;
    }
    debug_assert_eq!(start, khs.len(), "run ends must cover the presentation");
    let ordered_keys = !keys_hashed || equal_idx(keys_col, keys_col, &ka, &kb).into_iter().all(|e| e);
    let ordered_vals = p.leaf
        || corgi::arrange::leaf_slice(vals_col).is_some()
        || matches!(vals_col, CValue::Unit(_))
        || (vals_hashed && equal_idx(vals_col, vals_col, &va, &vb).into_iter().all(|e| e));
    if !ordered_keys || !ordered_vals || !bridge.is_empty() {
        return false;
    }

    debug_assert!({
        let mut start = 0usize;
        let sorted = run_ends.iter().all(|&end| {
            let sorted = (start + 1..end).all(|i| {
                (khs[i - 1], vids[i - 1]) < (khs[i], vids[i])
                    || ((khs[i - 1], vids[i - 1]) == (khs[i], vids[i]) && p.cmp_times(i - 1, i) != std::cmp::Ordering::Greater)
            });
            start = end;
            sorted
        });
        sorted && start == khs.len()
    }, "identity ids do not preserve selected chunk order");

    // The record being consolidated: its `(key id, value id)`, the index whose time it carries,
    // and the diff so far. The time is read in place to compare and copied once, at the push.
    let mut current: Option<((u64, u64), usize, Diff)> = None;
    let mut accumulate = |kv, index: usize| {
        if current.as_ref().is_some_and(|&(ckv, ci, _)| ckv == kv && p.cmp_times(ci, index) == std::cmp::Ordering::Equal) {
            current.as_mut().unwrap().2 += diffs[index];
        } else {
            if let Some((_, ci, d)) = current.take() {
                if d != 0 { p.push_into(vids, ci, d, bridge); }
            }
            current = Some((kv, index, diffs[index]));
        }
    };

    if run_ends.len() == 1 {
        for index in 0..run_ends[0] {
            accumulate((khs[index], vids[index]), index);
        }
        drop(accumulate);
        if let Some((_, ci, d)) = current {
            if d != 0 { p.push_into(vids, ci, d, bridge); }
        }
        return true;
    }

    // One head per run, least `(key id, value id, time)` first; the time compared in place.
    struct Head<'a, T: ColTime> { kv: (u64, u64), run: usize, index: usize, p: &'a Presented<T> }
    impl<T: ColTime> PartialEq for Head<'_, T> { fn eq(&self, o: &Self) -> bool { self.cmp(o) == std::cmp::Ordering::Equal } }
    impl<T: ColTime> Eq for Head<'_, T> {}
    impl<T: ColTime> PartialOrd for Head<'_, T> { fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) } }
    impl<T: ColTime> Ord for Head<'_, T> {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            self.kv.cmp(&o.kv)
                .then_with(|| self.p.cmp_times(self.index, o.index))
                .then_with(|| (self.run, self.index).cmp(&(o.run, o.index)))
        }
    }
    let mut heap: BinaryHeap<Reverse<Head<T>>> = BinaryHeap::new();
    let mut lo = 0usize;
    for (run, &hi) in run_ends.iter().enumerate() {
        heap.push(Reverse(Head { kv: (khs[lo], vids[lo]), run, index: lo, p }));
        lo = hi;
    }
    while let Some(mut head) = heap.peek_mut() {
        let (kv, run, index) = (head.0.kv, head.0.run, head.0.index);
        accumulate(kv, index);
        let end = run_ends[run];
        if index + 1 < end {
            let next = index + 1;
            head.0.kv = (khs[next], vids[next]);
            head.0.index = next;
        } else {
            std::collections::binary_heap::PeekMut::pop(head);
        }
    }
    drop(accumulate);
    if let Some((_, ci, d)) = current {
        if d != 0 { p.push_into(vids, ci, d, bridge); }
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
        bridge: &mut Bridge<ColTimes<T>, Diff>,
    ) {
        if self.leaf.is_some() {
            let p = collect_present(chunks, keys, true);
            if !p.is_empty() && !merge_present(&p, &p.vids, bridge) {
                p.extend_into(&p.vids, bridge);
                bridge.consolidate();
            }
            return;
        }
        let p = collect_present(chunks, keys, false);
        if p.is_empty() {
            return;
        }
        let vids = val_ids(&p.vals_col);
        let merged = merge_present(&p, &vids, bridge);
        if !merged {
            p.extend_into(&vids, bridge);
            bridge.consolidate();
        }
        // Sized once: growing the map as it fills was 7% of a retire-bound run (ast).
        self.in_index.reserve(vids.len());
        for (row, &vid) in vids.iter().enumerate() { self.in_index.entry(vid).or_insert(*len + row); }
        *len += p.vals_col.len();
        let Presented { keys_col, vals_col, khs, .. } = p;
        blocks.push(vals_col);
        self.register_keys(keys_col, &khs);
    }

    /// The one value crossing for a retire: every `(key, time)` bracket at once. Builds the output
    /// value COLUMN directly per reducer, registers it (id → row) into the val pool, and returns the
    /// proxy `(value_id, diff)` deltas with per-bracket ends. `input[k] = (rep index into the input
    /// presentation, accumulated diff)` and `vids[k]` its value id; the bracket `i` is
    /// `input[ends[i-1]..ends[i]]`, non-empty and in ascending value-id order (the tactic hands
    /// over consolidated accumulations).
    ///
    /// A leaf value is its own identifier, so a bracket of leaf values arrives sorted by the
    /// value's unsigned bits, and DDIR's signed order is that order with the sign-bit half moved
    /// to the front: Min is the first value with the sign bit set, else the first value; Collect
    /// is the two halves in turn. Neither sorts. A structured value takes the segmented
    /// structural sort over DDIR's order.
    fn reduce_brackets(&mut self, ends: &[usize], input: &[(usize, Diff)], vids: &[u64]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        let mut out_diffs: Vec<Diff> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::with_capacity(ends.len());
        let out_ids: Vec<u64>;
        let leaf = self.leaf.is_some() || corgi::arrange::leaf_slice(&self.in_vals).is_some();
        debug_assert!({
            let mut start = 0;
            ends.iter().all(|&end| { let ok = vids[start..end].windows(2).all(|w| w[0] <= w[1]); start = end; ok })
        }, "a bracket arrives in ascending value-id order");

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
                out_ids = val_ids(&col);
                if self.leaf.is_none() { self.register_vals(col, &out_ids); }
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
                out_ids = val_ids(&col); // all equal (unit content hash)
                if self.leaf.is_none() { self.register_vals(col, &out_ids); }
            }
            Reducer::Min => {
                // The structural minimum over values with NON-ZERO net. The sign does not select
                // candidates: DD presents every non-zero accumulation. Filtering to `> 0` here both
                // drops all-negative keys and can pick a different minimum when a bracket mixes signs.
                // Gather all candidates across brackets into one column, segment by
                // bracket, and one corgi `sort_blocks` gives every bracket's argmin at once
                // (`perm[block_start]`). The order is DDIR's, over the values as the program wrote
                // them (the stored form's hash lane dropped); the winning ROW is taken columnar,
                // in its stored form, and so reuses its input value id.
                let mut min_reps: Vec<usize> = Vec::new(); // per emitted bracket: the winning rep
                let mut cand_reps: Vec<usize> = Vec::new(); // input presentation rep index per candidate
                let mut labels: Vec<u64> = Vec::new(); // dense segment id per candidate
                let mut block_starts: Vec<usize> = Vec::new(); // per emitted bracket: start offset in cand_reps
                let mut start = 0;
                for &end in ends {
                    if leaf {
                        let (mut first, mut first_neg) = (None, None);
                        for k in start..end {
                            if input[k].1 == 0 {
                                continue;
                            }
                            if first.is_none() {
                                first = Some(k);
                            }
                            if vids[k] >> 63 == 1 {
                                first_neg = Some(k);
                                break;
                            }
                        }
                        if let Some(k) = first_neg.or(first) {
                            min_reps.push(input[k].0);
                            out_diffs.push(1);
                        }
                    } else {
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
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if !cand_reps.is_empty() {
                    let cand_col = gather(&recover_val(&self.in_vals), &cand_reps);
                    let (perm, _) = sort_blocks(&labels, &signed_order_view(cand_col));
                    min_reps.extend(block_starts.iter().map(|&lo| cand_reps[perm[lo]]));
                }
                if min_reps.is_empty() {
                    return (Vec::new(), out_ends);
                }
                if self.leaf.is_some() {
                    // The winners are their own identifiers; the output is built from them at
                    // `finish`, so nothing is pooled.
                    out_ids = min_reps.iter().map(|&k| vids[k]).collect();
                } else {
                    let col = gather(&self.in_vals, &min_reps);
                    out_ids = val_ids(&col);
                    self.register_vals(col, &out_ids);
                }
            }
            Reducer::Collect => {
                // One row per bracket: the values sorted in DDIR observable order,
                // each repeated by its diff, as a `List`. One `sort_blocks` orders every bracket's
                // entries at once; element rows are then taken columnar. A bracket emits iff some
                // value has NON-ZERO net (as Distinct/Min: DD invokes the reducer only for a key
                // with input, and the row reducer then lists the positive copies — an empty list
                // when every net is negative). A bracket whose values all cancelled is a key with
                // no input: it must emit nothing, or a retracted key keeps a stale (empty) list.
                let mut elem_reps: Vec<usize> = Vec::new();
                let mut bracket_ends: Vec<usize> = Vec::with_capacity(ends.len());
                let mut entry_reps: Vec<usize> = Vec::new();
                let mut entry_diffs: Vec<Diff> = Vec::new();
                let mut labels: Vec<u64> = Vec::new();
                let mut blocks: Vec<(usize, usize)> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for (bi, &end) in ends.iter().enumerate() {
                    if input[start..end].iter().any(|&(_, d)| d != 0) {
                        if leaf {
                            // the sign-bit half first, then the rest, each entry by its diff
                            // (max(0, ·) copies): DDIR's signed order, read off the id order.
                            let split = start + vids[start..end].partition_point(|&v| v >> 63 == 0);
                            for k in (split..end).chain(start..split) {
                                for _ in 0..input[k].1.max(0) {
                                    elem_reps.push(input[k].0);
                                }
                            }
                            bracket_ends.push(elem_reps.len());
                        } else {
                            let lo = entry_reps.len();
                            for k in start..end {
                                entry_reps.push(input[k].0);
                                entry_diffs.push(input[k].1);
                                labels.push(bi as u64);
                            }
                            blocks.push((lo, entry_reps.len()));
                        }
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                let in_vals = recover_val(&self.in_vals);
                if !entry_reps.is_empty() {
                    let perm = sort_blocks(&labels, &signed_order_view(gather(&in_vals, &entry_reps))).0;
                    // Expand each bracket's sorted entries by their diff (max(0, ·) copies).
                    for (lo, hi) in blocks {
                        for &e in &perm[lo..hi] {
                            for _ in 0..entry_diffs[e].max(0) {
                                elem_reps.push(entry_reps[e]);
                            }
                        }
                        bracket_ends.push(elem_reps.len());
                    }
                }
                // A window whose lists are all empty still has an element SHAPE — the input
                // values' — and the column must carry it, or this batch's `List<()>` meets the
                // next batch's `List<T>` where the two are concatenated. `gather` at no indices
                // is the empty column of that shape.
                let elems = gather(&in_vals, &elem_reps);
                let col = present_val(CValue::List(Bounds::offsets(bracket_ends), Box::new(elems)));
                out_ids = val_ids(&col);
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
    type Times = ColTimes<T>;

    fn begin(&mut self, _description: Description<T>) {
        // Open the output session for this retire; reset the per-retire resolution pools.
        self.reset_pools();
        self.rows = (Vec::new(), Vec::new(), ColTimes::new(), Vec::new());
        self.leaf = None;
        self.leaf_ids.clear();
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>, changed: &[u64], from: &mut Option<u64>, window: &mut ReduceWindow<ColTimes<T>, Diff, Diff>) {
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
        // The leaf fast path is decided per retire from the stored shapes, which every chunk of
        // the arrangement shares.
        if self.leaf.is_none() && matches!(self.reducer, Reducer::Count | Reducer::Distinct | Reducer::Min) {
            let sample = chunks_of(instance.source_batches).into_iter().chain(novel_chunks.iter().copied())
                .chain(chunks_of(instance.output_batches)).find(|c| !c.diffs().is_empty());
            if let Some(c) = sample {
                if corgi::arrange::leaf_slice(c.keys()).is_some() && corgi::arrange::leaf_slice(c.vals()).is_some() {
                    self.leaf = Some((gather(c.keys(), &[]), gather(c.vals(), &[])));
                }
            }
        }
        let mut keys: Vec<u64> = Vec::new();
        // The seeds are the novel batches' RAW (key_hash, time) support, recorded here — before the
        // merged presentation below, whose consolidation may net a novel record away entirely. The
        // key hashes come from the scan the key list needs anyway; the times are row copies.
        for ch in novel_chunks.iter() {
            let khs = key_ids(ch.keys());
            for (i, kh) in khs.iter().enumerate() {
                window.seeds.push_from(*kh, ch.times(), i);
            }
            keys.extend(khs);
        }
        window.seeds.sort_dedup();
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
        let out_chunks = chunks_of(instance.output_batches);
        if self.leaf.is_some() {
            // The output of Count is its own id (a 1-field product of it), of Distinct the unit
            // and of Min an input value: the output column is rebuilt from ids at `finish`.
            let p = collect_present(&out_chunks, &keys, true);
            if !p.is_empty() && !merge_present(&p, &p.vids, &mut window.output) {
                p.extend_into(&p.vids, &mut window.output);
                window.output.consolidate();
            }
            return;
        }
        let p = collect_present(&out_chunks, &keys, false);
        if !p.is_empty() {
            let vids = val_ids(&p.vals_col);
            let merged = merge_present(&p, &vids, &mut window.output);
            if !merged {
                p.extend_into(&vids, &mut window.output);
                window.output.consolidate();
            }
            let Presented { keys_col, vals_col, khs, .. } = p;
            self.register_keys(keys_col, &khs);
            self.register_vals(vals_col, &vids);
        }
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Resolve input value_ids to `in_vals` rows, reduce (desired output), then difference the
        // desired against the presented current output per key: correction = desired − current.
        let in_rows: Vec<(usize, Diff)> = if self.leaf.is_some() {
            // A leaf value needs no row: its id is the value, and `vids` carries it.
            input.iter().enumerate().map(|(k, &(_, d))| (k, d)).collect()
        } else {
            input.iter()
                .map(|&(vid, d)| (*self.in_index.get(&vid).expect("input value_id presented this window"), d))
                .collect()
        };
        let in_vids: Vec<u64> = input.iter().map(|&(vid, _)| vid).collect();
        let (desired, desired_ends) = self.reduce_brackets(in_ends, &in_rows, &in_vids);

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

    fn emit(&mut self, records: &Bridge<ColTimes<T>, Diff>) {
        if self.leaf.is_some() {
            // The ids are the data: keep them, and the times as row copies.
            let (_, _, times, diffs) = &mut self.rows;
            for i in 0..records.len() {
                self.leaf_ids.push(records.ids[i]);
                times.push_ref(&records.times, i);
                diffs.push(records.diffs[i]);
            }
            return;
        }
        // Resolve each correction's key/value proxies to pool rows and accumulate; the time is a
        // row copy out of the tactic's column.
        for i in 0..records.len() {
            let (kh, vid) = records.ids[i];
            let kr = *self.key_index.get(&kh).expect("key resolvable this retire");
            let vr = *self.val_index.get(&vid).expect("value resolvable this retire");
            let (krows, vrows, times, diffs) = &mut self.rows;
            krows.push(kr);
            vrows.push(vr);
            times.push_ref(&records.times, i);
            diffs.push(records.diffs[i]);
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        if let Some((ktemplate, vtemplate)) = self.leaf.take() {
            let (_, _, times, diffs) = std::mem::take(&mut self.rows);
            let ids = std::mem::take(&mut self.leaf_ids);
            if times.is_empty() { return None; }
            let keys = leaf_like(&ktemplate, ids.iter().map(|id| id.0).collect());
            let vids: Vec<u64> = ids.iter().map(|id| id.1).collect();
            let vals = match self.reducer {
                Reducer::Count => CValue::Prod(vec![CValue::u64(vids)]),
                Reducer::Distinct => CValue::Unit(vids.len()),
                Reducer::Min => leaf_like(&vtemplate, vids),
                Reducer::Collect => unreachable!("the leaf fast path excludes Collect"),
            };
            return Some(Rc::new(columns_to_batch(keys, vals, times, diffs)));
        }
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

    /// Leaf Min and Collect read DDIR's signed order off the bracket's id order: the sign-bit half
    /// first. Checked against the structural path on the same brackets (a 1-field product is a
    /// leaf to `val_ids` but structured to `leaf_slice`... no: both peel, so the structured path
    /// is forced by wrapping the values in a 2-field product with a constant second field).
    #[test]
    fn leaf_min_and_collect_match_the_structural_order() {
        let vals: Vec<u64> = vec![5, (-3i64) as u64, 7, (-1i64) as u64, 0, 9];
        // brackets over rows {0,2,1}, {4,3}, {5}, {0} with a zero diff, in ascending id order.
        let ends = vec![3, 5, 6, 7];
        let input: Vec<(usize, Diff)> = vec![(0, 2), (2, 1), (1, 1), (4, 1), (3, -1), (5, 1), (0, 0)];
        let vids: Vec<u64> = input.iter().map(|&(r, _)| vals[r]).collect();
        for reducer in [Reducer::Min, Reducer::Collect] {
            let mut leaf = CorgiReduceBackend::<u64>::new(reducer.clone());
            leaf.in_vals = CValue::u64(vals.clone());
            let (leaf_out, leaf_ends) = leaf.reduce_brackets(&ends, &input, &vids);
            let mut structured = CorgiReduceBackend::<u64>::new(reducer.clone());
            structured.in_vals = CValue::Prod(vec![CValue::u64(vals.clone()), CValue::u64(vec![0; vals.len()])]);
            let (str_out, str_ends) = structured.reduce_brackets(&ends, &input, &vids);
            assert_eq!(leaf_ends, str_ends, "{reducer:?}");
            assert_eq!(leaf_out.len(), str_out.len(), "{reducer:?}");
            // compare the produced values through each pool, as the values the program sees.
            let leaf_pool = recover_val(&concat_columns(&leaf.val_blocks));
            let str_pool = recover_val(&concat_columns(&structured.val_blocks));
            let leaf_rows: Vec<usize> = leaf_out.iter().map(|(id, _)| leaf.val_index[id]).collect();
            let str_rows: Vec<usize> = str_out.iter().map(|(id, _)| structured.val_index[id]).collect();
            let got = gather(&leaf_pool, &leaf_rows);
            let want = match gather(&str_pool, &str_rows) {
                CValue::Prod(mut cols) => cols.remove(0),
                CValue::List(b, elems) => match *elems { CValue::Prod(mut cols) => CValue::List(b, Box::new(cols.remove(0))), _ => unreachable!() },
                other => other,
            };
            assert_eq!(got, want, "{reducer:?}");
        }
    }

    fn compound_keys(hashes: Vec<u64>, real: Vec<u64>) -> CValue {
        CValue::Prod(vec![CValue::u64(hashes), CValue::Prod(vec![CValue::u64(real), CValue::u64(vec![0, 0])])])
    }

    /// One chunk per run, presented at `changed`, merged: `(merged?, bridge as rows)`.
    fn present_runs<T: ColTime + Ord>(runs: Vec<(CValue, CValue, Vec<T>, Vec<Diff>)>, changed: &[u64]) -> (bool, Vec<((u64, u64), T, Diff)>) {
        let chunks: Vec<CorgiChunk<T, Diff>> = runs.into_iter().map(|(k, v, t, d)| CorgiChunk::from_columns(k, v, t.into_iter().collect(), d)).collect();
        let refs: Vec<&CorgiChunk<T, Diff>> = chunks.iter().collect();
        let p = collect_present(&refs, changed, false);
        let vids = val_ids(&p.vals_col);
        let mut bridge = Bridge::default();
        let merged = merge_present(&p, &vids, &mut bridge);
        if !merged {
            p.extend_into(&vids, &mut bridge);
            bridge.consolidate();
        }
        let rows = (0..bridge.len()).map(|i| (bridge.ids[i], bridge.times.get(i), bridge.diffs[i])).collect();
        (merged, rows)
    }

    #[test]
    fn merge_present_accepts_ordered_compound_keys() {
        let keys = compound_keys(vec![1, 2], vec![7, 8]);
        let vals = CValue::u64(vec![10, 20]);
        let (merged, bridge) = present_runs(vec![(keys, vals, vec![0u64, 0], vec![1, 1])], &[1, 2]);
        assert!(merged);
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
            let mut expected = BTreeMap::new();
            let mut columns = Vec::new();
            for run in &runs[..count] {
                for &(kv, time, diff) in run {
                    *expected.entry((kv, time)).or_insert(0) += diff;
                }
                columns.push((
                    CValue::u64(run.iter().map(|r| r.0.0).collect()),
                    CValue::u64(run.iter().map(|r| r.0.1).collect()),
                    run.iter().map(|r| r.1).collect::<Vec<_>>(),
                    run.iter().map(|r| r.2).collect::<Vec<_>>(),
                ));
            }
            let (merged, bridge) = present_runs(columns, &[1, 3, 4]);
            assert!(merged);
            let expected: Vec<_> = expected.into_iter().filter(|(_, d)| *d != 0)
                .map(|((kv, time), diff)| (kv, time, diff)).collect();
            assert_eq!(bridge, expected, "run count: {count}");
        }
    }

    /// A structured value under its hash lane merges in identifier order too, across runs,
    /// with equal values consolidating and the ids read off the lane.
    #[test]
    fn merge_present_accepts_hashed_values_across_runs() {
        use crate::corgi::chunk::present_val;
        let list = |rows: &[&[u64]]| {
            let mut ends = Vec::new();
            let mut elems = Vec::new();
            for r in rows { elems.extend_from_slice(r); ends.push(elems.len()); }
            present_val(CValue::List(Bounds::offsets(ends), Box::new(CValue::u64(elems))))
        };
        let a = (CValue::u64(vec![1, 1, 2]), list(&[&[5, 6], &[7], &[5, 6]]), vec![0u64, 1, 0], vec![1, 1, 1]);
        let b = (CValue::u64(vec![1, 2]), list(&[&[5, 6], &[9]]), vec![0u64, 0], vec![-1, 1]);
        let (merged, bridge) = present_runs(vec![a, b], &[1, 2]);
        assert!(merged);
        // key 1: [5,6] cancels at time 0, [7] stays at time 1. key 2: [5,6] and [9] at 0.
        let ids: Vec<u64> = corgi::hash(&CValue::List(Bounds::offsets(vec![2, 3, 4]), Box::new(CValue::u64(vec![5, 6, 7, 9]))));
        let mut want = vec![((1, ids[1]), 1u64, 1), ((2, ids[0]), 0, 1), ((2, ids[2]), 0, 1)];
        want.sort();
        assert_eq!(bridge, want);
    }

    #[test]
    fn merge_present_rejects_a_compound_hash_collision_within_a_run() {
        let keys = compound_keys(vec![1, 1], vec![7, 8]);
        let vals = CValue::u64(vec![10, 20]);
        let (merged, bridge) = present_runs(vec![(keys, vals, vec![0u64, 0], vec![1, 1])], &[1]);
        assert!(!merged);
        assert_eq!(bridge.len(), 2);
    }
}
