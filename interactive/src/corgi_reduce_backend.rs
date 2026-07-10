//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_hash, value_id, time, diff)`; this backend supplies only:
//!
//!   * hashing — `key_hash`/`value_id` are corgi `hash_rows` over the corgi key/val COLUMNS
//!     (columnar, content-addressed, so ids coincide across the output→input boundary); DD never
//!     hashes.
//!   * the value callback — `reduce_many` runs ONE crossing per retire over every `(key, time)`
//!     bracket, building the output value COLUMNS directly (Count → a `u64` prim, Distinct → a
//!     `Unit`, Min → the chosen input rows, Collect → a `List`), never through DDIR rows.
//!   * materialize — resolve proxy ids back to real columns by `gather` from per-retire pools and
//!     seal a `CorgiChunk` batch column-natively.
//!
//! Transcode-free: the real keys/values never leave corgi columns. Ids are resolved to rows by
//! integer index (`key_index`/`val_index` → offsets into the concatenated `key_blocks`/`val_blocks`
//! pools), not by carrying `DValue`s. Min/Collect's ordering is corgi's own — one `sort_blocks` per
//! retire orders every bracket's candidates (Min = each block's first, Collect = each block's sorted
//! run, expanded by diff). This uses corgi's STRUCTURAL order, which equals DDIR `Ord` for the
//! non-negative scalar/tuple values these reductions see (all 6 canonical programs); it diverges only
//! for negative ints (corgi's leaf compare is unsigned) and list-valued compares (corgi lists order
//! length-first) — neither arises here. A signed/​list-general order would need a corgi order fix
//! (offset-binary leaf or lex-first lists), not a change here.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;

use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::int_proxy::ProxyBridge;
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{gather, gather_lanes, hash_rows, sort_blocks};
use corgi::{Bounds, Shape, Value as CValue};

use crate::corgi_chunk::{columns_to_batch, CorgiChunk};
use crate::ir::Diff;
use crate::parse::Reducer;

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

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
    /// Output tiling for `begin`/`emit`/`finish`: the tile descriptions, and per-tile accumulated
    /// output rows `(key row, value row, time, diff)` (pool indices, gathered into columns at `finish`).
    tiles: Vec<Description<T>>,
    tile_rows: Vec<(Vec<usize>, Vec<usize>, Vec<T>, Vec<Diff>)>,
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
            tiles: Vec::new(),
            tile_rows: Vec::new(),
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

/// Id column for a key/value column. For a PRIMITIVE column — a bare 64-bit `Prim`, or a 1-field
/// `Prod([Prim(64)])` — the value itself is already a collision-free id (`i64 as u64` is a bijection),
/// so pass it straight through and skip the `hash_rows` content hash. Compound shapes (Unit / List /
/// Sum / multi-field `Prod`) still hash. The id is used ONLY as an identity for netting/dedup — its
/// numeric order is never relied upon — so the raw two's-complement `u64` is correct even for negative
/// ints (no order-preserving swizzle needed). Must be applied CONSISTENTLY at every id site (both
/// value presentations AND the freshly-produced `reduce_brackets` outputs), else `desired − current`
/// nets across mismatched ids for the same value.
fn ids(col: &CValue) -> Vec<u64> {
    match corgi::shape_of_value(col) {
        Shape::Prim(64) => col.clone().into_u64("ids"),
        Shape::Prod(ref fs) if fs.len() == 1 && matches!(fs[0], Shape::Prim(64)) => match col {
            CValue::Prod(fields) => fields[0].clone().into_u64("ids"),
            _ => unreachable!("shape Prod but value not Prod"),
        },
        _ => hash_rows(col),
    }
}

/// Concatenate selected records across a run of chunks into parallel `(keys_col, vals_col)` corgi
/// columns plus per-record `(key_hash, time, diff)`. `keep(kh)` decides inclusion by key hash.
fn collect_present<T, F>(chunks: &[&CorgiChunk<T, Diff>], mut keep: F) -> (CValue, CValue, Vec<u64>, Vec<T>, Vec<Diff>)
where
    T: Timestamp + Lattice,
    F: FnMut(u64) -> bool,
{
    let key_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.keys())).collect();
    let val_srcs: Vec<Option<&CValue>> = chunks.iter().map(|c| Some(c.vals())).collect();
    let (mut tags, mut offs) = (Vec::new(), Vec::new());
    let (mut khs, mut times, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
    for (ci, ch) in chunks.iter().enumerate() {
        let kh = ids(ch.keys());
        for i in 0..kh.len() {
            if keep(kh[i]) {
                tags.push(ci);
                offs.push(i);
                khs.push(kh[i]);
                times.push(ch.times()[i].clone());
                diffs.push(ch.diffs()[i]);
            }
        }
    }
    if tags.is_empty() {
        return (CValue::Unit(0), CValue::Unit(0), khs, times, diffs);
    }
    let keys_col = gather_lanes(&key_srcs, &tags, &offs);
    let vals_col = gather_lanes(&val_srcs, &tags, &offs);
    (keys_col, vals_col, khs, times, diffs)
}

/// All chunks of a batch list, flattened (empty chunks included — `hash_rows` yields nothing for them).
fn chunks_of<T>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>>
where
    T: Timestamp + Lattice,
{
    batches.iter().flat_map(|b| b.chunks.iter()).collect()
}

impl<T> CorgiReduceBackend<T>
where
    T: Timestamp + Lattice + Ord,
{
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
                // Present iff any value has positive net; output value is unit (a `Unit` column).
                let mut present = 0usize;
                let mut start = 0;
                for &end in ends {
                    if input[start..end].iter().any(|&(_, d)| d > 0) {
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
                // The DDIR `min` over the positive-diff values, in corgi's structural order (== DDIR
                // `Ord` for the non-negative scalar/tuple values these reductions see; see module doc).
                // Gather all positive-diff candidates across brackets into one column, segment by
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
                        if input[k].1 > 0 {
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
                let (perm, _) = sort_blocks(&labels, &cand_col);
                let min_reps: Vec<usize> = block_starts.iter().map(|&lo| cand_reps[perm[lo]]).collect();
                let col = gather(&self.in_vals, &min_reps);
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
            Reducer::Collect => {
                // One row per bracket: the values sorted in corgi structural order (== DDIR `Ord` here),
                // each repeated by its diff, as a `List`. One `sort_blocks` orders every bracket's
                // entries at once; element rows are then taken columnar. Every bracket emits (empty
                // list if all diffs ≤ 0), matching the row reducer.
                let mut entry_reps: Vec<usize> = Vec::new();
                let mut entry_diffs: Vec<Diff> = Vec::new();
                let mut labels: Vec<u64> = Vec::new();
                let mut blocks: Vec<(usize, usize)> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for (bi, &end) in ends.iter().enumerate() {
                    let lo = entry_reps.len();
                    for k in start..end {
                        entry_reps.push(input[k].0);
                        entry_diffs.push(input[k].1);
                        labels.push(bi as u64);
                    }
                    blocks.push((lo, entry_reps.len()));
                    out_diffs.push(1);
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
                let elems = if elem_reps.is_empty() { CValue::Unit(0) } else { gather(&self.in_vals, &elem_reps) };
                let col = CValue::List(Bounds::Offsets(bracket_ends), Box::new(elems));
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
        }

        let outs = out_ids.into_iter().zip(out_diffs).collect();
        (outs, out_ends)
    }
}

impl<T> ProxyReduceBackend<CBatch<T>, CBatch<T>> for CorgiReduceBackend<T>
where
    T: Timestamp + Lattice + Ord,
{
    type RIn = Diff;
    type ROut = Diff;

    fn seed_times(&self, instance: &ReduceInstance<'_, CBatch<T>, CBatch<T>>) -> Vec<(u64, T)> {
        // The batch's raw (key_hash, time) support — hash the novel KEY columns only, one entry per
        // record, sorted by key_hash. Seeds may over-derive (a non-changing seed yields a zero delta),
        // so this superset of b.support suffices; `instance.lower` is not applied (see ReduceInstance).
        let mut out: Vec<(u64, T)> = Vec::new();
        for ch in chunks_of(instance.input_batches) {
            let kh = ids(ch.keys());
            for (h, t) in kh.into_iter().zip(ch.times()) {
                out.push((h, t.clone()));
            }
        }
        out.sort_by_key(|(k, _)| *k);
        out
    }

    fn begin(&mut self, tiles: &[Description<T>]) {
        // Open a tiled output session for this retire; reset the per-retire resolution pools.
        self.reset_pools();
        self.tiles = tiles.to_vec();
        self.tile_rows = (0..tiles.len()).map(|_| (Vec::new(), Vec::new(), Vec::new(), Vec::new())).collect();
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, CBatch<T>, CBatch<T>>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<T, Diff, Diff>> {
        // Single window: present ALL remaining changed keys at once (bounded-memory windowing is a
        // later refinement). `changed` is ascending, so `binary_search` is the changed-key filter.
        if *cursor >= changed.len() {
            return None;
        }
        let keys: Vec<u64> = changed[*cursor..].to_vec();
        *cursor = changed.len();
        let present = |chunks: &[&CorgiChunk<T, Diff>]| collect_present(chunks, |h| keys.binary_search(&h).is_ok());

        // Input presentation: accumulated history ∪ novel delta, restricted to the window's keys.
        // value_id = content hash of the value (equal values share an id → the tactic nets them);
        // `in_index` resolves an id back to a representative in_vals row for `reduce_corrections`.
        let mut in_chunks = chunks_of(instance.input_batches);
        in_chunks.extend(chunks_of(instance.source_batches));
        let (in_keys, in_vals, in_khs, in_times, in_diffs) = present(&in_chunks);
        self.in_index = IdMap::default();
        let input: ProxyBridge<T, Diff> = if in_khs.is_empty() {
            self.in_vals = CValue::Unit(0);
            Vec::new()
        } else {
            let vids = ids(&in_vals);
            for (r, &vid) in vids.iter().enumerate() { self.in_index.entry(vid).or_insert(r); }
            self.in_vals = in_vals;
            self.register_keys(in_keys, &in_khs);
            let mut b: ProxyBridge<T, Diff> =
                (0..in_khs.len()).map(|i| ((in_khs[i], vids[i]), in_times[i].clone(), in_diffs[i])).collect();
            consolidate_updates(&mut b);
            b
        };

        // Output-history presentation, same keys (register keys + values for correction resolution).
        let (o_keys, o_vals, o_khs, o_times, o_diffs) = present(&chunks_of(instance.output_batches));
        let output: ProxyBridge<T, Diff> = if o_khs.is_empty() {
            Vec::new()
        } else {
            let vids = ids(&o_vals);
            self.register_keys(o_keys, &o_khs);
            self.register_vals(o_vals, &vids);
            let mut b: ProxyBridge<T, Diff> =
                (0..o_khs.len()).map(|i| ((o_khs[i], vids[i]), o_times[i].clone(), o_diffs[i])).collect();
            consolidate_updates(&mut b);
            b
        };

        Some(ReduceWindow { keys, input, output })
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

    fn emit(&mut self, tile: usize, records: &[((u64, u64), T, Diff)]) {
        // Resolve each correction's key/value proxies to pool rows and accumulate into the tile.
        for rec in records {
            let ((kh, vid), t, d) = (rec.0, &rec.1, rec.2);
            let kr = *self.key_index.get(&kh).expect("key resolvable this retire");
            let vr = *self.val_index.get(&vid).expect("value resolvable this retire");
            let (krows, vrows, times, diffs) = &mut self.tile_rows[tile];
            krows.push(kr);
            vrows.push(vr);
            times.push(t.clone());
            diffs.push(d);
        }
    }

    fn finish(&mut self) -> Vec<CBatch<T>> {
        // Seal each tile: gather its accumulated (key, val) pool rows into columns, one CorgiChunk batch.
        let key_pool = concat_columns(&self.key_blocks);
        let val_pool = concat_columns(&self.val_blocks);
        let tiles = std::mem::take(&mut self.tiles);
        let tile_rows = std::mem::take(&mut self.tile_rows);
        tiles.into_iter().zip(tile_rows).map(|(desc, (krows, vrows, times, diffs))| {
            let keys = gather(&key_pool, &krows);
            let vals = gather(&val_pool, &vrows);
            Rc::new(columns_to_batch(keys, vals, times, diffs, desc))
        }).collect()
    }
}
