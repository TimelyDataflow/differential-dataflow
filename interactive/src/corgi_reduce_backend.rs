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
//! pools), not by carrying `DValue`s. The one residual untranscode is Min/Collect's *ordering* of a
//! bracket's few candidate values — the reduction contract is DDIR `Ord`, which is not corgi's
//! structural order (unsigned/­kind-blind), so the semantic order is recovered per bracket; the
//! chosen value ROWS are still taken columnar (`gather`), never rebuilt.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::collections::HashMap;
use std::rc::Rc;

use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::int_proxy::ProxyChunk;
use differential_dataflow::operators::int_proxy::ProxyReduceBackend;

use corgi::arrange::{gather, gather_lanes, hash_rows};
use corgi::{Bounds, Value as CValue};

use crate::corgi_chunk::{columns_to_batch, CorgiChunk};
use crate::corgi_logic::untranscode;
use crate::ir::Diff;
use crate::parse::Reducer;

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// A corgi reduce backend for a single `Reducer`. All per-retire scratch is corgi columns + integer
/// id→row-index maps; nothing carries a `DValue`.
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    /// Input value column aligned with the current input presentation's records (for Min/Collect).
    in_vals: CValue,
    /// The input presentation's `value_id` per record (Min reuses an input value's id).
    in_value_ids: Vec<u64>,
    /// Key-resolution pool for the current retire: `key_hash → row index` into the concatenation of
    /// `key_blocks` (representative keys from the input + output presentations).
    key_index: HashMap<u64, usize>,
    key_blocks: Vec<CValue>,
    key_len: usize,
    /// Value-resolution pool for the current retire: `value_id → row index` into the concatenation of
    /// `val_blocks` (output-history values + values minted by `reduce_many`).
    val_index: HashMap<u64, usize>,
    val_blocks: Vec<CValue>,
    val_len: usize,
    _t: std::marker::PhantomData<T>,
}

impl<T> CorgiReduceBackend<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceBackend {
            reducer,
            in_vals: CValue::Unit(0),
            in_value_ids: Vec::new(),
            key_index: HashMap::new(),
            key_blocks: Vec::new(),
            key_len: 0,
            val_index: HashMap::new(),
            val_blocks: Vec::new(),
            val_len: 0,
            _t: std::marker::PhantomData,
        }
    }

    /// Clear the resolution pools at the start of a retire.
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
        let kh = hash_rows(ch.keys());
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
                out_ids = hash_rows(&col);
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
                out_ids = hash_rows(&col); // all equal (unit content hash)
                self.register_vals(col, &out_ids);
            }
            Reducer::Min => {
                // The DDIR `min` over the positive-diff values. Order is DDIR `Ord`, not corgi's, so
                // untranscode the (few) candidate rows to pick the winner; the winning ROW is then
                // taken columnar and reuses its input value id.
                let mut chosen: Vec<usize> = Vec::new(); // input presentation rep indices
                let mut start = 0;
                for &end in ends {
                    let cand: Vec<usize> = (start..end).filter(|&k| input[k].1 > 0).collect();
                    if !cand.is_empty() {
                        let idx: Vec<usize> = cand.iter().map(|&k| input[k].0).collect();
                        let g = gather(&self.in_vals, &idx);
                        let dv = untranscode(g.clone(), &corgi::shape_of_value(&g));
                        let best = (0..dv.len()).min_by(|&a, &b| dv[a].cmp(&dv[b])).unwrap();
                        chosen.push(input[cand[best]].0);
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if chosen.is_empty() {
                    return (Vec::new(), out_ends);
                }
                let col = gather(&self.in_vals, &chosen);
                out_ids = chosen.iter().map(|&r| self.in_value_ids[r]).collect();
                self.register_vals(col, &out_ids);
            }
            Reducer::Collect => {
                // One row per bracket: the sorted (DDIR `Ord`) values, each repeated by its diff, as a
                // `List`. Element rows are taken columnar; only the ordering needs untranscode.
                let mut elem_reps: Vec<usize> = Vec::new();
                let mut bracket_ends: Vec<usize> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for &end in ends {
                    let ks: Vec<usize> = (start..end).collect();
                    let idx: Vec<usize> = ks.iter().map(|&k| input[k].0).collect();
                    let g = gather(&self.in_vals, &idx);
                    let dv = untranscode(g.clone(), &corgi::shape_of_value(&g));
                    let mut order: Vec<usize> = (0..ks.len()).collect();
                    order.sort_by(|&a, &b| dv[a].cmp(&dv[b]));
                    for oi in order {
                        for _ in 0..input[ks[oi]].1.max(0) {
                            elem_reps.push(input[ks[oi]].0);
                        }
                    }
                    bracket_ends.push(elem_reps.len());
                    out_diffs.push(1);
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                let elems = if elem_reps.is_empty() { CValue::Unit(0) } else { gather(&self.in_vals, &elem_reps) };
                let col = CValue::List(Bounds::Offsets(bracket_ends), Box::new(elems));
                out_ids = hash_rows(&col);
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

    fn key_hashes(&self, batches: &[CBatch<T>]) -> Vec<u64> {
        let mut hashes = Vec::new();
        for ch in chunks_of(batches) {
            hashes.extend(hash_rows(ch.keys()));
        }
        hashes.sort_unstable();
        hashes.dedup();
        hashes
    }

    fn present_input(&mut self, history: &[CBatch<T>], novel: &[CBatch<T>], keys: &[u64]) -> ProxyChunk<T, Diff> {
        self.reset_pools();

        // Read novel whole + history filtered to the changed keys (columnar semijoin). Novel chunks
        // come first, so the first `novel_len` candidate records are the (unfiltered) novel side.
        let mut chunks = chunks_of(novel);
        chunks.extend(chunks_of(history));
        let novel_len: usize = chunks_of(novel).iter().map(|c| c.keys().len()).sum();
        let mut seen = 0usize;
        let (keys_col, vals_col, khs, times, diffs) = collect_present(&chunks, |h| {
            let keep = seen < novel_len || keys.binary_search(&h).is_ok();
            seen += 1;
            keep
        });

        if khs.is_empty() {
            self.in_vals = CValue::Unit(0);
            self.in_value_ids = Vec::new();
            return ProxyChunk::default();
        }
        let vids = hash_rows(&vals_col);
        let (chunk, reps) = ProxyChunk::from_unsorted(khs, vids, times, diffs);
        self.in_vals = gather(&vals_col, &reps);
        self.in_value_ids = chunk.value_ids().to_vec();
        // Register representative keys (aligned with the sorted presentation) for materialize.
        let rep_keys = gather(&keys_col, &reps);
        self.register_keys(rep_keys, chunk.key_hashes());
        chunk
    }

    fn present_output(&mut self, batches: &[CBatch<T>], keys: &[u64]) -> ProxyChunk<T, Diff> {
        let chunks = chunks_of(batches);
        let (keys_col, vals_col, khs, times, diffs) = collect_present(&chunks, |h| keys.binary_search(&h).is_ok());
        if khs.is_empty() {
            return ProxyChunk::default();
        }
        let vids = hash_rows(&vals_col);
        let (chunk, reps) = ProxyChunk::from_unsorted(khs, vids, times, diffs);
        // Register representative output keys and values (aligned with the sorted presentation).
        let rep_keys = gather(&keys_col, &reps);
        self.register_keys(rep_keys, chunk.key_hashes());
        let rep_vals = gather(&vals_col, &reps);
        self.register_vals(rep_vals, chunk.value_ids());
        chunk
    }

    fn reduce(&mut self, _key_hash: u64, input: &[(usize, Diff)]) -> Vec<(u64, Diff)> {
        self.reduce_brackets(&[input.len()], input).0
    }

    fn reduce_many(&mut self, _keys: &[u64], ends: &[usize], input: &[(usize, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        self.reduce_brackets(ends, input)
    }

    fn materialize(&mut self, records: ProxyChunk<T, Diff>, description: Description<T>) -> CBatch<T> {
        let key_pool = concat_columns(&self.key_blocks);
        let val_pool = concat_columns(&self.val_blocks);
        let kidx: Vec<usize> = (0..records.len())
            .map(|i| *self.key_index.get(&records.key_hashes()[i]).expect("key presented this retire"))
            .collect();
        let vidx: Vec<usize> = (0..records.len())
            .map(|i| *self.val_index.get(&records.value_ids()[i]).expect("value presented or minted this retire"))
            .collect();
        let keys = gather(&key_pool, &kidx);
        let vals = gather(&val_pool, &vidx);
        Rc::new(columns_to_batch(keys, vals, records.times().to_vec(), records.diffs().to_vec(), description))
    }
}
