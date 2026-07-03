//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_hash, value_id, time, diff)`; this backend supplies only:
//!
//!   * hashing — `key_hash`/`value_id` are corgi `hash_rows` over the corgi key/val COLUMNS
//!     (columnar, content-addressed, so ids coincide across the output→input boundary); DD never
//!     hashes.
//!   * the value callback — `reduce_many` runs ONE crossing per retire over every `(key, time)`
//!     bracket (Count/Distinct are integer-only; Min/Collect gather+untranscode the needed values
//!     once, never once per key), then mints output ids by hashing the produced value column.
//!   * materialize — resolve ids back to real `(key, val)` rows and seal a `CorgiChunk` batch.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read; a `find`-seek was measured slower).

use std::collections::HashMap;
use std::rc::Rc;

use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Builder as _;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::int_proxy::ProxyChunk;
use differential_dataflow::operators::int_proxy::ProxyReduceBackend;

use corgi::arrange::{gather, gather_lanes, hash_rows};
use corgi::Value as CValue;

use crate::corgi_chunk::{CorgiChunk, CorgiChunkBuilder};
use crate::corgi_logic::{infer_shape_cols, transcode, untranscode};
use crate::ir::{Diff, Value as DValue};
use crate::parse::Reducer;

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// A corgi reduce backend for a single `Reducer`. Per-retire scratch (`in_vals`, `out_vals`) is set
/// by the presentations and read by `reduce_many`/`materialize`; `keys` persists changed keys'
/// `key_hash → key` across retires (so pending keys survive without touching backend state).
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    /// `key_hash → key row` for the changed keys: primed from each retire's novel batches, pruned to
    /// the changed set on entry (so pending keys survive between retires and nothing else accretes).
    keys: HashMap<u64, DValue>,
    /// Val column aligned with the current input presentation's records (rep rows), for Min/Collect.
    in_vals: CValue,
    /// `value_id → output value` for the current retire: primed by `present_output`, extended by
    /// minting in `reduce_many`; rebuilt fresh each retire.
    out_vals: HashMap<u64, DValue>,
    _t: std::marker::PhantomData<T>,
}

impl<T> CorgiReduceBackend<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceBackend {
            reducer,
            keys: HashMap::new(),
            in_vals: CValue::Unit(0),
            out_vals: HashMap::new(),
            _t: std::marker::PhantomData,
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
    /// The one value crossing for a retire: every `(key, time)` bracket at once. Count/Distinct read
    /// only diffs; Min/Collect gather+untranscode the bracket values ONCE (not per key). Output ids
    /// are minted by hashing the produced value column (one `hash_rows`), agreeing with
    /// `present_output` on equal values.
    fn reduce_brackets(&mut self, ends: &[usize], input: &[(usize, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Values are only needed for the order/content-sensitive reducers.
        let need_vals = matches!(self.reducer, Reducer::Min | Reducer::Collect);
        let vals_rows: Vec<DValue> = if need_vals && !input.is_empty() {
            let idx: Vec<usize> = input.iter().map(|&(i, _)| i).collect();
            let g = gather(&self.in_vals, &idx);
            untranscode(g.clone(), &corgi::shape_of_value(&g))
        } else {
            Vec::new()
        };

        let mut produced: Vec<(DValue, Diff)> = Vec::new();
        let mut out_ends = Vec::with_capacity(ends.len());
        let mut start = 0;
        for &end in ends {
            match self.reducer {
                Reducer::Count => {
                    let c: Diff = input[start..end].iter().map(|&(_, d)| d).sum();
                    if c > 0 {
                        produced.push((DValue::Tuple(vec![DValue::Int(c)]), 1));
                    }
                }
                Reducer::Distinct => {
                    if input[start..end].iter().any(|&(_, d)| d > 0) {
                        produced.push((DValue::unit(), 1));
                    }
                }
                Reducer::Min => {
                    let m = (start..end)
                        .filter(|&k| input[k].1 > 0)
                        .map(|k| &vals_rows[k])
                        .min();
                    if let Some(m) = m {
                        produced.push((m.clone(), 1));
                    }
                }
                Reducer::Collect => {
                    // The reducer contract presents values in `Ord` order; the bracket is in
                    // value-id (hash) order, so sort by value first — the List's element order
                    // is value-significant (unlike Min/Count/Distinct).
                    let mut order: Vec<usize> = (start..end).collect();
                    order.sort_by(|&a, &b| vals_rows[a].cmp(&vals_rows[b]));
                    let mut items = Vec::new();
                    for k in order {
                        for _ in 0..input[k].1.max(0) {
                            items.push(vals_rows[k].clone());
                        }
                    }
                    produced.push((DValue::List(items), 1));
                }
            }
            out_ends.push(produced.len());
            start = end;
        }

        // Mint output ids: hash the produced value column columnar, and record id → value.
        if produced.is_empty() {
            return (Vec::new(), out_ends);
        }
        let vrows: Vec<DValue> = produced.iter().map(|(v, _)| v.clone()).collect();
        let col = transcode(&vrows, &infer_shape_cols(&vrows));
        let ids = hash_rows(&col);
        for (&id, (v, _)) in ids.iter().zip(&produced) {
            self.out_vals.entry(id).or_insert_with(|| v.clone());
        }
        let outs = ids.into_iter().zip(produced).map(|(id, (_, d))| (id, d)).collect();
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
        // Prune hash→key to the changed set (what survives = the pending keys), and shrink so a past
        // big retire's table doesn't tax later walks.
        self.keys.retain(|h, _| keys.binary_search(h).is_ok());
        self.keys.shrink_to_fit();

        // Prime the map from the novel batches (delta-sized, all keys changed): untranscode their key
        // columns once and record hash → key.
        for ch in chunks_of(novel) {
            if ch.keys().len() == 0 {
                continue;
            }
            let kh = hash_rows(ch.keys());
            let krows = untranscode(ch.keys().clone(), &corgi::shape_of_value(ch.keys()));
            for (h, k) in kh.into_iter().zip(krows) {
                self.keys.entry(h).or_insert(k);
            }
        }

        // Read novel whole + history filtered to the changed keys (columnar semijoin).
        let mut chunks = chunks_of(novel);
        chunks.extend(chunks_of(history));
        let novel_len: usize = chunks_of(novel).iter().map(|c| c.keys().len()).sum();
        let mut seen = 0usize;
        let (_keys_col, vals_col, khs, times, diffs) = collect_present(&chunks, |h| {
            let keep = seen < novel_len || keys.binary_search(&h).is_ok();
            seen += 1;
            keep
        });
        // NB: `seen` advances per candidate record in chunk order; novel chunks come first, so the
        // first `novel_len` records are the (unfiltered) novel side and the rest is history.

        if khs.is_empty() {
            self.in_vals = CValue::Unit(0);
            return ProxyChunk::default();
        }
        let vids = hash_rows(&vals_col);
        let (chunk, reps) = ProxyChunk::from_unsorted(khs, vids, times, diffs);
        self.in_vals = gather(&vals_col, &reps);
        chunk
    }

    fn present_output(&mut self, batches: &[CBatch<T>], keys: &[u64]) -> ProxyChunk<T, Diff> {
        self.out_vals = HashMap::new();
        let chunks = chunks_of(batches);
        let (_keys_col, vals_col, khs, times, diffs) = collect_present(&chunks, |h| keys.binary_search(&h).is_ok());
        if khs.is_empty() {
            return ProxyChunk::default();
        }
        let vids = hash_rows(&vals_col);
        let (chunk, reps) = ProxyChunk::from_unsorted(khs, vids, times, diffs);
        // Record value_id → output value for materialize (representative rows, by the reps).
        let rep_vals = gather(&vals_col, &reps);
        let vrows = untranscode(rep_vals, &corgi::shape_of_value(&vals_col));
        for (i, v) in vrows.into_iter().enumerate() {
            self.out_vals.entry(chunk.value_ids()[i]).or_insert(v);
        }
        chunk
    }

    fn reduce(&mut self, _key_hash: u64, input: &[(usize, Diff)]) -> Vec<(u64, Diff)> {
        self.reduce_brackets(&[input.len()], input).0
    }

    fn reduce_many(&mut self, _keys: &[u64], ends: &[usize], input: &[(usize, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        self.reduce_brackets(ends, input)
    }

    fn materialize(&mut self, records: ProxyChunk<T, Diff>, description: Description<T>) -> CBatch<T> {
        let mut rows: Vec<((DValue, DValue), T, Diff)> = (0..records.len())
            .map(|i| {
                let k = self.keys.get(&records.key_hashes()[i]).expect("key presented this retire").clone();
                let v = self.out_vals.get(&records.value_ids()[i]).expect("value presented or minted this retire").clone();
                ((k, v), records.times()[i].clone(), records.diffs()[i])
            })
            .collect();
        let mut builder = CorgiChunkBuilder::<T, Diff>::with_capacity(0, 0, rows.len());
        builder.push(&mut rows);
        Rc::new(builder.done(description))
    }
}
