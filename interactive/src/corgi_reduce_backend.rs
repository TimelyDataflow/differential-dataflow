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
use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::int_proxy::ProxyChunk;
use differential_dataflow::operators::int_proxy::ProxyReduceBackend;

use corgi::arrange::{gather, gather_lanes, hash_rows, sort_blocks};
use corgi::{Bounds, Value as CValue};

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
    /// Input value column aligned with the current input presentation's records (for Min/Collect).
    in_vals: CValue,
    /// The input presentation's `value_id` per record (Min reuses an input value's id).
    in_value_ids: Vec<u64>,
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
            in_value_ids: Vec::new(),
            key_index: IdMap::default(),
            key_blocks: Vec::new(),
            key_len: 0,
            val_index: IdMap::default(),
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
                out_ids = min_reps.iter().map(|&r| self.in_value_ids[r]).collect();
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
        if std::env::var("CORGI_DBG").is_ok() {
            eprintln!("  [RETIRE] changed_has_node2_hash? novel_chunks={} hist_chunks={}", chunks_of(novel).len(), chunks_of(history).len());
            // RAW: every node-2 record in the incoming arrangement batches (novel + history), BEFORE
            // present's changed-key filter — i.e. what the CorgiChunk arrange actually produced.
            for (tag, chs) in [("novel", chunks_of(novel)), ("hist", chunks_of(history))] {
                for ch in &chs {
                    if ch.keys().len() == 0 { continue; }
                    let kr = crate::corgi_logic::untranscode(ch.keys().clone(), &corgi::shape_of_value(ch.keys()));
                    let vr = crate::corgi_logic::untranscode(ch.vals().clone(), &corgi::shape_of_value(ch.vals()));
                    for i in 0..kr.len() {
                        if format!("{:?}", kr[i]).contains("Int(2)") {
                            eprintln!("  [RAW-{tag}] key={:?} val={:?} t={:?} d={}", kr[i], vr[i], ch.times()[i], ch.diffs()[i]);
                        }
                    }
                }
            }
            // PIN: what present actually returns to the tactic (after filter + consolidate).
            let kcol = gather(&keys_col, &reps);
            let krows = crate::corgi_logic::untranscode(kcol.clone(), &corgi::shape_of_value(&kcol));
            let vrows = crate::corgi_logic::untranscode(self.in_vals.clone(), &corgi::shape_of_value(&self.in_vals));
            for i in 0..chunk.len() {
                if format!("{:?}", krows[i]).contains("Int(2)") {
                    eprintln!("  [PIN] key={:?} val={:?} t={:?} d={}", krows[i], vrows[i], chunk.times()[i], chunk.diffs()[i]);
                }
            }
        }
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
        let rep_vals = gather(&vals_col, &reps);
        if std::env::var("CORGI_DBG").is_ok() {
            let kr = crate::corgi_logic::untranscode(rep_keys.clone(), &corgi::shape_of_value(&rep_keys));
            let vr = crate::corgi_logic::untranscode(rep_vals.clone(), &corgi::shape_of_value(&rep_vals));
            for i in 0..chunk.len() {
                if format!("{:?}", kr[i]).contains("Int(2)") {
                    eprintln!("  [POUT-current] key={:?} val={:?} t={:?} d={}", kr[i], vr[i], chunk.times()[i], chunk.diffs()[i]);
                }
            }
        }
        self.register_keys(rep_keys, chunk.key_hashes());
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
        if std::env::var("CORGI_DBG").is_ok() {
            let kr = crate::corgi_logic::untranscode(keys.clone(), &corgi::shape_of_value(&keys));
            let vr = crate::corgi_logic::untranscode(vals.clone(), &corgi::shape_of_value(&vals));
            for i in 0..records.len() {
                if format!("{:?}", kr[i]).contains("Int(2)") {
                    eprintln!("  [MAT-emit] key={:?} val={:?} t={:?} d={}", kr[i], vr[i], records.times()[i], records.diffs()[i]);
                }
            }
        }
        Rc::new(columns_to_batch(keys, vals, records.times().to_vec(), records.diffs().to_vec(), description))
    }
}
