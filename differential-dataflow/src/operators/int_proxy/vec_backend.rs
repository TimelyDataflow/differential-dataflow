//! A reference [`ProxyReduceBackend`] over [`VecChunk`] storage.
//!
//! This backend exists to demonstrate the backend recipe in plain `Vec`-of-rows form, and to give
//! the proxy tactic a counterpart that can be tested and benchmarked against the cursor tactic on
//! identical storage: both can drive a reduction over the same hash-keyed
//! [`ChunkSpine`](crate::trace::chunk::vec::ChunkSpine) arrangement (see `tests/int_proxy.rs` and
//! `tests/int_proxy_bench.rs`).
//!
//! # The recipe
//!
//! The wiring arranges `coll.map(|(k, v)| (k.hashed(), (k, v)))` into a `ChunkSpine` whose key is
//! the `u64` key hash and whose value is the full `(key, val)` pair.
//!
//! Per window, the backend presents three proxy bridges — novel input, prior input, and prior output.
//! The identifiers chosen are based on ordinal position. Unfortunately, they are chosen independently
//! for the novel and prior inputs, meaning that empty input collections may be supplied to the logic
//! for evaluation, where the logic should be ignored and zero -> zero enforced. This could be fixed
//! with a more attentive identifier selection.
//!
//! The backend crawls all keys at once, rather than respecting the window setting.
//! This is a defect that should be fixed, but the backend shouldn't be used at scale.
//! The backend clones keys and values like it is being paid to waste cycles.
//! Generally, this is not a high-performance backend, but shouldn't be abysmal.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use timely::container::PushInto;
use timely::progress::Timestamp;

use crate::consolidation::{consolidate, consolidate_updates};
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::chunk::ChunkBatch;
use crate::trace::chunk::vec::VecChunk;
use crate::trace::Description;

use super::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

/// The batch type of a hash-keyed [`ChunkSpine`](crate::trace::chunk::vec::ChunkSpine): payload
/// `D` is `(K, V)` on the input side and `(K, W)` on the output side.
type VBatch<D, T, R> = Rc<ChunkBatch<VecChunk<u64, D, T, R>>>;

/// A reference [`ProxyReduceBackend`] over [`VBatch`] storage.
pub struct VecReduceBackend<K, V, W, T, R, L> {
    /// User supplied reduce closure.
    logic: L,
    /// Configuration: keys per window, to size the steps the backend performs.
    window_size: usize,

    /// All active keys, either novel input or supplied as externally changed.
    /// This is *not* windowed, which is a defect to fix.
    keys_cache: Vec<u64>,
    /// A state bit indicating that the keys cache should be rebuild (each begin).
    keys_stale: bool,

    /// Resolves an input value id (a per-window ordinal) to its `(key, val)` row.
    in_pool: Vec<(K, V)>,
    /// Resolves an output value id to its `(key, out)` row, for the current window only.
    out_pool: Vec<(K, W)>,
    /// Interns `(key, out)` rows to their output id, for the current window only.
    /// Lookup-only: the non-determinism of the map's iteration order is never observed.
    out_ids: HashMap<(K, W), u64>,

    /// The retire's output tile descriptions, and the chunks accumulated for each.
    tiles: Vec<Description<T>>,
    tile_chunks: Vec<Vec<VecChunk<u64, (K, W), T, R>>>,
    /// Scratch to re-order one `emit`'s output by types, rather than transient identifiers.
    stage: Vec<((u64, (K, W)), T, R)>,
}

impl<K, V, W, T, R, L> VecReduceBackend<K, V, W, T, R, L> {
    /// A backend deferring value semantics to `logic`, covering the key space in windows.
    pub fn new(logic: L) -> Self { Self::with_window(logic, 1 << 12) }

    /// A backend with an explicit window size, in keys.
    pub fn with_window(logic: L, window_size: usize) -> Self {
        VecReduceBackend {
            logic,
            window_size: window_size.max(1),
            keys_cache: Vec::new(),
            keys_stale: true,
            in_pool: Vec::new(),
            out_pool: Vec::new(),
            out_ids: HashMap::new(),
            tiles: Vec::new(),
            tile_chunks: Vec::new(),
            stage: Vec::new(),
        }
    }
}

impl<K, V, W, T, R, L> ProxyReduceBackend<VBatch<(K, V), T, R>, VBatch<(K, W), T, R>>
    for VecReduceBackend<K, V, W, T, R, L>
where
    K: Ord + Clone + std::hash::Hash + 'static,
    V: Ord + Clone + 'static,
    W: Ord + Clone + std::hash::Hash + 'static,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Clone + 'static,
    L: FnMut(&K, &[(V, R)], &mut Vec<(W, R)>, &mut Vec<(W, R)>),
{
    type RIn = R;
    type ROut = R;

    fn begin(&mut self, tiles: &[Description<T>]) {
        self.keys_stale = true;
        self.tiles = tiles.to_vec();
        self.tile_chunks = (0..tiles.len()).map(|_| Vec::new()).collect();
    }

    #[inline(never)]
    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, VBatch<(K, V), T, R>, VBatch<(K, W), T, R>>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<T, R, R>,
    ) {
        let Some(start) = *from else { return };

        // If the first window: form a list of all active keys, novel or changed.
        // TODO: this is wasteful; the in-order chunk keys could be merged instead.
        if self.keys_stale {
            self.keys_stale = false;
            self.keys_cache.clear();
            for batch in instance.input_batches.iter() {
                for chunk in batch.chunks.iter() {
                    self.keys_cache.extend(chunk.as_slice().iter().map(|r| r.0.0));
                }
            }
            self.keys_cache.sort_unstable();
            self.keys_cache.dedup();
            if !changed.is_empty() {
                let mut merged = Vec::with_capacity(self.keys_cache.len() + changed.len());
                let (mut a, mut b) = (0usize, 0usize);
                while a < self.keys_cache.len() || b < changed.len() {
                    let key = match (self.keys_cache.get(a), changed.get(b)) {
                        (Some(x), Some(y)) => *x.min(y),
                        (Some(x), None) => *x,
                        (None, Some(y)) => *y,
                        (None, None) => unreachable!("loop condition ensures one is present"),
                    };
                    if self.keys_cache.get(a) == Some(&key) { a += 1; }
                    if changed.get(b) == Some(&key) { b += 1; }
                    merged.push(key);
                }
                self.keys_cache = merged;
            }
        }

        // Determine the range of active keys to process in this window.
        let lo = self.keys_cache.partition_point(|k| *k < start);
        if lo == self.keys_cache.len() {
            *from = None;
            return;
        }
        let hi = (lo + self.window_size).min(self.keys_cache.len());
        *from = if hi == self.keys_cache.len() { None } else { Some(self.keys_cache[hi]) };
        let keys = &self.keys_cache[lo..hi];

        // The two input runs, presented apart on ordinal ids from a shared pool. Ids ascend with
        // the walk, so each bridge emerges sorted by `((hash, id), time)` with nothing to sort or
        // consolidate; a value in both runs gets two ids, reconciled by value in the corrections.
        self.in_pool.clear();
        let pool = &mut self.in_pool;
        let mut last: Option<u64> = None;
        merged_run(instance.source_batches, keys, |hash, data, time, diff| {
            if last != Some(hash) || pool.last() != Some(data) {
                pool.push(data.clone());
                last = Some(hash);
            }
            window.history.push(((hash, (pool.len() - 1) as u64), time.clone(), diff.clone()));
        });
        let mut last: Option<u64> = None;
        merged_run(instance.input_batches, keys, |hash, data, time, diff| {
            if last != Some(hash) || pool.last() != Some(data) {
                pool.push(data.clone());
                last = Some(hash);
            }
            window.novel.push(((hash, (pool.len() - 1) as u64), time.clone(), diff.clone()));
        });

        // The output history, interned into the id namespace corrections mint into.
        self.out_ids.clear();
        self.out_pool.clear();
        let (out_pool, out_ids) = (&mut self.out_pool, &mut self.out_ids);
        merged_run(instance.output_batches, keys, |hash, data, time, diff| {
            let id = *out_ids.entry(data.clone()).or_insert_with(|| {
                out_pool.push(data.clone());
                (out_pool.len() - 1) as u64
            });
            window.output.push(((hash, id), time.clone(), diff.clone()));
        });
    }

    #[inline(never)]
    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, R)],
        out_ends: &[usize],
        output: &[(u64, R)],
    ) -> (Vec<(u64, R)>, Vec<usize>) {
        let mut corr: Vec<(u64, R)> = Vec::new();
        let mut corr_ends: Vec<usize> = Vec::with_capacity(keys.len());
        let (mut is, mut os) = (0usize, 0usize);
        let mut updates: Vec<(W, R)> = Vec::new();
        let mut input_vals: Vec<(V, R)> = Vec::new();
        let mut current: Vec<(W, R)> = Vec::new();
        for i in 0..keys.len() {
            let (ie, oe) = (in_ends[i], out_ends[i]);
            // No-collision fast path: when the whole bracket is one real key, the per-key grouping
            // below can be skipped. Testing only the bracket's ENDPOINTS would be wrong: neither id
            // space is key-ordered across a bracket. Input ids are ordinals minted history-run
            // first and then novel, so the order is `history by key, then novel by key`; output ids
            // are interned, with corrections appended after the presentation. Either can read
            // `[A, B, A]`, whose endpoints agree while its interior does not. So resolve every id
            // and require them all to agree — a linear pass over data the fast path is about to
            // clone anyway, against the general path's per-key maps.
            let single_key: Option<K> = {
                let mut only: Option<&K> = None;
                let mut agree = true;
                for (vid, _) in &input[is..ie] {
                    let k = &self.in_pool[*vid as usize].0;
                    match only {
                        None => only = Some(k),
                        Some(prev) if prev == k => {}
                        Some(_) => { agree = false; break }
                    }
                }
                if agree {
                    for (vid, _) in &output[os..oe] {
                        let k = &self.out_pool[*vid as usize].0;
                        match only {
                            None => only = Some(k),
                            Some(prev) if prev == k => {}
                            Some(_) => { agree = false; break }
                        }
                    }
                }
                if agree { only.cloned() } else { None }
            };
            if let Some(key) = single_key {
                input_vals.clear();
                input_vals.extend(input[is..ie].iter().map(|(vid, d)| (self.in_pool[*vid as usize].1.clone(), d.clone())));
                consolidate(&mut input_vals);
                current.clear();
                current.extend(output[os..oe].iter().map(|(vid, d)| (self.out_pool[*vid as usize].1.clone(), d.clone())));
                consolidate(&mut current);
                updates.clear();
                (self.logic)(&key, &input_vals, &mut current, &mut updates);
                consolidate(&mut updates);
                for (w, d) in updates.drain(..) {
                    let key_w = (key.clone(), w);
                    let id = *self.out_ids.entry(key_w.clone()).or_insert_with(|| {
                        self.out_pool.push(key_w);
                        (self.out_pool.len() - 1) as u64
                    });
                    corr.push((id, d));
                }
            } else {
                let mut ins: BTreeMap<K, Vec<(V, R)>> = BTreeMap::new();
                for (vid, d) in &input[is..ie] {
                    let (k, v) = &self.in_pool[*vid as usize];
                    ins.entry(k.clone()).or_default().push((v.clone(), d.clone()));
                }
                let mut outs: BTreeMap<K, Vec<(W, R)>> = BTreeMap::new();
                for (vid, d) in &output[os..oe] {
                    let (k, w) = &self.out_pool[*vid as usize];
                    outs.entry(k.clone()).or_default().push((w.clone(), d.clone()));
                }
                let mut real_keys: Vec<K> = ins.keys().chain(outs.keys()).cloned().collect();
                real_keys.sort();
                real_keys.dedup();
                for key in real_keys {
                    let mut ivals = ins.remove(&key).unwrap_or_default();
                    consolidate(&mut ivals);
                    let mut cur = outs.remove(&key).unwrap_or_default();
                    consolidate(&mut cur);
                    updates.clear();
                    (self.logic)(&key, &ivals, &mut cur, &mut updates);
                    consolidate(&mut updates);
                    for (w, d) in updates.drain(..) {
                        let key_w = (key.clone(), w);
                        let id = *self.out_ids.entry(key_w.clone()).or_insert_with(|| {
                            self.out_pool.push(key_w);
                            (self.out_pool.len() - 1) as u64
                        });
                        corr.push((id, d));
                    }
                }
            }
            corr_ends.push(corr.len());
            is = ie;
            os = oe;
        }
        (corr, corr_ends)
    }

    #[inline(never)]
    fn emit(&mut self, tile: usize, records: &[((u64, u64), T, R)]) {
        self.stage.clear();
        for ((h, vid), t, d) in records {
            let row = self.out_pool[*vid as usize].clone();
            self.stage.push(((*h, row), t.clone(), d.clone()));
        }
        // TODO: could consolidate only within a hash key, rather than the whole chunk.
        consolidate_updates(&mut self.stage);
        let chunks = &mut self.tile_chunks[tile];
        for update in self.stage.drain(..) {
            if chunks.last().is_none_or(|c| c.as_slice().len() >= <VecChunk<u64, (K, W), T, R> as crate::trace::chunk::Chunk>::TARGET) {
                chunks.push(VecChunk::default());
            }
            chunks.last_mut().expect("pushed above if absent").push_into(update);
        }
    }

    #[inline(never)]
    fn finish(&mut self) -> Vec<VBatch<(K, W), T, R>> {
        self.in_pool.clear();
        self.out_pool.clear();
        self.out_ids.clear();
        let tiles = std::mem::take(&mut self.tiles);
        let tile_chunks = std::mem::take(&mut self.tile_chunks);
        tiles
            .into_iter()
            .zip(tile_chunks)
            .map(|(desc, chunks)| Rc::new(ChunkBatch::new(chunks, desc)))
            .collect()
    }
}

/// Merge-walks `batches`, restricted to `keys`, invoking `logic` on each consolidated non-zero update.
///
/// The merge-walk is in order of `(hash, data, time)`.
///
/// TODO: Not actually correct at the moment, in that the consolidation does not yet occur.
#[inline(never)]
fn merged_run<D, T, R>(
    batches: &[VBatch<D, T, R>],
    keys: &[u64],
    mut logic: impl FnMut(u64, &D, &T, &R),
) where
    D: Ord + Clone + 'static,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Clone + 'static,
{
    if keys.is_empty() {
        return;
    }
    let n = batches.len();
    let (mut ci, mut oi) = (vec![0usize; n], vec![0usize; n]);
    let mut cur: Vec<&[((u64, D), T, R)]> = vec![&[]; n];
    // Seek each batch to the first record with hash at or above the window's first key.
    let k0 = keys[0];
    for b in 0..n {
        let chunks = &batches[b].chunks;
        ci[b] = chunks.partition_point(|c| c.as_slice().last().is_some_and(|r| r.0.0 < k0));
        cur[b] = chunks.get(ci[b]).map(|c| c.as_slice()).unwrap_or(&[]);
        oi[b] = cur[b].partition_point(|r| r.0.0 < k0);
    }
    let mut scratch: Vec<(&D, &T, &R)> = Vec::new();
    let mut ki = 0usize;
    loop {
        // Least hash among the batch heads.
        let mut minh: Option<u64> = None;
        for b in 0..n {
            if let Some(r) = cur[b].get(oi[b]) {
                if minh.is_none_or(|m| r.0.0 < m) {
                    minh = Some(r.0.0);
                }
            }
        }
        let Some(h) = minh else { break };
        while ki < keys.len() && keys[ki] < h {
            ki += 1;
        }
        if ki >= keys.len() {
            break;
        }
        if keys[ki] != h {
            // `h` is not wanted. Seeking every batch to the next key that IS wanted costs a binary
            // search per batch and lands at or above it, so at most one unwanted hash is visited
            // per wanted key. Walking `h`'s records instead would make the whole pass linear in the
            // accumulated history rather than in what is asked for — which on an iterative
            // computation, where a retire asks for a handful of scattered keys, is the difference
            // between presenting the window and re-reading the trace. When the key set is dense
            // this branch is simply never taken, so there is no threshold to tune.
            let next = keys[ki];
            for b in 0..n {
                loop {
                    let Some(chunk) = batches[b].chunks.get(ci[b]) else { cur[b] = &[]; break };
                    let slice = chunk.as_slice();
                    if slice.last().is_some_and(|r| r.0.0 < next) {
                        ci[b] += 1;
                        oi[b] = 0;
                        continue;
                    }
                    cur[b] = slice;
                    oi[b] += slice[oi[b]..].partition_point(|r| r.0.0 < next);
                    // Chunks are non-empty (`ChunkBatch::new` asserts it) and the guard above
                    // skipped any whose last key is below `next`, so this chunk holds a record at
                    // or above `next` and the search cannot land at the end. The branch below is
                    // unreachable; it is kept so a violated invariant degrades to a slower walk
                    // rather than to a batch that silently reads as drained.
                    debug_assert!(oi[b] < slice.len(), "a chunk kept by the skip guard must hold a record at or above the sought key");
                    if oi[b] >= slice.len() {
                        ci[b] += 1;
                        oi[b] = 0;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }
        scratch.clear();
        for b in 0..n {
            while let Some(r) = cur[b].get(oi[b]) {
                if r.0.0 != h {
                    break;
                }
                scratch.push((&r.0.1, &r.1, &r.2));
                oi[b] += 1;
                if oi[b] >= cur[b].len() {
                    ci[b] += 1;
                    oi[b] = 0;
                    cur[b] = batches[b].chunks.get(ci[b]).map(|c| c.as_slice()).unwrap_or(&[]);
                }
            }
        }
        {
            scratch.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
            for (d, t, r) in scratch.drain(..) {
                logic(h, d, t, r);
            }
        }
    }
}
