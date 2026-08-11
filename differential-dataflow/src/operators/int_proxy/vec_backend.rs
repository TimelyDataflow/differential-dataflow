//! A reference [`ProxyReduceBackend`] over [`VecChunk`] storage.
//!
//! This backend exists to demonstrate the backend recipe in plain `Vec`-of-rows form, and to give
//! the proxy tactic a counterpart that can be tested and benchmarked against the cursor tactic on
//! identical storage: both can drive a reduction over the same hash-keyed
//! [`ChunkSpine`](crate::trace::chunk::vec::ChunkSpine) arrangement (see `tests/int_proxy.rs` and
//! `tests/int_proxy_bench.rs`). Unlike the corgi backend it covers the key space in bounded
//! windows, exercising the harness's multi-window path.
//!
//! # The recipe
//!
//! The wiring arranges `coll.map(|(k, v)| (k.hashed(), (k, v)))` into a `ChunkSpine` whose key is
//! the `u64` key hash and whose value is the full `(key, val)` pair. Retaining the real key as
//! data is what makes hash collisions survivable: the operator isolates work by hash, and this
//! backend re-groups each hash bracket by the real key before applying `logic` (see
//! [`the module docs`](super) on the two integers).
//!
//! Per window, the backend presents three runs — accumulated history, the novel delta, and the
//! output history — as `((hash, value_id), time, diff)` bridges. Input value ids are **ordinals**:
//! minted in presentation order (which is `(hash, value, time)` order, so bridges emerge sorted),
//! resolved through a per-window pool, and never persisted. The history and novel runs mint ids
//! independently; a value present in both gets two ids, which is harmless because
//! [`reduce_corrections`](ProxyReduceBackend::reduce_corrections) resolves ids to values and
//! consolidates *by value* before applying `logic`. Output ids are interned (value -> id) instead,
//! because corrections mint values that must share the namespace of the presented output history.
//!
//! Time handling remains zero lines: the tactic owns all lattice logic, and this backend only ever
//! clones times through.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use timely::container::PushInto;
use timely::progress::Timestamp;

use crate::consolidation::{consolidate, consolidate_updates, consolidate_updates_from};
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::chunk::ChunkBatch;
use crate::trace::chunk::vec::VecChunk;
use crate::trace::Description;

use super::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

/// The batch type of a hash-keyed [`ChunkSpine`](crate::trace::chunk::vec::ChunkSpine): payload
/// `D` is `(K, V)` on the input side and `(K, W)` on the output side.
type VBatch<D, T, R> = Rc<ChunkBatch<VecChunk<u64, D, T, R>>>;

/// Walks `batches` restricted to the ascending `keys`, emitting each record as
/// `(hash, &payload, &time, &diff)` in `(hash, payload, time)` order.
///
/// Per hash bracket, the batches' contiguous runs are gathered and sorted by `(payload, time)`, so
/// equal payloads meet across batches and each payload's times arrive in order (batch intervals
/// are disjoint, so cross-batch times need ordering but never summing). The walk seeks to the
/// first requested key and stops after the last, so a bounded window pays for its own range.
fn merged_run<D, T, R>(
    batches: &[VBatch<D, T, R>],
    keys: &[u64],
    mut sink: impl FnMut(u64, &D, &T, &R),
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
                sink(h, d, t, r);
            }
        }
    }
}

/// A reference [`ProxyReduceBackend`] over hash-keyed [`VecChunk`] storage.
///
/// `logic` is differential's general four-argument reduce closure: it receives the real key, the
/// accumulated input values, the tentative accumulated output, and appends the output updates it
/// deems necessary.
pub struct VecReduceBackend<K, V, W, T, R, L> {
    logic: L,
    /// Keys per window: bounds the live presentation, and exercises the multi-window path.
    window_size: usize,
    /// The retire's relevant keys — those the novel batches touch, merged with `changed` — built
    /// once per retire and consumed by hash range from there.
    keys_cache: Vec<u64>,
    /// Whether `keys_cache` is still the previous retire's. Set by `begin`, which runs once per
    /// retire before any window, so the rebuild does not depend on the value the harness opens
    /// `from` with.
    keys_stale: bool,
    /// Resolves an input value id (a per-window ordinal) to its `(key, val)` row.
    in_pool: Vec<(K, V)>,
    /// Resolves an output value id to its `(key, out)` row; spans the whole retire, because
    /// corrections mint ids that later windows' presentations and `emit` must agree on.
    out_pool: Vec<(K, W)>,
    /// Interns `(key, out)` rows to their output id. Lookup-only: the map's iteration order is
    /// never observed, so the hasher cannot affect what the operator produces.
    out_ids: HashMap<(K, W), u64>,
    /// The retire's output tile descriptions, and the rows accumulated for each.
    tiles: Vec<Description<T>>,
    tile_rows: Vec<Vec<((u64, (K, W)), T, R)>>,
}

impl<K, V, W, T, R, L> VecReduceBackend<K, V, W, T, R, L> {
    /// A backend deferring value semantics to `logic`, covering the key space in default-sized
    /// windows.
    pub fn new(logic: L) -> Self {
        Self::with_window(logic, 1 << 12)
    }

    /// A backend with an explicit window size, in keys. Small sizes exercise the harness's
    /// multi-window path; `usize::MAX` presents a single window, like the corgi backend.
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
            tile_rows: Vec::new(),
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
        self.out_pool.clear();
        self.out_ids.clear();
        self.tiles = tiles.to_vec();
        self.tile_rows = (0..tiles.len()).map(|_| Vec::new()).collect();
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, VBatch<(K, V), T, R>, VBatch<(K, W), T, R>>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<T, R, R>,
    ) {
        let Some(start) = *from else { return };

        // First window of the retire: gather the relevant keys — those the novel batches touch,
        // merged with the `changed` keys the harness supplies. Discovered in the scan the
        // presentation needs anyway; later windows slice this by hash range.
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
        merged_run(instance.source_batches, keys, |h, d, t, r| {
            if last != Some(h) || pool.last() != Some(d) {
                pool.push(d.clone());
                last = Some(h);
            }
            window.history.push(((h, (pool.len() - 1) as u64), t.clone(), r.clone()));
        });
        let mut last: Option<u64> = None;
        merged_run(instance.input_batches, keys, |h, d, t, r| {
            if last != Some(h) || pool.last() != Some(d) {
                pool.push(d.clone());
                last = Some(h);
            }
            window.novel.push(((h, (pool.len() - 1) as u64), t.clone(), r.clone()));
        });

        // The output history, interned into the id namespace corrections mint into.
        let (out_pool, out_ids) = (&mut self.out_pool, &mut self.out_ids);
        merged_run(instance.output_batches, keys, |h, d, t, r| {
            let id = *out_ids.entry(d.clone()).or_insert_with(|| {
                out_pool.push(d.clone());
                (out_pool.len() - 1) as u64
            });
            window.output.push(((h, id), t.clone(), r.clone()));
        });
        consolidate_updates(&mut window.output);
    }

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

    fn emit(&mut self, tile: usize, records: &[((u64, u64), T, R)]) {
        // A call carries the whole of every key hash it mentions, and calls arrive in ascending
        // hash order, so the tile stays hash-ordered and only the run just appended can need
        // reordering — by the real `(key, out)` value, since interned ids are in first-seen order
        // rather than value order. Consolidating here rather than over the whole tile at `finish`
        // is the difference between a sort per emit and one sort of everything the retire produced.
        let mark = self.tile_rows[tile].len();
        debug_assert!(
            self.tile_rows[tile].last().is_none_or(|last| records.first().is_none_or(|r| last.0.0 <= r.0.0)),
            "emit must arrive in ascending key-hash order",
        );
        for ((h, vid), t, d) in records {
            let row = self.out_pool[*vid as usize].clone();
            self.tile_rows[tile].push(((*h, row), t.clone(), d.clone()));
        }
        consolidate_updates_from(&mut self.tile_rows[tile], mark);
    }

    fn finish(&mut self) -> Vec<VBatch<(K, W), T, R>> {
        let tiles = std::mem::take(&mut self.tiles);
        let tile_rows = std::mem::take(&mut self.tile_rows);
        tiles
            .into_iter()
            .zip(tile_rows)
            .map(|(desc, rows)| {
                // Already ordered and consolidated: `emit` did it a run at a time.
                let chunks: Vec<VecChunk<u64, (K, W), T, R>> = rows
                    .chunks(<VecChunk<u64, (K, W), T, R> as crate::trace::chunk::Chunk>::TARGET)
                    .map(|piece| {
                        let mut chunk = VecChunk::default();
                        for update in piece {
                            chunk.push_into(update.clone());
                        }
                        chunk
                    })
                    .collect();
                Rc::new(ChunkBatch::new(chunks, desc))
            })
            .collect()
    }
}
