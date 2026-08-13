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
//! Identifiers are ordinal positions minted by a single [`MergedWalk`] over the novel batches and the
//! prior ones together, so a value carries ONE identifier whichever run it appears in and a novel
//! retraction cancels against its own history. They are minted after the walk consolidates, so a value
//! whose records all cancel never reaches the tactic and never spends an identifier.
//!
//! The backend clones keys and values like it is being paid to waste cycles.
//! Generally, this is not a high-performance backend, but shouldn't be abysmal.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use timely::container::PushInto;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

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
    /// Configuration: records per window, bounding what the three presentations cost at once.
    window_size: usize,

    /// The current window's active keys, in ascending order: the input pass records them, and the
    /// output pass replays them as its own active set.
    keys_cache: Vec<u64>,
    /// The current window's hashes carrying more than one real key, ascending.
    collisions: Vec<u64>,
    /// Per entry of `keys_cache`, the identifier of that key's first input value, if it had one.
    reps: Vec<Option<u64>>,

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

    /// A backend with an explicit window budget, in presented records.
    pub fn with_window(logic: L, window_size: usize) -> Self {
        VecReduceBackend {
            logic,
            window_size: window_size.max(1),
            keys_cache: Vec::new(),
            collisions: Vec::new(),
            reps: Vec::new(),
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

        // Populating the novel and prior input bridges.
        self.in_pool.clear();
        self.keys_cache.clear();
        self.collisions.clear();
        self.reps.clear();
        let mut walk = MergedWalk::new(
            instance.input_batches,
            instance.source_batches,
            changed,
            start,
            instance.lower,
        );
        let mut drawn: Vec<Drawn<(K, V), T, R>> = Vec::new();
        let mut seed_scratch: Vec<T> = Vec::new();
        let mut budget = self.window_size;
        while let Some(key) = walk.advance(&mut drawn) {
            self.keys_cache.push(key);
            budget = budget.saturating_sub(drawn.len());
            let rep = self.in_pool.len() as u64;
            let (mut single, mut collides): (Option<&K>, bool) = (None, false);
            let mut last: Option<&(K, V)> = None;
            seed_scratch.clear();
            for ((data, novel, time), diff) in drawn.drain(..) {
                if last != Some(data) {
                    match single {
                        None => single = Some(&data.0),
                        Some(only) => collides |= *only != data.0,
                    }
                    self.in_pool.push(data.clone());
                    last = Some(data);
                }
                let id = (self.in_pool.len() - 1) as u64;
                // The novel time support seeds interest, recorded RAW before any netting: a record
                // that cancels below must still seed. (Novel records are drawn unadvanced; only
                // prior records are advanced to the compaction frontier.)
                if novel {
                    seed_scratch.push(time.clone());
                }
                // Novel and prior share the id space and arrive (id, time)-adjacent, so an exactly
                // cancelling pair meets here and nets away; its time survives in the seeds.
                if let Some(((k2, i2), t2, d2)) = window.input.last_mut() {
                    if *k2 == key && *i2 == id && *t2 == time {
                        d2.plus_equals(&diff);
                        if d2.is_zero() {
                            window.input.pop();
                        }
                        continue;
                    }
                }
                window.input.push(((key, id), time, diff));
            }
            // Per key, seeds arrive in (value, time) order; the contract wants (key, time) order.
            seed_scratch.sort();
            seed_scratch.dedup();
            window.seeds.extend(seed_scratch.drain(..).map(|t| (key, t)));
            self.reps.push(single.map(|_| rep));
            if collides { self.collisions.push(key); }
            if budget == 0 { break; }
        }
        *from = walk.due();

        // Populating the output bridge.
        self.out_ids.clear();
        self.out_pool.clear();
        let mut owalk = MergedWalk::new(
            &[],
            instance.output_batches,
            &self.keys_cache,
            start,
            instance.lower,
        );
        let mut odrawn: Vec<Drawn<(K, W), T, R>> = Vec::new();
        let mut index = 0;
        while let Some(key) = owalk.advance(&mut odrawn) {
            debug_assert_eq!(self.keys_cache.get(index), Some(&key), "the output pass replays the input pass's keys");
            let (mut single, mut collides): (Option<&K>, bool) = (None, false);
            let mut last: Option<&(K, W)> = None;
            for ((data, _, time), diff) in odrawn.drain(..) {
                if last != Some(data) {
                    match single {
                        None => single = Some(&data.0),
                        Some(only) => collides |= *only != data.0,
                    }
                    self.out_ids.insert(data.clone(), self.out_pool.len() as u64);
                    self.out_pool.push(data.clone());
                    last = Some(data);
                }
                let id = (self.out_pool.len() - 1) as u64;
                window.output.push(((key, id), time, diff));
            }
            // The two passes agree or the hash collides. Neither can see this alone: a hash whose
            // input has fully cancelled for one real key still carries that key's stale output.
            if let (Some(out_key), Some(rep)) = (single, self.reps.get(index).copied().flatten()) {
                collides |= self.in_pool[rep as usize].0 != *out_key;
            }
            if collides { self.collisions.push(key); }
            index += 1;
        }
        // The two passes each contribute in ascending order, but one after the other.
        self.collisions.sort_unstable();
        self.collisions.dedup();
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
            // No hash collision fast path, expected to be the most common case.
            let collides = !self.collisions.is_empty() && self.collisions.binary_search(&keys[i]).is_ok();
            let single_key: Option<K> = if collides { None } else {
                input[is..ie].first().map(|(vid, _)| self.in_pool[*vid as usize].0.clone())
                    .or_else(|| output[os..oe].first().map(|(vid, _)| self.out_pool[*vid as usize].0.clone()))
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

/// One record drawn by [`MergedWalk::advance`], keyed for consolidation.
///
/// The key is `(data, novel, time)`: grouping by data is what mints one identifier per distinct
/// value, and the `novel` flag separates the two roles *within* a value so that consolidation
/// never cancels a novel update against a prior one. The data is borrowed from the batch — it is
/// cloned once per surviving value, when its identifier is minted, and never per record.
type Drawn<'a, D, T, R> = ((&'a D, bool, T), R);

/// The length of `slice`'s prefix of records whose hash is `key`, which must be non-empty.
///
/// Found by doubling rather than by bisecting the whole slice: a key's run within one chunk is
/// usually a handful of records, and a binary search over the thousands that follow it would cost
/// several times the run it is measuring.
fn run_len<D, T, R>(slice: &[((u64, D), T, R)], key: u64) -> usize {
    debug_assert!(slice.first().is_some_and(|r| r.0.0 == key), "the run must start at `key`");
    let mut step = 1;
    while step < slice.len() && slice[step].0.0 == key { step <<= 1; }
    let lower = step >> 1;
    let upper = step.min(slice.len());
    lower + slice[lower..upper].partition_point(|r| r.0.0 <= key)
}

/// A resumable merge-walk over a list of batches, delivering one key at a time in `(hash, data,
/// time)` order.
///
/// The list is logically one, with batches `[0, novel)` the retire's novel input and the rest its
/// prior history. Only the novel batches and `changed` make a key ACTIVE. The prior batches are
/// seeked to whatever key the active set names and are never walked in search of one, which is what
/// keeps a window proportional to the work asked for rather than to the accumulated trace: on an
/// iterative computation, where a retire asks for a scattered handful of keys, the difference is
/// between presenting a window and re-reading the whole trace.
///
/// The state is a `(chunk, offset)` cursor per batch, an index into `changed`, and the next active
/// hash. Nothing is materialized ahead of the walk, so a caller can stop at any key boundary and
/// resume from the hash left in [`due`](Self::due) — which is exactly the windowing the backend owes
/// [`ProxyReduceBackend::next_window`].
struct MergedWalk<'a, D: Ord + Clone + 'static, T: Lattice + Timestamp, R: Semigroup + Ord + Clone + 'static> {
    /// The novel batches, then the prior ones; [`batch`](Self::batch) reads the pair as one list.
    novel: &'a [VBatch<D, T, R>],
    prior: &'a [VBatch<D, T, R>],
    /// Externally flagged keys, ascending: active whatever the batches hold.
    changed: &'a [u64],
    /// Cursor into `changed`.
    ci: usize,
    /// Per-batch `(chunk, offset)` cursor, in `batch` order.
    pos: Vec<(usize, usize)>,
    /// The least active hash at or above the cursors, or `None` once the key space is spent.
    due: Option<u64>,
    /// The compaction frontier prior records are advanced to as they are drawn. Advancing before
    /// the sort is what lets the consolidation that follows collapse a long-but-quiet history to one
    /// record per value: advancement is only useful because something merges the collisions it
    /// creates. An empty frontier advances nothing.
    frontier: AntichainRef<'a, T>,
}

impl<'a, D, T, R> MergedWalk<'a, D, T, R>
where
    D: Ord + Clone + 'static,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Clone + 'static,
{
    /// A walk over `novel ++ prior`, positioned at the first active key at or above `from`.
    fn new(
        novel: &'a [VBatch<D, T, R>],
        prior: &'a [VBatch<D, T, R>],
        changed: &'a [u64],
        from: u64,
        frontier: AntichainRef<'a, T>,
    ) -> Self {
        let count = novel.len() + prior.len();
        let mut walk = MergedWalk {
            novel,
            prior,
            changed,
            ci: changed.partition_point(|k| *k < from),
            pos: vec![(0, 0); count],
            due: None,
            frontier,
        };
        for b in 0..count {
            walk.seek_to(b, from);
        }
        walk.refresh();
        walk
    }

    /// The number of batches, novel and prior together.
    fn count(&self) -> usize { self.novel.len() + self.prior.len() }

    /// The `b`-th batch of the logical list.
    fn batch(&self, b: usize) -> &'a ChunkBatch<VecChunk<u64, D, T, R>> {
        if b < self.novel.len() { &self.novel[b] } else { &self.prior[b - self.novel.len()] }
    }

    /// The hash the `b`-th batch's cursor sits on, or `None` if it is drained.
    fn head(&self, b: usize) -> Option<u64> {
        let (chunk, offset) = self.pos[b];
        self.batch(b).chunks.get(chunk).and_then(|c| c.as_slice().get(offset)).map(|r| r.0.0)
    }

    /// The least active hash at or above the cursors, or `None` once the key space is spent.
    ///
    /// A key is active if a novel batch holds a record for it or it appears in `changed`. Prior
    /// history alone never activates a key: with no novel update and nothing due, its reduction has
    /// the same input and the same output as it did last round, so there is nothing to reconcile.
    fn due(&self) -> Option<u64> { self.due }

    /// Recompute the next active hash from the novel heads and the `changed` cursor.
    fn refresh(&mut self) {
        let from_novel = (0..self.novel.len()).filter_map(|b| self.head(b)).min();
        let from_changed = self.changed.get(self.ci).copied();
        self.due = [from_novel, from_changed].into_iter().flatten().min();
    }

    /// Seek the `b`-th batch's cursor to the first record with hash at or above `key`.
    ///
    /// Both steps are binary searches: over the chunk last-keys, which ascend across a batch and so
    /// form a sparse index that never touches a record, and then over the landed chunk's records.
    fn seek_to(&mut self, b: usize, key: u64) {
        // Already positioned, or drained. Keys are drawn in ascending order, so a cursor sitting at
        // or above `key` is already at the FIRST record at or above it: everything it passed was at
        // or below the previous key. This is the dense case — most keys, most batches — and paying
        // two binary searches per batch per key there costs more than the whole walk.
        if self.head(b).is_none_or(|h| h >= key) { return; }
        let chunks = &self.batch(b).chunks;
        let (mut chunk, mut offset) = self.pos[b];
        let skip = chunks[chunk.min(chunks.len())..]
            .partition_point(|c| c.as_slice().last().is_some_and(|r| r.0.0 < key));
        if skip > 0 {
            chunk += skip;
            offset = 0;
        }
        if let Some(slice) = chunks.get(chunk).map(|c| c.as_slice()) {
            offset += slice[offset..].partition_point(|r| r.0.0 < key);
        }
        self.pos[b] = (chunk, offset);
    }

    /// Draw the `b`-th batch's run of records for `key` into `into`, advancing its cursor past them.
    ///
    /// A key's run can straddle chunks, so this walks chunks until one ends the run; within each it
    /// takes the whole run at once rather than a record at a time.
    fn take_run(&mut self, b: usize, key: u64, into: &mut Vec<Drawn<'a, D, T, R>>) {
        let novel = b < self.novel.len();
        let chunks = &self.batch(b).chunks;
        while let Some(slice) = chunks.get(self.pos[b].0).map(|c| c.as_slice()) {
            let start = self.pos[b].1;
            // The cursor is at or above `key`; if it is above, this batch holds nothing for it.
            // Most batches hold nothing for most keys, so this is the common exit and it must cost
            // one comparison rather than a search.
            if slice.get(start).is_none_or(|r| r.0.0 != key) { return; }
            let end = start + run_len(&slice[start..], key);
            into.extend(slice[start..end].iter().map(|record| {
                let mut time = record.1.clone();
                // Novel times are the interesting-time seeds the tactic's schedule is stated over,
                // so they are drawn unadvanced; only the prior history is lifted.
                if !novel { time.advance_by(self.frontier); }
                ((&record.0.1, novel, time), record.2.clone())
            }));
            if end < slice.len() {
                self.pos[b].1 = end;
                return;
            }
            self.pos[b] = (self.pos[b].0 + 1, 0);
        }
    }

    /// Draw every record of the next active key, and advance to the one after it.
    ///
    /// Returns the key drawn, or `None` once the key space is spent. `into` is left sorted by
    /// `(data, novel, time)` and consolidated, so a value whose records all cancel is gone before
    /// the caller ever sees it — and so never spends an identifier.
    fn advance(&mut self, into: &mut Vec<Drawn<'a, D, T, R>>) -> Option<u64> {
        let key = self.due?;
        into.clear();
        for b in 0..self.count() {
            self.seek_to(b, key);
            self.take_run(b, key, into);
        }
        consolidate(into);
        if self.changed.get(self.ci) == Some(&key) { self.ci += 1; }
        self.refresh();
        Some(key)
    }
}
