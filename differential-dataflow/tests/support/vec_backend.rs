//! The in-memory Vec backend (the reference implementation): real data in [`VecChunk`]
//! arrangements, `key_hash = hash(key)` and `value_id = hash((key, value))` under the crate's
//! [`Hashable`] (a deterministic, process-wide-stable content hash — the "system-wide hash
//! function" every backend must fix for itself). Hashing `(key, value)` (not `value` alone)
//! makes each `value_id` identify its record, so the seam can hand ids back and the backend
//! resolves them through `value_id → (key, value)` maps — no positional alignment retained.
//!
//! The backend honors the tactics' delta-proportionality restrictions: a filtered
//! present *seeks* the requested keys in the batch lists rather than scanning them, so
//! per-retire (reduce) and per-unit (join) work tracks the delta, not the accumulated
//! trace — the same access pattern as the cursor tactics. The `work_is_delta_proportional`
//! tests pin this.
//!
//! It does **not** implement the rest of the collision-recovery recipe (verify-at-`cross`,
//! partition-by-real-key in `reduce`): under a `key_hash` collision it still reduces the two
//! keys' records together / cross-matches them, *accepting the birthday-bound risk* for
//! simplicity, as the reference backend is entitled to. A production backend that needs
//! exactness adds the recipe on top of the already key-qualified ids; see the collision-risk
//! docs in [`operators::int_proxy`](differential_dataflow::operators::int_proxy).
//!
//! [`VecChunk`]: differential_dataflow::trace::chunk::vec::VecChunk
//! [`Hashable`]: differential_dataflow::hashable::Hashable

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::difference::{Abelian, Multiply, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::{Builder, Description, Navigable};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBuilder};
use differential_dataflow::operators::int_proxy::ProxyBridge;
use differential_dataflow::trace::chunk::vec::VecChunk;

use differential_dataflow::operators::int_proxy::{ProxyJoinBackend, JoinInstance};
use differential_dataflow::operators::int_proxy::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

/// The backend's stable content hash: same input → same `u64`, everywhere in the
/// process, with no registry.
pub fn stable_hash<D: Hash>(data: &D) -> u64 {
    data.hashed()
}

/// A batch of the reference backend's arrangements.
pub type RefBatch<K, V, T, R> = Rc<ChunkBatch<VecChunk<K, V, T, R>>>;

/// Parallel proxy and real columns under construction, one entry per record.
struct Rows<K, V, T, R> {
    keys: Vec<u64>,
    vals: Vec<u64>,
    times: Vec<T>,
    diffs: Vec<R>,
    reals: Vec<(K, V)>,
}

impl<K, V, T, R> Rows<K, V, T, R> {
    fn new() -> Self {
        Rows { keys: Vec::new(), vals: Vec::new(), times: Vec::new(), diffs: Vec::new(), reals: Vec::new() }
    }
}

impl<K, V, T, R> Rows<K, V, T, R>
where
    K: Ord + Clone + Hash + 'static,
    V: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp,
    R: Ord + Semigroup + 'static,
{
    /// Append one key's `(val, time, diff)` records read at the cursor's current key.
    fn read_key(&mut self, cursor: &mut <RefBatch<K, V, T, R> as Navigable>::Cursor, batch: &RefBatch<K, V, T, R>, key: &K, kh: u64) {
        while let Some(val) = cursor.get_val(batch) {
            // The value id hashes `(key, value)`, not just `value`: it must identify the
            // record unambiguously so `cross`/`reduce` can resolve it back, and so keys
            // that collide on `kh` are still distinguished (per the seam's value-id contract).
            let vh = stable_hash(&(key, val));
            cursor.map_times(batch, |t, d| {
                self.keys.push(kh);
                self.vals.push(vh);
                self.times.push(t.clone());
                self.diffs.push(d.clone());
                self.reals.push((key.clone(), val.clone()));
            });
            cursor.step_val(batch);
        }
    }

    /// Read every record of `batches` (the unfiltered, delta-sized read).
    fn scan(&mut self, batches: &[RefBatch<K, V, T, R>]) {
        for batch in batches {
            let mut cursor = batch.cursor();
            while let Some(key) = cursor.get_key(batch) {
                let key = key.clone();
                self.read_key(&mut cursor, batch, &key, stable_hash(&key));
                cursor.step_key(batch);
            }
        }
    }

    /// Read exactly the records of `keys` (sorted) from `batches`, by seeking — the
    /// filtered read costs `O(|keys| · log)` per batch plus the matched records, never
    /// the batch size.
    fn seek(&mut self, batches: &[RefBatch<K, V, T, R>], keys: &[K]) {
        debug_assert!(keys.windows(2).all(|w| w[0] < w[1]));
        for batch in batches {
            let mut cursor = batch.cursor();
            for key in keys {
                cursor.seek_key(batch, key);
                if cursor.get_key(batch) == Some(key) {
                    self.read_key(&mut cursor, batch, key, stable_hash(key));
                }
            }
        }
    }

    /// Advance every loaded time by `frontier` (the instance's `lower`), so the subsequent
    /// consolidate collapses the historical tail. Sound for the accumulation: outputs are read
    /// only at times `>= frontier`, so advancing below it preserves them.
    fn advance(&mut self, frontier: AntichainRef<T>) {
        for time in self.times.iter_mut() {
            time.advance_by(frontier);
        }
    }

    /// Sort and consolidate the proxy run, and return a `(key_hash, value_id) → (key, value)`
    /// map for resolving the bridge keys the seam hands back. Keying off `(key_hash, value_id)`
    /// — the full bridge key — matches how the seam names records and lets value ids be
    /// intra-hash; no positional alignment survives the presentation.
    fn present(self) -> (ProxyBridge<T, R>, HashMap<(u64, u64), (K, V)>) {
        let mut reals = HashMap::with_capacity(self.reals.len());
        for i in 0..self.vals.len() {
            reals.entry((self.keys[i], self.vals[i])).or_insert_with(|| self.reals[i].clone());
        }
        let chunk = sort_consolidate(self.keys, self.vals, self.times, self.diffs);
        (chunk, reals)
    }
}

/// The distinct keys among `reals` whose hash appears in the sorted `filter`, sorted by
/// the *key* order (ready for seeking).
fn keys_matching<'a, K: Ord + Clone + Hash + 'a, V: 'a>(reals: impl Iterator<Item = &'a (K, V)>, filter: &[u64]) -> Vec<K> {
    let mut keys: Vec<K> = reals
        .filter(|(k, _)| filter.binary_search(&stable_hash(k)).is_ok())
        .map(|(k, _)| k.clone())
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// The join backend: presents [`VecChunk`] batches by hashing, applies a
/// `(key, val0, val1) → data` projection to matched index pairs, and emits
/// `Vec<(data, time, diff)>` containers.
///
/// A filtered present resolves the filter's hashes to real keys through the *other*
/// side's presentation (the fresh side, presented first, unfiltered) and seeks them.
pub struct VecJoinBackend<K, V0, V1, D, L> {
    logic: L,
    /// `(key_hash, value_id) → (key, value)` for the current first-input presentation.
    left: HashMap<(u64, u64), (K, V0)>,
    /// `(key_hash, value_id) → (key, value)` for the current second-input presentation.
    right: HashMap<(u64, u64), (K, V1)>,
    marker: PhantomData<fn() -> D>,
}

impl<K, V0, V1, D, L> VecJoinBackend<K, V0, V1, D, L> {
    /// A backend applying `logic` to each matched `(key, val0, val1)`.
    pub fn new(logic: L) -> Self {
        VecJoinBackend { logic, left: HashMap::new(), right: HashMap::new(), marker: PhantomData }
    }
}

impl<K, V0, V1, T, R0, R1, RO, D, L> ProxyJoinBackend<RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>> for VecJoinBackend<K, V0, V1, D, L>
where
    K: Ord + Clone + Hash + 'static,
    V0: Ord + Clone + Hash + 'static,
    V1: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp,
    R0: Ord + Semigroup + Multiply<R1, Output = RO> + 'static,
    R1: Ord + Semigroup + 'static,
    RO: Semigroup + 'static,
    D: 'static,
    L: FnMut(&K, &V0, &V1) -> D,
{
    type R0 = R0;
    type R1 = R1;
    type ROut = RO;
    type Output = Vec<(D, T, RO)>;

    fn present0(&mut self, instance: &JoinInstance<'_, RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>>, filter: Option<&[u64]>) -> ProxyBridge<T, R0> {
        let mut rows = Rows::new();
        match filter {
            None => rows.scan(instance.batches0),
            Some(f) => rows.seek(instance.batches0, &keys_matching(self.right.values(), f)),
        }
        rows.advance(instance.lower);
        let (chunk, reals) = rows.present();
        self.left = reals;
        chunk
    }

    fn present1(&mut self, instance: &JoinInstance<'_, RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>>, filter: Option<&[u64]>) -> ProxyBridge<T, R1> {
        let mut rows = Rows::new();
        match filter {
            None => rows.scan(instance.batches1),
            Some(f) => rows.seek(instance.batches1, &keys_matching(self.left.values(), f)),
        }
        rows.advance(instance.lower);
        let (chunk, reals) = rows.present();
        self.right = reals;
        chunk
    }

    fn cross(&mut self, _instance: &JoinInstance<'_, RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>>, left: &[(u64, u64)], right: &[(u64, u64)], times: Vec<T>, diffs: Vec<RO>) -> Vec<(D, T, RO)> {
        let mut out = Vec::with_capacity(left.len());
        for (((&l, &r), t), d) in left.iter().zip(right).zip(times).zip(diffs) {
            let (k, v0) = &self.left[&l];
            let (_, v1) = &self.right[&r];
            out.push(((self.logic)(k, v0, v1), t, d));
        }
        out
    }
}

/// The reduce backend: presents [`VecChunk`] batches by hashing, applies the
/// user's reduction logic (row-reduce-shaped: `(key, &[(val, diff)], &mut output)`)
/// per key, mints output ids by hashing produced values, and materializes proxy records
/// back into [`VecChunk`] output batches.
///
/// The changed-key restriction is honored by *seeking*: the accumulated input and output
/// histories are read only at the changed keys. Every changed hash is resolvable to its
/// key — this retire's touched keys from the (delta-sized) novel batches, pending keys
/// from the retire that pended them (the `keys` map persists exactly those entries).
/// Changed keys presented per window, in the reference. Small so the tests exercise the
/// multi-window path (parity is independent of the value; a real backend would size by
/// materialized bytes, which needs hash-native storage — see DESIGN.md F8/F7).
const WINDOW_KEYS: usize = 1024;

pub struct VecReduceBackend<K, V, V2, T, ROut, L> {
    logic: L,
    /// `key_hash → key` for the changed keys: primed once per retire from the novel batches (on
    /// the first `next_window`), pruned to the changed set (so pending keys survive between retires
    /// and nothing else accumulates). Retire-wide: `emit` needs it for every window's keys.
    keys: HashMap<u64, K>,
    /// `(key_hash, value_id) → input value` for the CURRENT window's input presentation.
    in_vals: HashMap<(u64, u64), V>,
    /// `(key_hash, value_id) → output value` for the CURRENT window: primed by its output-history
    /// presentation and by minting in `reduce`, consumed by `emit`.
    out_vals: HashMap<(u64, u64), V2>,
    /// The output session opened by `begin`: per tile, its description and the resolved records
    /// `emit` has accumulated. `finish` sorts, consolidates, and builds each into a batch.
    tiles: Vec<(Description<T>, Vec<((K, V2), T, ROut)>)>,
}

impl<K, V, V2, T, ROut, L> VecReduceBackend<K, V, V2, T, ROut, L> {
    /// A backend applying `logic` — shaped like the row reduce's closure — per key.
    pub fn new(logic: L) -> Self {
        VecReduceBackend { logic, keys: HashMap::new(), in_vals: HashMap::new(), out_vals: HashMap::new(), tiles: Vec::new() }
    }
}

impl<K, V, V2, T, RIn, ROut, L> ProxyReduceBackend<RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>> for VecReduceBackend<K, V, V2, T, ROut, L>
where
    K: Ord + Clone + Hash + 'static,
    V: Ord + Clone + Hash + 'static,
    V2: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp,
    RIn: Ord + Semigroup + 'static,
    ROut: Ord + Abelian + 'static,
    L: FnMut(&K, &[(&V, RIn)], &mut Vec<(V2, ROut)>),
{
    type RIn = RIn;
    type ROut = ROut;

    fn seed_times(&self, instance: &ReduceInstance<'_, RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>>) -> Vec<(u64, T)> {
        // The batch's raw (key_hash, time) support: hash keys only (no value work), one
        // entry per record, sorted by key_hash. Never merged with stored history, so no
        // compacted record can cancel a seed; `instance.lower` is not applied (times are
        // already `>= lower`, so advancing is a no-op, and seeds must stay raw regardless).
        let mut out = Vec::new();
        for batch in instance.input_batches {
            let mut cursor = batch.cursor();
            while let Some(k) = cursor.get_key(batch) {
                let kh = stable_hash(k);
                while cursor.get_val(batch).is_some() {
                    cursor.map_times(batch, |t, _| out.push((kh, t.clone())));
                    cursor.step_val(batch);
                }
                cursor.step_key(batch);
            }
        }
        out.sort_by_key(|(k, _)| *k);
        out
    }

    fn begin(&mut self, tiles: &[Description<T>]) {
        // Open one builder-accumulator per output tile; `emit` fills them, `finish` seals them.
        self.tiles = tiles.iter().map(|d| (d.clone(), Vec::new())).collect();
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<T, RIn, ROut>> {
        if *cursor == 0 {
            // Prime `key_hash → key` for every changed key, once, from the delta-sized novel
            // batches. Prune the retained map to the changed set first — what survives is exactly
            // the pending keys — and shrink, so a past big retire's table doesn't linger.
            self.keys.retain(|h, _| changed.binary_search(h).is_ok());
            self.keys.shrink_to_fit();
            let mut novel: Rows<K, V, T, RIn> = Rows::new();
            novel.scan(instance.input_batches);
            for (k, _) in &novel.reals {
                self.keys.entry(stable_hash(k)).or_insert_with(|| k.clone());
            }
        }
        if *cursor >= changed.len() {
            return None;
        }
        let end = (*cursor + WINDOW_KEYS).min(changed.len());
        let window_hashes = &changed[*cursor..end];
        // This window's real keys (one per hash — collisions keep the first, the reference's
        // live-dangerously policy), sorted & deduped for seeking.
        let mut window_real: Vec<K> = window_hashes.iter().filter_map(|h| self.keys.get(h).cloned()).collect();
        window_real.sort();
        window_real.dedup();
        // Present the input (novel ∪ accumulated history) for just this window's keys — seeking
        // both, so peak read memory tracks the window, not the whole retire. Advance to `lower`
        // and consolidate so the presented run tracks the delta, not all of history.
        let mut input: Rows<K, V, T, RIn> = Rows::new();
        input.seek(instance.input_batches, &window_real);
        input.seek(instance.source_batches, &window_real);
        input.advance(instance.lower);
        let (in_chunk, in_reals) = input.present();
        self.in_vals = in_reals.into_iter().map(|(kv, (_, v))| (kv, v)).collect();
        // Present the output history for the same keys.
        let mut output: Rows<K, V2, T, ROut> = Rows::new();
        output.seek(instance.output_batches, &window_real);
        output.advance(instance.lower);
        let (out_chunk, out_reals) = output.present();
        self.out_vals = out_reals.into_iter().map(|(kv, (_, v))| (kv, v)).collect();
        *cursor = end;
        Some(ReduceWindow { input: in_chunk, output: out_chunk, keys: window_hashes.to_vec() })
    }

    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, RIn)],
        out_ends: &[usize],
        output: &[(u64, ROut)],
    ) -> (Vec<(u64, ROut)>, Vec<usize>) {
        let mut corrections = Vec::new();
        let mut correction_ends = Vec::with_capacity(keys.len());
        let (mut istart, mut ostart) = (0usize, 0usize);
        for (k, &key_hash) in keys.iter().enumerate() {
            let (iend, oend) = (in_ends[k], out_ends[k]);
            let key = self.keys.get(&key_hash).expect("key presented this retire");
            // Desired output = logic(resolved input, presented in Ord order).
            let mut pairs: Vec<(&V, RIn)> = input[istart..iend]
                .iter()
                .map(|(vid, d)| (self.in_vals.get(&(key_hash, *vid)).expect("value presented this window"), d.clone()))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            let mut delta: Vec<(V2, ROut)> = Vec::new();
            (self.logic)(key, &pairs, &mut delta);
            // Correction = desired − current, keyed by the REAL output value (so it cancels
            // robustly, without relying on value-id minting to collide). Current output ids
            // resolve against the window's `out_vals`.
            for (vid, d) in output[ostart..oend].iter() {
                let v = self.out_vals.get(&(key_hash, *vid)).expect("output value presented or minted this window").clone();
                let mut nd = d.clone();
                nd.negate();
                delta.push((v, nd));
            }
            differential_dataflow::consolidation::consolidate(&mut delta);
            // Mint ids for the surviving corrections (the same `(key, value)` hash as elsewhere).
            for (v, d) in delta.drain(..) {
                let id = stable_hash(&(key, &v));
                self.out_vals.entry((key_hash, id)).or_insert(v);
                corrections.push((id, d));
            }
            correction_ends.push(corrections.len());
            istart = iend;
            ostart = oend;
        }
        (corrections, correction_ends)
    }

    fn emit(&mut self, tile: usize, records: &[((u64, u64), T, ROut)]) {
        // Resolve the current window's output-delta proxies to real records and stash them in the
        // tile's accumulator — no value round-trips through the tactic. Resolve fully before
        // touching `self.tiles`, to keep the `keys`/`out_vals` reads off the `tiles` borrow.
        let resolved: Vec<((K, V2), T, ROut)> = records
            .iter()
            .map(|((kh, vid), t, d)| {
                let k = self.keys.get(kh).expect("key presented this retire").clone();
                let v = self.out_vals.get(&(*kh, *vid)).expect("value presented or minted this window").clone();
                ((k, v), t.clone(), d.clone())
            })
            .collect();
        self.tiles[tile].1.extend(resolved);
    }

    fn finish(&mut self) -> Vec<RefBatch<K, V2, T, ROut>> {
        // Seal each tile: sort + consolidate its accumulated real records (the proxy hash order is
        // not the arrangement's), then build. Cross-window contributions are key-disjoint, so this
        // is the only consolidation needed.
        std::mem::take(&mut self.tiles)
            .into_iter()
            .map(|(description, mut rows)| {
                differential_dataflow::consolidation::consolidate_updates(&mut rows);
                // Feed the builder TARGET-sized chunks: handing it one giant chunk would make its
                // settle peel TARGET-sized pieces off the front, copying the tail each time —
                // quadratic in the batch size.
                let mut builder = ChunkBuilder::<VecChunk<K, V2, T, ROut>>::with_capacity(0, 0, 0);
                let mut rows = rows.into_iter().peekable();
                while rows.peek().is_some() {
                    let mut chunk = VecChunk::default();
                    for row in rows.by_ref().take(<VecChunk<K, V2, T, ROut> as differential_dataflow::trace::chunk::Chunk>::TARGET) {
                        use timely::container::PushInto;
                        chunk.push_into(row);
                    }
                    builder.push(&mut chunk);
                }
                builder.done(description)
            })
            .collect()
    }
}

/// Sort `(key, val, time)` and consolidate equal triples into the run in the bridge shape
/// `((key_hash, value_id), time, diff)`. Real records are resolved by `value_id` map (see
/// `Rows::present`), so this needs no representative-index bookkeeping.
fn sort_consolidate<T: Clone + Ord, R: Semigroup + Clone>(
    keys: Vec<u64>, vals: Vec<u64>, times: Vec<T>, diffs: Vec<R>,
) -> ProxyBridge<T, R> {
    let n = times.len();
    debug_assert!(keys.len() == n && vals.len() == n && diffs.len() == n);
    let perm = sort_perm(&keys, &vals, &times);
    let mut records = Vec::new();
    let mut i = 0;
    while i < n {
        let r = perm[i];
        let (k, v, t) = (keys[r], vals[r], &times[r]);
        let mut d = diffs[r].clone();
        let mut j = i + 1;
        while j < n && { let s = perm[j]; keys[s] == k && vals[s] == v && times[s] == *t } {
            d.plus_equals(&diffs[perm[j]]);
            j += 1;
        }
        if !d.is_zero() {
            records.push(((k, v), t.clone(), d));
        }
        i = j;
    }
    records
}

/// A sorting permutation for `(key_hash, value_id, time)`: small runs by unstable sort, large by
/// an MSD bucket sort on the top byte of `key_hash` (a content hash, so uniform), each bucket
/// finished by the full comparison.
fn sort_perm<T: Ord>(keys: &[u64], vals: &[u64], times: &[T]) -> Vec<usize> {
    let n = keys.len();
    let cmp = |&a: &usize, &b: &usize| (keys[a], vals[a], &times[a]).cmp(&(keys[b], vals[b], &times[b]));
    if n < 512 {
        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_unstable_by(cmp);
        return perm;
    }
    let bucket = |i: usize| (keys[i] >> 56) as usize;
    let mut counts = [0usize; 256];
    for i in 0..n { counts[bucket(i)] += 1; }
    let mut starts = [0usize; 257];
    for b in 0..256 { starts[b + 1] = starts[b] + counts[b]; }
    let mut perm = vec![0usize; n];
    let mut cursor = starts;
    for i in 0..n {
        let b = bucket(i);
        perm[cursor[b]] = i;
        cursor[b] += 1;
    }
    for b in 0..256 {
        perm[starts[b]..starts[b + 1]].sort_unstable_by(cmp);
    }
    perm
}
