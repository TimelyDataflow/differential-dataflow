//! The in-memory reference backend: real data in [`VecChunk`] arrangements, ids by
//! hashing the real keys and values with the crate's [`Hashable`] (a deterministic,
//! process-wide-stable content hash — the "system-wide hash function" every backend must
//! fix for itself).
//!
//! The backend honors the tactics' delta-proportionality restrictions: a filtered
//! present *seeks* the requested keys in the batch lists rather than scanning them, so
//! per-retire (reduce) and per-unit (join) work tracks the delta, not the accumulated
//! trace — the same access pattern as the cursor tactics. The `work_is_delta_proportional`
//! tests pin this.
//!
//! [`VecChunk`]: crate::trace::chunk::vec::VecChunk
//! [`Hashable`]: crate::hashable::Hashable

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

use timely::progress::Timestamp;

use crate::difference::{Abelian, Multiply, Semigroup};
use crate::hashable::Hashable;
use crate::lattice::Lattice;
use crate::trace::{Builder, Description, Navigable};
use crate::trace::cursor::Cursor;
use crate::trace::chunk::{ChunkBatch, ChunkBuilder};
use crate::trace::chunk::int_proxy::ProxyChunk;
use crate::trace::chunk::vec::VecChunk;

use super::join::ProxyJoinBackend;
use super::reduce::ProxyReduceBackend;

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
            let vh = stable_hash(val);
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

    /// Sort, consolidate, and align: the proxy run plus the real record per run entry.
    fn present(self) -> (ProxyChunk<T, R>, Vec<(K, V)>) {
        let (chunk, reps) = ProxyChunk::from_unsorted(self.keys, self.vals, self.times, self.diffs);
        let aligned = reps.iter().map(|&i| self.reals[i].clone()).collect();
        (chunk, aligned)
    }
}

/// The distinct keys among `reals` whose hash appears in the sorted `filter`, sorted by
/// the *key* order (ready for seeking).
fn keys_matching<K: Ord + Clone + Hash, V>(reals: &[(K, V)], filter: &[u64]) -> Vec<K> {
    let mut keys: Vec<K> = reals
        .iter()
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
    /// Real records aligned with the current first-input presentation.
    left: Vec<(K, V0)>,
    /// Real records aligned with the current second-input presentation.
    right: Vec<(K, V1)>,
    marker: PhantomData<fn() -> D>,
}

impl<K, V0, V1, D, L> VecJoinBackend<K, V0, V1, D, L> {
    /// A backend applying `logic` to each matched `(key, val0, val1)`.
    pub fn new(logic: L) -> Self {
        VecJoinBackend { logic, left: Vec::new(), right: Vec::new(), marker: PhantomData }
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

    fn present0(&mut self, batches: &[RefBatch<K, V0, T, R0>], filter: Option<&[u64]>) -> ProxyChunk<T, R0> {
        let mut rows = Rows::new();
        match filter {
            None => rows.scan(batches),
            Some(f) => rows.seek(batches, &keys_matching(&self.right, f)),
        }
        let (chunk, reals) = rows.present();
        self.left = reals;
        chunk
    }

    fn present1(&mut self, batches: &[RefBatch<K, V1, T, R1>], filter: Option<&[u64]>) -> ProxyChunk<T, R1> {
        let mut rows = Rows::new();
        match filter {
            None => rows.scan(batches),
            Some(f) => rows.seek(batches, &keys_matching(&self.left, f)),
        }
        let (chunk, reals) = rows.present();
        self.right = reals;
        chunk
    }

    fn cross(&mut self, left: &[usize], right: &[usize], times: Vec<T>, diffs: Vec<RO>) -> Vec<(D, T, RO)> {
        let mut out = Vec::with_capacity(left.len());
        for (((&l, &r), t), d) in left.iter().zip(right).zip(times).zip(diffs) {
            let (k, v0) = &self.left[l];
            let (_, v1) = &self.right[r];
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
pub struct VecReduceBackend<K, V, V2, L> {
    logic: L,
    /// `key_hash → key` for the changed keys: primed from each retire's novel batches,
    /// pruned to the changed set on entry (so pending keys survive between retires and
    /// nothing else accumulates).
    keys: HashMap<u64, K>,
    /// Real values aligned with the current input presentation.
    in_vals: Vec<V>,
    /// `value_id → output value` for the current retire, primed by the output
    /// presentation and by minting; cleared each retire.
    out_vals: HashMap<u64, V2>,
}

impl<K, V, V2, L> VecReduceBackend<K, V, V2, L> {
    /// A backend applying `logic` — shaped like the row reduce's closure — per key.
    pub fn new(logic: L) -> Self {
        VecReduceBackend { logic, keys: HashMap::new(), in_vals: Vec::new(), out_vals: HashMap::new() }
    }

    /// The changed keys, in key order, ready for seeking.
    fn seek_keys(&self) -> Vec<K>
    where
        K: Ord + Clone,
    {
        let mut keys: Vec<K> = self.keys.values().cloned().collect();
        keys.sort();
        keys
    }
}

impl<K, V, V2, T, RIn, ROut, L> ProxyReduceBackend<RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>> for VecReduceBackend<K, V, V2, L>
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

    fn present_novel(&mut self, novel: &[RefBatch<K, V, T, RIn>]) -> ProxyChunk<T, RIn> {
        // The batch's own support: read the (delta-sized) novel batches alone, never
        // merged with stored history, so no compacted record can cancel a seed.
        let mut rows = Rows::new();
        rows.scan(novel);
        rows.present().0
    }

    fn present_input(&mut self, history: &[RefBatch<K, V, T, RIn>], novel: &[RefBatch<K, V, T, RIn>], keys: &[u64]) -> ProxyChunk<T, RIn> {
        // Prune the hash→key map to the changed set: what survives from earlier retires
        // is exactly the pending keys, so the map is bounded by the delta. Shrink too —
        // `retain` keeps the backing table, and a table sized by a past big retire would
        // make every later `values()` walk pay for its capacity, not its contents.
        self.keys.retain(|h, _| keys.binary_search(h).is_ok());
        self.keys.shrink_to_fit();
        // The novel batches are delta-sized and their keys are all changed: read them
        // fully, and prime the map from what was read.
        let mut rows = Rows::new();
        rows.scan(novel);
        for (k, _) in &rows.reals {
            self.keys.entry(stable_hash(k)).or_insert_with(|| k.clone());
        }
        // The accumulated history is not scanned: seek exactly the changed keys.
        rows.seek(history, &self.seek_keys());
        let (chunk, reals) = rows.present();
        self.in_vals = reals.into_iter().map(|(_, v)| v).collect();
        chunk
    }

    fn present_output(&mut self, batches: &[RefBatch<K, V2, T, ROut>], _keys: &[u64]) -> ProxyChunk<T, ROut> {
        // The id → value map is per-retire state; start it afresh. A fresh map, not
        // `clear()`: clearing keeps the backing table, whose capacity a past big retire
        // set, and later walks would pay for it.
        self.out_vals = HashMap::new();
        // Seek the changed keys (the same set `present_input` resolved).
        let mut rows = Rows::new();
        rows.seek(batches, &self.seek_keys());
        let (chunk, reals) = rows.present();
        for (i, (_, v)) in reals.into_iter().enumerate() {
            self.out_vals.entry(chunk.value_ids()[i]).or_insert(v);
        }
        chunk
    }

    fn reduce(&mut self, key_hash: u64, input: &[(usize, RIn)]) -> Vec<(u64, ROut)> {
        let key = self.keys.get(&key_hash).expect("key presented this retire");
        // The value semantics are the backend's: the reduction contract presents
        // values in their `Ord` order, so sort the (hash-ordered) accumulation.
        let mut pairs: Vec<(&V, RIn)> = input.iter().map(|&(i, ref d)| (&self.in_vals[i], d.clone())).collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let mut produced = Vec::new();
        (self.logic)(key, &pairs, &mut produced);
        produced
            .into_iter()
            .map(|(v, d)| {
                let id = stable_hash(&v);
                self.out_vals.entry(id).or_insert(v);
                (id, d)
            })
            .collect()
    }

    fn materialize(&mut self, records: ProxyChunk<T, ROut>, description: Description<T>) -> RefBatch<K, V2, T, ROut> {
        // Resolve ids to real data and re-order by the *real* record order — the proxy
        // ordering (by hash) is not the arrangement's ordering.
        let mut rows: Vec<((K, V2), T, ROut)> = (0..records.len())
            .map(|i| {
                let k = self.keys.get(&records.key_hashes()[i]).expect("key presented this retire").clone();
                let v = self.out_vals.get(&records.value_ids()[i]).expect("value presented or minted this retire").clone();
                ((k, v), records.times()[i].clone(), records.diffs()[i].clone())
            })
            .collect();
        crate::consolidation::consolidate_updates(&mut rows);
        // Feed the builder TARGET-sized chunks: handing it one giant chunk would make its
        // settle peel TARGET-sized pieces off the front, copying the remaining tail each
        // time — quadratic in the batch size.
        let mut builder = ChunkBuilder::<VecChunk<K, V2, T, ROut>>::with_capacity(0, 0, 0);
        let mut rows = rows.into_iter().peekable();
        while rows.peek().is_some() {
            let mut chunk = VecChunk::default();
            for row in rows.by_ref().take(<VecChunk<K, V2, T, ROut> as crate::trace::chunk::Chunk>::TARGET) {
                use timely::container::PushInto;
                chunk.push_into(row);
            }
            builder.push(&mut chunk);
        }
        builder.done(description)
    }
}
