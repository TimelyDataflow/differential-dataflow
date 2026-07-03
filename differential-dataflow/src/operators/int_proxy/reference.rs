//! The in-memory reference backend: real data in [`VecChunk`] arrangements, ids by
//! hashing the real keys and values with the crate's [`Hashable`] (a deterministic,
//! process-wide-stable content hash — the "system-wide hash function" every backend must
//! fix for itself).
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

/// Read `batches` (restricted to keys whose hash is in the sorted `filter`, if any)
/// into parallel proxy and real columns, one entry per `(key, val, time, diff)` record.
fn read_rows<K, V, T, R>(
    batches: &[RefBatch<K, V, T, R>],
    filter: Option<&[u64]>,
    keys: &mut Vec<u64>,
    vals: &mut Vec<u64>,
    times: &mut Vec<T>,
    diffs: &mut Vec<R>,
    reals: &mut Vec<(K, V)>,
) where
    K: Ord + Clone + Hash + 'static,
    V: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp,
    R: Ord + Semigroup + 'static,
{
    for batch in batches {
        let mut cursor = batch.cursor();
        while let Some(k) = cursor.get_key(batch) {
            let kh = stable_hash(k);
            if filter.is_none_or(|f| f.binary_search(&kh).is_ok()) {
                while let Some(v) = cursor.get_val(batch) {
                    let vh = stable_hash(v);
                    cursor.map_times(batch, |t, d| {
                        keys.push(kh);
                        vals.push(vh);
                        times.push(t.clone());
                        diffs.push(d.clone());
                        reals.push((k.clone(), v.clone()));
                    });
                    cursor.step_val(batch);
                }
            }
            cursor.step_key(batch);
        }
    }
}

/// Present a batch list as a sorted, consolidated proxy run plus the aligned real
/// records (entry `i` is presented record `i`'s real `(key, val)`).
fn present<K, V, T, R>(batches: &[&[RefBatch<K, V, T, R>]], filter: Option<&[u64]>) -> (ProxyChunk<T, R>, Vec<(K, V)>)
where
    K: Ord + Clone + Hash + 'static,
    V: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp,
    R: Ord + Semigroup + 'static,
{
    let (mut ks, mut vs) = (Vec::new(), Vec::new());
    let (mut ts, mut ds) = (Vec::new(), Vec::new());
    let mut reals = Vec::new();
    for list in batches {
        read_rows(list, filter, &mut ks, &mut vs, &mut ts, &mut ds, &mut reals);
    }
    let (chunk, reps) = ProxyChunk::from_unsorted(ks, vs, ts, ds);
    let aligned = reps.iter().map(|&i| reals[i].clone()).collect();
    (chunk, aligned)
}

/// The join backend: presents [`VecChunk`] batches by hashing, applies a
/// `(key, val0, val1) → data` projection to matched index pairs, and emits
/// `Vec<(data, time, diff)>` containers.
pub struct VecJoinBackend<K, V0, V1, D, L> {
    logic: L,
    /// Real records aligned with the current first-input presentation.
    left: Vec<(K, V0)>,
    /// Real values aligned with the current second-input presentation.
    right: Vec<V1>,
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

    fn present0(&mut self, batches: &[RefBatch<K, V0, T, R0>]) -> ProxyChunk<T, R0> {
        let (chunk, reals) = present(&[batches], None);
        self.left = reals;
        chunk
    }

    fn present1(&mut self, batches: &[RefBatch<K, V1, T, R1>]) -> ProxyChunk<T, R1> {
        let (chunk, reals) = present(&[batches], None);
        self.right = reals.into_iter().map(|(_, v)| v).collect();
        chunk
    }

    fn cross(&mut self, left: &[usize], right: &[usize], times: Vec<T>, diffs: Vec<RO>) -> Vec<(D, T, RO)> {
        let mut out = Vec::with_capacity(left.len());
        for (((&l, &r), t), d) in left.iter().zip(right).zip(times).zip(diffs) {
            let (k, v0) = &self.left[l];
            let v1 = &self.right[r];
            out.push(((self.logic)(k, v0, v1), t, d));
        }
        out
    }
}

/// The reduce backend: presents [`VecChunk`] batches by hashing, applies the
/// user's reduction logic (row-reduce-shaped: `(key, &[(val, diff)], &mut output)`)
/// per key, mints output ids by hashing produced values, and materializes proxy records
/// back into [`VecChunk`] output batches.
pub struct VecReduceBackend<K, V, V2, L> {
    logic: L,
    /// `key_hash → key`, primed by the presentations of the current retire.
    keys: HashMap<u64, K>,
    /// Real values aligned with the current input presentation.
    in_vals: Vec<V>,
    /// `value_id → output value`, primed by the output presentation and by minting.
    out_vals: HashMap<u64, V2>,
}

impl<K, V, V2, L> VecReduceBackend<K, V, V2, L> {
    /// A backend applying `logic` — shaped like the row reduce's closure — per key.
    pub fn new(logic: L) -> Self {
        VecReduceBackend { logic, keys: HashMap::new(), in_vals: Vec::new(), out_vals: HashMap::new() }
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

    fn key_hashes(&self, batches: &[RefBatch<K, V, T, RIn>]) -> Vec<u64> {
        let mut hashes = Vec::new();
        for batch in batches {
            let mut cursor = batch.cursor();
            while let Some(k) = cursor.get_key(batch) {
                hashes.push(stable_hash(k));
                cursor.step_key(batch);
            }
        }
        hashes.sort_unstable();
        hashes.dedup();
        hashes
    }

    fn present_input(&mut self, history: &[RefBatch<K, V, T, RIn>], novel: &[RefBatch<K, V, T, RIn>], keys: &[u64]) -> ProxyChunk<T, RIn> {
        let (chunk, reals) = present(&[history, novel], Some(keys));
        for (k, _) in &reals {
            self.keys.entry(stable_hash(k)).or_insert_with(|| k.clone());
        }
        self.in_vals = reals.into_iter().map(|(_, v)| v).collect();
        chunk
    }

    fn present_output(&mut self, batches: &[RefBatch<K, V2, T, ROut>], keys: &[u64]) -> ProxyChunk<T, ROut> {
        let (chunk, reals) = present(&[batches], Some(keys));
        for (i, (k, v)) in reals.into_iter().enumerate() {
            self.keys.entry(chunk.key_hashes()[i]).or_insert(k);
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
        let mut chunk = VecChunk::default();
        for row in rows {
            use timely::container::PushInto;
            chunk.push_into(row);
        }
        let mut builder = ChunkBuilder::<VecChunk<K, V2, T, ROut>>::with_capacity(0, 0, 0);
        builder.push(&mut chunk);
        builder.done(description)
    }
}
