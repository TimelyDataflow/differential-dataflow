//! A reference [`ProxyHalfJoinBackend`] over differential's `VecChunk` storage, in plain
//! `Vec`-of-rows form: the half-join counterpart of differential's `VecReduceBackend`, and the
//! backend `tests/proxy_half_join.rs` compares with the cursor half join on identical storage.

use std::rc::Rc;

use timely::progress::Timestamp;

use differential_dataflow::Hashable;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::int_proxy::{KeyPosition, ProxyBridge};
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::vec::VecChunk;

use super::proxy::{data_time, HalfJoinInstance, HalfJoinMatches, HalfJoinStream, Lifted, ProxyHalfJoinBackend};

/// The batch type of a hash-keyed [`ChunkSpine`](differential_dataflow::trace::chunk::vec::ChunkSpine):
/// updates `D` under their key's hash. Differential's `vec_backend` has the same alias, privately.
pub type VBatch<D, T, R> = Rc<ChunkBatch<VecChunk<u64, D, T, R>>>;

/// The length of `slice`'s prefix of records whose hash is `key`, which must be non-empty.
fn run_len<D, T, R>(slice: &[((u64, D), T, R)], key: u64) -> usize {
    debug_assert!(slice.first().is_some_and(|r| r.0.0 == key), "the run must start at `key`");
    let mut step = 1;
    while step < slice.len() && slice[step].0.0 == key { step <<= 1; }
    let lower = step >> 1;
    let upper = step.min(slice.len());
    lower + slice[lower..upper].partition_point(|r| r.0.0 <= key)
}

/// A reference [`ProxyHalfJoinBackend`] over [`VBatch`] storage.
///
/// The stream containers are `Vec<((K, V, T), T, R)>`: a `(key, val, time as data)` row at the
/// update's own time, as `dogs3`'s `half_join` streams them. The arrangement is hash-keyed, as the
/// reduce backend's is. Proxies are positions: a stream value's index in the staged, hash-sorted
/// list, and an arranged value's index in a pool of distinct rows cloned for the current block,
/// so that the arranged bridge is consolidated. A hash shared by several keys is matched in
/// proxy space and told apart by comparing keys in `cross`.
///
/// Arranged times are presented as stored; nothing advances them (see the half-join module).
pub struct VecHalfJoinBackend<K, V, V2, T, R, L> {
    /// User supplied output function over `(key, val, val2)`.
    logic: L,
    /// Configuration: stream updates per block.
    window_size: usize,
    /// The distinct arranged rows presented in the current block, by arranged proxy.
    pool: Vec<(K, V2)>,
    /// The block's arranged records, consolidated before proxies are minted.
    staged: Vec<((u64, (K, V2)), T, R)>,
    _marker: std::marker::PhantomData<(V, T, R)>,
}

impl<K, V, V2, T, R, L> VecHalfJoinBackend<K, V, V2, T, R, L> {
    /// A backend deferring value semantics to `logic`, covering the key space in blocks.
    pub fn new(logic: L) -> Self { Self::with_window(logic, 1 << 12) }

    /// A backend with an explicit block budget, in stream updates.
    pub fn with_window(logic: L, window_size: usize) -> Self {
        VecHalfJoinBackend { logic, window_size: window_size.max(1), pool: Vec::new(), staged: Vec::new(), _marker: std::marker::PhantomData }
    }
}

impl<K, V, V2, T, R, L, D> ProxyHalfJoinBackend<T, VBatch<(K, V2), T, R>, Vec<((K, V, T), T, R)>>
    for VecHalfJoinBackend<K, V, V2, T, R, L>
where
    K: Hashable<Output: Into<u64>> + Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    T: Lattice + Timestamp,
    R: Semigroup + Multiply<R, Output = R> + Ord + Clone + 'static,
    L: FnMut(&K, &V, &V2) -> D,
{
    type Key = u64;
    type V0 = u64;
    type V1 = u64;
    type R0 = R;
    type R1 = R;
    type ROut = R;
    type Output = Vec<((D, T), T, R)>;
    type Stream = Vec<((u64, (K, V, T)), T, R)>;

    /// The released updates, keyed by hash and sorted.
    fn stage(&mut self, chunks: Vec<Vec<((K, V, T), T, R)>>) -> Self::Stream {
        let mut staged: Vec<_> = chunks.into_iter().flatten()
            .map(|((k, v, data), t, r)| ((k.hashed().into(), (k, v, data)), t, r))
            .collect();
        staged.sort();
        staged
    }

    fn advance(
        &mut self,
        instance: &HalfJoinInstance<VBatch<(K, V2), T, R>, Self::Stream>,
        from: &mut KeyPosition<u64>,
        stream: &mut HalfJoinStream<T, R>,
        arranged: &mut ProxyBridge<T, R>,
    ) {
        let start = match *from { KeyPosition::Start => 0, KeyPosition::At(key) => key, KeyPosition::End => return };
        let staged = &instance.stream;
        let lo = staged.partition_point(|u| u.0.0 < start);
        // Whole keys, until the budget is spent.
        let mut hi = lo;
        let mut budget = self.window_size;
        while hi < staged.len() && budget > 0 {
            let end = hi + run_len(&staged[hi..], staged[hi].0.0);
            budget = budget.saturating_sub(end - hi);
            hi = end;
        }
        *from = if hi < staged.len() { KeyPosition::At(staged[hi].0.0) } else { KeyPosition::End };

        self.pool.clear();
        self.staged.clear();
        let mut i = lo;
        while i < hi {
            let hash = staged[i].0.0;
            let end = i + run_len(&staged[i..], hash);
            for (index, ((_, (_, _, data)), time, diff)) in staged[i..end].iter().enumerate() {
                stream.push(((hash, (i + index) as u64), Lifted { time: time.clone(), data: data.clone() }, diff.clone()));
            }
            // Every arranged row of this hash, from every chunk of every batch, as stored.
            for batch in instance.batches.iter() {
                for chunk in batch.chunks.iter() {
                    let slice = chunk.as_slice();
                    let mut p = slice.partition_point(|r| r.0.0 < hash);
                    while p < slice.len() && slice[p].0.0 == hash {
                        self.staged.push(slice[p].clone());
                        p += 1;
                    }
                }
            }
            i = end;
        }
        // Equal rows from different chunks net, and share one proxy: the bridge is then sorted
        // and consolidated by `((hash, proxy), time)` because proxies are minted in row order.
        consolidate_updates(&mut self.staged);
        for ((hash, row), time, diff) in self.staged.drain(..) {
            if self.pool.last() != Some(&row) { self.pool.push(row); }
            arranged.push(((hash, (self.pool.len() - 1) as u64), time, diff));
        }
    }

    fn cross(
        &mut self,
        instance: &HalfJoinInstance<VBatch<(K, V2), T, R>, Self::Stream>,
        stream: &HalfJoinStream<T, R>,
        arranged: &ProxyBridge<T, R>,
        matches: &mut HalfJoinMatches<R>,
        output: &mut Vec<Self::Output>,
    ) {
        let mut out = Vec::with_capacity(matches.pairs.len());
        for (&(a, b), diff) in matches.pairs.iter().zip(&matches.diffs) {
            let ((_, v0), lifted, _) = &stream[a];
            let ((_, v1), time2, _) = &arranged[b];
            let (_, (key, val, _)) = &instance.stream[*v0 as usize].0;
            let (key2, val2) = &self.pool[*v1 as usize];
            // A shared hash is not a shared key.
            if key != key2 { continue; }
            out.push((((self.logic)(key, val, val2), data_time(lifted, time2)), lifted.time.clone(), diff.clone()));
        }
        if !out.is_empty() { output.push(out); }
    }
}
