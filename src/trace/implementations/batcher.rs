//! A general purpose `Batcher` implementation based on radix sort.

use timely::progress::frontier::Antichain;
use timely_sort::{MSBRadixSorter, RadixSorterBase};

use ::Ring;
use hashable::Hashable;

use lattice::Lattice;
use trace::{Batch, Batcher, Builder};

/// Creates batches from unordered tuples.
pub struct RadixBatcher<K: Hashable, V, T: PartialOrd, R: Ring, B: Batch<K, V, T, R>> {
    phantom: ::std::marker::PhantomData<B>,
    buffers: Vec<Vec<((K, V), T, R)>>,
    sorted: usize,
    sorter: MSBRadixSorter<((K, V), T, R)>,
    stash: Vec<Vec<((K, V), T, R)>>,
    lower: Vec<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, R, B> RadixBatcher<K, V, T, R, B>
where 
    K: Ord+Clone+Hashable, 
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Ring,
    B: Batch<K, V, T, R> 
{
    // Provides an allocated buffer, either from stash or through allocation.
    fn empty(&mut self) -> Vec<((K, V), T, R)> {
        self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10))
    }

    // Compacts the representation of data in self.buffer and self.buffers.
    // This could, in principle, move data into a trace, because it is even more 
    // compact in that form, and easier to merge as compared to re-sorting.
    #[inline(never)]
    fn compact(&mut self) {
        self.sorter.sort_and(&mut self.buffers, &|x: &((K,V),T,R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));
        self.sorter.rebalance(&mut self.stash, 256);
        self.sorted = self.buffers.len();
        self.stash.clear(); // <-- too aggressive?
    }

    #[inline(never)]
    fn segment(&mut self, upper: &[T]) -> Vec<Vec<((K,V),T,R)>> {

        let mut to_keep = Vec::new();   // updates that are not yet ready.
        let mut to_seal = Vec::new();   // updates that are ready to go.

        let mut to_keep_tail = self.empty();
        let mut to_seal_tail = self.empty();

        // swing through each buffer, each element, and partition
        for mut buffer in self.buffers.drain(..) {
            for ((key, val), time, diff) in buffer.drain(..) {
                if !upper.iter().any(|t| t.le(&time)) {
                    if to_seal_tail.len() == to_seal_tail.capacity() {
                        if to_seal_tail.len() > 0 {
                            to_seal.push(to_seal_tail);
                        }
                        to_seal_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
                    }
                    to_seal_tail.push(((key, val), time, diff));
                }
                else {
                    if to_keep_tail.len() == to_keep_tail.capacity() {
                        if to_keep_tail.len() > 0 {
                            to_keep.push(to_keep_tail);
                        }
                        to_keep_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
                    }
                    to_keep_tail.push(((key, val), time, diff));
                }
            }
            self.stash.push(buffer);
        }

        if to_keep_tail.len() > 0 { to_keep.push(to_keep_tail); }
        if to_seal_tail.len() > 0 { to_seal.push(to_seal_tail); }

        self.buffers = to_keep;
        to_seal
    }
}

impl<K, V, T, R, B> Batcher<K, V, T, R, B> for RadixBatcher<K, V, T, R, B> 
where 
    K: Ord+Clone+Hashable, 
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Ring,
    B: Batch<K, V, T, R>, 
{
    fn new() -> Self { 
        RadixBatcher { 
            phantom: ::std::marker::PhantomData,
            buffers: Vec::new(),
            sorter: MSBRadixSorter::new(),
            sorted: 0,
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: vec![T::min()],
        } 
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: &mut Vec<((K,V),T,R)>) {

        // If we have spare capacity, copy contents rather than appending list.
        if self.buffers.last().map(|buf| buf.len() + batch.len() <= buf.capacity()) == Some(true) {
            self.buffers.last_mut().map(|buf| buf.extend(batch.drain(..)));
        }
        else {
            self.buffers.push(::std::mem::replace(batch, Vec::new()));
        }

        // If we have accepted a lot of data since our last compaction, compact again!
        if self.buffers.len() > ::std::cmp::max(2 * self.sorted, 1_000) {
            self.compact();
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time 
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more 
    // updates with times not greater or equal to `upper`.
    #[inline(never)]
    fn seal(&mut self, upper: &[T]) -> B {

        // TODO: We filter and then consolidate; we could consolidate and then filter, for general
        //       health of compact state. Would mean that repeated `seal` calls wouldn't have to 
        //       re-sort the data if we tracked a dirty bit. Maybe wouldn't be all that helpful, 
        //       if we aren't running a surplus of data (if the optimistic compaction isn't helpful).
        //
        // Until timely dataflow gets multiple capabilities per message, we will probably want to
        // consider sealing multiple batches at once, as we will get multiple requests with nearly
        // the same `upper`, as we retire a few capabilities in sequence. Food for thought, anyhow.
        //
        // Our goal here is to partition stashed updates into "those to keep" and "those to sort"
        // as efficiently as possible. In particular, if we can avoid lot of allocations, re-using
        // the allocations we already have, I would be delighted.

        // Extract data we plan to seal and ship.
        let mut to_seal = self.segment(upper);

        // Sort the data; this uses top-down MSB radix sort with an early exit to consolidate_vec.
        self.sorter.sort_and(&mut to_seal, &|x: &((K,V),T,R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));        

        // TODO: Add a `with_capacity` method to the trait, to pre-allocate space.
        let count = to_seal.iter().map(|x| x.len()).sum();
        let mut builder = B::Builder::with_capacity(count);
        for buffer in to_seal.iter_mut() {
            for ((key, val), time, diff) in buffer.drain(..) {
                debug_assert!(!diff.is_zero());
                builder.push((key, val, time, diff));
            }
        }

        // Recycle the consumed buffers, if appropriate.
        self.sorter.rebalance(&mut to_seal, 256);

        // Return the finished layer with its bounds.
        let result = builder.done(&self.lower[..], upper, &self.lower[..]);
        self.lower = upper.to_vec();
        result
    }

    fn frontier(&mut self) -> &[T] {
        self.frontier = Antichain::new();
        for buffer in &self.buffers {
            for &(_, ref time, _) in buffer {
                self.frontier.insert(time.clone());
            }
        }
        self.frontier.elements()
    }
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
#[inline(always)]
fn consolidate_vec<K: Ord+Hashable+Clone, V: Ord+Clone, T:Ord+Clone, R: Ring>(slice: &mut Vec<((K,V),T,R)>) {

    // IMPORTANT: This needs to order by the key's Hashable implementation!
    slice.sort_by(|&((ref k1, ref v1), ref t1, _),&((ref k2, ref v2), ref t2, _)| 
        (k1.hashed(), k1, v1, t1).cmp(&(k2.hashed(), k2, v2, t2))
    );
    for index in 1 .. slice.len() {
        if slice[index].0 == slice[index - 1].0 && slice[index].1 == slice[index - 1].1 {
            slice[index].2 = slice[index].2 + slice[index - 1].2;
            slice[index - 1].2 = R::zero();
        }
    }
    slice.retain(|x| !x.2.is_zero());
}