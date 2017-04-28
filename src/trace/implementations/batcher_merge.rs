//! A general purpose `Batcher` implementation based on radix sort.

use timely::progress::frontier::Antichain;
use timely_sort::{MSBRadixSorter, RadixSorterBase};

use ::Diff;
use hashable::Hashable;

use lattice::Lattice;
use trace::{Batch, Batcher, Builder, Cursor};

/// Creates batches from unordered tuples.
pub struct RadixBatcher<K: Hashable, V, T: PartialOrd, R: Diff, B: Batch<K, V, T, R>> {
    phantom: ::std::marker::PhantomData<B>,
    buffers: Vec<Vec<((K, V), T, R)>>,
    sorted: Option<B>,
    sorter: MSBRadixSorter<((K, V), T, R)>,
    lower: Vec<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, R, B> RadixBatcher<K, V, T, R, B>
where 
    K: Ord+Clone+Hashable, 
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Diff,
    B: Batch<K, V, T, R> 
{
    // converts a buffer of data into a batch. 
    fn batch_buffers(&mut self) -> B {

        let count = self.buffers.iter().map(|x| x.len()).sum();
        let mut builder = B::Builder::with_capacity(count);
        for buffer in self.buffers.iter_mut() {
            for ((key, val), time, diff) in buffer.drain(..) {
                debug_assert!(!diff.is_zero());
                builder.push((key, val, time, diff));
            }
        }

        // Recycle the consumed buffers, if appropriate.
        self.sorter.rebalance(&mut self.buffers, 256);
        self.buffers.clear();
        builder.done(&self.lower[..], &self.lower[..], &self.lower[..])
    }

    // Compacts the representation of data in self.buffer and self.buffers.
    // This could, in principle, move data into a trace, because it is even more 
    // compact in that form, and easier to merge as compared to re-sorting.
    #[inline(never)]
    fn compact(&mut self) {

        self.sorter.sort_and(&mut self.buffers, &|x: &((K,V),T,R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));
        let batch = self.batch_buffers();
        if self.sorted.is_some() {
            self.sorted = Some(self.sorted.take().unwrap().merge(&batch));
        }
        else {
            self.sorted = Some(batch);
        }
    }
}

impl<K, V, T, R, B> Batcher<K, V, T, R, B> for RadixBatcher<K, V, T, R, B> 
where 
    K: Ord+Clone+Hashable, 
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Diff,
    B: Batch<K, V, T, R>, 
{
    fn new() -> Self { 
        RadixBatcher { 
            phantom: ::std::marker::PhantomData,
            buffers: Vec::new(),
            sorter: MSBRadixSorter::new(),
            sorted: None,
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
        // This uses the "rule" that a buffer has about 1,000 element in it. Totally fake!
        if self.buffers.len() > ::std::cmp::max(self.sorted.as_ref().map(|x| x.len()).unwrap_or(0) / 1_000, 1_000) {
            self.compact();
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time 
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more 
    // updates with times not greater or equal to `upper`.
    #[inline(never)]
    fn seal(&mut self, upper: &[T]) -> B {

        // We start with likely some buffers in `self.buffers`, and maybe a batch in `self.sorted`.
        //
        // These data are possibly a mix of times, which may include times greater or equal to elements
        // of `upper`, which should *not* be included in the output batch. Consequently, we need to be
        // ready to swing through both self.buffers and self.sorted and segment the data into what we
        // seal and what we keep.
        // 
        // We can either segment and then sort, or sort and then segment. I'm not exactly clear on which
        // is obviously better than the other, so for now let's do the easiest one. I'm not exatly clear
        // which one is easier yet either.
        //
        // As we will need to be able to segment self.sorted, a batch, we can probably just write the 
        // batch segmentation code, from one batch into two batches, and leave whatever we keep behind
        // in `self.sorted`. That means we should 

        let mut builder_keep = B::Builder::new();
        let mut builder_seal = B::Builder::new();

        // TODO: make this `clear()` when that lands.
        self.frontier = Antichain::new();
        self.compact();

        if let Some(batch) = self.sorted.take() {

            let mut cursor = batch.cursor();
            while cursor.key_valid() {
                let key: K = cursor.key().clone();
                while cursor.val_valid() {
                    let val: V = cursor.val().clone();
                    cursor.map_times(|time, diff| {
                        if upper.iter().any(|t| t.less_equal(time)) {
                            builder_keep.push((key.clone(), val.clone(), time.clone(), diff));
                            self.frontier.insert(time.clone());
                        }
                        else {
                            builder_seal.push((key.clone(), val.clone(), time.clone(), diff));
                        }
                    });
                    cursor.step_val();
                }
                cursor.step_key();
            }
        }

        let keep = builder_keep.done(&upper[..], &upper[..], &upper[..]);
        let seal = builder_seal.done(&self.lower[..], &upper[..], &self.lower[..]);
        self.lower = upper.to_vec();
        self.sorted = if keep.len() > 0 { Some(keep) } else { None };
        seal
    }

    // the frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> &[T] {
        self.frontier.elements()
    }
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
#[inline(always)]
fn consolidate_vec<K: Ord+Hashable+Clone, V: Ord+Clone, T:Ord+Clone, R: Diff>(slice: &mut Vec<((K,V),T,R)>) {

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