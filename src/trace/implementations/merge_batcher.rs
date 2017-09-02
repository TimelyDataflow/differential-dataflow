//! A general purpose `Batcher` implementation based on radix sort.

use timely::progress::frontier::Antichain;

use ::Diff;

use lattice::Lattice;
use trace::{Batch, Batcher, Builder, Cursor};

/// Creates batches from unordered tuples.
pub struct MergeBatcher<K: Ord, V: Ord, T: PartialOrd, R: Diff, B: Batch<K, V, T, R>> {
    sorted: Vec<B>,
    lower: Vec<T>,
    frontier: Antichain<T>,
    phantom: ::std::marker::PhantomData<(K,V,R)>,
}

impl<K, V, T, R, B> Batcher<K, V, T, R, B> for MergeBatcher<K, V, T, R, B> 
where 
    K: Ord+Clone, 
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Diff,
    B: Batch<K, V, T, R>, 
{
    fn new() -> Self { 
        MergeBatcher { 
            sorted: Vec::new(),
            frontier: Antichain::new(),
            lower: vec![T::minimum()],
            phantom: ::std::marker::PhantomData,
        } 
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: &mut Vec<((K,V),T,R)>) {

        batch.sort_by(|&(ref kv1, ref t1, _),&(ref kv2, ref t2, _)| (kv1, t1).cmp(&(kv2, t2)));
        for index in 1 .. batch.len() {
            if batch[index].0 == batch[index - 1].0 && batch[index].1 == batch[index - 1].1 {
                batch[index].2 = batch[index].2 + batch[index - 1].2;
                batch[index - 1].2 = R::zero();
            }
        }
        batch.retain(|x| !x.2.is_zero());

        let mut builder = B::Builder::with_capacity(batch.len());
        for ((key, val), time, diff) in batch.drain(..) {
            builder.push((key, val, time, diff));
        }

        let batch = builder.done(&self.lower[..], &self.lower[..], &self.lower[..]);
        self.sorted.push(batch);

        while self.sorted.len() > 1 && self.sorted[self.sorted.len()-1].len() > self.sorted[self.sorted.len()-2].len() / 2 {
            let batch1 = self.sorted.pop().unwrap();
            let batch2 = self.sorted.pop().unwrap();
            self.sorted.push(batch1.merge(&batch2));
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

        while self.sorted.len() > 1 {
            let batch1 = self.sorted.pop().unwrap();
            let batch2 = self.sorted.pop().unwrap();
            self.sorted.push(batch1.merge(&batch2));
        }

        if let Some(batch) = self.sorted.pop() {

            let (mut cursor, storage) = batch.cursor();
            while cursor.key_valid(&storage) {
                let key: &K = cursor.key(&storage);
                while cursor.val_valid(&storage) {
                    let val: &V = cursor.val(&storage);
                    cursor.map_times(&storage, |time, diff| {
                        if upper.iter().any(|t| t.less_equal(time)) {
                            builder_keep.push((key.clone(), val.clone(), time.clone(), diff));
                            self.frontier.insert(time.clone());
                        }
                        else {
                            builder_seal.push((key.clone(), val.clone(), time.clone(), diff));
                        }
                    });
                    cursor.step_val(&storage);
                }
                cursor.step_key(&storage);
            }
        }

        let keep = builder_keep.done(&upper[..], &upper[..], &upper[..]);
        let seal = builder_seal.done(&self.lower[..], &upper[..], &self.lower[..]);
        self.lower = upper.to_vec();
        if keep.len() > 0 { self.sorted.push(keep); }
        seal
    }

    // the frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> &[T] {
        self.frontier.elements()
    }
}