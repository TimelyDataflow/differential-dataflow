//! A general purpose `Batcher` implementation based on radix sort.

use std::collections::VecDeque;
use std::marker::PhantomData;

use timely::communication::message::RefOrMut;
use timely::{Container, PartialOrder};
use timely::logging::WorkerIdentifier;
use timely::logging_core::Logger;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::consolidation::consolidate_updates;
use crate::difference::Semigroup;
use crate::logging::{BatcherEvent, DifferentialEvent};
use crate::trace::{Batcher, Builder};

/// Creates batches from unordered tuples.
pub struct MergeBatcher<M, T>
where
    M: Merger,
{
    /// each power-of-two length list of allocations. Do not push/pop directly but use the corresponding functions.
    queue: Vec<Vec<M::Batch>>,
    /// Stash of empty batches
    stash: Vec<M::Batch>,
    /// Thing to accept data, merge chains, and talk to the builder.
    merger: M,
    /// Logger for size accounting.
    logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
    /// Timely operator ID.
    operator_id: usize,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<T>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<T>,
}

impl<M, T> Batcher for MergeBatcher<M, T>
where
    M: Merger<Time=T>,
    T: Timestamp,
{
    type Input = M::Input;
    type Output = M::Batch;
    type Time = T;

    fn new(logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            merger: M::default(),
            queue: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(T::minimum()),
        }
    }

    fn push_batch(&mut self, batch: RefOrMut<M::Input>) {
        let batch = self.merger.accept(batch, &mut self.stash);
        self.insert_chain(batch);
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    fn seal<B: Builder<Input=Self::Output, Time=Self::Time>>(&mut self, upper: Antichain<T>) -> B::Output {
        // Finish
        let batch = self.merger.finish(&mut self.stash);
        if !batch.is_empty() {
            self.queue_push(batch);
        }

        // Merge all remaining chains into a single chain.
        while self.queue.len() > 1 {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }
        let merged = self.queue_pop().unwrap_or_default();

        // Extract readied data.
        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();

        self.merger.extract(merged, upper.borrow(), &mut self.frontier, &mut readied, &mut kept, &mut self.stash);

        if !kept.is_empty() {
            self.queue_push(kept);
        }

        self.stash.clear();

        let seal = B::from_batches(&mut readied, self.lower.borrow(), upper.borrow(), Antichain::from_elem(T::minimum()).borrow());
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<T> {
        self.frontier.borrow()
    }
}
impl<M, T> MergeBatcher<M, T>
where
    M: Merger
{
    fn insert_chain(&mut self, chain: Vec<<M as Merger>::Batch>) {
        if !chain.is_empty() {
            self.queue_push(chain);
            while self.queue.len() > 1 && (self.queue[self.queue.len() - 1].len() >= self.queue[self.queue.len() - 2].len() / 2) {
                let list1 = self.queue_pop().unwrap();
                let list2 = self.queue_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue_push(merged);
            }
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(&mut self, list1: Vec<M::Batch>, list2: Vec<M::Batch>) -> Vec<M::Batch> {
        self.account(list1.iter().chain(list2.iter()).map(M::account), -1);

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        self.merger.merge(list1, list2, &mut output, &mut self.stash);

        output
    }

    /// Pop a batch from `self.queue` and account size changes.
    #[inline]
    fn queue_pop(&mut self) -> Option<Vec<M::Batch>> {
        let batch = self.queue.pop();
        self.account(batch.iter().flatten().map(M::account), -1);
        batch
    }

    /// Push a batch to `self.queue` and account size changes.
    #[inline]
    fn queue_push(&mut self, batch: Vec<M::Batch>) {
        self.account(batch.iter().map(M::account), 1);
        self.queue.push(batch);
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the [`TimelyStack`]s passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    #[inline]
    fn account<I: IntoIterator<Item=(usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
        if let Some(logger) = &self.logger {
            let (mut records, mut size, mut capacity, mut allocations) = (0isize, 0isize, 0isize, 0isize);
            for (records_, size_, capacity_, allocations_) in items {
                records = records.saturating_add_unsigned(records_);
                size = size.saturating_add_unsigned(size_);
                capacity = capacity.saturating_add_unsigned(capacity_);
                allocations = allocations.saturating_add_unsigned(allocations_);
            }
            logger.log(BatcherEvent {
                operator: self.operator_id,
                records_diff: records * diff,
                size_diff: size * diff,
                capacity_diff: capacity * diff,
                allocations_diff: allocations * diff,
            })
        }
    }
}

impl<M: Merger, T> Drop for MergeBatcher<M, T> {
    fn drop(&mut self) {
        while self.queue_pop().is_some() { }
    }
}

/// A trait to describe interesting moments in a merge batcher.
pub trait Merger: Default {
    /// The type of update containers received from inputs.
    type Input;
    /// The internal representation of batches of data.
    type Batch: Container;
    /// The type of time in frontiers to extract updates.
    type Time;
    /// Accept a fresh batch of input data.
    fn accept(&mut self, batch: RefOrMut<Self::Input>, stash: &mut Vec<Self::Batch>) -> Vec<Self::Batch>;
    /// Finish processing any stashed data.
    fn finish(&mut self, stash: &mut Vec<Self::Batch>) -> Vec<Self::Batch>;
    /// Merge chains into an output chain.
    fn merge(&mut self, list1: Vec<Self::Batch>, list2: Vec<Self::Batch>, output: &mut Vec<Self::Batch>, stash: &mut Vec<Self::Batch>);
    /// Extract ready updates based on the `upper` frontier.
    fn extract(&mut self, merged: Vec<Self::Batch>, upper: AntichainRef<Self::Time>, frontier: &mut Antichain<Self::Time>, readied: &mut Vec<Self::Batch>, keep: &mut Vec<Self::Batch>, stash: &mut Vec<Self::Batch>);

    /// Account size and allocation changes. Returns a tuple of (records, size, capacity, allocations).
    fn account(batch: &Self::Batch) -> (usize, usize, usize, usize);
}

/// A merger that knows how to accept and maintain chains of vectors.
pub struct VecMerger<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for VecMerger<T> {
    fn default() -> Self {
        Self {_marker: PhantomData}
    }
}

impl<T> VecMerger<T> {
    const BUFFER_SIZE_BYTES: usize = 1 << 13;
    fn preferred_buffer_size(&self) -> usize {
        let size = ::std::mem::size_of::<T>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Helper to get pre-sized vector from the stash.
    #[inline]
    fn empty(&self, stash: &mut Vec<Vec<T>>) -> Vec<T> {
        stash.pop().unwrap_or_else(|| Vec::with_capacity(self.preferred_buffer_size()))
    }

    /// Helper to return a batch to the stash.
    #[inline]
    fn recycle(&self, mut batch: Vec<T>, stash: &mut Vec<Vec<T>>) {
        // TODO: Should we limit the size of `stash`?
        if batch.capacity() == self.preferred_buffer_size() /*&& stash.len() < 2*/ {
            batch.clear();
            stash.push(batch);
        }
    }
}

impl<D: Ord+Clone+'static, T: Ord + PartialOrder + Clone+'static,R: Semigroup+'static> Merger for VecMerger<(D,T,R)> {
    type Time = T;
    type Input = Vec<(D,T,R)>;
    type Batch = Vec<(D,T,R)>;

    fn accept(&mut self, batch: RefOrMut<Vec<(D,T,R)>>, stash: &mut Vec<Self::Batch>) -> Vec<Vec<(D, T, R)>> {
        // `batch` is either a shared reference or an owned allocations.
        let mut owned = match batch {
            RefOrMut::Ref(vec) => {
                let mut owned = self.empty(stash);
                owned.clone_from(vec);
                owned
            }
            RefOrMut::Mut(vec) => std::mem::take(vec)
        };
        consolidate_updates(&mut owned);
        if owned.capacity() == self.preferred_buffer_size() {
            vec![owned]
        } else {
            let mut chain = Vec::with_capacity((owned.len() + self.preferred_buffer_size() - 1) / self.preferred_buffer_size());
            let mut iter = owned.drain(..).peekable();
            while iter.peek().is_some() {
                let mut batch = self.empty(stash);
                batch.extend((&mut iter).take(self.preferred_buffer_size()));
                chain.push(batch);
            }
            chain
        }
    }

    fn finish(&mut self, _stash: &mut Vec<Vec<(D, T, R)>>) -> Vec<Vec<(D, T, R)>> {
        vec![]
    }

    fn merge(&mut self, list1: Vec<Vec<(D, T, R)>>, list2: Vec<Vec<(D, T, R)>>, output: &mut Vec<Vec<(D, T, R)>>, stash: &mut Vec<Vec<(D, T, R)>>) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();
        let mut head1 = VecDeque::from(list1.next().unwrap_or_default());
        let mut head2 = VecDeque::from(list2.next().unwrap_or_default());

        let mut result = self.empty(stash);

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {

                let cmp = {
                    let x = head1.front().unwrap();
                    let y = head2.front().unwrap();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                use std::cmp::Ordering;
                match cmp {
                    Ordering::Less    => result.push(head1.pop_front().unwrap()),
                    Ordering::Greater => result.push(head2.pop_front().unwrap()),
                    Ordering::Equal   => {
                        let (data1, time1, mut diff1) = head1.pop_front().unwrap();
                        let (_data2, _time2, diff2) = head2.pop_front().unwrap();
                        diff1.plus_equals(&diff2);
                        if !diff1.is_zero() {
                            result.push((data1, time1, diff1));
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.empty(stash);
            }

            if head1.is_empty() {
                let done1 = Vec::from(head1);
                self.recycle(done1, stash);
                head1 = VecDeque::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                let done2 = Vec::from(head2);
                self.recycle(done2, stash);
                head2 = VecDeque::from(list2.next().unwrap_or_default());
            }
        }

        if !result.is_empty() { output.push(result); }
        else { self.recycle(result, stash); }

        if !head1.is_empty() {
            let mut result = self.empty(stash);
            for item1 in head1 { result.push(item1); }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty(stash);
            for item2 in head2 { result.push(item2); }
            output.push(result);
        }
        output.extend(list2);
    }

    fn extract(&mut self, merged: Vec<Vec<(D, T, R)>>, upper: AntichainRef<Self::Time>, frontier: &mut Antichain<Self::Time>, readied: &mut Vec<Vec<(D, T, R)>>, kept: &mut Vec<Vec<(D, T, R)>>, stash: &mut Vec<Vec<(D, T, R)>>) {
        let mut keep = self.empty(stash);
        let mut ready = self.empty(stash);

        for mut buffer in merged {
            for (data, time, diff) in buffer.drain(..) {
                if upper.less_equal(&time) {
                    frontier.insert(time.clone());
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.empty(stash);
                    }
                    keep.push((data, time, diff));
                }
                else {
                    if ready.len() == ready.capacity() && !ready.is_empty() {
                        readied.push(ready);
                        ready = self.empty(stash);
                    }
                    ready.push((data, time, diff));
                }
            }
            // Recycling buffer.
            self.recycle(buffer, stash);
        }
        // Finish the kept data.
        if !keep.is_empty() {
            kept.push(keep);
        }
        if !ready.is_empty() {
            readied.push(ready);
        }
    }

    fn account(batch: &Self::Batch) -> (usize, usize, usize, usize) {
        (batch.len(), 0, 0, 0)
    }
}
