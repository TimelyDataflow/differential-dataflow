//! A general purpose `Batcher` implementation based on radix sort.

use std::collections::VecDeque;

use timely::communication::message::RefOrMut;
use timely::logging::WorkerIdentifier;
use timely::logging_core::Logger;
use timely::progress::{frontier::Antichain, Timestamp};

use crate::difference::Semigroup;
use crate::logging::{BatcherEvent, DifferentialEvent};
use crate::trace::{Batcher, Builder};

/// Creates batches from unordered tuples.
pub struct MergeBatcher<K, V, T, D> {
    sorter: MergeSorter<(K, V), T, D>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, D> Batcher for MergeBatcher<K, V, T, D>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Timestamp,
    D: Semigroup,
{
    type Item = ((K,V),T,D);
    type Time = T;

    fn new(logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>, operator_id: usize) -> Self {
        MergeBatcher {
            sorter: MergeSorter::new(logger, operator_id),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(T::minimum()),
        }
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: RefOrMut<Vec<Self::Item>>) {
        // `batch` is either a shared reference or an owned allocations.
        match batch {
            RefOrMut::Ref(reference) => {
                // This is a moment at which we could capture the allocations backing
                // `batch` into a different form of region, rather than just  cloning.
                let mut owned: Vec<_> = self.sorter.empty();
                owned.clone_from(reference);
                self.sorter.push(&mut owned);
            },
            RefOrMut::Mut(reference) => {
                self.sorter.push(reference);
            }
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline(never)]
    fn seal<B: Builder<Item=Self::Item, Time=Self::Time>>(&mut self, upper: Antichain<T>) -> B::Output {

        let mut merged = Vec::new();
        self.sorter.finish_into(&mut merged);

        // Determine the number of distinct keys, values, and updates,
        // and form a builder pre-sized for these numbers.
        let mut builder = {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            let mut prev_keyval = None;
            for buffer in merged.iter() {
                for ((key, val), time, _) in buffer.iter() {
                    if !upper.less_equal(time) {
                        if let Some((p_key, p_val)) = prev_keyval {
                            if p_key != key {
                                keys += 1;
                                vals += 1;
                            }
                            else if p_val != val {
                                vals += 1;
                            }
                            upds += 1;
                        }
                        prev_keyval = Some((key, val));
                    }
                }
            }
            B::with_capacity(keys, vals, upds)
        };

        let mut kept = Vec::new();
        let mut keep = Vec::new();

        self.frontier.clear();

        // TODO: Re-use buffer, rather than dropping.
        for mut buffer in merged.drain(..) {
            for ((key, val), time, diff) in buffer.drain(..) {
                if upper.less_equal(&time) {
                    self.frontier.insert(time.clone());
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.sorter.empty();
                    }
                    keep.push(((key, val), time, diff));
                }
                else {
                    builder.push(((key, val), time, diff));
                }
            }
            // Recycling buffer.
            self.sorter.push(&mut buffer);
        }

        // Finish the kept data.
        if !keep.is_empty() {
            kept.push(keep);
        }
        if !kept.is_empty() {
            self.sorter.push_list(kept);
        }

        // Drain buffers (fast reclaimation).
        // TODO : This isn't obviously the best policy, but "safe" wrt footprint.
        //        In particular, if we are reading serialized input data, we may
        //        prefer to keep these buffers around to re-fill, if possible.
        let mut buffer = Vec::new();
        self.sorter.push(&mut buffer);
        // We recycle buffers with allocations (capacity, and not zero-sized).
        while buffer.capacity() > 0 && std::mem::size_of::<((K,V),T,D)>() > 0 {
            buffer = Vec::new();
            self.sorter.push(&mut buffer);
        }

        let seal = builder.done(self.lower.clone(), upper.clone(), Antichain::from_elem(T::minimum()));
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<T> {
        self.frontier.borrow()
    }
}

struct MergeSorter<D, T, R> {
    /// each power-of-two length list of allocations. Do not push/pop directly but use the corresponding functions.
    queue: Vec<Vec<Vec<(D, T, R)>>>,
    stash: Vec<Vec<(D, T, R)>>,
    logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
    operator_id: usize,
}

impl<D: Ord, T: Ord, R: Semigroup> MergeSorter<D, T, R> {

    const BUFFER_SIZE_BYTES: usize = 1 << 13;

    fn buffer_size() -> usize {
        let size = ::std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    #[inline]
    fn new(logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            queue: Vec::new(),
            stash: Vec::new(),
        }
    }

    #[inline]
    pub fn empty(&mut self) -> Vec<(D, T, R)> {
        self.stash.pop().unwrap_or_else(|| Vec::with_capacity(Self::buffer_size()))
    }

    #[inline]
    pub fn push(&mut self, batch: &mut Vec<(D, T, R)>) {
        // TODO: Reason about possible unbounded stash growth. How to / should we return them?
        // TODO: Reason about mis-sized vectors, from deserialized data; should probably drop.
        let mut batch = if self.stash.len() > 2 {
            ::std::mem::replace(batch, self.stash.pop().unwrap())
        }
        else {
            ::std::mem::take(batch)
        };

        if !batch.is_empty() {
            crate::consolidation::consolidate_updates(&mut batch);
            self.account([batch.len()], 1);
            self.queue_push(vec![batch]);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue_pop().unwrap();
                let list2 = self.queue_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue_push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want
    // to break it down to be so.
    pub fn push_list(&mut self, list: Vec<Vec<(D, T, R)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }
        self.queue_push(list);
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<Vec<(D, T, R)>>) {
        while self.queue.len() > 1 {
            let list1 = self.queue_pop().unwrap();
            let list2 = self.queue_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue_push(merged);
        }

        if let Some(mut last) = self.queue_pop() {
            ::std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<Vec<(D, T, R)>>, list2: Vec<Vec<(D, T, R)>>) -> Vec<Vec<(D, T, R)>> {
        self.account(list1.iter().chain(list2.iter()).map(Vec::len), -1);

        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = VecDeque::from(list1.next().unwrap_or_default());
        let mut head2 = VecDeque::from(list2.next().unwrap_or_default());

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {

                let cmp = {
                    let x = head1.front().unwrap();
                    let y = head2.front().unwrap();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
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
                result = self.empty();
            }

            if head1.is_empty() {
                let done1 = Vec::from(head1);
                if done1.capacity() == Self::buffer_size() { self.stash.push(done1); }
                head1 = VecDeque::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                let done2 = Vec::from(head2);
                if done2.capacity() == Self::buffer_size() { self.stash.push(done2); }
                head2 = VecDeque::from(list2.next().unwrap_or_default());
            }
        }

        if !result.is_empty() { output.push(result); }
        else if result.capacity() > 0 { self.stash.push(result); }

        if !head1.is_empty() {
            let mut result = self.empty();
            for item1 in head1 { result.push(item1); }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty();
            for item2 in head2 { result.push(item2); }
            output.push(result);
        }
        output.extend(list2);

        output
    }
}

impl<D, T, R> MergeSorter<D, T, R> {
    /// Pop a batch from `self.queue` and account size changes.
    #[inline]
    fn queue_pop(&mut self) -> Option<Vec<Vec<(D, T, R)>>> {
        let batch = self.queue.pop();
        self.account(batch.iter().flatten().map(Vec::len), -1);
        batch
    }

    /// Push a batch to `self.queue` and account size changes.
    #[inline]
    fn queue_push(&mut self, batch: Vec<Vec<(D, T, R)>>) {
        self.account(batch.iter().map(Vec::len), 1);
        self.queue.push(batch);
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the [`TimelyStack`]s passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    fn account<I: IntoIterator<Item=usize>>(&self, items: I, diff: isize) {
        if let Some(logger) = &self.logger {
            let mut records= 0isize;
            for len in items {
                records = records.saturating_add_unsigned(len);
            }
            logger.log(BatcherEvent {
                operator: self.operator_id,
                records_diff: records * diff,
                size_diff: 0,
                capacity_diff: 0,
                allocations_diff: 0,
            })
        }
    }
}

impl<D, T, R> Drop for MergeSorter<D, T, R> {
    fn drop(&mut self) {
        while self.queue_pop().is_some() { }
    }
}
