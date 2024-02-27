//! A general purpose `Batcher` implementation based on radix sort for TimelyStack.

use std::collections::VecDeque;
use crate::difference::Semigroup;
use crate::logging::{BatcherEvent, DifferentialEvent};
use crate::trace::{Batcher, Builder};
use timely::communication::message::RefOrMut;
use timely::logging::WorkerIdentifier;
use timely::logging_core::Logger;
use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::PartialOrder;

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
            lower: Antichain::from_elem(<T as Timestamp>::minimum()),
        }
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: RefOrMut<Vec<Self::Item>>) {
        // `batch` is either a shared reference or an owned allocations.
        match batch {
            RefOrMut::Ref(reference) => {
                // This is a moment at which we could capture the allocations backing
                // `batch` into a different form of region, rather than just  cloning.
                self.sorter.push(&mut reference.clone());
            }
            RefOrMut::Mut(reference) => {
                self.sorter.push(reference);
            }
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline]
    fn seal<B: Builder<Item=Self::Item, Time=Self::Time>>(&mut self, upper: Antichain<T>) -> B::Output {
        self.frontier.clear();
        let merged = self.sorter.extract_into(upper.borrow(), &mut self.frontier);

        // Determine the number of distinct keys, values, and updates,
        // and form a builder pre-sized for these numbers.
        let mut builder = {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            let mut prev_keyval = None;
            for ((key, val), _time, _) in merged.iter().map(|t| t.iter()).flatten() {
                if let Some((p_key, p_val)) = prev_keyval {
                    if p_key != key {
                        keys += 1;
                        vals += 1;
                    } else if p_val != val {
                        vals += 1;
                    }
                } else {
                    keys += 1;
                    vals += 1;
                }
                upds += 1;
                prev_keyval = Some((key, val));
            }
            B::with_capacity(keys, vals, upds)
        };

        for buffer in merged.into_iter() {
            for datum in &buffer[..] {
                builder.copy(datum);
            }
            // Recycling buffer.
            self.sorter.recycle(buffer);
        }

        // Drain buffers (fast reclamation).
        self.sorter.clear_stash();

        let seal = builder.done(
            self.lower.clone(),
            upper.clone(),
            Antichain::from_elem(T::minimum()),
        );
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
    queue: Vec<Vec<(Antichain<T>, Vec<(D, T, R)>)>>,
    stash: Vec<Vec<(D, T, R)>>,
    pending: Vec<(D, T, R)>,
    logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
    operator_id: usize,
}

impl<D: Ord, T: Clone + PartialOrder + Ord, R: Semigroup> MergeSorter<D, T, R> {

    const BUFFER_SIZE_BYTES: usize = 64 << 10;

    /// Buffer size (number of elements) to use for new/empty buffers.
    const fn buffer_size() -> usize {
        let size = std::mem::size_of::<(D, T, R)>();
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
            pending: Vec::new(),
        }
    }

    #[inline]
    fn empty(&mut self) -> Vec<(D, T, R)> {
        self.stash.pop().unwrap_or_else(|| Vec::with_capacity(Self::buffer_size()))
    }

    /// Remove all elements from the stash.
    fn clear_stash(&mut self) {
        self.stash.clear();
    }

    /// Insert an empty buffer into the stash. Panics if the buffer is not empty.
    fn recycle(&mut self, mut buffer: Vec<(D, T, R)>) {
        if buffer.capacity() == Self::buffer_size() && self.stash.len() < 2 {
            buffer.clear();
            self.stash.push(buffer);
        }
    }

    fn push(&mut self, batch: &mut Vec<(D, T, R)>) {
        // Ensure `self.pending` has a capacity of `Self::pending_buffer_size`.
        if self.pending.capacity() < Self::buffer_size() {
            self.pending
                .reserve(Self::buffer_size() - self.pending.capacity());
        }

        while !batch.is_empty() {
            self.pending.extend(
                batch.drain(
                    ..std::cmp::min(batch.len(), self.pending.capacity() - self.pending.len()),
                ),
            );
            if self.pending.len() == self.pending.capacity() {
                crate::consolidation::consolidate_updates(&mut self.pending);
                if self.pending.len() > self.pending.capacity() / 2 {
                    // Flush if `self.pending` is more than half full after consolidation.
                    self.flush_pending();
                }
            }
        }
    }

    /// Move all elements in `pending` into `queue`. The data in `pending` must be compacted and
    /// sorted. After this function returns, `self.pending` is empty.
    fn flush_pending(&mut self) {
        if !self.pending.is_empty() {
            let mut stack = self.empty();
            let mut frontier = Antichain::new();
            for tuple in self.pending.drain(..) {
                frontier.insert_ref(&tuple.1);
                stack.push(tuple);
            }
            let batch = vec![(frontier, stack)];
            self.account(&batch, 1);
            self.queue.push(batch);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() > self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }
        }
    }

    /// Maintain the internal chain structure. Ensures that:
    /// * All chains are sorted by size.
    /// * Within each chain, adjacent blocks are reduced, i.e., their combined length is larger than
    ///   the block size.
    /// * All chains are of geometrically increasing length.
    fn maintain(&mut self) {
        self.account(self.queue.iter().flatten(), -1);

        // Step 1: Canonicalize each chain by adjacent blocks that combined fit into a single block.
        for chain in &mut self.queue {
            let mut target: Vec<(Antichain<T>, Vec<_>)> = Vec::with_capacity(chain.len());
            for (frontier, block) in chain.drain(..) {
                if target.last().map_or(false, |(_, last)| {
                    last.len() + block.len() <= Self::buffer_size()
                }) {
                    // merge `target.last()` with `block`
                    let (last_frontier, last) = target.last_mut().unwrap();
                    for item in block.into_iter() {
                        last_frontier.insert_ref(&item.1);
                        last.push(item);
                    }
                } else {
                    target.push((frontier, block));
                }
            }
            *chain = target;
        }

        // Step 2: Sort queue by chain length. Depending on how much we extracted,
        // the chains might be mis-ordered.
        self.queue.sort_by_key(|chain| std::cmp::Reverse(chain.len()));

        // Step 3: Merge chains that are within a power of two.
        let mut index = self.queue.len().saturating_sub(1);
        while index > 0 {
            if self.queue[index-1].len() / 2 < self.queue[index].len() {
                // Chains at `index-1` and `index` are within a factor of two, merge them.
                let list1 = self.queue.remove(index-1);
                let list2 = std::mem::take(&mut self.queue[index-1]);
                self.queue[index-1] = self.merge_by(list1, list2);
            }
            index -= 1;
        }

        self.account(self.queue.iter().flatten(), 1);
    }

    /// Extract all data that is not in advance of `upper`. Record the lower bound of the remaining
    /// data's time in `frontier`.
    fn extract_into(
        &mut self,
        upper: AntichainRef<T>,
        frontier: &mut Antichain<T>,
    ) -> Vec<Vec<(D, T, R)>> {
        // Flush pending data
        crate::consolidation::consolidate_updates(&mut self.pending);
        self.flush_pending();

        let mut keep = self.empty();
        let mut keep_frontier = Antichain::new();
        let mut ship = self.empty();
        let mut ship_list = Vec::default();

        self.account(self.queue.iter().flatten(), -1);

        // Walk all chains, separate ready data from data to keep.
        for mut chain in std::mem::take(&mut self.queue).drain(..) {
            let mut block_list = Vec::default();
            let mut keep_list = Vec::default();
            for (block_frontier, mut block) in chain.drain(..) {
                // Is any data ready to be shipped?
                let any = !PartialOrder::less_equal(&upper, &block_frontier.borrow());
                // Is all data ready to be shipped?
                let all = any && block.iter().all(|(_, t, _)| !upper.less_equal(t));

                if all {
                    // All data is ready, push what we accumulated, stash whole block.
                    if !ship.is_empty() {
                        block_list.push((Antichain::new(), std::mem::replace(&mut ship, self.empty())));
                    }
                    block_list.push((Antichain::new(), block));
                } else if any {
                    // Iterate block, sorting items into ship and keep
                    for datum in block.drain(..) {
                        if upper.less_equal(&datum.1) {
                            frontier.insert_ref(&datum.1);
                            keep_frontier.insert_ref(&datum.1);
                            keep.push(datum);
                            if keep.capacity() == keep.len() {
                                // remember keep
                                keep_list.push((std::mem::take(&mut keep_frontier), std::mem::replace(&mut keep, self.empty())));
                            }
                        } else {
                            ship.push(datum);
                            if ship.capacity() == ship.len() {
                                // Ship is full, push in on the block list, get an empty one.
                                block_list.push((Antichain::new(), std::mem::replace(&mut ship, self.empty())));
                            }
                        }
                    }
                    // Recycle leftovers
                    self.recycle(block);
                } else {
                    // Keep the entire block.
                    if !keep.is_empty() {
                        keep_list.push((std::mem::take(&mut keep_frontier), std::mem::replace(&mut keep, self.empty())));
                    }
                    keep_list.push((block_frontier, block));
                }
            }

            // Capture any residue left after iterating blocks.
            if !ship.is_empty() {
                block_list.push((Antichain::new(), std::mem::replace(&mut ship, self.empty())));
            }
            if !keep.is_empty() {
                keep_list.push((std::mem::take(&mut keep_frontier), std::mem::replace(&mut keep, self.empty())));
            }

            // Collect finished chains
            if !block_list.is_empty() {
                ship_list.push(block_list);
            }
            if !keep_list.is_empty() {
                self.queue.push(keep_list);
            }
        }

        self.account(self.queue.iter().flatten(), 1);

        if ship_list.len() > 0 {
            self.maintain();
        }

        while ship_list.len() > 1 {
            let list1 = ship_list.pop().unwrap();
            let list2 = ship_list.pop().unwrap();
            ship_list.push(self.merge_by(list1, list2));
        }

        // Pop the last element, or return an empty chain.
        ship_list.pop().unwrap_or_default().into_iter().map(|(_, list)| list).collect()
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(
        &mut self,
        list1: Vec<(Antichain<T>, Vec<(D, T, R)>)>,
        list2: Vec<(Antichain<T>, Vec<(D, T, R)>)>,
    ) -> Vec<(Antichain<T>, Vec<(D, T, R)>)> {
        use std::cmp::Ordering;
        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = VecDeque::from(list1.next().map(|(_, list)| list).unwrap_or_default());
        let mut head2 = VecDeque::from(list2.next().map(|(_, list)| list).unwrap_or_default());

        let mut frontier = Antichain::new();
        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {

                let cmp = {
                    let x = head1.front().unwrap();
                    let y = head2.front().unwrap();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                match cmp {
                    Ordering::Less    => {
                        let datum = head1.pop_front().unwrap();
                        frontier.insert_ref(&datum.1);
                        result.push(datum);
                    },
                    Ordering::Greater => {
                        let datum = head2.pop_front().unwrap();
                        frontier.insert_ref(&datum.1);
                        result.push(datum);
                    },
                    Ordering::Equal   => {
                        let (data1, time1, mut diff1) = head1.pop_front().unwrap();
                        let (_data2, _time2, diff2) = head2.pop_front().unwrap();
                        diff1.plus_equals(&diff2);
                        if !diff1.is_zero() {
                            frontier.insert_ref(&time1);
                            result.push((data1, time1, diff1));
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                let frontier = std::mem::take(&mut frontier);
                output.push((frontier, result));
                result = self.empty();
            }

            if head1.is_empty() {
                self.recycle(Vec::from(head1));
                head1 = VecDeque::from(list1.next().map(|(_, list)| list).unwrap_or_default());
            }
            if head2.is_empty() {
                self.recycle(Vec::from(head2));
                head2 = VecDeque::from(list2.next().map(|(_, list)| list).unwrap_or_default());
            }
        }

        if result.len() > 0 {
            output.push((std::mem::take(&mut frontier), result));
        } else {
            self.recycle(result);
        }

        if !head1.is_empty() {
            let mut result = self.empty();
            for item1 in head1 {
                frontier.insert_ref(&item1.1);
                result.push(item1);
            }
            output.push((std::mem::take(&mut frontier), result));
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty();
            for item2 in head2 {
                frontier.insert_ref(&item2.1);
                result.push(item2);
            }
            output.push((std::mem::take(&mut frontier), result));
        }
        output.extend(list2);

        output
    }
}

impl<D, T, R> MergeSorter<D, T, R>
{
    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the [`TimelyStack`]s passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    fn account<'a, I: IntoIterator<Item = &'a (Antichain<T>, Vec<(D, T, R)>)>>(
        &self,
        items: I,
        diff: isize,
    ) where D: 'a, T: 'a, R: 'a {
        if let Some(logger) = &self.logger {
            let mut records = 0isize;
            for stack in items {
                records = records.saturating_add_unsigned(stack.1.len());
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
        self.account(self.queue.iter().flatten(), -1);
    }
}
