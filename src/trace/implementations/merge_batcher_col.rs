//! A general purpose `Batcher` implementation based on radix sort for TimelyStack.

use std::cmp::Ordering;
use timely::{Container, Data, PartialOrder};
use timely::communication::message::RefOrMut;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::progress::frontier::{Antichain, AntichainRef};
use crate::consolidation::consolidate_updates;

use crate::difference::Semigroup;
use crate::trace::Builder;
use crate::trace::implementations::merge_batcher::Merger;

/// A merger for timely stacks
pub struct ColumnationMerger<T> {
    pending: Vec<T>,
}

impl<T> Default for ColumnationMerger<T> {
    fn default() -> Self {
        Self { pending: Vec::default() }
    }
}

impl<T: Columnation> ColumnationMerger<T> {
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

    /// Buffer size for pending updates, currently 4 * [`Self::buffer_size`].
    fn pending_buffer_size(&self) -> usize {
        self.preferred_buffer_size() * 1
    }

    /// Helper to get pre-sized vector from the stash.
    #[inline]
    fn empty(&self, stash: &mut Vec<TimelyStack<T>>) -> TimelyStack<T> {
        stash.pop().unwrap_or_else(|| TimelyStack::with_capacity(self.preferred_buffer_size()))
    }

    /// Helper to return a batch to the stash.
    #[inline]
    fn recycle(&self, mut batch: TimelyStack<T>, stash: &mut Vec<TimelyStack<T>>) {
        // TODO: Should we limit the size of `stash`?
        if batch.capacity() == self.preferred_buffer_size() /*&& stash.len() < 2*/ {
            batch.clear();
            stash.push(batch);
        }
    }
}

impl<K, V, T,R> Merger for ColumnationMerger<((K,V), T, R)>
where
    K: Columnation + Ord + Data,
    V: Columnation + Ord + Data,
    T: Columnation + Ord + PartialOrder + Data,
    R: Columnation + Semigroup + 'static,
{
    type Time = T;
    type Input = Vec<((K,V),T,R)>;
    type Batch = TimelyStack<((K,V),T,R)>;
    type Output = ((K,V),T,R);

    fn accept(&mut self, batch: RefOrMut<Self::Input>, stash: &mut Vec<Self::Batch>) -> Vec<Self::Batch> {
        // `batch` is either a shared reference or an owned allocations.
        let mut batch: Vec<_> = match batch {
            RefOrMut::Ref(vec) => {
                let mut owned = Vec::default();
                owned.clone_from(vec);
                owned
            }
            RefOrMut::Mut(vec) => std::mem::take(vec)
        };
        // Ensure `self.pending` has a capacity of `Self::pending_buffer_size`.
        if self.pending.capacity() < self.pending_buffer_size() {
            self.pending.reserve(self.pending_buffer_size() - self.pending.capacity());
        }

        let mut output = Vec::default();

        while !batch.is_empty() {
            self.pending.extend(batch.drain(..std::cmp::min(batch.len(), self.pending.capacity() - self.pending.len())));
            if self.pending.len() == self.pending.capacity() {
                consolidate_updates(&mut self.pending);
                if self.pending.len() > self.pending.capacity() / 2 {
                    // Flush if `self.pending` is more than half full after consolidation.
                    let mut stack = self.empty(stash);
                    stack.reserve_items(self.pending.iter());
                    for tuple in self.pending.drain(..) {
                        stack.copy(&tuple);
                    }
                    output.push(stack);
                }
            }
        }

        output
    }

    fn finish(&mut self, _stash: &mut Vec<Self::Batch>) -> Vec<Self::Batch> {
        vec![]
    }

    fn merge(&mut self, list1: Vec<Self::Batch>, list2: Vec<Self::Batch>, output: &mut Vec<Self::Batch>, stash: &mut Vec<Self::Batch>) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = TimelyStackQueue::from(list1.next().unwrap_or_default());
        let mut head2 = TimelyStackQueue::from(list2.next().unwrap_or_default());

        let mut result = self.empty(stash);

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {

                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                match cmp {
                    Ordering::Less    => { result.copy(head1.pop()); }
                    Ordering::Greater => { result.copy(head2.pop()); }
                    Ordering::Equal   => {
                        let (data1, time1, diff1) = head1.pop();
                        let (_data2, _time2, diff2) = head2.pop();
                        let mut diff1 = diff1.clone();
                        diff1.plus_equals(diff2);
                        if !diff1.is_zero() {
                            result.copy_destructured(data1, time1, &diff1);
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.empty(stash);
            }

            if head1.is_empty() {
                self.recycle(head1.done(), stash);
                head1 = TimelyStackQueue::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                self.recycle(head2.done(), stash);
                head2 = TimelyStackQueue::from(list2.next().unwrap_or_default());
            }
        }

        if result.len() > 0 {
            output.push(result);
        } else {
            self.recycle(result, stash);
        }

        if !head1.is_empty() {
            let mut result = self.empty(stash);
            result.reserve_items(head1.iter());
            for item in head1.iter() { result.copy(item); }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty(stash);
            result.reserve_items(head2.iter());
            for item in head2.iter() { result.copy(item); }
            output.push(result);
        }
        output.extend(list2);
    }

    fn extract(&mut self, merged: Vec<Self::Batch>, upper: AntichainRef<Self::Time>, frontier: &mut Antichain<Self::Time>, readied: &mut Vec<Self::Batch>, kept: &mut Vec<Self::Batch>, stash: &mut Vec<Self::Batch>) {
        let mut keep = self.empty(stash);
        let mut ready = self.empty(stash);

        for buffer in merged {
            for d @ (_data, time, _diff) in buffer.iter() {
                if upper.less_equal(time) {
                    frontier.insert(time.clone());
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.empty(stash);
                    }
                    keep.copy(d);
                }
                else {
                    if ready.len() == ready.capacity() && !ready.is_empty() {
                        readied.push(ready);
                        ready = self.empty(stash);
                    }
                    ready.copy(d);
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

    fn seal<B: Builder<Input=Self::Output, Time=Self::Time>>(chain: &mut Vec<Self::Batch>, lower: AntichainRef<Self::Time>, upper: AntichainRef<Self::Time>, since: AntichainRef<Self::Time>) -> B::Output {
        let mut builder = {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            let mut prev_keyval = None;
            for buffer in chain.iter() {
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
                        } else {
                            keys += 1;
                            vals += 1;
                            upds += 1;
                        }
                        prev_keyval = Some((key, val));
                    }
                }
            }
            B::with_capacity(keys, vals, upds)
        };

        for datum in chain.iter().map(|ts| ts.iter()).flatten() {
            builder.copy(datum);
        }

        builder.done(lower.to_owned(), upper.to_owned(), since.to_owned())
    }

    fn account(batch: &Self::Batch) -> (usize, usize, usize, usize) {
        let (mut size, mut capacity, mut allocations) = (0, 0, 0);
        let cb = |siz, cap| {
            size += siz;
            capacity += cap;
            allocations += 1;
        };
        batch.heap_size(cb);
        (batch.len(), size, capacity, allocations)
    }
}


struct TimelyStackQueue<T: Columnation> {
    list: TimelyStack<T>,
    head: usize,
}

impl<T: Columnation> Default for TimelyStackQueue<T> {
    fn default() -> Self {
        Self::from(Default::default())
    }
}

impl<T: Columnation> TimelyStackQueue<T> {

    fn pop(&mut self) -> &T {
        self.head += 1;
        &self.list[self.head - 1]
    }

    fn peek(&self) -> &T {
        &self.list[self.head]
    }

    fn from(list: TimelyStack<T>) -> Self {
        TimelyStackQueue {
            list,
            head: 0,
        }
    }

    fn done(self) -> TimelyStack<T> {
        self.list
    }

    fn is_empty(&self) -> bool { self.head == self.list[..].len() }

    /// Return an iterator over the remaining elements.
    fn iter(&self) -> impl Iterator<Item=&T> + Clone + ExactSizeIterator {
        self.list[self.head..].iter()
    }
}
