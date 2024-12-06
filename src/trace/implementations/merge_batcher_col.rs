//! A general purpose `Batcher` implementation based on radix sort for TimelyStack.

use std::cmp::Ordering;
use std::marker::PhantomData;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::{Container, Data, PartialOrder};

use crate::difference::Semigroup;
use crate::trace::implementations::merge_batcher::Merger;

/// A merger for timely stacks
pub struct ColumnationMerger<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for ColumnationMerger<T> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: Columnation> ColumnationMerger<T> {
    const BUFFER_SIZE_BYTES: usize = 64 << 10;
    fn chunk_capacity(&self) -> usize {
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
    fn empty(&self, stash: &mut Vec<TimelyStack<T>>) -> TimelyStack<T> {
        stash.pop().unwrap_or_else(|| TimelyStack::with_capacity(self.chunk_capacity()))
    }

    /// Helper to return a chunk to the stash.
    #[inline]
    fn recycle(&self, mut chunk: TimelyStack<T>, stash: &mut Vec<TimelyStack<T>>) {
        // TODO: Should we limit the size of `stash`?
        if chunk.capacity() == self.chunk_capacity() {
            chunk.clear();
            stash.push(chunk);
        }
    }
}

impl<K, V, T, R> Merger for ColumnationMerger<((K, V), T, R)>
where
    K: Columnation + Ord + Data,
    V: Columnation + Ord + Data,
    T: Columnation + Ord + PartialOrder + Data,
    R: Columnation + Semigroup + 'static,
{
    type Time = T;
    type Chunk = TimelyStack<((K, V), T, R)>;

    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>) {
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
                    Ordering::Less => {
                        result.copy(head1.pop());
                    }
                    Ordering::Greater => {
                        result.copy(head2.pop());
                    }
                    Ordering::Equal => {
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
            for item in head1.iter() {
                result.copy(item);
            }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty(stash);
            result.reserve_items(head2.iter());
            for item in head2.iter() {
                result.copy(item);
            }
            output.push(result);
        }
        output.extend(list2);
    }

    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        let mut keep = self.empty(stash);
        let mut ready = self.empty(stash);

        for buffer in merged {
            for d @ (_data, time, _diff) in buffer.iter() {
                if upper.less_equal(time) {
                    frontier.insert_ref(time);
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.empty(stash);
                    }
                    keep.copy(d);
                } else {
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

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        let (mut size, mut capacity, mut allocations) = (0, 0, 0);
        let cb = |siz, cap| {
            size += siz;
            capacity += cap;
            allocations += 1;
        };
        chunk.heap_size(cb);
        (chunk.len(), size, capacity, allocations)
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
        TimelyStackQueue { list, head: 0 }
    }

    fn done(self) -> TimelyStack<T> {
        self.list
    }

    fn is_empty(&self) -> bool {
        self.head == self.list[..].len()
    }

    /// Return an iterator over the remaining elements.
    fn iter(&self) -> impl Iterator<Item = &T> + Clone {
        self.list[self.head..].iter()
    }
}
