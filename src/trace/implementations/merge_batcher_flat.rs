//! A general purpose `Batcher` implementation for FlatStack.

use std::cmp::Ordering;
use std::marker::PhantomData;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::{Container, Data, PartialOrder};
use timely::container::flatcontainer::{Push, FlatStack, Region, ReserveItems};
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};

use crate::difference::Semigroup;
use crate::trace::implementations::merge_batcher::Merger;
use crate::trace::Builder;
use crate::trace::cursor::IntoOwned;

/// A merger for flat stacks. `T` describes the
pub struct FlatcontainerMerger<T, R, MC> {
    _marker: PhantomData<(T, R, MC)>,
}

impl<T, R, MC> Default for FlatcontainerMerger<T, R, MC> {
    fn default() -> Self {
        Self { _marker: PhantomData, }
    }
}

impl<T, R, MC: Region> FlatcontainerMerger<T, R, MC> {
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
    fn chunk_capacity(&self) -> usize {
        let size = ::std::mem::size_of::<MC::Index>();
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
    fn empty(&self, stash: &mut Vec<FlatStack<MC>>) -> FlatStack<MC> {
        stash.pop().unwrap_or_else(|| FlatStack::with_capacity(self.chunk_capacity()))
    }

    /// Helper to return a chunk to the stash.
    #[inline]
    fn recycle(&self, mut chunk: FlatStack<MC>, stash: &mut Vec<FlatStack<MC>>) {
        // TODO: Should we limit the size of `stash`?
        if chunk.capacity() == self.chunk_capacity() {
            chunk.clear();
            stash.push(chunk);
        }
    }
}

/// Behavior to dissect items of chunks in the merge batcher
pub trait MergerChunk: Region {
    /// The key of the update
    type Key<'a>: Ord where Self: 'a;
    /// The value of the update
    type Val<'a>: Ord where Self: 'a;
    /// The time of the update
    type Time<'a>: Ord where Self: 'a;
    /// The diff of the update
    type Diff<'a> where Self: 'a;

    /// Split a read item into its constituents. Must be cheap.
    fn into_parts<'a>(item: Self::ReadItem<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time<'a>, Self::Diff<'a>);
}

impl<K,V,T,R> MergerChunk for TupleABCRegion<TupleABRegion<K, V>, T, R>
where
    K: Region,
    for<'a> K::ReadItem<'a>: Ord,
    V: Region,
    for<'a> V::ReadItem<'a>: Ord,
    T: Region,
    for<'a> T::ReadItem<'a>: Ord,
    R: Region,
{
    type Key<'a> = K::ReadItem<'a> where Self: 'a;
    type Val<'a> = V::ReadItem<'a> where Self: 'a;
    type Time<'a> = T::ReadItem<'a> where Self: 'a;
    type Diff<'a> = R::ReadItem<'a> where Self: 'a;

    fn into_parts<'a>(((key, val), time, diff): Self::ReadItem<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time<'a>, Self::Diff<'a>) {
        (key, val, time, diff)
    }
}

impl<T, R, FR> Merger for FlatcontainerMerger<T, R, FR>
where
    for<'a> T: Ord + PartialOrder + PartialOrder<FR::Time<'a>> + Data,
    for<'a> R: Default + Semigroup + Semigroup<FR::Diff<'a>> + Data,
    for<'a> FR: MergerChunk + Clone + 'static
        + ReserveItems<<FR as Region>::ReadItem<'a>>
        + Push<<FR as Region>::ReadItem<'a>>
        + Push<((FR::Key<'a>, FR::Val<'a>), FR::Time<'a>, &'a R)>
        + Push<((FR::Key<'a>, FR::Val<'a>), FR::Time<'a>, FR::Diff<'a>)>,
    for<'a> FR::Time<'a>: PartialOrder<T> + Copy + IntoOwned<'a, Owned=T>,
    for<'a> FR::Diff<'a>: IntoOwned<'a, Owned=R>,
    for<'a> FR::ReadItem<'a>: std::fmt::Debug,
{
    type Time = T;
    type Chunk = FlatStack<FR>;
    type Output = FlatStack<FR>;

    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = <FlatStackQueue<FR>>::from(list1.next().unwrap_or_default());
        let mut head2 = <FlatStackQueue<FR>>::from(list2.next().unwrap_or_default());

        let mut result = self.empty(stash);

        let mut diff = R::default();

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {
            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {
                let cmp = {
                    let (key1, val1, time1, _diff) = FR::into_parts(head1.peek());
                    let (key2, val2, time2, _diff) = FR::into_parts(head2.peek());
                    ((key1, val1), time1).cmp(&((key2, val2), time2))
                };
                // TODO: The following less/greater branches could plausibly be a good moment for
                // `copy_range`, on account of runs of records that might benefit more from a
                // `memcpy`.
                match cmp {
                    Ordering::Less => {
                        result.copy(head1.pop());
                    }
                    Ordering::Greater => {
                        result.copy(head2.pop());
                    }
                    Ordering::Equal => {
                        let (key, val, time1, diff1) = FR::into_parts(head1.pop());
                        let (_key, _val, _time2, diff2) = FR::into_parts(head2.pop());
                        diff1.clone_onto(&mut diff);
                        diff.plus_equals(&diff2);
                        if !diff.is_zero() {
                            result.copy(((key, val), time1, &diff));
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
                head1 = FlatStackQueue::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                self.recycle(head2.done(), stash);
                head2 = FlatStackQueue::from(list2.next().unwrap_or_default());
            }
        }

        while !head1.is_empty() {
            let advance = result.capacity() - result.len();
            let iter = head1.iter().take(advance);
            result.reserve_items(iter.clone());
            for item in iter {
                result.copy(item);
            }
            output.push(result);
            head1.advance(advance);
            result = self.empty(stash);
        }
        if !result.is_empty() {
            output.push(result);
            result = self.empty(stash);
        }
        output.extend(list1);
        self.recycle(head1.done(), stash);

        while !head2.is_empty() {
            let advance = result.capacity() - result.len();
            let iter = head2.iter().take(advance);
            result.reserve_items(iter.clone());
            for item in iter {
                result.copy(item);
            }
            output.push(result);
            head2.advance(advance);
            result = self.empty(stash);
        }
        output.extend(list2);
        self.recycle(head2.done(), stash);
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
            for (key, val, time, diff) in buffer.iter().map(FR::into_parts) {
                if upper.less_equal(&time) {
                    frontier.insert_with(&time, |time| (*time).into_owned());
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.empty(stash);
                    }
                    keep.copy(((key, val), time, diff));
                } else {
                    if ready.len() == ready.capacity() && !ready.is_empty() {
                        readied.push(ready);
                        ready = self.empty(stash);
                    }
                    ready.copy(((key, val), time, diff));
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

    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        chain: &mut Vec<Self::Chunk>,
        lower: AntichainRef<Self::Time>,
        upper: AntichainRef<Self::Time>,
        since: AntichainRef<Self::Time>,
    ) -> B::Output {
        let mut keys = 0;
        let mut vals = 0;
        let mut upds = 0;
        {
            let mut prev_keyval = None;
            for buffer in chain.iter() {
                for (key, val, time, _diff) in buffer.iter().map(FR::into_parts) {
                    if !upper.less_equal(&time) {
                        if let Some((p_key, p_val)) = prev_keyval {
                            debug_assert!(p_key <= key);
                            debug_assert!(p_key != key || p_val <= val);
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
                }
            }
        }
        let mut builder = B::with_capacity(keys, vals, upds);
        for mut chunk in chain.drain(..) {
            builder.push(&mut chunk);
        }

        builder.done(lower.to_owned(), upper.to_owned(), since.to_owned())
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

struct FlatStackQueue<R: Region> {
    list: FlatStack<R>,
    head: usize,
}

impl<R: Region> Default for FlatStackQueue<R> {
    fn default() -> Self {
        Self::from(Default::default())
    }
}

impl<R: Region> FlatStackQueue<R> {
    fn pop(&mut self) -> R::ReadItem<'_> {
        self.head += 1;
        self.list.get(self.head - 1)
    }

    fn peek(&self) -> R::ReadItem<'_> {
        self.list.get(self.head)
    }

    fn from(list: FlatStack<R>) -> Self {
        FlatStackQueue { list, head: 0 }
    }

    fn done(self) -> FlatStack<R> {
        self.list
    }

    fn is_empty(&self) -> bool {
        self.head >= self.list.len()
    }

    /// Return an iterator over the remaining elements.
    fn iter(&self) -> impl Iterator<Item = R::ReadItem<'_>> + Clone {
        self.list.iter().skip(self.head)
    }

    fn advance(&mut self, consumed: usize) {
        self.head += consumed;
    }
}
