//! A general purpose `Batcher` implementation for FlatStack.

use std::cmp::Ordering;
use std::marker::PhantomData;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::{Container, Data, PartialOrder};
use timely::container::flatcontainer::{Push, FlatStack, Region, ReserveItems};
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};
use timely::progress::Timestamp;
use crate::difference::{IsZero, Semigroup};
use crate::lattice::Lattice;
use crate::trace::implementations::merge_batcher::Merger;
use crate::trace::Builder;
use crate::trace::cursor::IntoOwned;
use crate::trace::implementations::Update;

/// A merger for flat stacks.
///
/// `MC` is a [`Region`] that implements [`RegionUpdate`].
pub struct FlatcontainerMerger<MC> {
    _marker: PhantomData<MC>,
}

impl<MC> Default for FlatcontainerMerger<MC> {
    fn default() -> Self {
        Self { _marker: PhantomData, }
    }
}

impl<MC: Region> FlatcontainerMerger<MC> {
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

impl<K,V,T,R> Update for TupleABCRegion<TupleABRegion<K, V>, T, R>
where
    K: Region,
    K::Owned: Clone + Ord + 'static,
    V: Region,
    V::Owned: Clone + Ord + 'static,
    T: Region,
    T::Owned: Clone + Ord + Timestamp + Lattice + 'static,
    R: Region,
    R::Owned: Clone + Ord + Semigroup + 'static,
    for<'a> K::ReadItem<'a>: Copy + Ord,
    for<'a> V::ReadItem<'a>: Copy + Ord,
    for<'a> T::ReadItem<'a>: Copy + Ord,
    for<'a> R::ReadItem<'a>: Copy + Ord,
{
    type Key = K::Owned;
    type Val = V::Owned;
    type Time = T::Owned;
    type Diff = R::Owned;
    type ItemRef<'a> = ((K::ReadItem<'a>, V::ReadItem<'a>), T::ReadItem<'a>, R::ReadItem<'a>) where Self: 'a;
    type KeyGat<'a> = K::ReadItem<'a> where Self: 'a;
    type ValGat<'a> = V::ReadItem<'a> where Self: 'a;
    type TimeGat<'a> = T::ReadItem<'a> where Self: 'a;
    type DiffGat<'a> = R::ReadItem<'a> where Self: 'a;

    fn into_parts<'a>(item: Self::ItemRef<'a>) -> (Self::KeyGat<'a>, Self::ValGat<'a>, Self::TimeGat<'a>, Self::DiffGat<'a>) {
        let ((key, val), time, diff) = item;
        (key, val, time, diff)
    }

    fn reborrow_key<'b, 'a: 'b>(item: Self::KeyGat<'a>) -> Self::KeyGat<'b> where Self: 'a { K::reborrow(item) }

    fn reborrow_val<'b, 'a: 'b>(item: Self::ValGat<'a>) -> Self::ValGat<'b> where Self: 'a { V::reborrow(item) }

    fn reborrow_time<'b, 'a: 'b>(item: Self::TimeGat<'a>) -> Self::TimeGat<'b> where Self: 'a { T::reborrow(item) }

    fn reborrow_diff<'b, 'a: 'b>(item: Self::DiffGat<'a>) -> Self::DiffGat<'b> where Self: 'a { R::reborrow(item) }
}

impl<MC> Merger for FlatcontainerMerger<MC>
where
    for<'a> MC: Update<ItemRef<'a>=<MC as Region>::ReadItem<'a>> + Clone + 'static
        + ReserveItems<<MC as Region>::ReadItem<'a>>
        + Push<<MC as Region>::ReadItem<'a>>
        + Push<((MC::KeyGat<'a>, MC::ValGat<'a>), MC::TimeGat<'a>, &'a MC::Diff)>
        + Push<((MC::KeyGat<'a>, MC::ValGat<'a>), MC::TimeGat<'a>, MC::DiffGat<'a>)>,
    for<'a> MC::TimeGat<'a>: PartialOrder<MC::Time> + Copy + IntoOwned<'a, Owned=MC::Time>,
    for<'a> MC::DiffGat<'a>: IntoOwned<'a, Owned = MC::Diff>,
    for<'a> MC::Time: Ord + PartialOrder + PartialOrder<MC::TimeGat<'a>> + Data,
    for<'a> MC::Diff: Default + Semigroup + Semigroup<MC::DiffGat<'a>> + Data,
{
    type Time = MC::Time;
    type Chunk = FlatStack<MC>;
    type Output = FlatStack<MC>;

    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = <FlatStackQueue<MC>>::from(list1.next().unwrap_or_default());
        let mut head2 = <FlatStackQueue<MC>>::from(list2.next().unwrap_or_default());

        let mut result = self.empty(stash);

        let mut diff = MC::Diff::default();

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {
            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {
                let cmp = {
                    let (key1, val1, time1, _diff) = MC::into_parts(head1.peek());
                    let (key2, val2, time2, _diff) = MC::into_parts(head2.peek());
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
                        let (key, val, time1, diff1) = MC::into_parts(head1.pop());
                        let (_key, _val, _time2, diff2) = MC::into_parts(head2.pop());
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
            for (key, val, time, diff) in buffer.iter().map(MC::into_parts) {
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
                for (key, val, time, _diff) in buffer.iter().map(MC::into_parts) {
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
