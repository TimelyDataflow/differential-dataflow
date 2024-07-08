//! A general purpose `Batcher` implementation for FlatStack.

use std::cmp::Ordering;
use std::marker::PhantomData;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::{Container, Data, PartialOrder};
use timely::container::flatcontainer::{Push, FlatStack, Region, ReserveItems};
use timely::container::flatcontainer::impls::index::IndexContainer;
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};

use crate::difference::{IsZero, Semigroup};
use crate::trace::implementations::merge_batcher::Merger;
use crate::trace::Builder;
use crate::trace::cursor::IntoOwned;

/// A merger for flat stacks.
///
/// `R` is a [`Region`] that implements [`RegionUpdate`].
pub struct FlatcontainerMerger<R, S = Vec<<R as Region>::Index>> {
    _marker: PhantomData<(R, S)>,
}

impl<R, S> Default for FlatcontainerMerger<R, S> {
    fn default() -> Self {
        Self { _marker: PhantomData, }
    }
}

impl<R, S> FlatcontainerMerger<R, S>
where
    R: Region,
    S: IndexContainer<<R as Region>::Index> + Clone + 'static,
{
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
    fn chunk_capacity(&self) -> usize {
        let size = ::std::mem::size_of::<R::Index>();
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
    fn empty(&self, stash: &mut Vec<FlatStack<R, S>>) -> FlatStack<R, S> {
        stash.pop().unwrap_or_else(|| FlatStack::with_capacity(self.chunk_capacity()))
    }

    /// Helper to return a chunk to the stash.
    #[inline]
    fn recycle(&self, mut chunk: FlatStack<R, S>, stash: &mut Vec<FlatStack<R, S>>) {
        // TODO: Should we limit the size of `stash`?
        chunk.clear();
        stash.push(chunk);
    }
}

/// Behavior to dissect items of chunks in the merge batcher
pub trait RegionUpdate: Region {
    /// The key of the update
    type Key<'a>: Copy + Ord where Self: 'a;
    /// The value of the update
    type Val<'a>: Copy + Ord where Self: 'a;
    /// The time of the update
    type Time<'a>: Copy + Ord + IntoOwned<'a, Owned = Self::TimeOwned> where Self: 'a;
    /// The owned time type.
    type TimeOwned;
    /// The diff of the update
    type Diff<'a>: Copy + IntoOwned<'a, Owned = Self::DiffOwned> where Self: 'a;
    /// The owned diff type.
    type DiffOwned;

    /// Split a read item into its constituents. Must be cheap.
    fn into_parts<'a>(item: Self::ReadItem<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time<'a>, Self::Diff<'a>);

    /// Converts a key into one with a narrower lifetime.
    #[must_use]
    fn reborrow_key<'b, 'a: 'b>(item: Self::Key<'a>) -> Self::Key<'b>
    where
        Self: 'a;

    /// Converts a value into one with a narrower lifetime.
    #[must_use]
    fn reborrow_val<'b, 'a: 'b>(item: Self::Val<'a>) -> Self::Val<'b>
    where
        Self: 'a;

    /// Converts a time into one with a narrower lifetime.
    #[must_use]
    fn reborrow_time<'b, 'a: 'b>(item: Self::Time<'a>) -> Self::Time<'b>
    where
        Self: 'a;

    /// Converts a diff into one with a narrower lifetime.
    #[must_use]
    fn reborrow_diff<'b, 'a: 'b>(item: Self::Diff<'a>) -> Self::Diff<'b>
    where
        Self: 'a;
}

impl<KR, VR, TR, RR> RegionUpdate for TupleABCRegion<TupleABRegion<KR, VR>, TR, RR>
where
    KR: Region,
    for<'a> KR::ReadItem<'a>: Copy + Ord,
    VR: Region,
    for<'a> VR::ReadItem<'a>: Copy + Ord,
    TR: Region,
    for<'a> TR::ReadItem<'a>: Copy + Ord,
    RR: Region,
    for<'a> RR::ReadItem<'a>: Copy + Ord,
{
    type Key<'a> = KR::ReadItem<'a> where Self: 'a;
    type Val<'a> = VR::ReadItem<'a> where Self: 'a;
    type Time<'a> = TR::ReadItem<'a> where Self: 'a;
    type TimeOwned = TR::Owned;
    type Diff<'a> = RR::ReadItem<'a> where Self: 'a;
    type DiffOwned = RR::Owned;

    fn into_parts<'a>(((key, val), time, diff): Self::ReadItem<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time<'a>, Self::Diff<'a>) {
        (key, val, time, diff)
    }

    fn reborrow_key<'b, 'a: 'b>(item: Self::Key<'a>) -> Self::Key<'b>
    where
        Self: 'a
    {
        KR::reborrow(item)
    }

    fn reborrow_val<'b, 'a: 'b>(item: Self::Val<'a>) -> Self::Val<'b>
    where
        Self: 'a
    {
        VR::reborrow(item)
    }

    fn reborrow_time<'b, 'a: 'b>(item: Self::Time<'a>) -> Self::Time<'b>
    where
        Self: 'a
    {
        TR::reborrow(item)
    }

    fn reborrow_diff<'b, 'a: 'b>(item: Self::Diff<'a>) -> Self::Diff<'b>
    where
        Self: 'a
    {
        RR::reborrow(item)
    }
}

impl<R, S> Merger for FlatcontainerMerger<R, S>
where
    for<'a> R: Region
        + RegionUpdate
        + Clone
        + ReserveItems<<R as Region>::ReadItem<'a>>
        + Push<<R as Region>::ReadItem<'a>>
        + Push<((R::Key<'a>, R::Val<'a>), R::Time<'a>, &'a R::DiffOwned)>
        + Push<((R::Key<'a>, R::Val<'a>), R::Time<'a>, R::Diff<'a>)>
        + 'static,
    for<'a> R::Time<'a>: PartialOrder<R::TimeOwned> + Copy + IntoOwned<'a, Owned=R::TimeOwned>,
    for<'a> R::Diff<'a>: IntoOwned<'a, Owned = R::DiffOwned>,
    for<'a> R::TimeOwned: Ord + PartialOrder + PartialOrder<R::Time<'a>> + Data,
    for<'a> R::DiffOwned: Default + Semigroup + Semigroup<R::Diff<'a>> + Data,
    S: IndexContainer<<R as Region>::Index> + Clone + 'static,
{
    type Time = R::TimeOwned;
    type Chunk = FlatStack<R, S>;
    type Output = FlatStack<R, S>;

    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = <FlatStackQueue<R, S>>::from(list1.next().unwrap_or_default());
        let mut head2 = <FlatStackQueue<R, S>>::from(list2.next().unwrap_or_default());

        let mut result = self.empty(stash);

        let mut diff = R::DiffOwned::default();

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {
            while (result.len() < self.chunk_capacity()) && !head1.is_empty() && !head2.is_empty() {
                let cmp = {
                    let (key1, val1, time1, _diff) = R::into_parts(head1.peek());
                    let (key2, val2, time2, _diff) = R::into_parts(head2.peek());
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
                        let (key, val, time1, diff1) = R::into_parts(head1.pop());
                        let (_key, _val, _time2, diff2) = R::into_parts(head2.pop());
                        diff1.clone_onto(&mut diff);
                        diff.plus_equals(&diff2);
                        if !diff.is_zero() {
                            result.copy(((key, val), time1, &diff));
                        }
                    }
                }
            }

            if result.len() == self.chunk_capacity() {
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
            let advance = self.chunk_capacity().saturating_sub(result.len());
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
            let advance = self.chunk_capacity().saturating_sub(result.len());
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
            for (key, val, time, diff) in buffer.iter().map(R::into_parts) {
                if upper.less_equal(&time) {
                    frontier.insert_with(&time, |time| (*time).into_owned());
                    if keep.len() == self.chunk_capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.empty(stash);
                    }
                    keep.copy(((key, val), time, diff));
                } else {
                    if ready.len() == self.chunk_capacity() && !ready.is_empty() {
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
                for (key, val, time, _diff) in buffer.iter().map(R::into_parts) {
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

struct FlatStackQueue<R, S>
{
    list: FlatStack<R, S>,
    head: usize,
}

impl<R, S> Default for FlatStackQueue<R, S>
where
    R: Region,
    S: IndexContainer<<R as Region>::Index>,
{
    fn default() -> Self {
        Self::from(FlatStack::default())
    }
}

impl<R, S> FlatStackQueue<R, S>
where
    R: Region,
    S: IndexContainer<<R as Region>::Index>,
{
    fn pop(&mut self) -> R::ReadItem<'_> {
        self.head += 1;
        self.list.get(self.head - 1)
    }

    fn peek(&self) -> R::ReadItem<'_> {
        self.list.get(self.head)
    }

    fn from(list: FlatStack<R, S>) -> Self {
        FlatStackQueue { list, head: 0 }
    }

    fn done(self) -> FlatStack<R, S> {
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
