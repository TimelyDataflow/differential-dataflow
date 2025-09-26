//! A `Batcher` implementation based on merge sort.
//!
//! The `MergeBatcher` requires support from two types, a "chunker" and a "merger".
//! The chunker receives input batches and consolidates them, producing sorted output
//! "chunks" that are fully consolidated (no adjacent updates can be accumulated).
//! The merger implements the [`Merger`] trait, and provides hooks for manipulating
//! sorted "chains" of chunks as needed by the merge batcher: merging chunks and also
//! splitting them apart based on time.
//!
//! Implementations of `MergeBatcher` can be instantiated through the choice of both
//! the chunker and the merger, provided their respective output and input types align.

use std::marker::PhantomData;

use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::container::{ContainerBuilder, PushInto};

use crate::logging::{BatcherEvent, Logger};
use crate::trace::{Batcher, Builder, Description};

/// Creates batches from containers of unordered tuples.
///
/// To implement `Batcher`, the container builder `C` must accept `&mut Input` as inputs,
/// and must produce outputs of type `M::Chunk`.
pub struct MergeBatcher<Input, C, M: Merger> {
    /// Transforms input streams to chunks of sorted, consolidated data.
    chunker: C,
    /// A sequence of power-of-two length lists of sorted, consolidated containers.
    ///
    /// Do not push/pop directly but use the corresponding functions ([`Self::chain_push`]/[`Self::chain_pop`]).
    chains: Vec<Vec<M::Chunk>>,
    /// Stash of empty chunks, recycled through the merging process.
    stash: Vec<M::Chunk>,
    /// Merges consolidated chunks, and extracts the subset of an update chain that lies in an interval of time.
    merger: M,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<M::Time>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<M::Time>,
    /// Logger for size accounting.
    logger: Option<Logger>,
    /// Timely operator ID.
    operator_id: usize,
    /// The `Input` type needs to be called out as the type of container accepted, but it is not otherwise present.
    _marker: PhantomData<Input>,
}

impl<Input, C, M> Batcher for MergeBatcher<Input, C, M>
where
    C: ContainerBuilder<Container=M::Chunk> + for<'a> PushInto<&'a mut Input>,
    M: Merger<Time: Timestamp>,
{
    type Input = Input;
    type Time = M::Time;
    type Output = M::Chunk;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            chunker: C::default(),
            merger: M::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(M::Time::minimum()),
            _marker: PhantomData,
        }
    }

    /// Push a container of data into this merge batcher. Updates the internal chain structure if
    /// needed.
    fn push_container(&mut self, container: &mut Input) {
        self.chunker.push_into(container);
        while let Some(chunk) = self.chunker.extract() {
            let chunk = std::mem::take(chunk);
            self.insert_chain(vec![chunk]);
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(&mut self, upper: Antichain<M::Time>) -> B::Output {
        // Finish
        while let Some(chunk) = self.chunker.finish() {
            let chunk = std::mem::take(chunk);
            self.insert_chain(vec![chunk]);
        }

        // Merge all remaining chains into a single chain.
        while self.chains.len() > 1 {
            let list1 = self.chain_pop().unwrap();
            let list2 = self.chain_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // Extract readied data.
        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();

        self.merger.extract(merged, upper.borrow(), &mut self.frontier, &mut readied, &mut kept, &mut self.stash);

        if !kept.is_empty() {
            self.chain_push(kept);
        }

        self.stash.clear();

        let description = Description::new(self.lower.clone(), upper.clone(), Antichain::from_elem(M::Time::minimum()));
        let seal = B::seal(&mut readied, description);
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<'_, M::Time> {
        self.frontier.borrow()
    }
}

impl<Input, C, M: Merger> MergeBatcher<Input, C, M> {
    /// Insert a chain and maintain chain properties: Chains are geometrically sized and ordered
    /// by decreasing length.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            while self.chains.len() > 1 && (self.chains[self.chains.len() - 1].len() >= self.chains[self.chains.len() - 2].len() / 2) {
                let list1 = self.chain_pop().unwrap();
                let list2 = self.chain_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.chain_push(merged);
            }
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(&mut self, list1: Vec<M::Chunk>, list2: Vec<M::Chunk>) -> Vec<M::Chunk> {
        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        self.merger.merge(list1, list2, &mut output, &mut self.stash);

        output
    }

    /// Pop a chain and account size changes.
    #[inline]
    fn chain_pop(&mut self) -> Option<Vec<M::Chunk>> {
        let chain = self.chains.pop();
        self.account(chain.iter().flatten().map(M::account), -1);
        chain
    }

    /// Push a chain and account size changes.
    #[inline]
    fn chain_push(&mut self, chain: Vec<M::Chunk>) {
        self.account(chain.iter().map(M::account), 1);
        self.chains.push(chain);
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the iterator passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    #[inline]
    fn account<I: IntoIterator<Item = (usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
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

impl<Input, C, M: Merger> Drop for MergeBatcher<Input, C, M> {
    fn drop(&mut self) {
        // Cleanup chain to retract accounting information.
        while self.chain_pop().is_some() {}
    }
}

/// A trait to describe interesting moments in a merge batcher.
pub trait Merger: Default {
    /// The internal representation of chunks of data.
    type Chunk: Default;
    /// The type of time in frontiers to extract updates.
    type Time;
    /// Merge chains into an output chain.
    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>);
    /// Extract ready updates based on the `upper` frontier.
    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    );

    /// Account size and allocation changes. Returns a tuple of (records, size, capacity, allocations).
    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize);
}

pub use container::{VecMerger, ColMerger};

pub mod container {

    //! A general purpose `Merger` implementation for arbitrary containers.
    //!
    //! The implementation requires implementations of two traits, `ContainerQueue` and `MergerChunk`.
    //! The `ContainerQueue` trait is meant to wrap a container and provide iterable access to it, as
    //! well as the ability to return the container when iteration is complete.
    //! The `MergerChunk` trait is meant to be implemented by containers, and it explains how container
    //! items should be interpreted with respect to times, and with respect to differences.
    //! These two traits exist instead of a stack of constraints on the structure of the associated items
    //! of the containers, allowing them to perform their functions without destructuring their guts.
    //!
    //! Standard implementations exist in the `vec`, `columnation`, and `flat_container` modules.

    use std::cmp::Ordering;
    use std::marker::PhantomData;
    use timely::container::{PushInto, SizableContainer};
    use timely::progress::frontier::{Antichain, AntichainRef};
    use timely::{Accountable, Data, PartialOrder};
    use timely::container::DrainContainer;
    use crate::trace::implementations::merge_batcher::Merger;

    /// An abstraction for a container that can be iterated over, and conclude by returning itself.
    pub trait ContainerQueue<C: DrainContainer> {
        /// Returns either the next item in the container, or the container itself.
        fn next_or_alloc(&mut self) -> Result<C::Item<'_>, C>;
        /// Indicates whether `next_or_alloc` will return `Ok`, and whether `peek` will return `Some`.
        fn is_empty(&self) -> bool;
        /// Compare the heads of two queues, where empty queues come last.
        fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering;
        /// Create a new queue from an existing container.
        fn from(container: C) -> Self;
    }

    /// Behavior to dissect items of chunks in the merge batcher
    pub trait MergerChunk : Accountable + DrainContainer + SizableContainer + Default {
        /// An owned time type.
        ///
        /// This type is provided so that users can maintain antichains of something, in order to track
        /// the forward movement of time and extract intervals from chains of updates.
        type TimeOwned;
        /// The owned diff type.
        ///
        /// This type is provided so that users can provide an owned instance to the `push_and_add` method,
        /// to act as a scratch space when the type is substantial and could otherwise require allocations.
        type DiffOwned: Default;

        /// Relates a borrowed time to antichains of owned times.
        ///
        /// If `upper` is less or equal to `time`, the method returns `true` and ensures that `frontier` reflects `time`.
        fn time_kept(time1: &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool;

        /// Push an entry that adds together two diffs.
        ///
        /// This is only called when two items are deemed mergeable by the container queue.
        /// If the two diffs added together is zero do not push anything.
        fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, stash: &mut Self::DiffOwned);

        /// Account the allocations behind the chunk.
        // TODO: Find a more universal home for this: `Container`?
        fn account(&self) -> (usize, usize, usize, usize) {
            let (size, capacity, allocations) = (0, 0, 0);
            (usize::try_from(self.record_count()).unwrap(), size, capacity, allocations)
        }

        /// Clear the chunk, to be reused.
        fn clear(&mut self);
    }

    /// A merger for arbitrary containers.
    ///
    /// `MC` is a `Container` that implements [`MergerChunk`].
    /// `CQ` is a [`ContainerQueue`] supporting `MC`.
    pub struct ContainerMerger<MC, CQ> {
        _marker: PhantomData<(MC, CQ)>,
    }

    impl<MC, CQ> Default for ContainerMerger<MC, CQ> {
        fn default() -> Self {
            Self { _marker: PhantomData, }
        }
    }

    impl<MC: MergerChunk, CQ> ContainerMerger<MC, CQ> {
        /// Helper to get pre-sized vector from the stash.
        #[inline]
        fn empty(&self, stash: &mut Vec<MC>) -> MC {
            stash.pop().unwrap_or_else(|| {
                let mut container = MC::default();
                container.ensure_capacity(&mut None);
                container
            })
        }
        /// Helper to return a chunk to the stash.
        #[inline]
        fn recycle(&self, mut chunk: MC, stash: &mut Vec<MC>) {
            // TODO: Should we only retain correctly sized containers?
            chunk.clear();
            stash.push(chunk);
        }
    }

    impl<MC, CQ> Merger for ContainerMerger<MC, CQ>
    where
        for<'a> MC: MergerChunk<TimeOwned: Ord + PartialOrder + Data> + Clone + PushInto<<MC as DrainContainer>::Item<'a>> + 'static,
        CQ: ContainerQueue<MC>,
    {
        type Time = MC::TimeOwned;
        type Chunk = MC;

        // TODO: Consider integrating with `ConsolidateLayout`.
        fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>) {
            let mut list1 = list1.into_iter();
            let mut list2 = list2.into_iter();

            let mut head1 = CQ::from(list1.next().unwrap_or_default());
            let mut head2 = CQ::from(list2.next().unwrap_or_default());

            let mut result = self.empty(stash);

            let mut diff_owned = Default::default();

            // while we have valid data in each input, merge.
            while !head1.is_empty() && !head2.is_empty() {
                while !result.at_capacity() && !head1.is_empty() && !head2.is_empty() {
                    let cmp = head1.cmp_heads(&head2);
                    // TODO: The following less/greater branches could plausibly be a good moment for
                    // `copy_range`, on account of runs of records that might benefit more from a
                    // `memcpy`.
                    match cmp {
                        Ordering::Less => {
                            result.push_into(head1.next_or_alloc().ok().unwrap());
                        }
                        Ordering::Greater => {
                            result.push_into(head2.next_or_alloc().ok().unwrap());
                        }
                        Ordering::Equal => {
                            let item1 = head1.next_or_alloc().ok().unwrap();
                            let item2 = head2.next_or_alloc().ok().unwrap();
                            result.push_and_add(item1, item2, &mut diff_owned);
                       }
                    }
                }

                if result.at_capacity() {
                    output.push_into(result);
                    result = self.empty(stash);
                }

                if head1.is_empty() {
                    self.recycle(head1.next_or_alloc().err().unwrap(), stash);
                    head1 = CQ::from(list1.next().unwrap_or_default());
                }
                if head2.is_empty() {
                    self.recycle(head2.next_or_alloc().err().unwrap(), stash);
                    head2 = CQ::from(list2.next().unwrap_or_default());
                }
            }

            // TODO: recycle `head1` rather than discarding.
            while let Ok(next) = head1.next_or_alloc() {
                result.push_into(next);
                if result.at_capacity() {
                    output.push_into(result);
                    result = self.empty(stash);
                }
            }
            if !result.is_empty() {
                output.push_into(result);
                result = self.empty(stash);
            }
            output.extend(list1);

            // TODO: recycle `head2` rather than discarding.
            while let Ok(next) = head2.next_or_alloc() {
                result.push_into(next);
                if result.at_capacity() {
                    output.push(result);
                    result = self.empty(stash);
                }
            }
            if !result.is_empty() {
                output.push_into(result);
                // result = self.empty(stash);
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

            for mut buffer in merged {
                for item in buffer.drain() {
                    if MC::time_kept(&item, &upper, frontier) {
                        if keep.at_capacity() && !keep.is_empty() {
                            kept.push(keep);
                            keep = self.empty(stash);
                        }
                        keep.push_into(item);
                    } else {
                        if ready.at_capacity() && !ready.is_empty() {
                            readied.push(ready);
                            ready = self.empty(stash);
                        }
                        ready.push_into(item);
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

        /// Account the allocations behind the chunk.
        fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
            chunk.account()
        }
    }

    pub use vec::VecMerger;
    /// Implementations of `ContainerQueue` and `MergerChunk` for `Vec` containers.
    pub mod vec {

        use std::collections::VecDeque;
        use timely::progress::{Antichain, frontier::AntichainRef};
        use crate::difference::Semigroup;
        use super::{ContainerQueue, MergerChunk};

        /// A `Merger` implementation backed by vector containers.
        pub type VecMerger<D, T, R> = super::ContainerMerger<Vec<(D, T, R)>, std::collections::VecDeque<(D, T, R)>>;

        impl<D: Ord, T: Ord, R> ContainerQueue<Vec<(D, T, R)>> for VecDeque<(D, T, R)> {
            fn next_or_alloc(&mut self) -> Result<(D, T, R), Vec<(D, T, R)>> {
                if self.is_empty() {
                    Err(Vec::from(std::mem::take(self)))
                }
                else {
                    Ok(self.pop_front().unwrap())
                }
            }
            fn is_empty(&self) -> bool {
                self.is_empty()
            }
            fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
                let (data1, time1, _) = self.front().unwrap();
                let (data2, time2, _) = other.front().unwrap();
                (data1, time1).cmp(&(data2, time2))
            }
            fn from(list: Vec<(D, T, R)>) -> Self {
                <Self as From<_>>::from(list)
            }
        }

        impl<D: Ord + 'static, T: Ord + timely::PartialOrder + Clone + 'static, R: Semigroup + 'static> MergerChunk for Vec<(D, T, R)> {
            type TimeOwned = T;
            type DiffOwned = ();

            fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool {
                if upper.less_equal(time) {
                    frontier.insert_with(&time, |time| time.clone());
                    true
                }
                else { false }
            }
            fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, _stash: &mut Self::DiffOwned) {
                let (data, time, mut diff1) = item1;
                let (_data, _time, diff2) = item2;
                diff1.plus_equals(&diff2);
                if !diff1.is_zero() {
                    self.push((data, time, diff1));
                }
            }
            fn account(&self) -> (usize, usize, usize, usize) {
                let (size, capacity, allocations) = (0, 0, 0);
                (self.len(), size, capacity, allocations)
            }
            #[inline] fn clear(&mut self) { Vec::clear(self) }
        }
    }

    pub use columnation::ColMerger;
    /// Implementations of `ContainerQueue` and `MergerChunk` for `TimelyStack` containers (columnation).
    pub mod columnation {

        use timely::progress::{Antichain, frontier::AntichainRef};
        use columnation::Columnation;

        use crate::containers::TimelyStack;
        use crate::difference::Semigroup;

        use super::{ContainerQueue, MergerChunk};

        /// A `Merger` implementation backed by `TimelyStack` containers (columnation).
        pub type ColMerger<D, T, R> = super::ContainerMerger<TimelyStack<(D,T,R)>,TimelyStackQueue<(D, T, R)>>;

        /// TODO
        pub struct TimelyStackQueue<T: Columnation> {
            list: TimelyStack<T>,
            head: usize,
        }

        impl<D: Ord + Columnation, T: Ord + Columnation, R: Columnation> ContainerQueue<TimelyStack<(D, T, R)>> for TimelyStackQueue<(D, T, R)> {
            fn next_or_alloc(&mut self) -> Result<&(D, T, R), TimelyStack<(D, T, R)>> {
                if self.is_empty() {
                    Err(std::mem::take(&mut self.list))
                }
                else {
                    Ok(self.pop())
                }
            }
            fn is_empty(&self) -> bool {
                self.head == self.list[..].len()
            }
            fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
                let (data1, time1, _) = self.peek();
                let (data2, time2, _) = other.peek();
                (data1, time1).cmp(&(data2, time2))
            }
            fn from(list: TimelyStack<(D, T, R)>) -> Self {
                TimelyStackQueue { list, head: 0 }
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
        }

        impl<D: Ord + Columnation + 'static, T: Ord + timely::PartialOrder + Clone + Columnation + 'static, R: Default + Semigroup + Columnation + 'static> MergerChunk for TimelyStack<(D, T, R)> {
            type TimeOwned = T;
            type DiffOwned = R;

            fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool {
                if upper.less_equal(time) {
                    frontier.insert_with(&time, |time| time.clone());
                    true
                }
                else { false }
            }
            fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, stash: &mut Self::DiffOwned) {
                let (data, time, diff1) = item1;
                let (_data, _time, diff2) = item2;
                stash.clone_from(diff1);
                stash.plus_equals(&diff2);
                if !stash.is_zero() {
                    self.copy_destructured(data, time, stash);
                }
            }
            fn account(&self) -> (usize, usize, usize, usize) {
                let (mut size, mut capacity, mut allocations) = (0, 0, 0);
                let cb = |siz, cap| {
                    size += siz;
                    capacity += cap;
                    allocations += 1;
                };
                self.heap_size(cb);
                (self.len(), size, capacity, allocations)
            }
            #[inline] fn clear(&mut self) { TimelyStack::clear(self) }
        }
    }
}
