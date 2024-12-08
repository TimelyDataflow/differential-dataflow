//! A general purpose `Batcher` implementation based on radix sort.

use std::collections::VecDeque;
use std::marker::PhantomData;

use timely::logging_core::Logger;
use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::{Container, PartialOrder};
use timely::container::{ContainerBuilder, PushInto};

use crate::difference::Semigroup;
use crate::logging::{BatcherEvent, DifferentialEvent};
use crate::trace::{Batcher, Builder, Description};
use crate::Data;

/// Creates batches from unordered tuples.
pub struct MergeBatcher<Input, C, M>
where
    C: ContainerBuilder<Container=M::Chunk>,
    M: Merger,
{
    /// each power-of-two length list of allocations.
    /// Do not push/pop directly but use the corresponding functions
    /// ([`Self::chain_push`]/[`Self::chain_pop`]).
    chains: Vec<Vec<M::Chunk>>,
    /// Stash of empty chunks
    stash: Vec<M::Chunk>,
    /// Chunker to transform input streams to chunks of data.
    chunker: C,
    /// Thing to accept data, merge chains, and talk to the builder.
    merger: M,
    /// Logger for size accounting.
    logger: Option<Logger<DifferentialEvent>>,
    /// Timely operator ID.
    operator_id: usize,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<M::Time>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<M::Time>,
    _marker: PhantomData<Input>,
}

impl<Input, C, M> Batcher for MergeBatcher<Input, C, M>
where
    C: ContainerBuilder<Container=M::Chunk> + Default + for<'a> PushInto<&'a mut Input>,
    M: Merger,
    M::Time: Timestamp,
{
    type Input = Input;
    type Time = M::Time;
    type Output = M::Chunk;

    fn new(logger: Option<Logger<DifferentialEvent>>, operator_id: usize) -> Self {
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
    fn frontier(&mut self) -> AntichainRef<M::Time> {
        self.frontier.borrow()
    }
}

impl<Input, C, M> MergeBatcher<Input, C, M>
where
    C: ContainerBuilder<Container=M::Chunk> + Default,
    M: Merger,
{
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

impl<Input, C, M> Drop for MergeBatcher<Input, C, M>
where
    C: ContainerBuilder<Container=M::Chunk> + Default,
    M: Merger,
{
    fn drop(&mut self) {
        // Cleanup chain to retract accounting information.
        while self.chain_pop().is_some() {}
    }
}

/// A trait to describe interesting moments in a merge batcher.
pub trait Merger: Default {
    /// The internal representation of chunks of data.
    type Chunk: Container;
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

pub mod container {

    //! A general purpose `Batcher` implementation for arbitrary containers.

    use std::cmp::Ordering;
    use std::marker::PhantomData;
    use timely::{Container, container::{PushInto, SizableContainer}};
    use timely::progress::frontier::{Antichain, AntichainRef};
    use timely::{Data, PartialOrder};

    use crate::trace::implementations::merge_batcher::Merger;

    /// An abstraction for a container that can be iterated over, and conclude by returning itself.
    pub trait ContainerQueue<C: Container> {
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
    pub trait MergerChunk : SizableContainer {
        /// The owned time type.
        type TimeOwned;
        /// The owned diff type.
        type DiffOwned: Default;

        /// Compares a borrowed time to an antichain of owned times.
        ///
        /// If `upper` is less or equal to `time`, the method returns true and ensures that `frontier` reflects `time`.
        fn time_kept(time1: &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool;

        /// Push an entry that adds together two diffs.
        ///
        /// This is only called when two items are deemed mergeable by the container queue.
        /// If the two diffs would cancel, do not push anything.
        fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, stash: &mut Self::DiffOwned);

        /// Account the allocations behind the chunk.
        // TODO: Find a more universal home for this: `Container`?
        fn account(&self) -> (usize, usize, usize, usize) {
            let (size, capacity, allocations) = (0, 0, 0);
            (self.len(), size, capacity, allocations)
        }
    }
    
    /// A merger for arbitrary containers.
    ///
    /// `MC` is a [`Container`] that implements [`MergerChunk`].
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
        fn recycle(&self, chunk: MC, stash: &mut Vec<MC>) {
            // TODO: Should we only retain correctly sized containers?
            stash.push(chunk);
        }

    }

    impl<MC, CQ> Merger for ContainerMerger<MC, CQ>
    where
        for<'a> MC: MergerChunk + Clone + PushInto<<MC as Container>::Item<'a>> + 'static,
        for<'a> MC::TimeOwned: Ord + PartialOrder + Data,
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
        pub type VecMerger<K, V, T, R> = super::ContainerMerger<Vec<((K, V), T, R)>, std::collections::VecDeque<((K, V), T, R)>>;

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
        }
    }

    pub use columnation::ColMerger;
    /// Implementations of `ContainerQueue` and `MergerChunk` for `TimelyStack` containers (columnation).
    pub mod columnation {

        use timely::progress::{Antichain, frontier::AntichainRef};
        use timely::container::columnation::TimelyStack;
        use timely::container::columnation::Columnation;
        use crate::difference::Semigroup;
        use super::{ContainerQueue, MergerChunk};

        /// A `Merger` implementation backed by `TimelyStack` containers (columnation).
        pub type ColMerger<K, V, T, R> = super::ContainerMerger<TimelyStack<((K,V),T,R)>,TimelyStackQueue<((K,V), T, R)>>;

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
                let (size, capacity, allocations) = (0, 0, 0);
                (self.len(), size, capacity, allocations)
            }
        }
    }

    pub use flat_container::FlatMerger;
    /// Implementations of `ContainerQueue` and `MergerChunk` for `FlatStack` containers (flat_container).
    ///
    /// This is currently non-functional, while we try and sort out some missing constraints that seem to
    /// allow the direct implementation to work, but the corresponding implementation here to not compile.
    pub mod flat_container {

        use timely::progress::{Antichain, frontier::AntichainRef};
        use timely::container::flatcontainer::{FlatStack, Region};
        use timely::container::flatcontainer::impls::tuple::TupleABCRegion;
        use timely::container::flatcontainer::Push;
        use crate::difference::{IsZero, Semigroup};
        use super::{ContainerQueue, MergerChunk};

        /// A `Merger` implementation backed by `FlatStack` containers (flat_container).
        pub type FlatMerger<K, V, T, R> = super::ContainerMerger<FlatStack<((K,V),T,R)>,FlatStackQueue<((K,V), T, R)>>;

        /// A queue implementation over a flat stack.
        pub struct FlatStackQueue<R: Region> {
            list: FlatStack<R>,
            head: usize,
        }

        impl<R: Region> ContainerQueue<FlatStack<R>> for FlatStackQueue<R> 
        where
            for<'a> R::ReadItem<'a>: Ord,
        {
            fn next_or_alloc(&mut self) -> Result<R::ReadItem<'_>, FlatStack<R>> {
                if self.is_empty() {
                    Err(std::mem::take(&mut self.list))
                }
                else {
                    Ok(self.pop())
                }
            }
            fn is_empty(&self) -> bool {
                self.head >= self.list.len()
            }
            fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
                self.peek().cmp(&other.peek())
            }
            fn from(list: FlatStack<R>) -> Self {
                FlatStackQueue { list, head: 0 }
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
        }

        impl<D,T,R> MergerChunk for FlatStack<TupleABCRegion<D, T, R>>
        where
            D: Region,
            for<'a> D::ReadItem<'a>: Ord,
            T: Region,
            for<'a> T::ReadItem<'a>: Ord,
            R: Region,
            R::Owned: Default + IsZero + for<'a> Semigroup<R::ReadItem<'a>>,
            TupleABCRegion<D, T, R>: for<'a,'b> Push<(D::ReadItem<'a>, T::ReadItem<'a>, &'b R::Owned)>,
        {
            type TimeOwned = T::Owned;
            type DiffOwned = R::Owned;

            fn time_kept(_time: &Self::Item<'_>, _upper: &AntichainRef<Self::TimeOwned>, _frontier: &mut Antichain<Self::TimeOwned>) -> bool {
                unimplemented!()
            }
            fn push_and_add<'a>(&mut self, _item1: <TupleABCRegion<D, T, R> as Region>::ReadItem<'a>, _item2: Self::Item<'a>, _stash: &mut Self::DiffOwned) {
                // let (_, _, _) = _item1;
                unimplemented!()
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
        }
    }
}

/// A merger that knows how to accept and maintain chains of vectors.
pub struct VecMerger<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for VecMerger<T> {
    fn default() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<T> VecMerger<T> {
    const BUFFER_SIZE_BYTES: usize = 8 << 10;
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
    fn empty(&self, stash: &mut Vec<Vec<T>>) -> Vec<T> {
        stash.pop().unwrap_or_else(|| Vec::with_capacity(self.chunk_capacity()))
    }

    /// Helper to return a chunk to the stash.
    #[inline]
    fn recycle(&self, mut chunk: Vec<T>, stash: &mut Vec<Vec<T>>) {
        // TODO: Should we limit the size of `stash`?
        if chunk.capacity() == self.chunk_capacity() {
            chunk.clear();
            stash.push(chunk);
        }
    }
}

impl<D, T, R> Merger for VecMerger<(D, T, R)>
where
    D: Data,
    T: Ord + PartialOrder + Clone + 'static,
    R: Semigroup + 'static,
{
    type Time = T;
    type Chunk = Vec<(D, T, R)>;

    fn merge(&mut self, list1: Vec<Self::Chunk>, list2: Vec<Self::Chunk>, output: &mut Vec<Self::Chunk>, stash: &mut Vec<Self::Chunk>) {
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
                    Ordering::Less => result.push(head1.pop_front().unwrap()),
                    Ordering::Greater => result.push(head2.pop_front().unwrap()),
                    Ordering::Equal => {
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

        if !result.is_empty() {
            output.push(result);
        } else {
            self.recycle(result, stash);
        }

        if !head1.is_empty() {
            let mut result = self.empty(stash);
            for item1 in head1 {
                result.push(item1);
            }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty(stash);
            for item2 in head2 {
                result.push(item2);
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

        for mut buffer in merged {
            for (data, time, diff) in buffer.drain(..) {
                if upper.less_equal(&time) {
                    frontier.insert_ref(&time);
                    if keep.len() == keep.capacity() && !keep.is_empty() {
                        kept.push(keep);
                        keep = self.empty(stash);
                    }
                    keep.push((data, time, diff));
                } else {
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
    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        (chunk.len(), 0, 0, 0)
    }
}
