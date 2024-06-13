//! A general purpose `Batcher` implementation based on radix sort.

use std::collections::VecDeque;
use std::marker::PhantomData;

use timely::communication::message::RefOrMut;
use timely::logging::WorkerIdentifier;
use timely::logging_core::Logger;
use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::{Container, PartialOrder};
use timely::container::{ContainerBuilder, PushInto};

use crate::difference::Semigroup;
use crate::logging::{BatcherEvent, DifferentialEvent};
use crate::trace::{Batcher, Builder};
use crate::Data;

/// Creates batches from unordered tuples.
pub struct MergeBatcher<Input, C, M, T>
where
    C: ContainerBuilder<Container=M::Chunk> + Default,
    M: Merger<Time = T>,
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
    logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>,
    /// Timely operator ID.
    operator_id: usize,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<T>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<T>,
    _marker: PhantomData<Input>,
}

impl<Input, C, M, T> Batcher for MergeBatcher<Input, C, M, T>
where
    C: ContainerBuilder<Container=M::Chunk> + Default + for<'a> PushInto<RefOrMut<'a, Input>>,
    M: Merger<Time = T>,
    T: Timestamp,
{
    type Input = Input;
    type Output = M::Output;
    type Time = T;

    fn new(logger: Option<Logger<DifferentialEvent, WorkerIdentifier>>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            chunker: C::default(),
            merger: M::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(T::minimum()),
            _marker: PhantomData,
        }
    }

    /// Push a container of data into this merge batcher. Updates the internal chain structure if
    /// needed.
    fn push_container(&mut self, container: RefOrMut<Input>) {
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
    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(&mut self, upper: Antichain<T>) -> B::Output {
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

        let seal = M::seal::<B>(&mut readied, self.lower.borrow(), upper.borrow(), Antichain::from_elem(T::minimum()).borrow());
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<T> {
        self.frontier.borrow()
    }
}

impl<Input, C, M, T> MergeBatcher<Input, C, M, T>
where
    C: ContainerBuilder<Container=M::Chunk> + Default,
    M: Merger<Time = T>,
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

impl<Input, C, M, T> Drop for MergeBatcher<Input, C, M, T>
where
    C: ContainerBuilder<Container=M::Chunk> + Default,
    M: Merger<Time = T>,
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
    /// The output type
    /// TODO: This should be replaced by `Chunk` or another container once the builder understands
    /// building from a complete chain.
    type Output;
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

    /// Build from a chain
    /// TODO: We can move this entirely to `MergeBatcher` once builders can accepts chains.
    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        chain: &mut Vec<Self::Chunk>,
        lower: AntichainRef<Self::Time>,
        upper: AntichainRef<Self::Time>,
        since: AntichainRef<Self::Time>,
    ) -> B::Output;

    /// Account size and allocation changes. Returns a tuple of (records, size, capacity, allocations).
    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize);
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

impl<K, V, T, R> Merger for VecMerger<((K, V), T, R)>
where
    K: Data,
    V: Data,
    T: Ord + PartialOrder + Clone + 'static,
    R: Semigroup + 'static,
{
    type Time = T;
    type Chunk = Vec<((K, V), T, R)>;
    type Output = Vec<((K, V), T, R)>;

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

    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        chain: &mut Vec<Self::Chunk>,
        lower: AntichainRef<Self::Time>,
        upper: AntichainRef<Self::Time>,
        since: AntichainRef<Self::Time>,
    ) -> B::Output {
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
        let mut builder = B::with_capacity(keys, vals, upds);

        for mut chunk in chain.drain(..) {
            builder.push(&mut chunk);
        }

        builder.done(lower.to_owned(), upper.to_owned(), since.to_owned())
    }

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        (chunk.len(), 0, 0, 0)
    }
}
