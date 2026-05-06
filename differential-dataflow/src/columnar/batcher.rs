//! A `Batcher` for `RecordedUpdates<U>` streams that consolidates input via
//! `TrieChunker` and merges sorted chains via the free functions in `trie_merger`.

use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::container::{ContainerBuilder, PushInto};

use crate::logging::Logger;
use crate::trace::{Batcher, Builder, Description};

use super::layout::ColumnarUpdate as Update;
use super::updates::UpdatesTyped;
use super::RecordedUpdates;
use super::arrangement::TrieChunker;
use super::arrangement::trie_merger;

/// Creates batches from `RecordedUpdates<U>` streams.
pub struct MergeBatcher<U: Update> {
    /// Transforms input streams to chunks of sorted, consolidated data.
    chunker: TrieChunker<U>,
    /// A sequence of power-of-two length lists of sorted, consolidated containers.
    chains: Vec<Vec<UpdatesTyped<U>>>,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<U::Time>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<U::Time>,
}

impl<U: Update<Time: Timestamp>> Batcher for MergeBatcher<U> {
    type Input = RecordedUpdates<U>;
    type Time = U::Time;
    type Output = UpdatesTyped<U>;

    fn new(_logger: Option<Logger>, _operator_id: usize) -> Self {
        Self {
            chunker: TrieChunker::default(),
            chains: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(U::Time::minimum()),
        }
    }

    /// Push a container of data into this merge batcher. Updates the internal chain structure if
    /// needed.
    fn push_container(&mut self, container: &mut RecordedUpdates<U>) {
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
    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(&mut self, upper: Antichain<U::Time>) -> B::Output {
        // Finish
        while let Some(chunk) = self.chunker.finish() {
            let chunk = std::mem::take(chunk);
            self.insert_chain(vec![chunk]);
        }

        // Merge all remaining chains into a single chain.
        while self.chains.len() > 1 {
            let list1 = self.chains.pop().unwrap();
            let list2 = self.chains.pop().unwrap();
            let merged = Self::merge_by(list1, list2);
            self.chains.push(merged);
        }
        let merged = self.chains.pop().unwrap_or_default();

        // Extract readied data.
        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();

        trie_merger::extract(merged, upper.borrow(), &mut self.frontier, &mut readied, &mut kept);

        if !kept.is_empty() {
            self.chains.push(kept);
        }

        let description = Description::new(self.lower.clone(), upper.clone(), Antichain::from_elem(U::Time::minimum()));
        let seal = B::seal(&mut readied, description);
        self.lower = upper;
        seal
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<'_, U::Time> {
        self.frontier.borrow()
    }
}

impl<U: Update> MergeBatcher<U> {
    /// Insert a chain and maintain chain properties: Chains are geometrically sized and ordered
    /// by decreasing length.
    fn insert_chain(&mut self, chain: Vec<UpdatesTyped<U>>) {
        if !chain.is_empty() {
            self.chains.push(chain);
            while self.chains.len() > 1 && (self.chains[self.chains.len() - 1].len() >= self.chains[self.chains.len() - 2].len() / 2) {
                let list1 = self.chains.pop().unwrap();
                let list2 = self.chains.pop().unwrap();
                let merged = Self::merge_by(list1, list2);
                self.chains.push(merged);
            }
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(list1: Vec<UpdatesTyped<U>>, list2: Vec<UpdatesTyped<U>>) -> Vec<UpdatesTyped<U>> {
        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        trie_merger::merge_batches(list1, list2, &mut output);

        output
    }
}
