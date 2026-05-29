//! A `Batcher` for columnar streams that merges sorted chains via the free
//! functions in `trie_merger`.
//!
//! Callers feed already-chunked, sorted-and-consolidated `UpdatesTyped<U>` into
//! the batcher via [`PushInto`]; forming such chunks from `RecordedUpdates<U>`
//! is the responsibility of the surrounding dataflow operator's chunker
//! (`TrieChunker`).

use std::collections::VecDeque;

use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};
use timely::container::PushInto;

use crate::logging::Logger;
use crate::trace::{Batcher, Description};

use super::layout::ColumnarUpdate as Update;
use super::updates::UpdatesTyped;
use super::arrangement::trie_merger;
use super::spill::{Entry, SpillPolicy};

/// Creates batches from chunks of sorted, consolidated columnar updates.
pub struct MergeBatcher<U: Update> {
    /// A sequence of power-of-two length chains of sorted, consolidated entries.
    /// Each entry is either an in-memory chunk or a handle to a paged-out chunk.
    chains: Vec<VecDeque<Entry<UpdatesTyped<U>>>>,
    /// Current lower frontier, we sealed up to here.
    lower: Antichain<U::Time>,
    /// The lower-bound frontier of the data, after the last call to seal.
    frontier: Antichain<U::Time>,
    /// Optional spill policy, consulted after each chain insert. `None` keeps
    /// everything resident.
    policy: Option<Box<dyn SpillPolicy<UpdatesTyped<U>>>>,
}

impl<U: Update<Time: Timestamp>> Batcher for MergeBatcher<U> {
    type Time = U::Time;
    type Output = UpdatesTyped<U>;

    fn new(_logger: Option<Logger>, _operator_id: usize) -> Self {
        Self {
            chains: Vec::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(U::Time::minimum()),
            policy: None,
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    fn seal(&mut self, upper: Antichain<U::Time>) -> (Vec<Self::Output>, Description<U::Time>) {
        // Merge all remaining chains into a single chain.
        while self.chains.len() > 1 {
            let list1 = self.chains.pop().unwrap();
            let list2 = self.chains.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.push_chain(merged);
        }
        let merged = self.chains.pop().unwrap_or_default();

        // Extract readied data, streaming. `merged` is consumed lazily via
        // `FetchIter`; ship-side chunks flow into `readied` for the
        // builder; kept-side chunks flow into a fresh chain that is offered
        // to the spill policy as each chunk lands, so kept never accumulates
        // resident in full.
        let mut readied: Vec<UpdatesTyped<U>> = Vec::new();
        let mut kept_chain: VecDeque<Entry<UpdatesTyped<U>>> = VecDeque::new();
        self.frontier.clear();
        {
            let policy = &mut self.policy;
            let frontier = &mut self.frontier;
            let ship = |chunk: UpdatesTyped<U>| readied.push(chunk);
            let keep = |chunk: UpdatesTyped<U>| {
                kept_chain.push_back(Entry::Typed(chunk));
                if let Some(p) = policy.as_mut() {
                    p.apply(&mut kept_chain);
                }
            };
            trie_merger::extract(
                FetchIter::new(merged),
                upper.borrow(),
                frontier,
                ship,
                keep,
            );
        }

        if !kept_chain.is_empty() {
            self.push_chain(kept_chain);
        }

        let description = Description::new(self.lower.clone(), upper.clone(), Antichain::from_elem(U::Time::minimum()));
        self.lower = upper;
        (readied, description)
    }

    /// The frontier of elements remaining after the most recent call to `self.seal`.
    #[inline]
    fn frontier(&mut self) -> AntichainRef<'_, U::Time> {
        self.frontier.borrow()
    }
}

impl<U: Update> PushInto<UpdatesTyped<U>> for MergeBatcher<U> {
    fn push_into(&mut self, chunk: UpdatesTyped<U>) {
        self.insert_chain(VecDeque::from([Entry::Typed(chunk)]));
    }
}

impl<U: Update> MergeBatcher<U> {
    /// Install a spill policy. Consulted after each chain insert.
    pub fn set_spill_policy(&mut self, policy: Box<dyn SpillPolicy<UpdatesTyped<U>>>) {
        self.policy = Some(policy);
    }

    /// Sum of records currently held in `Entry::Typed` chunks across all
    /// chains. `Entry::Paged` entries are excluded — they live on backing
    /// storage, not in the process heap. Spill policies bound this quantity;
    /// RSS may still grow due to materialize-on-merge.
    pub fn resident_records(&self) -> usize {
        self.chains
            .iter()
            .flat_map(|c| c.iter())
            .map(|e| match e {
                Entry::Typed(c) => {
                    use timely::Accountable;
                    c.record_count() as usize
                }
                Entry::Paged(_) => 0,
            })
            .sum()
    }

    /// Insert a chain and maintain chain properties: Chains are geometrically sized and ordered
    /// by decreasing length.
    fn insert_chain(&mut self, chain: VecDeque<Entry<UpdatesTyped<U>>>) {
        if !chain.is_empty() {
            self.push_chain(chain);
            while self.chains.len() > 1 && (self.chains[self.chains.len() - 1].len() >= self.chains[self.chains.len() - 2].len() / 2) {
                let list1 = self.chains.pop().unwrap();
                let list2 = self.chains.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.push_chain(merged);
            }
        }
    }

    /// Push a chain onto `chains` and consult the spill policy on the result.
    /// Following TD's `MergeQueue::extend`, which calls `policy.apply` after
    /// each queue extension. Applied to inputs and to merge / extract results
    /// alike, so threshold-style policies see multi-chunk chains.
    fn push_chain(&mut self, chain: VecDeque<Entry<UpdatesTyped<U>>>) {
        self.chains.push(chain);
        if let Some(policy) = self.policy.as_mut() {
            if let Some(top) = self.chains.last_mut() {
                policy.apply(top);
            }
        }
    }

    /// Merge two sorted chains. Inputs are streamed lazily through
    /// `FetchIter` so paged entries are fetched one group at a time.
    /// Output chunks flow through a sink that pushes into a fresh chain and
    /// invokes the spill policy after each emission, so the merge result can
    /// be paged out as it's produced rather than buffered in full.
    fn merge_by(
        &mut self,
        list1: VecDeque<Entry<UpdatesTyped<U>>>,
        list2: VecDeque<Entry<UpdatesTyped<U>>>,
    ) -> VecDeque<Entry<UpdatesTyped<U>>> {
        let mut output: VecDeque<Entry<UpdatesTyped<U>>> = VecDeque::new();
        let policy = &mut self.policy;
        let sink = |chunk: UpdatesTyped<U>| {
            output.push_back(Entry::Typed(chunk));
            if let Some(p) = policy.as_mut() {
                p.apply(&mut output);
            }
        };
        trie_merger::merge_batches(
            FetchIter::new(list1),
            FetchIter::new(list2),
            sink,
        );
        output
    }

}

/// Streaming iterator over a chain's chunks. Yields `Entry::Typed` chunks
/// directly; for `Entry::Paged`, calls `Fetch::fetch` on demand and yields
/// the resulting chunks one by one. Bounds materialized chunks to one fetch
/// group at a time (plus whatever the consumer is holding).
struct FetchIter<U: Update> {
    queue: VecDeque<Entry<UpdatesTyped<U>>>,
    pending: VecDeque<UpdatesTyped<U>>,
}

impl<U: Update> FetchIter<U> {
    fn new(queue: VecDeque<Entry<UpdatesTyped<U>>>) -> Self {
        Self { queue, pending: VecDeque::new() }
    }
}

impl<U: Update> Iterator for FetchIter<U> {
    type Item = UpdatesTyped<U>;
    fn next(&mut self) -> Option<UpdatesTyped<U>> {
        loop {
            if let Some(c) = self.pending.pop_front() {
                return Some(c);
            }
            match self.queue.pop_front()? {
                Entry::Typed(c) => return Some(c),
                Entry::Paged(handle) => match handle.fetch() {
                    Ok(chunks) => self.pending.extend(chunks),
                    Err(_) => panic!("Fetch::fetch failed; retry path not yet wired"),
                },
            }
        }
    }
}
