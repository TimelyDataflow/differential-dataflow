//! `ValColBuilder`: the ContainerBuilder that feeds the dataflow input side.
//!
//! Accepts flat `(k, v, t, d)` tuples via `PushInto`; when the internal tuple
//! container reaches a threshold, sorts + forms a `RecordedUpdates` trie and
//! queues it. `finish` produces one final trie from any remaining tuples.

use std::collections::VecDeque;
use columnar::{Clear, Columnar, Len, Push};

use super::layout::ColumnarUpdate as Update;
use super::updates::UpdatesTyped;
use super::RecordedUpdates;

type TupleContainer<U> = <(<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff) as Columnar>::Container;

/// A container builder that produces `RecordedUpdates` (sorted, consolidated trie + record count).
pub struct ValBuilder<U: Update> {
    /// Container that we're writing to.
    current: TupleContainer<U>,
    /// Empty allocation.
    empty: Option<RecordedUpdates<U>>,
    /// Completed containers pending to be sent.
    pending: VecDeque<RecordedUpdates<U>>,
}

impl<U: Update> ValBuilder<U> {
    /// Wrap `self.current` as a stride-1 `UpdatesTyped`, consolidate it, and queue.
    ///
    /// Reclaims the column allocations: takes `self.current` by value, consolidates
    /// (which builds a fresh trie via Push), then clears the now-stale columns and
    /// returns them to `self.current` so capacity is retained across flushes.
    fn flush(&mut self) {
        if self.current.len() == 0 { return; }
        let records = self.current.len();
        let raw: UpdatesTyped<U> = std::mem::take(&mut self.current).into();
        let updates = raw.consolidate().into();
        // Reclaim raw's column allocations.
        self.current = (raw.keys.values, raw.vals.values, raw.times.values, raw.diffs.values);
        self.current.clear();
        self.pending.push_back(RecordedUpdates { updates, records, consolidated: true });
    }
}

use timely::container::PushInto;
impl<T, U: Update> PushInto<T> for ValBuilder<U> where TupleContainer<U> : Push<T> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.current.push(item);
        if self.current.len() > crate::columnar::LINK_TARGET {
            self.flush();
        }
    }
}

impl<U: Update> Default for ValBuilder<U> {
    fn default() -> Self {
        ValBuilder {
            current: Default::default(),
            empty: None,
            pending: Default::default(),
        }
    }
}

use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
impl<U: Update> ContainerBuilder for ValBuilder<U> {
    type Container = RecordedUpdates<U>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.empty = self.pending.pop_front();
        self.empty.as_mut()
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.flush();
        self.extract()
    }
}

impl<U: Update> LengthPreservingContainerBuilder for ValBuilder<U> { }
