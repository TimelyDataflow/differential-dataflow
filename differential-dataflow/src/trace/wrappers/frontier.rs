//! Wrapper for frontiered trace.
//!
//! Wraps a trace with `since` and `upper` frontiers so that all exposed timestamps are first advanced
//! by the `since` frontier and restricted by the `upper` frontier. This presents a deterministic trace
//! on the interval `[since, upper)`, presenting only accumulations up to `since` (rather than partially
//! accumulated updates) and no updates at times greater or equal to `upper` (even as parts of batches
//! that span that time).

use timely::progress::{Antichain, frontier::AntichainRef};

use crate::trace::{BatchReader, Description, TraceReader};
use crate::lattice::Lattice;

/// Wrapper to provide trace to nested scope.
pub struct TraceFrontier<Tr: TraceReader> {
    trace: Tr,
    /// Frontier to which all update times will be advanced.
    since: Antichain<Tr::Time>,
    /// Frontier after which all update times will be suppressed.
    until: Antichain<Tr::Time>,
}

impl<Tr: TraceReader + Clone> Clone for TraceFrontier<Tr> {
    fn clone(&self) -> Self {
        TraceFrontier {
            trace: self.trace.clone(),
            since: self.since.clone(),
            until: self.until.clone(),
        }
    }
}

impl<Tr: TraceReader> TraceReader for TraceFrontier<Tr> {

    type Time = Tr::Time;
    type Batch = BatchFrontier<Tr::Batch>;

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        self.trace.map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), since, until)))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> { self.trace.get_physical_compaction() }

    fn batches_through(&mut self, upper: AntichainRef<'_, Tr::Time>) -> Option<Vec<Self::Batch>> {
        let storage = self.trace.batches_through(upper)?;
        let since = self.since.borrow();
        let until = self.until.borrow();
        Some(storage.into_iter().map(|batch| BatchFrontier::make_from(batch, since, until)).collect())
    }
}

impl<Tr: TraceReader> TraceFrontier<Tr> {
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, since: AntichainRef<'_, Tr::Time>, until: AntichainRef<'_, Tr::Time>) -> Self {
        TraceFrontier {
            trace,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }
}


/// Wrapper to provide batch to nested scope.
#[derive(Clone)]
pub struct BatchFrontier<B: BatchReader> {
    batch: B,
    since: Antichain<B::Time>,
    until: Antichain<B::Time>,
}

impl<B: BatchReader> BatchReader for BatchFrontier<B> {
    type Time = B::Time;
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<B::Time> { self.batch.description() }
}

impl<B: BatchReader> BatchFrontier<B> {
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, since: AntichainRef<B::Time>, until: AntichainRef<B::Time>) -> Self {
        BatchFrontier {
            batch,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }
}

impl<B: BatchReader> BatchFrontier<B> {
    /// The wrapped batch, whose times are neither advanced nor suppressed.
    ///
    /// Its times must be presented through [`BatchFrontier::advance_time`], which is the whole of
    /// the wrapper's read-side semantics.
    pub fn inner(&self) -> &B { &self.batch }

    /// Applies the wrapper's time semantics to a time of the wrapped batch.
    ///
    /// The time is advanced by `since`, which accumulates the updates at or before `since` rather
    /// than presenting them partially accumulated. The method returns whether the advanced time
    /// should be presented at all: times at or after `until` are suppressed, even when they are
    /// part of a batch that spans `until`.
    pub fn advance_time(&self, time: &mut B::Time) -> bool {
        time.advance_by(self.since.borrow());
        !self.until.less_equal(time)
    }
}
