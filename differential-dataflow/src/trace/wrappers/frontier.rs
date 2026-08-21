//! Wrapper for frontiered trace.
//!
//! Wraps a trace with `since` and `upper` frontiers so that all exposed timestamps are first advanced
//! by the `since` frontier and restricted by the `upper` frontier. This presents a deterministic trace
//! on the interval `[since, upper)`, presenting only accumulations up to `since` (rather than partially
//! accumulated updates) and no updates at times greater or equal to `upper` (even as parts of batches
//! that span that time).

use timely::progress::{Antichain, frontier::AntichainRef};

use crate::trace::{Batch, TraceReader};
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
    type Payload = BatchFrontier<Tr::Payload, Tr::Time>;

    fn map_batches<F: FnMut(&Batch<Tr::Time, Self::Payload>)>(&self, mut f: F) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        self.trace.map_batches(|batch| {
            let wrapped = Batch::new(
                batch.desc.clone(),
                batch.inner.clone().map(|p| BatchFrontier::make_from(p, since, until)),
            );
            f(&wrapped)
        })
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> { self.trace.get_physical_compaction() }

    fn batches_through(&mut self, upper: AntichainRef<'_, Tr::Time>) -> Option<Vec<Batch<Tr::Time, Self::Payload>>> {
        let storage = self.trace.batches_through(upper)?;
        let since = self.since.borrow();
        let until = self.until.borrow();
        Some(storage.into_iter().map(|batch| {
            Batch::new(batch.desc, batch.inner.map(|p| BatchFrontier::make_from(p, since, until)))
        }).collect())
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


/// Wrapper to provide a batch payload to a nested scope.
#[derive(Clone)]
pub struct BatchFrontier<B, T> {
    batch: B,
    since: Antichain<T>,
    until: Antichain<T>,
}

impl<B, T> BatchFrontier<B, T> {
    /// Makes a new payload wrapper
    pub fn make_from(batch: B, since: AntichainRef<T>, until: AntichainRef<T>) -> Self
    where T: Clone,
    {
        BatchFrontier {
            batch,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }

    /// The wrapped payload, whose times are neither advanced nor suppressed.
    ///
    /// Its times must be presented through [`BatchFrontier::advance_time`], which is the whole of
    /// the wrapper's read-side semantics.
    pub fn inner(&self) -> &B { &self.batch }

    /// Applies the wrapper's time semantics to a time of the wrapped payload.
    ///
    /// The time is advanced by `since`, which accumulates the updates at or before `since` rather
    /// than presenting them partially accumulated. The method returns whether the advanced time
    /// should be presented at all: times at or after `until` are suppressed, even when they are
    /// part of a batch that spans `until`.
    #[inline]
    pub fn advance_time(&self, time: &mut T) -> bool
    where T: Lattice + timely::progress::Timestamp,
    {
        time.advance_by(self.since.borrow());
        !self.until.less_equal(time)
    }
}
