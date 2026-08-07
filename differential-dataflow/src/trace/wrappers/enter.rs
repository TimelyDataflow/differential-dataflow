//! Wrappers to provide trace access to nested scopes.

// use timely::progress::nested::product::Product;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, frontier::AntichainRef};

use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description, TraceReader};

/// Wrapper to provide trace to nested scope.
pub struct TraceEnter<Tr: TraceReader, TInner> {
    trace: Tr,
    stash1: Antichain<Tr::Time>,
    stash2: Antichain<TInner>,
}

impl<Tr: TraceReader + Clone, TInner> Clone for TraceEnter<Tr, TInner> {
    fn clone(&self) -> Self {
        TraceEnter {
            trace: self.trace.clone(),
            stash1: Antichain::new(),
            stash2: Antichain::new(),
        }
    }
}

impl<Tr, TInner> TraceReader for TraceEnter<Tr, TInner>
where
    Tr: TraceReader,
    TInner: Refines<Tr::Time>+Lattice,
{
    type Batch = BatchEnter<Tr::Batch, TInner>;
    type Time = TInner;

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone()));
        })
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<'_, TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.set_logical_compaction(self.stash1.borrow());
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, TInner> {
        self.stash2.clear();
        for time in self.trace.get_logical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.set_physical_compaction(self.stash1.borrow());
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, TInner> {
        self.stash2.clear();
        for time in self.trace.get_physical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn batches_through(&mut self, upper: AntichainRef<TInner>) -> Option<Vec<Self::Batch>> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        let storage = self.trace.batches_through(self.stash1.borrow())?;
        Some(storage.into_iter().map(|batch| BatchEnter::make_from(batch)).collect())
    }
}

impl<Tr, TInner> TraceEnter<Tr, TInner>
where
    Tr: TraceReader,
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr) -> Self {
        TraceEnter {
            trace,
            stash1: Antichain::new(),
            stash2: Antichain::new(),
        }
    }
}


/// Wrapper to provide batch to nested scope.
#[derive(Clone)]
pub struct BatchEnter<B, TInner> {
    batch: B,
    description: Description<TInner>,
}

impl<B, TInner> BatchReader for BatchEnter<B, TInner>
where
    B: BatchReader,
    TInner: Refines<B::Time>+Lattice,
{
    type Time = TInner;
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
}

impl<B, TInner> BatchEnter<B, TInner> {
    /// The wrapped batch, whose times are those of the containing scope.
    ///
    /// Each of its times enters the nested scope as `TInner::to_inner(time)`; that rule is the
    /// whole of the wrapper's read-side semantics, and any reader of the wrapped batch must
    /// apply it.
    pub fn inner(&self) -> &B { &self.batch }
}

impl<B, TInner> BatchEnter<B, TInner>
where
    B: BatchReader,
    TInner: Refines<B::Time>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B) -> Self {
        let lower: Vec<_> = batch.description().lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            batch,
            description: Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since))
        }
    }
}
