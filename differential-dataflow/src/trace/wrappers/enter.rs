//! Wrappers to provide trace access to nested scopes.

use std::marker::PhantomData;

use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, frontier::AntichainRef};

use crate::lattice::Lattice;
use crate::trace::{Span, Description, TraceReader};

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

/// Converts a description of outer times to one of inner times.
pub fn enter_description<T: timely::progress::Timestamp, TInner: Refines<T>+Lattice>(desc: &Description<T>) -> Description<TInner> {
    let lower: Vec<_> = desc.lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
    let upper: Vec<_> = desc.upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
    let since: Vec<_> = desc.since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
    Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since))
}

/// Converts an outer batch to an inner batch: the description enters the scope, and the
/// updates is wrapped for its readers to do the same.
pub fn enter_batch<T: timely::progress::Timestamp, TInner: Refines<T>+Lattice, P>(batch: Span<T, P>) -> Span<TInner, BatchEnter<P, TInner>> {
    Span::new(enter_description(&batch.desc), batch.inner.map(BatchEnter::make_from))
}

impl<Tr, TInner> TraceReader for TraceEnter<Tr, TInner>
where
    Tr: TraceReader,
    TInner: Refines<Tr::Time>+Lattice,
{
    type Time = TInner;
    type Batch = BatchEnter<Tr::Batch, TInner>;

    fn map_spans<F: FnMut(&Span<TInner, Self::Batch>)>(&self, mut f: F) {
        self.trace.map_spans(|batch| {
            f(&enter_batch(batch.clone()));
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

    fn spans_through(&mut self, upper: AntichainRef<TInner>) -> Option<Vec<Span<TInner, Self::Batch>>> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        let storage = self.trace.spans_through(self.stash1.borrow())?;
        Some(storage.into_iter().map(enter_batch).collect())
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


/// Wrapper to provide a batch updates to a nested scope.
#[derive(Clone)]
pub struct BatchEnter<B, TInner> {
    batch: B,
    phantom: PhantomData<TInner>,
}

impl<B, TInner> BatchEnter<B, TInner> {
    /// The wrapped updates, whose times are those of the containing scope.
    ///
    /// Each of its times enters the nested scope as `TInner::to_inner(time)`; that rule is the
    /// whole of the wrapper's read-side semantics, and any reader of the wrapped updates must
    /// apply it.
    pub fn inner(&self) -> &B { &self.batch }

    /// Makes a new wrapper
    pub fn make_from(batch: B) -> Self {
        BatchEnter { batch, phantom: PhantomData }
    }
}
