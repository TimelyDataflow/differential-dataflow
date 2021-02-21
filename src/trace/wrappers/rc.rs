//! A reference-counted wrapper sharing one owned trace.
//!
//! The types in this module, `TraceBox` and `TraceRc` and meant to parallel `RcBox` and `Rc` in `std::rc`.
//!
//! The first typee is an owned trace with some information about the cumulative requirements of the shared
//! handles. This is roughly how much progress has each made, so we know which "read capabilities" they have
//! collectively dropped, and when it is safe to inform the trace of such progress.
//!
//! The second type is a wrapper which presents as a `TraceReader`, but whose methods for advancing its read
//! capabilities interact with the `TraceBox` rather than directly with the owned trace. Ideally, instances
//! `TraceRc` should appear indistinguishable from the underlying trace from a reading perspective, with the
//! exception that the trace may not compact its representation as fast as if it were exclusively owned.

use std::rc::Rc;
use std::cell::RefCell;

use timely::progress::{Antichain, frontier::{AntichainRef, MutableAntichain}};

use lattice::Lattice;
use trace::TraceReader;
use trace::cursor::Cursor;

/// A wrapper around a trace which tracks the frontiers of all referees.
///
/// This is an internal type, unlikely to be useful to higher-level programs, but exposed just in case.
/// This type is equivalent to a `RefCell`, in that it wraps the mutable state that multiple referrers
/// may influence.
pub struct TraceBox<Tr>
where
    Tr::Time: Lattice+Ord+Clone+'static,
    Tr: TraceReader
{
    /// accumulated holds on times for advancement.
    pub get_logical_compactions: MutableAntichain<Tr::Time>,
    /// accumulated holds on times for distinction.
    pub through_frontiers: MutableAntichain<Tr::Time>,
    /// The wrapped trace.
    pub trace: Tr,
}

impl<Tr> TraceBox<Tr>
where
    Tr::Time: Lattice+Ord+Clone+'static,
    Tr: TraceReader,
{
    /// Moves an existing trace into a shareable trace wrapper.
    ///
    /// The trace may already exist and have non-initial advance and distinguish frontiers. The boxing
    /// process will fish these out and make sure that they are used for the initial read capabilities.
    pub fn new(mut trace: Tr) -> Self {

        let mut advance = MutableAntichain::new();
        advance.update_iter(trace.get_logical_compaction().iter().cloned().map(|t| (t,1)));
        // for time in trace.get_logical_compaction() {
        //     advance.update(time, 1);
        // }

        let mut through = MutableAntichain::new();
        through.update_iter(trace.get_physical_compaction().iter().cloned().map(|t| (t,1)));
        // for time in trace.get_physical_compaction() {
        //     through.update(time, 1);
        // }

        TraceBox {
            get_logical_compactions: advance,
            through_frontiers: through,
            trace: trace,
        }
    }
    /// Replaces elements of `lower` with those of `upper`.
    pub fn adjust_get_logical_compaction(&mut self, lower: AntichainRef<Tr::Time>, upper: AntichainRef<Tr::Time>) {
        self.get_logical_compactions.update_iter(upper.iter().cloned().map(|t| (t,1)));
        self.get_logical_compactions.update_iter(lower.iter().cloned().map(|t| (t,-1)));
        self.trace.set_logical_compaction(self.get_logical_compactions.frontier());
    }
    /// Replaces elements of `lower` with those of `upper`.
    pub fn adjust_through_frontier(&mut self, lower: AntichainRef<Tr::Time>, upper: AntichainRef<Tr::Time>) {
        self.through_frontiers.update_iter(upper.iter().cloned().map(|t| (t,1)));
        self.through_frontiers.update_iter(lower.iter().cloned().map(|t| (t,-1)));
        self.trace.set_physical_compaction(self.through_frontiers.frontier());
    }
}

/// A handle to a shared trace.
///
/// As long as the handle exists, the wrapped trace should continue to exist and will not advance its
/// timestamps past the frontier maintained by the handle. The intent is that such a handle appears as
/// if it is a privately maintained trace, despite being backed by shared resources.
pub struct TraceRc<Tr>
where
    Tr::Time: Lattice+Ord+Clone+'static,
    Tr: TraceReader,
{
    get_logical_compaction: Antichain<Tr::Time>,
    through_frontier: Antichain<Tr::Time>,
    /// Wrapped trace. Please be gentle when using.
    pub wrapper: Rc<RefCell<TraceBox<Tr>>>,
}

impl<Tr> TraceReader for TraceRc<Tr>
where
    Tr::Time: Lattice+Ord+Clone+'static,
    Tr: TraceReader,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = Tr::Batch;
    type Cursor = Tr::Cursor;

    /// Sets frontier to now be elements in `frontier`.
    ///
    /// This change may not have immediately observable effects. It informs the shared trace that this
    /// handle no longer requires access to times other than those in the future of `frontier`, but if
    /// there are other handles to the same trace, it may not yet be able to compact.
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        self.wrapper.borrow_mut().adjust_get_logical_compaction(self.get_logical_compaction.borrow(), frontier);
        self.get_logical_compaction = frontier.to_owned();
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.get_logical_compaction.borrow() }
    /// Allows the trace to compact batches of times before `frontier`.
    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        self.wrapper.borrow_mut().adjust_through_frontier(self.through_frontier.borrow(), frontier);
        self.through_frontier = frontier.to_owned();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.through_frontier.borrow() }
    /// Creates a new cursor over the wrapped trace.
    fn cursor_through(&mut self, frontier: AntichainRef<Tr::Time>) -> Option<(Tr::Cursor, <Tr::Cursor as Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>>::Storage)> {
        ::std::cell::RefCell::borrow_mut(&self.wrapper).trace.cursor_through(frontier)
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, f: F) {
        ::std::cell::RefCell::borrow_mut(&self.wrapper).trace.map_batches(f)
    }
}

impl<Tr> TraceRc<Tr>
where
    Tr::Time: Lattice+Ord+Clone+'static,
    Tr: TraceReader,
{
    /// Allocates a new handle from an existing wrapped wrapper.
    pub fn make_from(trace: Tr) -> (Self, Rc<RefCell<TraceBox<Tr>>>) {

        let wrapped = Rc::new(RefCell::new(TraceBox::new(trace)));

        let handle = TraceRc {
            get_logical_compaction: wrapped.borrow().get_logical_compactions.frontier().to_owned(),
            through_frontier: wrapped.borrow().through_frontiers.frontier().to_owned(),
            wrapper: wrapped.clone(),
        };

        (handle, wrapped)
    }
}

impl<Tr> Clone for TraceRc<Tr>
where
    Tr::Time: Lattice+Ord+Clone,
    Tr: TraceReader,
{
    fn clone(&self) -> Self {
        // increase ref counts for this frontier
        self.wrapper.borrow_mut().adjust_get_logical_compaction(Antichain::new().borrow(), self.get_logical_compaction.borrow());
        self.wrapper.borrow_mut().adjust_through_frontier(Antichain::new().borrow(), self.through_frontier.borrow());
        TraceRc {
            get_logical_compaction: self.get_logical_compaction.clone(),
            through_frontier: self.through_frontier.clone(),
            wrapper: self.wrapper.clone(),
        }
    }
}

impl<Tr> Drop for TraceRc<Tr>
where
    Tr::Time: Lattice+Ord+Clone+'static,
    Tr: TraceReader,
{
    fn drop(&mut self) {
        self.wrapper.borrow_mut().adjust_get_logical_compaction(self.get_logical_compaction.borrow(), Antichain::new().borrow());
        self.wrapper.borrow_mut().adjust_through_frontier(self.through_frontier.borrow(), Antichain::new().borrow());
        self.get_logical_compaction = Antichain::new();
        self.through_frontier = Antichain::new();
    }
}