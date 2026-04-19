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

use crate::trace::TraceReader;

/// A wrapper around a trace which tracks the frontiers of all referees.
///
/// This is an internal type, unlikely to be useful to higher-level programs, but exposed just in case.
/// This type is equivalent to a `RefCell`, in that it wraps the mutable state that multiple referrers
/// may influence.
pub struct TraceBox<Tr: TraceReader> {
    /// accumulated holds on times for advancement.
    pub logical_compaction: MutableAntichain<Tr::Time>,
    /// accumulated holds on times for distinction.
    pub physical_compaction: MutableAntichain<Tr::Time>,
    /// The wrapped trace.
    pub trace: Tr,
}

impl<Tr: TraceReader> TraceBox<Tr> {
    /// Moves an existing trace into a shareable trace wrapper.
    ///
    /// The trace may already exist and have non-initial advance and distinguish frontiers. The boxing
    /// process will fish these out and make sure that they are used for the initial read capabilities.
    pub fn new(mut trace: Tr) -> Self {

        let mut logical_compaction = MutableAntichain::new();
        logical_compaction.update_iter(trace.get_logical_compaction().iter().cloned().map(|t| (t,1)));
        let mut physical_compaction = MutableAntichain::new();
        physical_compaction.update_iter(trace.get_physical_compaction().iter().cloned().map(|t| (t,1)));

        TraceBox {
            logical_compaction,
            physical_compaction,
            trace,
        }
    }
    /// Replaces elements of `lower` with those of `upper`.
    #[inline]
    pub fn adjust_logical_compaction(&mut self, lower: AntichainRef<Tr::Time>, upper: AntichainRef<Tr::Time>) {
        self.logical_compaction.update_iter(upper.iter().cloned().map(|t| (t,1)));
        self.logical_compaction.update_iter(lower.iter().cloned().map(|t| (t,-1)));
        self.trace.set_logical_compaction(self.logical_compaction.frontier());
    }
    /// Replaces elements of `lower` with those of `upper`.
    #[inline]
    pub fn adjust_physical_compaction(&mut self, lower: AntichainRef<Tr::Time>, upper: AntichainRef<Tr::Time>) {
        self.physical_compaction.update_iter(upper.iter().cloned().map(|t| (t,1)));
        self.physical_compaction.update_iter(lower.iter().cloned().map(|t| (t,-1)));
        self.trace.set_physical_compaction(self.physical_compaction.frontier());
    }
}
