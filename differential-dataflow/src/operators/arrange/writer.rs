//! Write endpoints for a sequence of batches.
//!
//! A `TraceWriterIntra` distributes batches to a shared trace (intra-dataflow sharing).
//! A `TraceWriterInter` additionally distributes them to a set of private queues
//! (inter-dataflow sharing).

use std::rc::{Rc, Weak};
use std::cell::RefCell;

use timely::progress::Antichain;

use crate::trace::{Trace, Batch, BatchReader};
use crate::trace::wrappers::rc::TraceBox;


use super::TraceInterQueueWriter;
use super::TraceReplayInstruction;

/// Write endpoint that maintains the frontier and shared trace.
///
/// Used for intra-dataflow sharing where readers consume the trace directly
/// rather than via a replay queue.
pub struct TraceWriterIntra<Tr: Trace> {
    /// Current upper limit.
    upper: Antichain<Tr::Time>,
    /// Shared trace, possibly absent (due to weakness).
    trace: Weak<RefCell<TraceBox<Tr>>>,
}

impl<Tr: Trace> TraceWriterIntra<Tr> {
    /// Creates a new `TraceWriterIntra`.
    pub fn new(upper: Vec<Tr::Time>, trace: Weak<RefCell<TraceBox<Tr>>>) -> Self {
        let mut temp = Antichain::new();
        temp.extend(upper);
        Self { upper: temp, trace }
    }

    /// The current upper frontier.
    pub fn upper(&self) -> &Antichain<Tr::Time> { &self.upper }

    /// Exerts merge effort, even without additional updates.
    pub fn exert(&mut self) {
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.exert();
        }
    }

    /// Advances the trace by `batch`.
    ///
    /// Asserts the batch is a valid continuation of the current frontier,
    /// updates the upper frontier, and pushes the batch into the shared trace.
    pub fn insert(&mut self, batch: Tr::Batch) {

        // Something is wrong if not a sequence.
        if !(&self.upper == batch.lower()) {
            println!("{:?} vs {:?}", self.upper, batch.lower());
        }
        assert!(&self.upper == batch.lower());
        assert!(batch.lower() != batch.upper());

        self.upper.clone_from(batch.upper());

        // push data to the trace, if it still exists.
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.insert(batch);
        }
    }

    /// Inserts an empty batch up to `upper`.
    pub fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if self.upper != upper {
            self.insert(Tr::Batch::empty(self.upper.clone(), upper));
        }
    }
}

impl<Tr: Trace> Drop for TraceWriterIntra<Tr> {
    fn drop(&mut self) { self.seal(Antichain::new()) }
}

/// Write endpoint that distributes batches to both a shared trace and private queues.
///
/// Used for inter-dataflow sharing: in addition to writing to the trace, each batch
/// is pushed onto any listener queues so that importing dataflows can observe it.
pub struct TraceWriterInter<Tr: Trace> {
    /// Inner writer maintaining the frontier and shared trace.
    inner: TraceWriterIntra<Tr>,
    /// A sequence of private queues into which batches are written.
    queues: Rc<RefCell<Vec<TraceInterQueueWriter<Tr>>>>,
}

impl<Tr: Trace> TraceWriterInter<Tr> {
    /// Creates a new `TraceWriterInter`.
    pub fn new(
        upper: Vec<Tr::Time>,
        trace: Weak<RefCell<TraceBox<Tr>>>,
        queues: Rc<RefCell<Vec<TraceInterQueueWriter<Tr>>>>
    ) -> Self
    {
        Self { inner: TraceWriterIntra::new(upper, trace), queues }
    }

    /// Creates a new `TraceWriterInter` from an existing `TraceWriterIntra` and queues.
    pub fn from_intra(
        inner: TraceWriterIntra<Tr>,
        queues: Rc<RefCell<Vec<TraceInterQueueWriter<Tr>>>>
    ) -> Self
    {
        Self { inner, queues }
    }

    /// Exerts merge effort, even without additional updates.
    pub fn exert(&mut self) { self.inner.exert() }

    /// Advances the trace by `batch`.
    ///
    /// The `hint` argument is either `None` in the case of an empty batch,
    /// or is `Some(time)` for a time less or equal to all updates in the
    /// batch and which is suitable for use as a capability.
    pub fn insert(&mut self, batch: Tr::Batch, hint: Option<Tr::Time>) {

        // push information to each listener that still exists.
        let mut borrow = self.queues.borrow_mut();
        for queue in borrow.iter_mut() {
            if let Some(pair) = queue.upgrade() {
                pair.1.borrow_mut().push_back(TraceReplayInstruction::Batch(batch.clone(), hint.clone()));
                pair.1.borrow_mut().push_back(TraceReplayInstruction::Frontier(batch.upper().clone()));
                pair.0.activate();
            }
        }
        borrow.retain(|w| w.upgrade().is_some());

        // push data to the trace and update the frontier.
        self.inner.insert(batch);
    }

    /// Inserts an empty batch up to `upper`.
    pub fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if *self.inner.upper() != upper {
            self.insert(Tr::Batch::empty(self.inner.upper().clone(), upper), None);
        }
    }
}

impl<Tr: Trace> Drop for TraceWriterInter<Tr> {
    fn drop(&mut self) { self.seal(Antichain::new()) }
}
