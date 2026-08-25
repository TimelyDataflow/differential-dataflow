//! Traits and datastructures representing a collection trace.
//!
//! A collection trace is a set of updates of the form `(key, val, time, diff)`, which determine the contents
//! of a collection at given times by accumulating updates whose time field is less or equal to the target field.
//!
//! The `Trace` trait describes those types and methods that a data structure must implement to be viewed as a
//! collection trace. This trait allows operator implementations to be generic with respect to the type of trace,
//! and allows various data structures to be interpretable as multiple different types of trace.

pub mod chunk;
pub mod cursor;
pub mod description;
pub mod implementations;
pub mod wrappers;

use timely::container::PushInto;
use timely::progress::{Antichain, frontier::AntichainRef};
use timely::progress::Timestamp;
use crate::lattice::Lattice;

use crate::logging::Logger;
pub use self::cursor::Cursor;
pub use self::cursor::Navigable;
pub use self::cursor::{BatchCursor, BatchKey, BatchVal, BatchValOwn, BatchDiff, BatchDiffGat, BatchTimeGat};
pub use self::description::Description;

/// A type used to express how much effort a trace should exert even in the absence of updates.
pub type ExertionLogic = std::sync::Arc<dyn for<'a> Fn(&'a [(usize, usize, usize)])->Option<usize>+Send+Sync>;

/// An interval of a trace's history: a description of the times it covers, and the batch of
/// updates within it, absent exactly when there are none.
///
/// The interval is never empty, but the batch may be missing; a span records that its times
/// happened and brought no updates. Reading the batch is the [`Navigable`] capability.
#[derive(Clone, Debug)]
pub struct Span<T, B> {
    /// The lower and upper bounds of contained update times, and the compaction frontier.
    pub desc: Description<T>,
    /// The updates within the interval; absent exactly when there are none.
    pub inner: Option<B>,
}

impl<T, B> Span<T, B> {
    /// A span from a description and the batch within it, absent when there are no updates.
    pub fn new(desc: Description<T>, inner: Option<B>) -> Self { Self { desc, inner } }
    /// All times in the span are greater or equal to an element of `lower`.
    pub fn lower(&self) -> &Antichain<T> { self.desc.lower() }
    /// All times in the span are not greater or equal to any element of `upper`.
    pub fn upper(&self) -> &Antichain<T> { self.desc.upper() }
    /// True if the span carries a batch of updates.
    ///
    /// This is about the updates, not the interval, which is never empty.
    pub fn has_updates(&self) -> bool { self.inner.is_some() }
}

impl<T: Timestamp, B> Span<T, B> {
    /// A span over the indicated interval carrying no updates.
    pub fn empty(lower: Antichain<T>, upper: Antichain<T>) -> Self {
        Self { desc: Description::new(lower, upper, Antichain::from_elem(T::minimum())), inner: None }
    }
}

/// The span type of a trace: [`Span`] instantiated at the trace's time and batch.
pub type SpanOf<Tr> = Span<<Tr as TraceReader>::Time, <Tr as TraceReader>::Batch>;

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which extends this trait with further methods
/// to update the contents of the trace. These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the underlying trace and cause work to happen).
pub trait TraceReader {

    /// The timestamp type of the trace's updates.
    ///
    /// Key/val/diff opinions live on the batches' cursors; the trace itself only needs time, to
    /// bound its contents and to drive compaction.
    type Time: Timestamp + Lattice;

    /// The batches of updates the trace's spans carry.
    ///
    /// A span is [`Span<Self::Time, Self::Batch>`](Span), pairing a description with a batch
    /// when its interval brought any updates. Reading a batch is the optional [`Navigable`]
    /// capability, required only where cursors are taken.
    type Batch: 'static + Clone;

    /// Acquires the non-empty sequence of spans covering updates at times not greater or equal to an
    /// element of `upper`.
    ///
    /// This method is expected to work if called with an `upper` that (i) was an observed bound in spans from
    /// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
    /// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return the spans. This
    /// should allow `upper` such as `&[]`, used to acquire all spans, though it is difficult to imagine other uses.
    fn spans_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Span<Self::Time, Self::Batch>>>;

    /// Acquires the batches of the spans covering times not greater or equal to an element
    /// of `upper`.
    ///
    /// Spans with no updates contribute nothing, so this is what callers taking cursors want;
    /// [`spans_through`](Self::spans_through) is for the drivers that also need the spans'
    /// descriptions.
    fn batches_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Self::Batch>> {
        Some(self.spans_through(upper)?.into_iter().filter_map(|b| b.inner).collect())
    }

    /// Advances the frontier that constrains logical compaction.
    ///
    /// Logical compaction is the ability of the trace to change the times of the updates it contains.
    /// Update times may be changed as long as their comparison to all query times beyond the logical compaction
    /// frontier remains unchanged. Practically, this means that groups of timestamps not beyond the frontier can
    /// be coalesced into fewer representative times.
    ///
    /// Logical compaction is important, as it allows the trace to forget historical distinctions between update
    /// times, and maintain a compact memory footprint over an unbounded update history.
    ///
    /// By advancing the logical compaction frontier, the caller unblocks merging of otherwise equivalent updates,
    /// but loses the ability to observe historical detail that is not beyond `frontier`.
    ///
    /// It is an error to call this method with a frontier not equal to or beyond the most recent arguments to
    /// this method, or the initial value of `get_logical_compaction()` if this method has not yet been called.
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>);

    /// Reports the logical compaction frontier.
    ///
    /// All update times beyond this frontier will be presented with their original times, and all update times
    /// not beyond this frontier will present as a time that compares identically with all query times beyond
    /// this frontier. Practically, update times not beyond this frontier should not be taken to be accurate as
    /// presented, and should be used carefully, only in accumulation to times that are beyond the frontier.
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time>;

    /// Advances the frontier that constrains physical compaction.
    ///
    /// Physical compaction is the ability of the trace to merge the batches of updates it maintains. Physical
    /// compaction does not change the updates or their timestamps, although it is also the moment at which
    /// logical compaction is most likely to happen.
    ///
    /// Physical compaction allows the trace to maintain a logarithmic number of batches of updates, which is
    /// what allows the trace to provide efficient random access by keys and values.
    ///
    /// By advancing the physical compaction frontier, the caller unblocks the merging of batches of updates,
    /// but loses the ability to create a cursor through any frontier not beyond `frontier`.
    ///
    /// It is an error to call this method with a frontier not equal to or beyond the most recent arguments to
    /// this method, or the initial value of `get_physical_compaction()` if this method has not yet been called.
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>);

    /// Reports the physical compaction frontier.
    ///
    /// All spans containing updates beyond this frontier will not be merged with other spans. This allows
    /// the caller to acquire the spans through any frontier beyond the physical compaction frontier, with the
    /// `spans_through()` method. This functionality is primarily of interest to the `join` operator, and any
    /// other operators who need to take notice of the physical structure of update batches.
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time>;

    /// Maps logic across the non-empty sequence of spans in the trace.
    ///
    /// This is currently used only to extract historical data to prime late-starting operators who want to reproduce
    /// the stream of spans moving past the trace.
    fn map_spans<F: FnMut(&Span<Self::Time, Self::Batch>)>(&self, f: F);

    /// Reads the upper frontier of committed times.
    ///
    ///
    #[inline]
    fn read_upper(&mut self, target: &mut Antichain<Self::Time>) {
        target.clear();
        target.insert(<Self::Time as timely::progress::Timestamp>::minimum());
        self.map_spans(|span| {
            target.clone_from(span.upper());
        });
    }

    /// Advances `upper` across any spans with no updates.
    ///
    /// A span carrying no updates whose `lower` bound equals the current
    /// contents of `upper` will advance `upper` to its `upper`.
    /// Taken across all spans, this should advance `upper` across
    /// update-free regions.
    fn advance_upper(&mut self, upper: &mut Antichain<Self::Time>) {
        self.map_spans(|span| {
            if !span.has_updates() && span.lower() == upper {
                upper.clone_from(span.upper());
            }
        });
    }

}

/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace itself is opinionated only about `Time`, which bounds its contents and drives its compaction.
/// Key, value, and diff opinions live on the batches' cursors, and are reached through [`Navigable`].
pub trait Trace : TraceReader {

    /// Allocates a new empty trace.
    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<crate::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self;

    /// Exert merge effort, even without updates.
    fn exert(&mut self);

    /// Sets the logic for exertion in the absence of updates.
    ///
    /// The function receives an iterator over batch levels, from large to small, as triples `(level, count, length)`,
    /// indicating the level, the number of batches, and their total length in updates. It should return a number of
    /// updates to perform, or `None` if no work is required.
    fn set_exert_logic(&mut self, logic: ExertionLogic);

    /// Introduces a span of updates to the trace.
    ///
    /// Spans describe the time intervals they contain, and they should be added to the trace in contiguous
    /// intervals. If a span arrives with a lower bound that does not equal the upper bound of the most recent
    /// addition, the trace will add a span with no updates. It is an error to then try to populate that region
    /// of time.
    ///
    /// This restriction could be relaxed, especially if we discover ways in which span interval order could
    /// commute. For now, the trace should complain, to the extent that it cares about contiguous intervals.
    fn insert(&mut self, span: Span<Self::Time, Self::Batch>);

    /// Introduces an update-free span concluding the trace.
    ///
    /// This method should be logically equivalent to introducing a span with no updates whose lower frontier
    /// equals the upper frontier of the most recently introduced span, and whose upper frontier is empty.
    fn close(&mut self);
}

/// Functionality for collecting and batching updates.
///
/// Accepts containers of type `Output` via [`PushInto`] and produces output batches of the same
/// type. Callers are responsible for converting raw input data into `Output` containers (e.g.
/// using a chunker) before pushing into the batcher.
pub trait Batcher: PushInto<Self::Output> {
    /// Type produced by the batcher, and also the type it consumes.
    type Output: Default;
    /// Times at which batches are formed.
    type Time: Timestamp;
    /// Allocates a new empty batcher.
    fn new(logger: Option<Logger>, operator_id: usize) -> Self;
    /// Returns all updates not greater or equal to an element of `upper`, as a sorted and
    /// consolidated chain together with the description that bounds them.
    ///
    /// The returned chain is suitable to hand directly to [`Builder::seal`].
    fn seal(&mut self, upper: Antichain<Self::Time>) -> (Vec<Self::Output>, Description<Self::Time>);
    /// Returns the lower envelope of contained update times.
    fn frontier(&mut self) -> AntichainRef<'_, Self::Time>;
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder: Sized {
    /// Input item type.
    type Input;
    /// Timestamp type.
    type Time: Timestamp;
    /// Output batch type.
    type Output;

    /// Allocates an empty builder.
    ///
    /// Ideally we deprecate this and insist all non-trivial building happens via `with_capacity()`.
    // #[deprecated]
    fn new() -> Self { Self::with_capacity(0, 0, 0) }
    /// Allocates an empty builder with capacity for the specified keys, values, and updates.
    ///
    /// They represent respectively the number of distinct `key`, `(key, val)`, and total updates.
    fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self;
    /// Adds a chunk of elements to the batch.
    ///
    /// Adds all elements from `chunk` to the builder and leaves `chunk` in an undefined state.
    fn push(&mut self, chunk: &mut Self::Input);
    /// Completes building and returns the batch, absent if no updates were pushed.
    fn done(self) -> Option<Self::Output>;

    /// Builds a batch from a chain of updates.
    ///
    /// This method relies on the chain only containing updates greater or equal to the lower frontier,
    /// and not greater or equal to the upper frontier, of the interval the caller means to describe.
    /// Chains must also be sorted and consolidated.
    fn seal(chain: &mut Vec<Self::Input>) -> Option<Self::Output>;
}

/// Blanket implementations for reference counted batches.
pub mod rc_blanket_impls {

    use std::rc::Rc;

    use super::{Navigable, Cursor};

    impl<B: Navigable> Navigable for Rc<B> {
        /// The type used to enumerate the batch's contents.
        type Cursor = RcBatchCursor<B::Cursor>;
        /// Acquires a cursor to the batch's contents.
        fn cursor(&self) -> Self::Cursor {
            RcBatchCursor::new((**self).cursor())
        }
    }

    /// Wrapper to provide cursor to nested scope.
    pub struct RcBatchCursor<C> {
        cursor: C,
    }

    impl<C> RcBatchCursor<C> {
        fn new(cursor: C) -> Self {
            RcBatchCursor {
                cursor,
            }
        }
    }

    impl<C: Cursor> Cursor for RcBatchCursor<C> {

        type Storage = Rc<C::Storage>;

        type Key<'a> = C::Key<'a>;
        type ValOwn = C::ValOwn;
        type Val<'a> = C::Val<'a>;
        type Time = C::Time;
        type TimeGat<'a> = C::TimeGat<'a>;
        type Diff = C::Diff;
        type DiffGat<'a> = C::DiffGat<'a>;
        type KeyContainer = C::KeyContainer;
        type ValContainer = C::ValContainer;
        type TimeContainer = C::TimeContainer;
        type DiffContainer = C::DiffContainer;

        #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
        #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

        #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage) }
        #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage) }

        #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(storage) }
        #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(storage) }

        #[inline]
        fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, logic: L) {
            self.cursor.map_times(storage, logic)
        }

        #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
        #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(storage, key) }

        #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
        #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(storage, val) }

        #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
        #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
    }

}
