//! Traits and datastructures representing a collection trace.
//!
//! A collection trace is a set of updates of the form `(key, val, time, diff)`, which determine the contents
//! of a collection at given times by accumulating updates whose time field is less or equal to the target field.
//!
//! The `Trace` trait describes those types and methods that a data structure must implement to be viewed as a
//! collection trace. This trait allows operator implementations to be generic with respect to the type of trace,
//! and allows various data structures to be interpretable as multiple different types of trace.

pub mod cursor;
pub mod description;
pub mod implementations;
pub mod layers;
pub mod wrappers;

use timely::communication::message::RefOrMut;
use timely::progress::{Antichain, frontier::AntichainRef};
use timely::progress::Timestamp;

// use ::difference::Semigroup;
pub use self::cursor::Cursor;
pub use self::description::Description;

/// A type used to express how much effort a trace should exert even in the absence of updates.
pub type ExertionLogic = std::sync::Arc<dyn for<'a> Fn(Box<dyn Iterator<Item=(usize, usize, usize)>+'a>)->Option<usize>+Send+Sync>;

//     The traces and batch and cursors want the flexibility to appear as if they manage certain types of keys and
//     values and such, while perhaps using other representations, I'm thinking mostly of wrappers around the keys
//     and vals that change the `Ord` implementation, or stash hash codes, or the like.
//
//     This complicates what requirements we make so that the trace is still usable by someone who knows only about
//     the base key and value types. For example, the complex types should likely dereference to the simpler types,
//    so that the user can make sense of the result as if they were given references to the simpler types. At the
//  same time, the collection should be formable from base types (perhaps we need an `Into` or `From` constraint)
//  and we should, somehow, be able to take a reference to the simple types to compare against the more complex
//  types. This second one is also like an `Into` or `From` constraint, except that we start with a reference and
//  really don't need anything more complex than a reference, but we can't form an owned copy of the complex type
//  without cloning it.
//
//  We could just start by cloning things. Worry about wrapping references later on.

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which extends this trait with further methods
/// to update the contents of the trace. These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the underlying trace and cause work to happen).
pub trait TraceReader {

    /// Key by which updates are indexed.
    type Key;
    /// Values associated with keys.
    type Val;
    /// Timestamps associated with updates
    type Time;
    /// Associated update.
    type R;

    /// The type of an immutable collection of updates.
    type Batch: BatchReader<Key = Self::Key, Val = Self::Val, Time = Self::Time, R = Self::R>+Clone+'static;

    /// Storage type for `Self::Cursor`. Likely related to `Self::Batch`.
    type Storage;

    /// The type used to enumerate the collections contents.
    type Cursor: Cursor<Self::Storage, Key = Self::Key, Val = Self::Val, Time = Self::Time, R = Self::R>;

    /// Provides a cursor over updates contained in the trace.
    fn cursor(&mut self) -> (Self::Cursor, Self::Storage) {
        if let Some(cursor) = self.cursor_through(Antichain::new().borrow()) {
            cursor
        }
        else {
            panic!("unable to acquire complete cursor for trace; is it closed?");
        }
    }

    /// Acquires a cursor to the restriction of the collection's contents to updates at times not greater or
    /// equal to an element of `upper`.
    ///
    /// This method is expected to work if called with an `upper` that (i) was an observed bound in batches from
    /// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
    /// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return a cursor. This
    /// should allow `upper` such as `&[]` as used by `self.cursor()`, though it is difficult to imagine other uses.
    fn cursor_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<(Self::Cursor, Self::Storage)>;

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
    /// By advancing the logical compaction frontier, the caller unblocks merging of otherwise equivalent udates,
    /// but loses the ability to observe historical detail that is not beyond `frontier`.
    ///
    /// It is an error to call this method with a frontier not equal to or beyond the most recent arguments to
    /// this method, or the initial value of `get_logical_compaction()` if this method has not yet been called.
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>);

    /// Deprecated form of `set_logical_compaction`.
    #[deprecated(since = "0.11", note = "please use `set_logical_compaction`")]
    fn advance_by(&mut self, frontier: AntichainRef<Self::Time>) {
        self.set_logical_compaction(frontier);
    }

    /// Reports the logical compaction frontier.
    ///
    /// All update times beyond this frontier will be presented with their original times, and all update times
    /// not beyond this frontier will present as a time that compares identically with all query times beyond
    /// this frontier. Practically, update times not beyond this frontier should not be taken to be accurate as
    /// presented, and should be used carefully, only in accumulation to times that are beyond the frontier.
    fn get_logical_compaction(&mut self) -> AntichainRef<Self::Time>;

    /// Deprecated form of `get_logical_compaction`.
    #[deprecated(since = "0.11", note = "please use `get_logical_compaction`")]
    fn advance_frontier(&mut self) -> AntichainRef<Self::Time> {
        self.get_logical_compaction()
    }

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
    fn set_physical_compaction(&mut self, frontier: AntichainRef<Self::Time>);

    /// Deprecated form of `set_physical_compaction`.
    #[deprecated(since = "0.11", note = "please use `set_physical_compaction`")]
    fn distinguish_since(&mut self, frontier: AntichainRef<Self::Time>) {
        self.set_physical_compaction(frontier);
    }

    /// Reports the physical compaction frontier.
    ///
    /// All batches containing updates beyond this frontier will not be merged with ohter batches. This allows
    /// the caller to create a cursor through any frontier beyond the physical compaction frontier, with the
    /// `cursor_through()` method. This functionality is primarily of interest to the `join` operator, and any
    /// other operators who need to take notice of the physical structure of update batches.
    fn get_physical_compaction(&mut self) -> AntichainRef<Self::Time>;

    /// Deprecated form of `get_physical_compaction`.
    #[deprecated(since = "0.11", note = "please use `get_physical_compaction`")]
    fn distinguish_frontier(&mut self) -> AntichainRef<Self::Time> {
        self.get_physical_compaction()
    }

    /// Maps logic across the non-empty sequence of batches in the trace.
    ///
    /// This is currently used only to extract historical data to prime late-starting operators who want to reproduce
    /// the stream of batches moving past the trace. It could also be a fine basis for a default implementation of the
    /// cursor methods, as they (by default) just move through batches accumulating cursors into a cursor list.
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F);

    /// Reads the upper frontier of committed times.
    ///
    ///
    #[inline]
    fn read_upper(&mut self, target: &mut Antichain<Self::Time>)
    where
        Self::Time: Timestamp,
    {
        target.clear();
        target.insert(<Self::Time as timely::progress::Timestamp>::minimum());
        self.map_batches(|batch| {
            target.clone_from(batch.upper());
        });
    }

    /// Advances `upper` by any empty batches.
    ///
    /// An empty batch whose `batch.lower` bound equals the current
    /// contents of `upper` will advance `upper` to `batch.upper`.
    /// Taken across all batches, this should advance `upper` across
    /// empty batch regions.
    fn advance_upper(&mut self, upper: &mut Antichain<Self::Time>)
    where
        Self::Time: Timestamp,
    {
        self.map_batches(|batch| {
            if batch.is_empty() && batch.lower() == upper {
                upper.clone_from(batch.upper());
            }
        });
    }

}

/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace must pretend to look like a collection of `(Key, Val, Time, isize)` tuples, but is permitted
/// to introduce new types `KeyRef`, `ValRef`, and `TimeRef` which can be dereference to the types above.
///
/// The trace must be constructable from, and navigable by the `Key`, `Val`, `Time` types, but does not need
/// to return them.
pub trait Trace : TraceReader
where <Self as TraceReader>::Batch: Batch {

    /// A type used to assemble batches from disordered updates.
    type Batcher: Batcher<Time = Self::Time>;
    /// A type used to assemble batches from ordered update sequences.
    type Builder: Builder<Item=<Self::Batcher as Batcher>::Item, Time=Self::Time, Output = Self::Batch>;

    /// Allocates a new empty trace.
    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<::logging::Logger>,
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

    /// Introduces a batch of updates to the trace.
    ///
    /// Batches describe the time intervals they contain, and they should be added to the trace in contiguous
    /// intervals. If a batch arrives with a lower bound that does not equal the upper bound of the most recent
    /// addition, the trace will add an empty batch. It is an error to then try to populate that region of time.
    ///
    /// This restriction could be relaxed, especially if we discover ways in which batch interval order could
    /// commute. For now, the trace should complain, to the extent that it cares about contiguous intervals.
    fn insert(&mut self, batch: Self::Batch);

    /// Introduces an empty batch concluding the trace.
    ///
    /// This method should be logically equivalent to introducing an empty batch whose lower frontier equals
    /// the upper frontier of the most recently introduced batch, and whose upper frontier is empty.
    fn close(&mut self);
}

/// A batch of updates whose contents may be read.
///
/// This is a restricted interface to batches of updates, which support the reading of the batch's contents,
/// but do not expose ways to construct the batches. This trait is appropriate for views of the batch, and is
/// especially useful for views derived from other sources in ways that prevent the construction of batches
/// from the type of data in the view (for example, filtered views, or views with extended time coordinates).
pub trait BatchReader
where
    Self: ::std::marker::Sized,
{
    /// Key by which updates are indexed.
    type Key;
    /// Values associated with keys.
    type Val;
    /// Timestamps associated with updates
    type Time: Timestamp;
    /// Associated update.
    type R;

    /// The type used to enumerate the batch's contents.
    type Cursor: Cursor<Self, Key = Self::Key, Val = Self::Val, Time = Self::Time, R = Self::R>;
    /// Acquires a cursor to the batch's contents.
    fn cursor(&self) -> Self::Cursor;
    /// The number of updates in the batch.
    fn len(&self) -> usize;
    /// True if the batch is empty.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Describes the times of the updates in the batch.
    fn description(&self) -> &Description<Self::Time>;

    /// All times in the batch are greater or equal to an element of `lower`.
    fn lower(&self) -> &Antichain<Self::Time> { self.description().lower() }
    /// All times in the batch are not greater or equal to any element of `upper`.
    fn upper(&self) -> &Antichain<Self::Time> { self.description().upper() }
}

/// An immutable collection of updates.
pub trait Batch : BatchReader where Self: ::std::marker::Sized {
    /// A type used to progressively merge batches.
    type Merger: Merger<Self>;

    /// Initiates the merging of consecutive batches.
    ///
    /// The result of this method can be exercised to eventually produce the same result
    /// that a call to `self.merge(other)` would produce, but it can be done in a measured
    /// fashion. This can help to avoid latency spikes where a large merge needs to happen.
    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<Self::Time>) -> Self::Merger {
        Self::Merger::new(self, other, compaction_frontier)
    }
}

/// Functionality for collecting and batching updates.
pub trait Batcher {
    /// Type of update pushed into the batcher.
    type Item;
    /// Times at which batches are formed.
    type Time: Timestamp;
    /// Allocates a new empty batcher.
    fn new() -> Self;
    /// Adds an unordered batch of elements to the batcher.
    fn push_batch(&mut self, batch: RefOrMut<Vec<Self::Item>>);
    /// Returns all updates not greater or equal to an element of `upper`.
    fn seal<B: Builder<Item=Self::Item, Time=Self::Time>>(&mut self, upper: Antichain<Self::Time>) -> B::Output;
    /// Returns the lower envelope of contained update times.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<Self::Time>;
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder {
    /// Input item type.
    type Item;
    /// Timestamp type.
    type Time: Timestamp;
    /// Output batch type.
    type Output;
    
    /// Allocates an empty builder.
    fn new() -> Self;
    /// Allocates an empty builder with some capacity.
    fn with_capacity(cap: usize) -> Self;
    /// Adds an element to the batch.
    ///
    /// The default implementation uses `self.copy` with references to the owned arguments.
    /// One should override it if the builder can take advantage of owned arguments.
    fn push(&mut self, element: Self::Item) {
        self.copy(&element);
    }
    /// Adds an element to the batch.
    fn copy(&mut self, element: &Self::Item);
    /// Adds an ordered sequence of elements to the batch.
    fn extend<I: Iterator<Item=Self::Item>>(&mut self, iter: I) {
        for item in iter { self.push(item); }
    }
    /// Completes building and returns the batch.
    fn done(self, lower: Antichain<Self::Time>, upper: Antichain<Self::Time>, since: Antichain<Self::Time>) -> Self::Output;
}

/// Represents a merge in progress.
pub trait Merger<Output: Batch> {
    /// Creates a new merger to merge the supplied batches, optionally compacting
    /// up to the supplied frontier.
    fn new(source1: &Output, source2: &Output, compaction_frontier: AntichainRef<Output::Time>) -> Self;
    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is non-zero after the call, the merging is complete and
    /// one should call `done` to extract the merged results.
    fn work(&mut self, source1: &Output, source2: &Output, fuel: &mut isize);
    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    fn done(self) -> Output;
}


/// Blanket implementations for reference counted batches.
pub mod rc_blanket_impls {

    use std::rc::Rc;

    use timely::progress::{Antichain, frontier::AntichainRef};
    use super::{Batch, BatchReader, Builder, Merger, Cursor, Description};

    impl<B: BatchReader> BatchReader for Rc<B> {
        type Key = B::Key;
        type Val = B::Val;
        type Time = B::Time;
        type R = B::R;

        /// The type used to enumerate the batch's contents.
        type Cursor = RcBatchCursor<B>;
        /// Acquires a cursor to the batch's contents.
        fn cursor(&self) -> Self::Cursor {
            RcBatchCursor::new((&**self).cursor())
        }

        /// The number of updates in the batch.
        fn len(&self) -> usize { (&**self).len() }
        /// Describes the times of the updates in the batch.
        fn description(&self) -> &Description<Self::Time> { (&**self).description() }
    }

    /// Wrapper to provide cursor to nested scope.
    pub struct RcBatchCursor<B: BatchReader> {
        cursor: B::Cursor,
    }

    impl<B: BatchReader> RcBatchCursor<B> {
        fn new(cursor: B::Cursor) -> Self {
            RcBatchCursor {
                cursor,
            }
        }
    }

    impl<B: BatchReader> Cursor<Rc<B>> for RcBatchCursor<B> {

        type Key = B::Key;
        type Val = B::Val;
        type Time = B::Time;
        type R = B::R;

        #[inline] fn key_valid(&self, storage: &Rc<B>) -> bool { self.cursor.key_valid(storage) }
        #[inline] fn val_valid(&self, storage: &Rc<B>) -> bool { self.cursor.val_valid(storage) }

        #[inline] fn key<'a>(&self, storage: &'a Rc<B>) -> &'a Self::Key { self.cursor.key(storage) }
        #[inline] fn val<'a>(&self, storage: &'a Rc<B>) -> &'a Self::Val { self.cursor.val(storage) }

        #[inline]
        fn map_times<L: FnMut(&Self::Time, &Self::R)>(&mut self, storage: &Rc<B>, logic: L) {
            self.cursor.map_times(storage, logic)
        }

        #[inline] fn step_key(&mut self, storage: &Rc<B>) { self.cursor.step_key(storage) }
        #[inline] fn seek_key(&mut self, storage: &Rc<B>, key: &Self::Key) { self.cursor.seek_key(storage, key) }

        #[inline] fn step_val(&mut self, storage: &Rc<B>) { self.cursor.step_val(storage) }
        #[inline] fn seek_val(&mut self, storage: &Rc<B>, val: &Self::Val) { self.cursor.seek_val(storage, val) }

        #[inline] fn rewind_keys(&mut self, storage: &Rc<B>) { self.cursor.rewind_keys(storage) }
        #[inline] fn rewind_vals(&mut self, storage: &Rc<B>) { self.cursor.rewind_vals(storage) }
    }

    /// An immutable collection of updates.
    impl<B: Batch> Batch for Rc<B> {
        type Merger = RcMerger<B>;
    }

    /// Wrapper type for building reference counted batches.
    pub struct RcBuilder<B: Builder> { builder: B }

    /// Functionality for building batches from ordered update sequences.
    impl<B: Builder> Builder for RcBuilder<B> {
        type Item = B::Item;
        type Time = B::Time;
        type Output = Rc<B::Output>;
        fn new() -> Self { RcBuilder { builder: B::new() } }
        fn with_capacity(cap: usize) -> Self { RcBuilder { builder: B::with_capacity(cap) } }
        fn push(&mut self, element: Self::Item) { self.builder.push(element) }
        fn copy(&mut self, element: &Self::Item) { self.builder.copy(element) }
        fn done(self, lower: Antichain<Self::Time>, upper: Antichain<Self::Time>, since: Antichain<Self::Time>) -> Rc<B::Output> { Rc::new(self.builder.done(lower, upper, since)) }
    }

    /// Wrapper type for merging reference counted batches.
    pub struct RcMerger<B:Batch> { merger: B::Merger }

    /// Represents a merge in progress.
    impl<B:Batch> Merger<Rc<B>> for RcMerger<B> {
        fn new(source1: &Rc<B>, source2: &Rc<B>, compaction_frontier: AntichainRef<B::Time>) -> Self { RcMerger { merger: B::begin_merge(source1, source2, compaction_frontier) } }
        fn work(&mut self, source1: &Rc<B>, source2: &Rc<B>, fuel: &mut isize) { self.merger.work(source1, source2, fuel) }
        fn done(self) -> Rc<B> { Rc::new(self.merger.done()) }
    }
}


/// Blanket implementations for reference counted batches.
pub mod abomonated_blanket_impls {

    extern crate abomonation;

    use abomonation::{Abomonation, measure};
    use abomonation::abomonated::Abomonated;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use super::{Batch, BatchReader, Builder, Merger, Cursor, Description};

    impl<B: BatchReader+Abomonation> BatchReader for Abomonated<B, Vec<u8>> {

        type Key = B::Key;
        type Val = B::Val;
        type Time = B::Time;
        type R = B::R;

        /// The type used to enumerate the batch's contents.
        type Cursor = AbomonatedBatchCursor<B>;
        /// Acquires a cursor to the batch's contents.
        fn cursor(&self) -> Self::Cursor {
            AbomonatedBatchCursor::new((&**self).cursor())
        }

        /// The number of updates in the batch.
        fn len(&self) -> usize { (&**self).len() }
        /// Describes the times of the updates in the batch.
        fn description(&self) -> &Description<Self::Time> { (&**self).description() }
    }

    /// Wrapper to provide cursor to nested scope.
    pub struct AbomonatedBatchCursor<B: BatchReader> {
        cursor: B::Cursor,
    }

    impl<B: BatchReader> AbomonatedBatchCursor<B> {
        fn new(cursor: B::Cursor) -> Self {
            AbomonatedBatchCursor {
                cursor,
            }
        }
    }

    impl<B: BatchReader+Abomonation> Cursor<Abomonated<B, Vec<u8>>> for AbomonatedBatchCursor<B> {

        type Key = B::Key;
        type Val = B::Val;
        type Time = B::Time;
        type R = B::R;

        #[inline] fn key_valid(&self, storage: &Abomonated<B, Vec<u8>>) -> bool { self.cursor.key_valid(storage) }
        #[inline] fn val_valid(&self, storage: &Abomonated<B, Vec<u8>>) -> bool { self.cursor.val_valid(storage) }

        #[inline] fn key<'a>(&self, storage: &'a Abomonated<B, Vec<u8>>) -> &'a Self::Key { self.cursor.key(storage) }
        #[inline] fn val<'a>(&self, storage: &'a Abomonated<B, Vec<u8>>) -> &'a Self::Val { self.cursor.val(storage) }

        #[inline]
        fn map_times<L: FnMut(&Self::Time, &Self::R)>(&mut self, storage: &Abomonated<B, Vec<u8>>, logic: L) {
            self.cursor.map_times(storage, logic)
        }

        #[inline] fn step_key(&mut self, storage: &Abomonated<B, Vec<u8>>) { self.cursor.step_key(storage) }
        #[inline] fn seek_key(&mut self, storage: &Abomonated<B, Vec<u8>>, key: &Self::Key) { self.cursor.seek_key(storage, key) }

        #[inline] fn step_val(&mut self, storage: &Abomonated<B, Vec<u8>>) { self.cursor.step_val(storage) }
        #[inline] fn seek_val(&mut self, storage: &Abomonated<B, Vec<u8>>, val: &Self::Val) { self.cursor.seek_val(storage, val) }

        #[inline] fn rewind_keys(&mut self, storage: &Abomonated<B, Vec<u8>>) { self.cursor.rewind_keys(storage) }
        #[inline] fn rewind_vals(&mut self, storage: &Abomonated<B, Vec<u8>>) { self.cursor.rewind_vals(storage) }
    }

    /// An immutable collection of updates.
    impl<B: Batch+Abomonation> Batch for Abomonated<B, Vec<u8>> {
        type Merger = AbomonatedMerger<B>;
    }

    /// Wrapper type for building reference counted batches.
    pub struct AbomonatedBuilder<B: Builder> { builder: B }

    /// Functionality for building batches from ordered update sequences.
    impl<B: Builder> Builder for AbomonatedBuilder<B>
    where
        B::Output: Abomonation,
    {
        type Item = B::Item;
        type Time = B::Time;
        type Output = Abomonated<B::Output, Vec<u8>>;
        fn new() -> Self { AbomonatedBuilder { builder: B::new() } }
        fn with_capacity(cap: usize) -> Self { AbomonatedBuilder { builder: B::with_capacity(cap) } }
        fn push(&mut self, element: Self::Item) { self.builder.push(element) }
        fn copy(&mut self, element: &Self::Item) { self.builder.copy(element) }
        fn done(self, lower: Antichain<Self::Time>, upper: Antichain<Self::Time>, since: Antichain<Self::Time>) -> Self::Output {
            let batch = self.builder.done(lower, upper, since);
            let mut bytes = Vec::with_capacity(measure(&batch));
            unsafe { abomonation::encode(&batch, &mut bytes).unwrap() };
            unsafe { Abomonated::<B::Output,_>::new(bytes).unwrap() }
        }
    }

    /// Wrapper type for merging reference counted batches.
    pub struct AbomonatedMerger<B:Batch> { merger: B::Merger }

    /// Represents a merge in progress.
    impl<B:Batch+Abomonation> Merger<Abomonated<B,Vec<u8>>> for AbomonatedMerger<B> {
        fn new(source1: &Abomonated<B,Vec<u8>>, source2: &Abomonated<B,Vec<u8>>, compaction_frontier: AntichainRef<B::Time>) -> Self {
            AbomonatedMerger { merger: B::begin_merge(source1, source2, compaction_frontier) }
        }
        fn work(&mut self, source1: &Abomonated<B,Vec<u8>>, source2: &Abomonated<B,Vec<u8>>, fuel: &mut isize) {
            self.merger.work(source1, source2, fuel)
        }
        fn done(self) -> Abomonated<B, Vec<u8>> {
            let batch = self.merger.done();
            let mut bytes = Vec::with_capacity(measure(&batch));
            unsafe { abomonation::encode(&batch, &mut bytes).unwrap() };
            unsafe { Abomonated::<B,_>::new(bytes).unwrap() }
        }
    }
}
