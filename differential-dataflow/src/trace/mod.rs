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
pub mod wrappers;

use timely::progress::{Antichain, frontier::AntichainRef};
use timely::progress::Timestamp;

use crate::logging::Logger;
pub use self::cursor::Cursor;
pub use self::description::Description;

use crate::trace::implementations::Data;

/// A type used to express how much effort a trace should exert even in the absence of updates.
pub type ExertionLogic = std::sync::Arc<dyn for<'a> Fn(&'a [(usize, usize, usize)])->Option<usize>+Send+Sync>;

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which extends this trait with further methods
/// to update the contents of the trace. These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the underlying trace and cause work to happen).
pub trait TraceReader {

    /// The owned key type.
    type Key: Data;
    /// The owned val type.
    type Val: Data;
    /// The owned time type.
    type Time: Data + crate::lattice::Lattice + Timestamp;
    /// The owned diff type.
    type Diff: Data + crate::difference::Semigroup;

    /// The type of an immutable collection of updates.
    type Batch:
        'static +
        Clone +
        BatchReader<
            Key = Self::Key,
            Val = Self::Val,
            Time = Self::Time,
            Diff = Self::Diff,
        >;

    /// Storage type for `Self::Cursor`. Likely related to `Self::Batch`.
    type Storage;

    /// The type used to enumerate the collections contents.
    type Cursor:
        Cursor<
            Key = Self::Key,
            Val = Self::Val,
            Time = Self::Time,
            Diff = Self::Diff,
            Storage = Self::Storage,
        >;

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
    fn cursor_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<(Self::Cursor, Self::Storage)>;

    /// Advances the frontier that constrains logical compaction.
    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>);

    /// Reports the logical compaction frontier.
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time>;

    /// Advances the frontier that constrains physical compaction.
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>);

    /// Reports the physical compaction frontier.
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time>;

    /// Maps logic across the non-empty sequence of batches in the trace.
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F);

    /// Reads the upper frontier of committed times.
    #[inline]
    fn read_upper(&mut self, target: &mut Antichain<Self::Time>) {
        target.clear();
        target.insert(<Self::Time as Timestamp>::minimum());
        self.map_batches(|batch| {
            target.clone_from(batch.upper());
        });
    }

    /// Advances `upper` by any empty batches.
    fn advance_upper(&mut self, upper: &mut Antichain<Self::Time>) {
        self.map_batches(|batch| {
            if batch.is_empty() && batch.lower() == upper {
                upper.clone_from(batch.upper());
            }
        });
    }
}

/// An append-only collection of `(key, val, time, diff)` tuples.
pub trait Trace : TraceReader<Batch: Batch> {

    /// Allocates a new empty trace.
    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<crate::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self;

    /// Exert merge effort, even without updates.
    fn exert(&mut self);

    /// Sets the logic for exertion in the absence of updates.
    fn set_exert_logic(&mut self, logic: ExertionLogic);

    /// Introduces a batch of updates to the trace.
    fn insert(&mut self, batch: Self::Batch);

    /// Introduces an empty batch concluding the trace.
    fn close(&mut self);
}

/// A batch of updates whose contents may be read.
pub trait BatchReader : Sized {

    /// The owned key type.
    type Key: Data;
    /// The owned val type.
    type Val: Data;
    /// The owned time type.
    type Time: Data + crate::lattice::Lattice + Timestamp;
    /// The owned diff type.
    type Diff: Data + crate::difference::Semigroup;

    /// The type used to enumerate the batch's contents.
    type Cursor:
        Cursor<
            Key = Self::Key,
            Val = Self::Val,
            Time = Self::Time,
            Diff = Self::Diff,
            Storage = Self,
        >;

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
pub trait Batch : BatchReader + Sized {
    /// A type used to progressively merge batches.
    type Merger: Merger<Self>;

    /// Initiates the merging of consecutive batches.
    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<Self::Time>) -> Self::Merger {
        Self::Merger::new(self, other, compaction_frontier)
    }

    /// Produce an empty batch over the indicated interval.
    fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self;
}

/// Functionality for collecting and batching updates.
pub trait Batcher {
    /// Type pushed into the batcher.
    type Input;
    /// Type produced by the batcher.
    type Output;
    /// Times at which batches are formed.
    type Time: Timestamp;
    /// Allocates a new empty batcher.
    fn new(logger: Option<Logger>, operator_id: usize) -> Self;
    /// Adds an unordered container of elements to the batcher.
    fn push_container(&mut self, batch: &mut Self::Input);
    /// Returns all updates not greater or equal to an element of `upper`.
    fn seal<B: Builder<Input=Self::Output, Time=Self::Time>>(&mut self, upper: Antichain<Self::Time>) -> B::Output;
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
    fn new() -> Self { Self::with_capacity(0, 0, 0) }
    /// Allocates an empty builder with capacity for the specified keys, values, and updates.
    fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self;
    /// Adds a chunk of elements to the batch.
    fn push(&mut self, chunk: &mut Self::Input);
    /// Completes building and returns the batch.
    fn done(self, description: Description<Self::Time>) -> Self::Output;

    /// Builds a batch from a chain of updates corresponding to the indicated lower and upper bounds.
    fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output;
}

/// Represents a merge in progress.
pub trait Merger<Output: Batch> {
    /// Creates a new merger to merge the supplied batches, optionally compacting
    /// up to the supplied frontier.
    fn new(source1: &Output, source2: &Output, compaction_frontier: AntichainRef<Output::Time>) -> Self;
    /// Perform some amount of work, decrementing `fuel`.
    fn work(&mut self, source1: &Output, source2: &Output, fuel: &mut isize);
    /// Extracts merged results.
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
        type Diff = B::Diff;

        type Cursor = RcBatchCursor<B::Cursor>;
        fn cursor(&self) -> Self::Cursor {
            RcBatchCursor::new((**self).cursor())
        }

        fn len(&self) -> usize { (**self).len() }
        fn description(&self) -> &Description<Self::Time> { (**self).description() }
    }

    /// Wrapper to provide cursor to nested scope.
    pub struct RcBatchCursor<C> {
        cursor: C,
    }

    impl<C> RcBatchCursor<C> {
        fn new(cursor: C) -> Self {
            RcBatchCursor { cursor }
        }
    }

    impl<C: Cursor> Cursor for RcBatchCursor<C> {

        type Key = C::Key;
        type Val = C::Val;
        type Time = C::Time;
        type Diff = C::Diff;
        type Storage = Rc<C::Storage>;

        #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
        #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

        #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Key> { self.cursor.key(storage) }
        #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Val> { self.cursor.val(storage) }

        #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Key>> { self.cursor.get_key(storage) }
        #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>> { self.cursor.get_val(storage) }

        #[inline]
        fn map_times<L: FnMut(columnar::Ref<'_, Self::Time>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &Self::Storage, logic: L) {
            self.cursor.map_times(storage, logic)
        }

        #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
        #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: columnar::Ref<'_, Self::Key>) { self.cursor.seek_key(storage, key) }

        #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
        #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: columnar::Ref<'_, Self::Val>) { self.cursor.seek_val(storage, val) }

        #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
        #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
    }

    /// An immutable collection of updates.
    impl<B: Batch> Batch for Rc<B> {
        type Merger = RcMerger<B>;
        fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
            Rc::new(B::empty(lower, upper))
        }
    }

    /// Wrapper type for building reference counted batches.
    pub struct RcBuilder<B: Builder> { builder: B }

    /// Functionality for building batches from ordered update sequences.
    impl<B: Builder> Builder for RcBuilder<B> {
        type Input = B::Input;
        type Time = B::Time;
        type Output = Rc<B::Output>;
        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self { RcBuilder { builder: B::with_capacity(keys, vals, upds) } }
        fn push(&mut self, input: &mut Self::Input) { self.builder.push(input) }
        fn done(self, description: Description<Self::Time>) -> Rc<B::Output> { Rc::new(self.builder.done(description)) }
        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            Rc::new(B::seal(chain, description))
        }
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
