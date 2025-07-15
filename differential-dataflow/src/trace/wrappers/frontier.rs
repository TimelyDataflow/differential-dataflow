//! Wrapper for frontiered trace.
//!
//! Wraps a trace with `since` and `upper` frontiers so that all exposed timestamps are first advanced
//! by the `since` frontier and restricted by the `upper` frontier. This presents a deterministic trace
//! on the interval `[since, upper)`, presenting only accumulations up to `since` (rather than partially
//! accumulated updates) and no updates at times greater or equal to `upper` (even as parts of batches
//! that span that time).

use timely::progress::{Antichain, frontier::AntichainRef};

use crate::trace::{TraceReader, BatchReader, Description};
use crate::trace::cursor::Cursor;
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

impl<Tr: TraceReader> LaidOut for TraceFrontier<Tr> {
    type Layout = (
        <Tr::Layout as Layout>::KeyContainer,
        <Tr::Layout as Layout>::ValContainer,
        Vec<Tr::Time>,
        <Tr::Layout as Layout>::DiffContainer,
        <Tr::Layout as Layout>::OffsetContainer,
    );
}

impl<Tr: TraceReader> TraceReader for TraceFrontier<Tr> {

    type Batch = BatchFrontier<Tr::Batch>;
    type Storage = Tr::Storage;
    type Cursor = CursorFrontier<Tr::Cursor, Tr::Time>;

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        self.trace.map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), since, until)))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_physical_compaction() }

    fn cursor_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<(Self::Cursor, Self::Storage)> {
        let since = self.since.borrow();
        let until = self.until.borrow();
        self.trace.cursor_through(upper).map(|(x,y)| (CursorFrontier::new(x, since, until), y))
    }
}

impl<Tr: TraceReader> TraceFrontier<Tr> {
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, since: AntichainRef<Tr::Time>, until: AntichainRef<Tr::Time>) -> Self {
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

impl<B: BatchReader> LaidOut for BatchFrontier<B> {
    type Layout = (
        <B::Layout as Layout>::KeyContainer,
        <B::Layout as Layout>::ValContainer,
        Vec<B::Time>,
        <B::Layout as Layout>::DiffContainer,
        <B::Layout as Layout>::OffsetContainer,
    );
}

impl<B: BatchReader> BatchReader for BatchFrontier<B> {

    type Cursor = BatchCursorFrontier<B::Cursor>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFrontier::new(self.batch.cursor(), self.since.borrow(), self.until.borrow())
    }
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

/// Wrapper to provide cursor to nested scope.
pub struct CursorFrontier<C, T> {
    cursor: C,
    since: Antichain<T>,
    until: Antichain<T>
}

use crate::trace::implementations::{Layout, LaidOut};
impl<C: Cursor> LaidOut for CursorFrontier<C, C::Time> {
    type Layout = (
        <C::Layout as Layout>::KeyContainer,
        <C::Layout as Layout>::ValContainer,
        Vec<C::Time>,
        <C::Layout as Layout>::DiffContainer,
        <C::Layout as Layout>::OffsetContainer,
    );
}

impl<C, T: Clone> CursorFrontier<C, T> {
    fn new(cursor: C, since: AntichainRef<T>, until: AntichainRef<T>) -> Self {
        CursorFrontier {
            cursor,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }
}

impl<C: Cursor> Cursor for CursorFrontier<C, C::Time> {

    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(storage) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(storage) }

    #[inline]
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        let mut temp: C::Time = <C::Time as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(storage, |time, diff| {
            C::clone_time_onto(time, &mut temp);
            temp.advance_by(since);
            if !until.less_equal(&temp) {
                logic(&temp, diff);
            }
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorFrontier<C: Cursor> {
    cursor: C,
    since: Antichain<C::Time>,
    until: Antichain<C::Time>,
}

impl<C: Cursor> LaidOut for BatchCursorFrontier<C> {
    type Layout = (
        <C::Layout as Layout>::KeyContainer,
        <C::Layout as Layout>::ValContainer,
        Vec<C::Time>,
        <C::Layout as Layout>::DiffContainer,
        <C::Layout as Layout>::OffsetContainer,
    );
}

impl<C: Cursor> BatchCursorFrontier<C> {
    fn new(cursor: C, since: AntichainRef<C::Time>, until: AntichainRef<C::Time>) -> Self {
        BatchCursorFrontier {
            cursor,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }
}

impl<C: Cursor<Storage: BatchReader>> Cursor for BatchCursorFrontier<C> {

    type Storage = BatchFrontier<C::Storage>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(&storage.batch) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(&storage.batch) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        let mut temp: C::Time = <C::Time as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(&storage.batch, |time, diff| {
            C::clone_time_onto(time, &mut temp);
            temp.advance_by(since);
            if !until.less_equal(&temp) {
                logic(&temp, diff);
            }
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
