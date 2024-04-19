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

impl<Tr: TraceReader> TraceReader for TraceFrontier<Tr> {
    type Key<'a> = Tr::Key<'a>;
    type KeyOwned = Tr::KeyOwned;
    type Val<'a> = Tr::Val<'a>;
    type Time = Tr::Time;
    type Diff = Tr::Diff;

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

impl<B: BatchReader> BatchReader for BatchFrontier<B> {
    type Key<'a> = B::Key<'a>;
    type KeyOwned = B::KeyOwned;
    type Val<'a> = B::Val<'a>;
    type Time = B::Time;
    type Diff = B::Diff;

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

impl<C, T> CursorFrontier<C, T> where T: Clone {
    fn new(cursor: C, since: AntichainRef<T>, until: AntichainRef<T>) -> Self {
        CursorFrontier {
            cursor,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }
}

impl<C: Cursor> Cursor for CursorFrontier<C, C::Time> {
    type Key<'a> = C::Key<'a>;
    type KeyOwned = C::KeyOwned;
    type Val<'a> = C::Val<'a>;
    type Time = C::Time;
    type Diff = C::Diff;

    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&Self::Time,&Self::Diff)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        let mut temp: C::Time = <C::Time as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(storage, |time, diff| {
            temp.clone_from(time);
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

impl<C: Cursor> BatchCursorFrontier<C> {
    fn new(cursor: C, since: AntichainRef<C::Time>, until: AntichainRef<C::Time>) -> Self {
        BatchCursorFrontier {
            cursor,
            since: since.to_owned(),
            until: until.to_owned(),
        }
    }
}

impl<C: Cursor> Cursor for BatchCursorFrontier<C>
where
    C::Storage: BatchReader,
{
    type Key<'a> = C::Key<'a>;
    type KeyOwned = C::KeyOwned;
    type Val<'a> = C::Val<'a>;
    type Time = C::Time;
    type Diff = C::Diff;

    type Storage = BatchFrontier<C::Storage>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&Self::Time,&Self::Diff)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let since = self.since.borrow();
        let until = self.until.borrow();
        let mut temp: C::Time = <C::Time as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(&storage.batch, |time, diff| {
            temp.clone_from(time);
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
