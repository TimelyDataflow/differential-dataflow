//! Wrapper for frontiered trace.
//!
//! Wraps a trace with a frontier so that all exposed timestamps are first advanced by the frontier.
//! This ensures that even for traces that have been advanced, all views provided through cursors
//! present deterministic times, independent of the compaction strategy.

use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};

use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;
use crate::lattice::Lattice;

/// Wrapper to provide trace to nested scope.
pub struct TraceFrontier<Tr>
where
    Tr: TraceReader,
{
    trace: Tr,
    frontier: Antichain<Tr::Time>,
}

impl<Tr> Clone for TraceFrontier<Tr>
where
    Tr: TraceReader+Clone,
    Tr::Time: Clone,
{
    fn clone(&self) -> Self {
        TraceFrontier {
            trace: self.trace.clone(),
            frontier: self.frontier.clone(),
        }
    }
}

impl<Tr> TraceReader for TraceFrontier<Tr>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp+Lattice,
    Tr::R: 'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = BatchFrontier<Tr::Batch>;
    type Cursor = CursorFrontier<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Cursor>;

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        let frontier = self.frontier.borrow();
        self.trace.map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), frontier)))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_physical_compaction() }

    fn cursor_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>>::Storage)> {
        let frontier = self.frontier.borrow();
        self.trace.cursor_through(upper).map(|(x,y)| (CursorFrontier::new(x, frontier), y))
    }
}

impl<Tr> TraceFrontier<Tr>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, frontier: AntichainRef<Tr::Time>) -> Self {
        TraceFrontier {
            trace,
            frontier: frontier.to_owned(),
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchFrontier<B: BatchReader> {
    batch: B,
    frontier: Antichain<B::Time>,
}

impl<B: BatchReader+Clone> Clone for BatchFrontier<B> where B::Time: Clone {
    fn clone(&self) -> Self {
        BatchFrontier {
            batch: self.batch.clone(),
            frontier: self.frontier.to_owned(),
        }
    }
}

impl<B> BatchReader for BatchFrontier<B>
where
    B: BatchReader,
    B::Time: Timestamp+Lattice,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor = BatchCursorFrontier<B>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFrontier::new(self.batch.cursor(), self.frontier.borrow())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<B::Time> { &self.batch.description() }
}

impl<B> BatchFrontier<B>
where
    B: BatchReader,
    B::Time: Timestamp+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, frontier: AntichainRef<B::Time>) -> Self {
        BatchFrontier {
            batch,
            frontier: frontier.to_owned(),
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFrontier<K, V, T, R, C: Cursor<K, V, T, R>> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    cursor: C,
    frontier: Antichain<T>,
}

impl<K, V, T: Clone, R, C: Cursor<K, V, T, R>> CursorFrontier<K, V, T, R, C> {
    fn new(cursor: C, frontier: AntichainRef<T>) -> Self {
        CursorFrontier {
            phantom: ::std::marker::PhantomData,
            cursor,
            frontier: frontier.to_owned(),
        }
    }
}

impl<K, V, T, R, C> Cursor<K, V, T, R> for CursorFrontier<K, V, T, R, C>
where
    C: Cursor<K, V, T, R>,
    T: Timestamp+Lattice,
{
    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&T,&R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let frontier = self.frontier.borrow();
        let mut temp: T = <T as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(storage, |time, diff| {
            temp.clone_from(time);
            temp.advance_by(frontier);
            logic(&temp, diff);
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorFrontier<B: BatchReader> {
    cursor: B::Cursor,
    frontier: Antichain<B::Time>,
}

impl<B: BatchReader> BatchCursorFrontier<B> where B::Time: Clone {
    fn new(cursor: B::Cursor, frontier: AntichainRef<B::Time>) -> Self {
        BatchCursorFrontier {
            cursor,
            frontier: frontier.to_owned(),
        }
    }
}

impl<B: BatchReader> Cursor<B::Key, B::Val, B::Time, B::R> for BatchCursorFrontier<B>
where
    B::Time: Timestamp+Lattice,
{
    type Storage = BatchFrontier<B>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a B::Key { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a B::Val { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&B::Time,&B::R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let frontier = self.frontier.borrow();
        let mut temp: B::Time = <B::Time as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(&storage.batch, |time, diff| {
            temp.clone_from(time);
            temp.advance_by(frontier);
            logic(&temp, diff);
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &B::Key) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &B::Val) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
