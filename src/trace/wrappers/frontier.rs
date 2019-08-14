//! Wrapper for frontiered trace.
//!
//! Wraps a trace with a frontier so that all exposed timestamps are first advanced by the frontier.
//! This ensures that even for traces that have been advanced, all views provided through cursors
//! present deterministic times, independent of the compaction strategy.

use timely::progress::Timestamp;

use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;
use crate::lattice::Lattice;

/// Wrapper to provide trace to nested scope.
pub struct TraceFrontier<Tr>
where
    Tr: TraceReader,
{
    trace: Tr,
    frontier: Vec<Tr::Time>,
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

    type Batch = BatchFrontier<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Batch>;
    type Cursor = CursorFrontier<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Cursor>;

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, mut f: F) {
        let frontier = &self.frontier[..];
        self.trace.map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), frontier)))
    }

    fn advance_by(&mut self, frontier: &[Tr::Time]) { self.trace.advance_by(frontier) }
    fn advance_frontier(&mut self) -> &[Tr::Time] { self.trace.advance_frontier() }

    fn distinguish_since(&mut self, frontier: &[Tr::Time]) { self.trace.distinguish_since(frontier) }
    fn distinguish_frontier(&mut self) -> &[Tr::Time] { self.trace.distinguish_frontier() }

    fn cursor_through(&mut self, upper: &[Tr::Time]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>>::Storage)> {
        let frontier = &self.frontier[..];
        self.trace.cursor_through(upper).map(|(x,y)| (CursorFrontier::new(x, frontier), y))
    }
}

impl<Tr> TraceFrontier<Tr>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, frontier: &[Tr::Time]) -> Self {
        TraceFrontier {
            trace,
            frontier: frontier.to_vec(),
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchFrontier<K, V, T, R, B> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    frontier: Vec<T>,
}

impl<K, V, T: Clone, R, B: Clone> Clone for BatchFrontier<K, V, T, R, B> {
    fn clone(&self) -> Self {
        BatchFrontier {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            frontier: self.frontier.clone(),
        }
    }
}

impl<K, V, T, R, B> BatchReader<K, V, T, R> for BatchFrontier<K, V, T, R, B>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp+Lattice,
{
    type Cursor = BatchCursorFrontier<K, V, T, R, B>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFrontier::new(self.batch.cursor(), &self.frontier[..])
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<T> { &self.batch.description() }
}

impl<K, V, T, R, B> BatchFrontier<K, V, T, R, B>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, frontier: &[T]) -> Self {
        BatchFrontier {
            phantom: ::std::marker::PhantomData,
            batch,
            frontier: frontier.to_vec(),
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFrontier<K, V, T, R, C: Cursor<K, V, T, R>> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    cursor: C,
    frontier: Vec<T>,
}

impl<K, V, T: Clone, R, C: Cursor<K, V, T, R>> CursorFrontier<K, V, T, R, C> {
    fn new(cursor: C, frontier: &[T]) -> Self {
        CursorFrontier {
            phantom: ::std::marker::PhantomData,
            cursor,
            frontier: frontier.to_vec(),
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
        let frontier = &self.frontier[..];
        let mut temp: T = Default::default();
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
pub struct BatchCursorFrontier<K, V, T, R, B: BatchReader<K, V, T, R>> {
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    cursor: B::Cursor,
    frontier: Vec<T>,
}

impl<K, V, T: Clone, R, B: BatchReader<K, V, T, R>> BatchCursorFrontier<K, V, T, R, B> {
    fn new(cursor: B::Cursor, frontier: &[T]) -> Self {
        BatchCursorFrontier {
            phantom: ::std::marker::PhantomData,
            cursor,
            frontier: frontier.to_vec(),
        }
    }
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>> Cursor<K, V, T, R> for BatchCursorFrontier<K, V, T, R, B>
where
    T: Timestamp+Lattice,
{
    type Storage = BatchFrontier<K, V, T, R, B>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&T,&R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let frontier = &self.frontier[..];
        let mut temp: T = Default::default();
        self.cursor.map_times(&storage.batch, |time, diff| {
            temp.clone_from(time);
            temp.advance_by(frontier);
            logic(&temp, diff);
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
