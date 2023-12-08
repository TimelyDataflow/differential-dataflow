//! Wrapper for filtered trace.

use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use crate::trace::{TraceReader, BatchReader, Description};
use crate::trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
pub struct TraceFilter<Tr, F> {
    trace: Tr,
    logic: F,
}

impl<Tr,F> Clone for TraceFilter<Tr, F>
where
    Tr: TraceReader+Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        TraceFilter {
            trace: self.trace.clone(),
            logic: self.logic.clone(),
        }
    }
}

impl<Tr, F> TraceReader for TraceFilter<Tr, F>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Time: Timestamp,
    Tr::Diff: 'static,
    F: FnMut(Tr::Key<'_>, Tr::Val<'_>)->bool+Clone+'static,
{
    type Key<'a> = Tr::Key<'a>;
    type KeyOwned = Tr::KeyOwned;
    type Val<'a> = Tr::Val<'a>;
    type ValOwned = Tr::ValOwned;
    type Time = Tr::Time;
    type Diff = Tr::Diff;

    type Batch = BatchFilter<Tr::Batch, F>;
    type Storage = Tr::Storage;
    type Cursor = CursorFilter<Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace
            .map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), logic.clone())))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_physical_compaction() }

    fn cursor_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<(Self::Cursor, Self::Storage)> {
        self.trace.cursor_through(upper).map(|(x,y)| (CursorFilter::new(x, self.logic.clone()), y))
    }
}

impl<Tr, F> TraceFilter<Tr, F>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: F) -> Self {
        TraceFilter {
            trace,
            logic,
        }
    }
}


/// Wrapper to provide batch to nested scope.
#[derive(Clone)]
pub struct BatchFilter<B, F> {
    batch: B,
    logic: F,
}

impl<B, F> BatchReader for BatchFilter<B, F>
where
    B: BatchReader,
    B::Time: Timestamp,
    F: FnMut(B::Key<'_>, B::Val<'_>)->bool+Clone+'static
{
    type Key<'a> = B::Key<'a>;
    type KeyOwned = B::KeyOwned;
    type Val<'a> = B::Val<'a>;
    type ValOwned = B::ValOwned;
    type Time = B::Time;
    type Diff = B::Diff;

    type Cursor = BatchCursorFilter<B::Cursor, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFilter::new(self.batch.cursor(), self.logic.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<B::Time> { self.batch.description() }
}

impl<B, F> BatchFilter<B, F>
where
    B: BatchReader,
    B::Time: Timestamp,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, logic: F) -> Self {
        BatchFilter {
            batch,
            logic,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFilter<C, F> {
    cursor: C,
    logic: F,
}

impl<C, F> CursorFilter<C, F> {
    fn new(cursor: C, logic: F) -> Self {
        CursorFilter {
            cursor,
            logic,
        }
    }
}

impl<C, F> Cursor for CursorFilter<C, F>
where
    C: Cursor,
    C::Time: Timestamp,
    F: FnMut(C::Key<'_>, C::Val<'_>)->bool+'static
{
    type Key<'a> = C::Key<'a>;
    type KeyOwned = C::KeyOwned;
    type Val<'a> = C::Val<'a>;
    type ValOwned = C::ValOwned;
    type Time = C::Time;
    type Diff = C::Diff;

    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&Self::Time,&Self::Diff)>(&mut self, storage: &Self::Storage, logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        if (self.logic)(key, val) {
            self.cursor.map_times(storage, logic)
        }
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorFilter<C, F> {
    cursor: C,
    logic: F,
}

impl<C, F> BatchCursorFilter<C, F> {
    fn new(cursor: C, logic: F) -> Self {
        BatchCursorFilter {
            cursor,
            logic,
        }
    }
}

impl<C: Cursor, F> Cursor for BatchCursorFilter<C, F>
where
    C::Time: Timestamp,
    F: FnMut(C::Key<'_>, C::Val<'_>)->bool+'static,
{
    type Key<'a> = C::Key<'a>;
    type KeyOwned = C::KeyOwned;
    type Val<'a> = C::Val<'a>;
    type ValOwned = C::ValOwned;
    type Time = C::Time;
    type Diff = C::Diff;

    type Storage = BatchFilter<C::Storage, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&Self::Time,&Self::Diff)>(&mut self, storage: &Self::Storage, logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        if (self.logic)(key, val) {
            self.cursor.map_times(&storage.batch, logic)
        }
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
