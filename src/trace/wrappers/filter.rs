//! Wrapper for filtered trace.

use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

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
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp,
    Tr::R: 'static,
    F: FnMut(&Tr::Key, &Tr::Val)->bool+Clone+'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = BatchFilter<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Batch, F>;
    type Cursor = CursorFilter<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace
            .map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), logic.clone())))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_physical_compaction() }

    fn cursor_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>>::Storage)> {
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
pub struct BatchFilter<K, V, T, R, B, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    logic: F,
}

impl<K, V, T, R, B: Clone, F: Clone> Clone for BatchFilter<K, V, T, R, B, F> {
    fn clone(&self) -> Self {
        BatchFilter {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            logic: self.logic.clone(),
        }
    }
}

impl<K, V, T, R, B, F> BatchReader<K, V, T, R> for BatchFilter<K, V, T, R, B, F>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    F: FnMut(&K, &V)->bool+Clone+'static
{
    type Cursor = BatchCursorFilter<K, V, T, R, B, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFilter::new(self.batch.cursor(), self.logic.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<T> { &self.batch.description() }
}

impl<K, V, T, R, B, F> BatchFilter<K, V, T, R, B, F>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, logic: F) -> Self {
        BatchFilter {
            phantom: ::std::marker::PhantomData,
            batch,
            logic,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFilter<K, V, T, R, C: Cursor<K, V, T, R>, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    cursor: C,
    logic: F,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, F> CursorFilter<K, V, T, R, C, F> {
    fn new(cursor: C, logic: F) -> Self {
        CursorFilter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<K, V, T, R, C, F> Cursor<K, V, T, R> for CursorFilter<K, V, T, R, C, F>
where
    C: Cursor<K, V, T, R>,
    T: Timestamp,
    F: FnMut(&K, &V)->bool+'static
{
    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&T,&R)>(&mut self, storage: &Self::Storage, logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        if (self.logic)(key, val) {
            self.cursor.map_times(storage, logic)
        }
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorFilter<K, V, T, R, B: BatchReader<K, V, T, R>, F> {
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    cursor: B::Cursor,
    logic: F,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> BatchCursorFilter<K, V, T, R, B, F> {
    fn new(cursor: B::Cursor, logic: F) -> Self {
        BatchCursorFilter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> Cursor<K, V, T, R> for BatchCursorFilter<K, V, T, R, B, F>
where
    T: Timestamp,
    F: FnMut(&K, &V)->bool+'static,
{
    type Storage = BatchFilter<K, V, T, R, B, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&T,&R)>(&mut self, storage: &Self::Storage, logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        if (self.logic)(key, val) {
            self.cursor.map_times(&storage.batch, logic)
        }
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
