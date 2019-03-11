//! Wrapper for filtered trace.

use std::rc::Rc;

use timely::progress::Timestamp;

use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
pub struct TraceFilter<K, V, T, R, Tr, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    trace: Tr,
    logic: Rc<F>,
}

impl<K,V,T,R,Tr,F> Clone for TraceFilter<K, V, T, R, Tr, F>
where
    Tr: TraceReader<K, V, T, R>+Clone,
{
    fn clone(&self) -> Self {
        TraceFilter {
            phantom: ::std::marker::PhantomData,
            trace: self.trace.clone(),
            logic: self.logic.clone(),
        }
    }
}

impl<K, V, T, R, Tr, F> TraceReader<K, V, T, R> for TraceFilter<K, V, T, R, Tr, F>
where
    Tr: TraceReader<K, V, T, R>,
    Tr::Batch: Clone,
    K: 'static,
    V: 'static,
    T: Timestamp,
    R: 'static,
    F: Fn(&K, &V)->bool+'static,
{
    type Batch = BatchFilter<K, V, T, R, Tr::Batch, F>;
    type Cursor = CursorFilter<K, V, T, R, Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&mut self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace
            .map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), logic.clone())))
    }

    fn advance_by(&mut self, frontier: &[T]) { self.trace.advance_by(frontier) }
    fn advance_frontier(&mut self) -> &[T] { self.trace.advance_frontier() }

    fn distinguish_since(&mut self, frontier: &[T]) { self.trace.distinguish_since(frontier) }
    fn distinguish_frontier(&mut self) -> &[T] { self.trace.distinguish_frontier() }

    fn cursor_through(&mut self, upper: &[T]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage)> {
        self.trace.cursor_through(upper).map(|(x,y)| (CursorFilter::new(x, self.logic.clone()), y))
    }
}

impl<K, V, T, R, Tr, F> TraceFilter<K, V, T, R, Tr, F>
where
    Tr: TraceReader<K, V, T, R>,
    T: Timestamp,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: Rc<F>) -> Self {
        TraceFilter {
            phantom: ::std::marker::PhantomData,
            trace,
            logic,
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchFilter<K, V, T, R, B, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    logic: Rc<F>,
}

impl<K, V, T, R, B: Clone, F> Clone for BatchFilter<K, V, T, R, B, F> {
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
    F: Fn(&K, &V)->bool+'static
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
    pub fn make_from(batch: B, logic: Rc<F>) -> Self {
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
    logic: Rc<F>,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, F> CursorFilter<K, V, T, R, C, F> {
    fn new(cursor: C, logic: Rc<F>) -> Self {
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
    F: Fn(&K, &V)->bool+'static
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
    logic: Rc<F>,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> BatchCursorFilter<K, V, T, R, B, F> {
    fn new(cursor: B::Cursor, logic: Rc<F>) -> Self {
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
    F: Fn(&K, &V)->bool+'static,
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
