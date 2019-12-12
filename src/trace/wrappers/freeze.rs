//! Wrappers to transform the timestamps of updates.
//!
//! These wrappers are primarily intended to support the re-use of a multi-version index
//! as if it were frozen at a particular (nested) timestamp. For example, if one wants to
//! re-use an index multiple times with minor edits, and only observe the edits at one
//! logical time (meaning: observing all edits less or equal to that time, advanced to that
//! time), this should allow that behavior.
//!
//! Informally, this wrapper is parameterized by a function `F: Fn(&T)->Option<T>` which
//! provides the opportunity to alter the time at which an update happens and to suppress
//! that update, if appropriate. For example, the function
//!
//! ```ignore
//! |t| if t.inner <= 10 { let mut t = t.clone(); t.inner = 10; Some(t) } else { None }
//! ```
//!
//! could be used to present all updates through inner iteration 10, but advanced to inner
//! iteration 10, as if they all occurred exactly at that moment.

use std::rc::Rc;

use timely::dataflow::Scope;
use timely::dataflow::operators::Map;

use operators::arrange::Arranged;
use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Freezes updates to an arrangement using a supplied function.
///
/// This method is experimental, and should be used with care. The intent is that the function
/// `func` can be used to restrict and lock in updates at a particular time, as suggested in the
/// module-level documentation.
pub fn freeze<G, T, F>(arranged: &Arranged<G, T>, func: F) -> Arranged<G, TraceFreeze<T, F>>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    T: TraceReader<Time=G::Timestamp>+Clone,
    T::Key: 'static,
    T::Val: 'static,
    T::R: 'static,
    T::Batch: BatchReader<T::Key, T::Val, G::Timestamp, T::R>,
    T::Cursor: Cursor<T::Key, T::Val, G::Timestamp, T::R>,
    F: Fn(&G::Timestamp)->Option<G::Timestamp>+'static,
{
    let func1 = Rc::new(func);
    let func2 = func1.clone();
    Arranged {
        stream: arranged.stream.map(move |bw| BatchFreeze::make_from(bw, func1.clone())),
        trace: TraceFreeze::make_from(arranged.trace.clone(), func2),
    }
}

/// Wrapper to provide trace to nested scope.
pub struct TraceFreeze<Tr, F>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Clone+'static,
    F: Fn(&Tr::Time)->Option<Tr::Time>,
{
    trace: Tr,
    func: Rc<F>,
}

impl<Tr,F> Clone for TraceFreeze<Tr, F>
where
    Tr: TraceReader+Clone,
    Tr::Time: Lattice+Clone+'static,
    F: Fn(&Tr::Time)->Option<Tr::Time>,
{
    fn clone(&self) -> Self {
        TraceFreeze {
            trace: self.trace.clone(),
            func: self.func.clone(),
        }
    }
}

impl<Tr, F> TraceReader for TraceFreeze<Tr, F>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Lattice+Clone+'static,
    Tr::R: 'static,
    F: Fn(&Tr::Time)->Option<Tr::Time>+'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = BatchFreeze<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Batch, F>;
    type Cursor = CursorFreeze<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&mut self, mut f: F2) {
        let func = &self.func;
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone(), func.clone()));
        })
    }

    fn advance_by(&mut self, frontier: &[Tr::Time]) { self.trace.advance_by(frontier) }
    fn advance_frontier(&mut self) -> &[Tr::Time] { self.trace.advance_frontier() }

    fn distinguish_since(&mut self, frontier: &[Tr::Time]) { self.trace.distinguish_since(frontier) }
    fn distinguish_frontier(&mut self) -> &[Tr::Time] { self.trace.distinguish_frontier() }

    fn cursor_through(&mut self, upper: &[Tr::Time]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>>::Storage)> {
        let func = &self.func;
        self.trace.cursor_through(upper)
            .map(|(cursor, storage)| (CursorFreeze::new(cursor, func.clone()), storage))
    }
}

impl<Tr, F> TraceFreeze<Tr, F>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Lattice+Clone+'static,
    Tr::R: 'static,
    F: Fn(&Tr::Time)->Option<Tr::Time>,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, func: Rc<F>) -> Self {
        TraceFreeze {
            trace: trace,
            func: func,
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchFreeze<K, V, T, R, B, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    batch: B,
    func: Rc<F>,
}

impl<K, V, T: Clone, R, B: Clone, F> Clone for BatchFreeze<K, V, T, R, B, F> {
    fn clone(&self) -> Self {
        BatchFreeze {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            func: self.func.clone(),
        }
    }
}

impl<K, V, T, R, B, F> BatchReader<K, V, T, R> for BatchFreeze<K, V, T, R, B, F>
where
    B: BatchReader<K, V, T, R>,
    T: Clone,
    F: Fn(&T)->Option<T>,
{
    type Cursor = BatchCursorFreeze<K, V, T, R, B, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFreeze::new(self.batch.cursor(), self.func.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<T> { self.batch.description() }
}

impl<K, V, T, R, B, F> BatchFreeze<K, V, T, R, B, F>
where
    B: BatchReader<K, V, T, R>,
    T: Clone,
    F: Fn(&T)->Option<T>
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, func: Rc<F>) -> Self {
        BatchFreeze {
            phantom: ::std::marker::PhantomData,
            batch: batch,
            func: func,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFreeze<K, V, T, R, C: Cursor<K, V, T, R>, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    cursor: C,
    func: Rc<F>,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, F> CursorFreeze<K, V, T, R, C, F> {
    fn new(cursor: C, func: Rc<F>) -> Self {
        CursorFreeze {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
            func: func,
        }
    }
}

impl<K, V, T, R, C, F> Cursor<K, V, T, R> for CursorFreeze<K, V, T, R, C, F>
where
    C: Cursor<K, V, T, R>,
    F: Fn(&T)->Option<T>,
{

    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline] fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let func = &self.func;
        self.cursor.map_times(storage, |time, diff| {
            if let Some(time) = func(time) {
                logic(&time, diff);
            }
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
pub struct BatchCursorFreeze<K, V, T, R, B: BatchReader<K, V, T, R>, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    cursor: B::Cursor,
    func: Rc<F>,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> BatchCursorFreeze<K, V, T, R, B, F> {
    fn new(cursor: B::Cursor, func: Rc<F>) -> Self {
        BatchCursorFreeze {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
            func: func,
        }
    }
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> Cursor<K, V, T, R> for BatchCursorFreeze<K, V, T, R, B, F>
where
    F: Fn(&T)->Option<T>,
{

    type Storage = BatchFreeze<K, V, T, R, B, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline] fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let func = &self.func;
        self.cursor.map_times(&storage.batch, |time, diff| {
            if let Some(time) = func(time) {
                logic(&time, diff);
            }
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
