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
use timely::progress::frontier::AntichainRef;

use crate::operators::arrange::Arranged;
use crate::trace::{TraceReader, BatchReader, Description};
use crate::trace::cursor::Cursor;

/// Freezes updates to an arrangement using a supplied function.
///
/// This method is experimental, and should be used with care. The intent is that the function
/// `func` can be used to restrict and lock in updates at a particular time, as suggested in the
/// module-level documentation.
pub fn freeze<G, T, F>(arranged: &Arranged<G, T>, func: F) -> Arranged<G, TraceFreeze<T, F>>
where
    G: Scope<Timestamp=T::Time>,
    T: TraceReader+Clone,
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
    F: Fn(&Tr::Time)->Option<Tr::Time>,
{
    trace: Tr,
    func: Rc<F>,
}

impl<Tr,F> Clone for TraceFreeze<Tr, F>
where
    Tr: TraceReader+Clone,
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
    F: Fn(&Tr::Time)->Option<Tr::Time>+'static,
{
    type Key<'a> = Tr::Key<'a>;
    type KeyOwned = Tr::KeyOwned;
    type Val<'a> = Tr::Val<'a>;
    type ValOwned = Tr::ValOwned;
    type Time = Tr::Time;
    type Diff = Tr::Diff;

    type Batch = BatchFreeze<Tr::Batch, F>;
    type Storage = Tr::Storage;
    type Cursor = CursorFreeze<Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&self, mut f: F2) {
        let func = &self.func;
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone(), func.clone()));
        })
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_physical_compaction() }

    fn cursor_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<(Self::Cursor, Self::Storage)> {
        let func = &self.func;
        self.trace.cursor_through(upper)
            .map(|(cursor, storage)| (CursorFreeze::new(cursor, func.clone()), storage))
    }
}

impl<Tr, F> TraceFreeze<Tr, F>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    F: Fn(&Tr::Time)->Option<Tr::Time>,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, func: Rc<F>) -> Self {
        Self { trace, func }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchFreeze<B, F> {
    batch: B,
    func: Rc<F>,
}

impl<B: Clone, F> Clone for BatchFreeze<B, F> {
    fn clone(&self) -> Self {
        BatchFreeze {
            batch: self.batch.clone(),
            func: self.func.clone(),
        }
    }
}

impl<B, F> BatchReader for BatchFreeze<B, F>
where
    B: BatchReader,
    F: Fn(&B::Time)->Option<B::Time>,
{
    type Key<'a> = B::Key<'a>;
    type KeyOwned = B::KeyOwned;
    type Val<'a> = B::Val<'a>;
    type ValOwned = B::ValOwned;
    type Time = B::Time;
    type Diff = B::Diff;

    type Cursor = BatchCursorFreeze<B::Cursor, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFreeze::new(self.batch.cursor(), self.func.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<B::Time> { self.batch.description() }
}

impl<B, F> BatchFreeze<B, F>
where
    B: BatchReader,
    F: Fn(&B::Time)->Option<B::Time>
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, func: Rc<F>) -> Self {
        Self { batch, func }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFreeze<C, F> {
    cursor: C,
    func: Rc<F>,
}

impl<C, F> CursorFreeze<C, F> {
    fn new(cursor: C, func: Rc<F>) -> Self {
        Self { cursor, func }
    }
}

impl<C, F> Cursor for CursorFreeze<C, F>
where
    C: Cursor,
    F: Fn(&C::Time)->Option<C::Time>,
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

    #[inline] fn map_times<L: FnMut(&Self::Time, &Self::Diff)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let func = &self.func;
        self.cursor.map_times(storage, |time, diff| {
            if let Some(time) = func(time) {
                logic(&time, diff);
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
pub struct BatchCursorFreeze<C, F> {
    cursor: C,
    func: Rc<F>,
}

impl<C, F> BatchCursorFreeze<C, F> {
    fn new(cursor: C, func: Rc<F>) -> Self {
        Self { cursor, func }
    }
}

// impl<C: Cursor<Storage=B, Time=B::Time>, B: BatchReader<Cursor=C>, F> Cursor for BatchCursorFreeze<B, F>
impl<C: Cursor, F> Cursor for BatchCursorFreeze<C, F>
where
    F: Fn(&C::Time)->Option<C::Time>,
{
    type Key<'a> = C::Key<'a>;
    type KeyOwned = C::KeyOwned;
    type Val<'a> = C::Val<'a>;
    type ValOwned = C::ValOwned;
    type Time = C::Time;
    type Diff = C::Diff;

    type Storage = BatchFreeze<C::Storage, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(&storage.batch) }

    #[inline] fn map_times<L: FnMut(&Self::Time, &Self::Diff)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let func = &self.func;
        self.cursor.map_times(&storage.batch, |time, diff| {
            if let Some(time) = func(time) {
                logic(&time, diff);
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
