//! Wrappers to provide trace access to nested scopes.

use std::rc::Rc;

use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
pub struct TraceEnter<Tr, TInner, F>
where
    Tr: TraceReader,
{
    trace: Tr,
    stash1: Vec<Tr::Time>,
    stash2: Vec<TInner>,
    logic: Rc<F>,
}

impl<Tr,TInner,F> Clone for TraceEnter<Tr, TInner,F>
where
    Tr: TraceReader+Clone,
{
    fn clone(&self) -> Self {
        TraceEnter {
            trace: self.trace.clone(),
            stash1: Vec::new(),
            stash2: Vec::new(),
            logic: self.logic.clone(),
        }
    }
}

impl<Tr, TInner,F> TraceReader for TraceEnter<Tr, TInner,F>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
    Tr::R: 'static,
    F: 'static,
    F: Fn(&Tr::Key, &Tr::Val, &Tr::Time)->TInner,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = TInner;
    type R = Tr::R;

    type Batch = BatchEnter<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Batch, TInner,F>;
    type Cursor = CursorEnter<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Cursor, TInner,F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&mut self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone(), logic.clone()));
        })
    }

    fn advance_by(&mut self, frontier: &[TInner]) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.push(time.clone().to_outer());
        }
        self.trace.advance_by(&self.stash1[..]);
    }
    fn advance_frontier(&mut self) -> &[TInner] {
        self.stash2.clear();
        for time in self.trace.advance_frontier().iter() {
            self.stash2.push(TInner::to_inner(time.clone()));
        }
        &self.stash2[..]
    }

    fn distinguish_since(&mut self, frontier: &[TInner]) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.push(time.clone().to_outer());
        }
        self.trace.distinguish_since(&self.stash1[..]);
    }
    fn distinguish_frontier(&mut self) -> &[TInner] {
        self.stash2.clear();
        for time in self.trace.distinguish_frontier().iter() {
            self.stash2.push(TInner::to_inner(time.clone()));
        }
        &self.stash2[..]
    }

    fn cursor_through(&mut self, upper: &[TInner]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, TInner, Tr::R>>::Storage)> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.push(time.clone().to_outer());
        }
        self.trace.cursor_through(&self.stash1[..]).map(|(x,y)| (CursorEnter::new(x, self.logic.clone()), y))
    }
}

impl<Tr, TInner, F> TraceEnter<Tr, TInner,F>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: Rc<F>) -> Self {
        TraceEnter {
            trace: trace,
            stash1: Vec::new(),
            stash2: Vec::new(),
            logic,
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchEnter<K, V, T, R, B, TInner, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    description: Description<TInner>,
    logic: Rc<F>,
}

impl<K, V, T, R, B: Clone, TInner: Clone, F> Clone for BatchEnter<K, V, T, R, B, TInner, F> {
    fn clone(&self) -> Self {
        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            description: self.description.clone(),
            logic: self.logic.clone(),
        }
    }
}

impl<K, V, T, R, B, TInner, F> BatchReader<K, V, TInner, R> for BatchEnter<K, V, T, R, B, TInner, F>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
    F: Fn(&K, &V, &T)->TInner,
{
    type Cursor = BatchCursorEnter<K, V, T, R, B, TInner, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.batch.cursor(), self.logic.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
}

impl<K, V, T, R, B, TInner, F> BatchEnter<K, V, T, R, B, TInner, F>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, logic: Rc<F>) -> Self {
        let lower: Vec<_> = batch.description().lower().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch,
            description: Description::new(&lower[..], &upper[..], &since[..]),
            logic,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorEnter<K, V, T, R, C: Cursor<K, V, T, R>, TInner, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R, TInner)>,
    cursor: C,
    logic: Rc<F>,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, TInner, F> CursorEnter<K, V, T, R, C, TInner, F> {
    fn new(cursor: C, logic: Rc<F>) -> Self {
        CursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<K, V, T, R, C, TInner, F> Cursor<K, V, TInner, R> for CursorEnter<K, V, T, R, C, TInner, F>
where
    C: Cursor<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
    F: Fn(&K, &V, &T)->TInner,
{
    type Storage = C::Storage;

    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline(always)]
    fn map_times<L: FnMut(&TInner, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &self.logic;
        self.cursor.map_times(storage, |time, diff| {
            logic(&logic2(key, val, time), diff)
        })
    }

    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }

    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorEnter<K, V, T, R, B: BatchReader<K, V, T, R>, TInner, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, TInner)>,
    cursor: B::Cursor,
    logic: Rc<F>,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, TInner, F> BatchCursorEnter<K, V, T, R, B, TInner, F> {
    fn new(cursor: B::Cursor, logic: Rc<F>) -> Self {
        BatchCursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<K, V, T, R, TInner, B: BatchReader<K, V, T, R>, F> Cursor<K, V, TInner, R> for BatchCursorEnter<K, V, T, R, B, TInner, F>
where
    T: Timestamp,
    TInner: Refines<T>+Lattice,
    F: Fn(&K, &V, &T)->TInner,
{
    type Storage = BatchEnter<K, V, T, R, B, TInner, F>;

    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline(always)]
    fn map_times<L: FnMut(&TInner, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &self.logic;
        self.cursor.map_times(&storage.batch, |time, diff| {
            logic(&logic2(key, val, time), diff)
        })
    }

    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}