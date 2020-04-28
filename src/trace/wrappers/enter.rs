//! Wrappers to provide trace access to nested scopes.

// use timely::progress::nested::product::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};

use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
pub struct TraceEnter<Tr, TInner>
where
    Tr: TraceReader,
{
    trace: Tr,
    stash1: Antichain<Tr::Time>,
    stash2: Antichain<TInner>,
}

impl<Tr,TInner> Clone for TraceEnter<Tr, TInner>
where
    Tr: TraceReader+Clone,
{
    fn clone(&self) -> Self {
        TraceEnter {
            trace: self.trace.clone(),
            stash1: Antichain::new(),
            stash2: Antichain::new(),
        }
    }
}

impl<Tr, TInner> TraceReader for TraceEnter<Tr, TInner>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp,
    Tr::R: 'static,
    TInner: Refines<Tr::Time>+Lattice,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = TInner;
    type R = Tr::R;

    type Batch = BatchEnter<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Batch, TInner>;
    type Cursor = CursorEnter<Tr::Key, Tr::Val, Tr::Time, Tr::R, Tr::Cursor, TInner>;

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, mut f: F) {
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone()));
        })
    }

    fn advance_by(&mut self, frontier: AntichainRef<TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.advance_by(self.stash1.borrow());
    }
    fn advance_frontier(&mut self) -> AntichainRef<TInner> {
        self.stash2.clear();
        for time in self.trace.advance_frontier().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn distinguish_since(&mut self, frontier: AntichainRef<TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.distinguish_since(self.stash1.borrow());
    }
    fn distinguish_frontier(&mut self) -> AntichainRef<TInner> {
        self.stash2.clear();
        for time in self.trace.distinguish_frontier().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn cursor_through(&mut self, upper: AntichainRef<TInner>) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, TInner, Tr::R>>::Storage)> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.cursor_through(self.stash1.borrow()).map(|(x,y)| (CursorEnter::new(x), y))
    }
}

impl<Tr, TInner> TraceEnter<Tr, TInner>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr) -> Self {
        TraceEnter {
            trace: trace,
            stash1: Antichain::new(),
            stash2: Antichain::new(),
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchEnter<K, V, T, R, B, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    description: Description<TInner>,
}

impl<K, V, T, R, B: Clone, TInner: Clone> Clone for BatchEnter<K, V, T, R, B, TInner> {
    fn clone(&self) -> Self {
        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            description: self.description.clone(),
        }
    }
}

impl<K, V, T, R, B, TInner> BatchReader<K, V, TInner, R> for BatchEnter<K, V, T, R, B, TInner>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    type Cursor = BatchCursorEnter<K, V, T, R, B, TInner>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.batch.cursor())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
}

impl<K, V, T, R, B, TInner> BatchEnter<K, V, T, R, B, TInner>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B) -> Self {
        let lower: Vec<_> = batch.description().lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch: batch,
            description: Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since))
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorEnter<K, V, T, R, C: Cursor<K, V, T, R>, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, T, R, TInner)>,
    cursor: C,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, TInner> CursorEnter<K, V, T, R, C, TInner> {
    fn new(cursor: C) -> Self {
        CursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
        }
    }
}

impl<K, V, T, R, C, TInner> Cursor<K, V, TInner, R> for CursorEnter<K, V, T, R, C, TInner>
where
    C: Cursor<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&TInner, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.map_times(storage, |time, diff| {
            logic(&TInner::to_inner(time.clone()), diff)
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
pub struct BatchCursorEnter<K, V, T, R, B: BatchReader<K, V, T, R>, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, R, TInner)>,
    cursor: B::Cursor,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, TInner> BatchCursorEnter<K, V, T, R, B, TInner> {
    fn new(cursor: B::Cursor) -> Self {
        BatchCursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
        }
    }
}

impl<K, V, T, R, TInner, B: BatchReader<K, V, T, R>> Cursor<K, V, TInner, R> for BatchCursorEnter<K, V, T, R, B, TInner>
where
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    type Storage = BatchEnter<K, V, T, R, B, TInner>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&TInner, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.map_times(&storage.batch, |time, diff| {
            logic(&TInner::to_inner(time.clone()), diff)
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
