//! Wrappers to provide trace access to nested scopes.

use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};

use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
///
/// Each wrapped update is presented with a timestamp determined by `logic`.
///
/// At the same time, we require a method `prior` that can "invert" timestamps,
/// and which will be applied to compaction frontiers as they are communicated
/// back to the wrapped traces. A better explanation is pending, and until that
/// happens use this construct at your own peril!
pub struct TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
{
    trace: Tr,
    stash1: Antichain<Tr::Time>,
    stash2: Antichain<TInner>,
    logic: F,
    prior: G,
}

impl<Tr,TInner,F,G> Clone for TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader+Clone,
    F: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        TraceEnter {
            trace: self.trace.clone(),
            stash1: Antichain::new(),
            stash2: Antichain::new(),
            logic: self.logic.clone(),
            prior: self.prior.clone(),
        }
    }
}

impl<Tr, TInner, F, G> TraceReader for TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
    Tr::R: 'static,
    F: 'static,
    F: FnMut(&Tr::Key, &Tr::Val, &Tr::Time)->TInner+Clone,
    G: FnMut(&TInner)->Tr::Time+Clone+'static,
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

    fn set_logical_compaction(&mut self, frontier: AntichainRef<TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert((self.prior)(time));
        }
        self.trace.set_logical_compaction(self.stash1.borrow());
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<TInner> {
        self.stash2.clear();
        for time in self.trace.get_logical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert((self.prior)(time));
        }
        self.trace.set_physical_compaction(self.stash1.borrow());
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<TInner> {
        self.stash2.clear();
        for time in self.trace.get_physical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn cursor_through(&mut self, upper: AntichainRef<TInner>) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Tr::Key, Tr::Val, TInner, Tr::R>>::Storage)> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.cursor_through(self.stash1.borrow()).map(|(x,y)| (CursorEnter::new(x, self.logic.clone()), y))
    }
}

impl<Tr, TInner, F, G> TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: F, prior: G) -> Self {
        TraceEnter {
            trace: trace,
            stash1: Antichain::new(),
            stash2: Antichain::new(),
            logic,
            prior,
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchEnter<K, V, T, R, B, TInner, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    description: Description<TInner>,
    logic: F,
}

impl<K, V, T, R, B: Clone, TInner: Clone, F: Clone> Clone for BatchEnter<K, V, T, R, B, TInner, F> {
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
    F: FnMut(&K, &V, &T)->TInner+Clone,
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
    pub fn make_from(batch: B, logic: F) -> Self {
        let lower: Vec<_> = batch.description().lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch,
            description: Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since)),
            logic,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorEnter<K, V, T, R, C: Cursor<K, V, T, R>, TInner, F> {
    phantom: ::std::marker::PhantomData<(K, V, T, R, TInner)>,
    cursor: C,
    logic: F,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, TInner, F> CursorEnter<K, V, T, R, C, TInner, F> {
    fn new(cursor: C, logic: F) -> Self {
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
    F: FnMut(&K, &V, &T)->TInner,
{
    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&TInner, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &mut self.logic;
        self.cursor.map_times(storage, |time, diff| {
            logic(&logic2(key, val, time), diff)
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
pub struct BatchCursorEnter<K, V, T, R, B: BatchReader<K, V, T, R>, TInner, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, TInner)>,
    cursor: B::Cursor,
    logic: F,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, TInner, F> BatchCursorEnter<K, V, T, R, B, TInner, F> {
    fn new(cursor: B::Cursor, logic: F) -> Self {
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
    F: FnMut(&K, &V, &T)->TInner,
{
    type Storage = BatchEnter<K, V, T, R, B, TInner, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&TInner, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &mut self.logic;
        self.cursor.map_times(&storage.batch, |time, diff| {
            logic(&logic2(key, val, time), diff)
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
