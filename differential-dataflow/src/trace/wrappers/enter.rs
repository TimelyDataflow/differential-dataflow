//! Wrappers to provide trace access to nested scopes.

use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, frontier::AntichainRef};

use crate::lattice::Lattice;
use crate::trace::{TraceReader, BatchReader, Cursor, Description};

/// Wrapper to provide trace to nested scope.
pub struct TraceEnter<Tr: TraceReader, TInner> {
    trace: Tr,
    stash1: Antichain<Tr::Time>,
    stash2: Antichain<TInner>,
}

impl<Tr: TraceReader + Clone, TInner: Clone> Clone for TraceEnter<Tr, TInner> {
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
    Tr: TraceReader<Batch: Clone>,
    TInner: crate::trace::implementations::Data + Refines<Tr::Time>+Lattice,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = TInner;
    type Diff = Tr::Diff;

    type Batch = BatchEnter<Tr::Batch, TInner>;
    type Storage = Tr::Storage;
    type Cursor = CursorEnter<Tr::Cursor, TInner>;

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone()));
        })
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<'_, TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.set_logical_compaction(self.stash1.borrow());
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, TInner> {
        self.stash2.clear();
        for time in self.trace.get_logical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.set_physical_compaction(self.stash1.borrow());
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, TInner> {
        self.stash2.clear();
        for time in self.trace.get_physical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn cursor_through(&mut self, upper: AntichainRef<TInner>) -> Option<(Self::Cursor, Self::Storage)> {
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
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr) -> Self {
        TraceEnter {
            trace,
            stash1: Antichain::new(),
            stash2: Antichain::new(),
        }
    }
}

/// Wrapper to provide batch to nested scope.
pub struct BatchEnter<B, TInner> {
    batch: B,
    description: Description<TInner>,
}

impl<B: Clone, TInner: Clone> Clone for BatchEnter<B, TInner> {
    fn clone(&self) -> Self {
        BatchEnter {
            batch: self.batch.clone(),
            description: self.description.clone(),
        }
    }
}

impl<B, TInner> BatchReader for BatchEnter<B, TInner>
where
    B: BatchReader,
    TInner: crate::trace::implementations::Data + Refines<B::Time>+Lattice,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = TInner;
    type Diff = B::Diff;

    type Cursor = BatchCursorEnter<B::Cursor, TInner>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.batch.cursor())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
}

impl<B, TInner> BatchEnter<B, TInner>
where
    B: BatchReader,
    TInner: Refines<B::Time>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B) -> Self {
        let lower: Vec<_> = batch.description().lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            batch,
            description: Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since))
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorEnter<C, TInner> {
    phantom: ::std::marker::PhantomData<TInner>,
    cursor: C,
}

impl<C, TInner> CursorEnter<C, TInner> {
    fn new(cursor: C) -> Self {
        CursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
        }
    }
}

impl<C, TInner> Cursor for CursorEnter<C, TInner>
where
    C: Cursor,
    TInner: crate::trace::implementations::Data + Refines<C::Time>+Lattice,
{
    type Key = C::Key;
    type Val = C::Val;
    type Time = TInner;
    type Diff = C::Diff;
    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Key> { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Val> { self.cursor.val(storage) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Key>> { self.cursor.get_key(storage) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>> { self.cursor.get_val(storage) }

    #[inline]
    fn map_times<L: FnMut(columnar::Ref<'_, TInner>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        // TODO: The enter cursor creates new TInner values by converting from the outer time.
        // This requires TInner to be stored somewhere so we can return a columnar::Ref to it.
        // For now this is a compilation placeholder that will need a design solution.
        // One approach: store a TInner container in the cursor and push/borrow from it.
        self.cursor.map_times(storage, |time, diff| {
            let inner_time = TInner::to_inner(<C::Time as columnar::Columnar>::into_owned(time));
            // FIXME: We need to return a columnar::Ref<'_, TInner> but we have an owned TInner.
            // This won't compile until we solve the owned→ref problem for synthesized times.
            let _ = (inner_time, diff);
            todo!("enter cursor map_times needs design work for columnar refs")
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: columnar::Ref<'_, Self::Key>) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: columnar::Ref<'_, Self::Val>) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}

/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorEnter<C, TInner> {
    phantom: ::std::marker::PhantomData<TInner>,
    cursor: C,
}

impl<C, TInner> BatchCursorEnter<C, TInner> {
    fn new(cursor: C) -> Self {
        BatchCursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
        }
    }
}

impl<TInner, C: Cursor> Cursor for BatchCursorEnter<C, TInner>
where
    TInner: crate::trace::implementations::Data + Refines<C::Time>+Lattice,
{
    type Key = C::Key;
    type Val = C::Val;
    type Time = TInner;
    type Diff = C::Diff;
    type Storage = BatchEnter<C::Storage, TInner>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Key> { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Val> { self.cursor.val(&storage.batch) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Key>> { self.cursor.get_key(&storage.batch) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>> { self.cursor.get_val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(columnar::Ref<'_, TInner>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        // Same TODO as CursorEnter::map_times above.
        self.cursor.map_times(&storage.batch, |time, diff| {
            let inner_time = TInner::to_inner(<C::Time as columnar::Columnar>::into_owned(time));
            let _ = (inner_time, diff);
            todo!("enter cursor map_times needs design work for columnar refs")
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: columnar::Ref<'_, Self::Key>) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: columnar::Ref<'_, Self::Val>) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
