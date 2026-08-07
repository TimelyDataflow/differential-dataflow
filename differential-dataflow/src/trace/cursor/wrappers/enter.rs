//! Cursor for the `enter` batch wrapper.

use timely::progress::timestamp::Refines;

use crate::lattice::Lattice;
use crate::trace::implementations::BatchContainer;
use crate::trace::wrappers::enter::BatchEnter;
use crate::trace::{BatchReader, Navigable};
use crate::trace::cursor::Cursor;

impl<B, TInner> Navigable for BatchEnter<B, TInner>
where
    B: BatchReader + Navigable,
    TInner: Refines<B::Time>+Lattice,
    TInner: Refines<<B::Cursor as Cursor>::Time>,
{
    type Cursor = BatchCursorEnter<B::Cursor, TInner>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.inner().cursor())
    }
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
    TInner: Refines<C::Time>+Lattice,
{
    type Storage = BatchEnter<C::Storage, TInner>;

    type Key<'a> = C::Key<'a>;
    type ValOwn = C::ValOwn;
    type Val<'a> = C::Val<'a>;
    type KeyContainer = C::KeyContainer;
    type ValContainer = C::ValContainer;
    type DiffContainer = C::DiffContainer;
    type Diff = C::Diff;
    type DiffGat<'a> = C::DiffGat<'a>;
    type TimeContainer = Vec<TInner>;
    type Time = <Vec<TInner> as BatchContainer>::Owned;
    type TimeGat<'a> = <Vec<TInner> as BatchContainer>::ReadItem<'a>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage.inner()) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage.inner()) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage.inner()) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage.inner()) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(storage.inner()) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(storage.inner()) }

    #[inline]
    fn map_times<L: FnMut(&TInner, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.map_times(storage.inner(), |time, diff| {
            logic(&TInner::to_inner(C::owned_time(time)), diff)
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage.inner()) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(storage.inner(), key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage.inner()) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(storage.inner(), val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage.inner()) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage.inner()) }
}
