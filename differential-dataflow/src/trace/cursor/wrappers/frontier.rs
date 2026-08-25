//! Cursor for the `frontier` batch wrapper.

use crate::trace::implementations::BatchContainer;
use crate::trace::wrappers::frontier::BatchFrontier;
use crate::trace::Navigable;
use crate::trace::cursor::Cursor;

impl<B> Navigable for BatchFrontier<B, <B::Cursor as Cursor>::Time>
where
    B: Navigable,
{
    type Cursor = BatchCursorFrontier<B::Cursor>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFrontier { cursor: self.inner().cursor() }
    }
}

/// Wrapper to provide cursor to nested scope.
///
/// The wrapper's `since` and `until` frontiers stay with the batch, which applies them through
/// [`BatchFrontier::advance_time`]; the cursor holds only the cursor it forwards to.
pub struct BatchCursorFrontier<C> {
    cursor: C,
}

impl<C> Cursor for BatchCursorFrontier<C>
where
    C: Cursor,
{
    type Storage = BatchFrontier<C::Storage, C::Time>;

    type Key<'a> = C::Key<'a>;
    type ValOwn = C::ValOwn;
    type Val<'a> = C::Val<'a>;
    type KeyContainer = C::KeyContainer;
    type ValContainer = C::ValContainer;
    type DiffContainer = C::DiffContainer;
    type Diff = C::Diff;
    type DiffGat<'a> = C::DiffGat<'a>;
    type TimeContainer = Vec<C::Time>;
    type Time = <Vec<C::Time> as BatchContainer>::Owned;
    type TimeGat<'a> = <Vec<C::Time> as BatchContainer>::ReadItem<'a>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage.inner()) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage.inner()) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { self.cursor.key(storage.inner()) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { self.cursor.val(storage.inner()) }

    #[inline] fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { self.cursor.get_key(storage.inner()) }
    #[inline] fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { self.cursor.get_val(storage.inner()) }

    #[inline]
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let mut temp: C::Time = <C::Time as timely::progress::Timestamp>::minimum();
        self.cursor.map_times(storage.inner(), |time, diff| {
            C::clone_time_onto(time, &mut temp);
            if storage.advance_time(&mut temp) {
                logic(&temp, diff);
            }
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage.inner()) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) { self.cursor.seek_key(storage.inner(), key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage.inner()) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>) { self.cursor.seek_val(storage.inner(), val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage.inner()) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage.inner()) }
}
