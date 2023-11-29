//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

// pub mod cursor_list;
// pub mod cursor_pair;
pub mod cursor_list;

pub use self::cursor_list::CursorList;

use std::borrow::Borrow;
/// A type that may be converted into and compared with another type.
///
/// The type must also be comparable with itself, and follow the same 
/// order as if converting instances to `T` and comparing the results.
pub trait MyTrait : Ord {
    /// Owned type into which this type can be converted.
    type Owned;
    /// Conversion from an instance of this type to the owned type.
    fn to_owned(self) -> Self::Owned;
    /// Indicates that `self <= other`; used for sorting.
    fn less_equal(&self, other: &Self::Owned) -> bool;
}

impl<'a, T: Ord+ToOwned+?Sized> MyTrait for &'a T {
    type Owned = T::Owned;
    fn to_owned(self) -> Self::Owned { self.to_owned() }
    fn less_equal(&self, other: &Self::Owned) -> bool { self.le(&other.borrow()) }
}


/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor {

    /// Key by which updates are indexed.
    type Key: ?Sized;
    /// Values associated with keys.
    type Val<'a>: Copy + Clone + MyTrait<Owned = Self::ValOwned>;
    /// Owned version of the above.
    type ValOwned: Ord + Clone;
    /// Timestamps associated with updates
    type Time;
    /// Associated update.
    type Diff: ?Sized;

    /// Storage required by the cursor.
    type Storage;

    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self, storage: &Self::Storage) -> bool;
    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values for this key.
    fn val_valid(&self, storage: &Self::Storage) -> bool;

    /// A reference to the current key. Asserts if invalid.
    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key;
    /// A reference to the current value. Asserts if invalid.
    fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a>;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<&'a Self::Key> {
        if self.key_valid(storage) { Some(self.key(storage)) } else { None }
    }
    /// Returns a reference to the current value, if valid.
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> {
        if self.val_valid(storage) { Some(self.val(storage)) } else { None }
    }

    /// Applies `logic` to each pair of time and difference. Intended for mutation of the
    /// closure's scope.
    fn map_times<L: FnMut(&Self::Time, &Self::Diff)>(&mut self, storage: &Self::Storage, logic: L);

    /// Advances the cursor to the next key.
    fn step_key(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key);

    /// Advances the cursor to the next value.
    fn step_val(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified value.
    fn seek_val<'a>(&mut self, storage: &Self::Storage, val: Self::Val<'a>);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);

    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec(&mut self, storage: &Self::Storage) -> Vec<((Self::Key, Self::ValOwned), Vec<(Self::Time, Self::Diff)>)>
    where
        Self::Key: Clone,
        Self::Time: Clone,
        Self::Diff: Clone,
    {
        let mut out = Vec::new();
        self.rewind_keys(storage);
        self.rewind_vals(storage);
        while self.key_valid(storage) {
            while self.val_valid(storage) {
                let mut kv_out = Vec::new();
                self.map_times(storage, |ts, r| {
                    kv_out.push((ts.clone(), r.clone()));
                });
                out.push(((self.key(storage).clone(), self.val(storage).to_owned()), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}
