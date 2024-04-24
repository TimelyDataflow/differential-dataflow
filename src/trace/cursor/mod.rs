//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

use timely::progress::Timestamp;
use crate::difference::Semigroup;
use crate::lattice::Lattice;

pub mod cursor_list;

pub use self::cursor_list::CursorList;

use std::borrow::Borrow;
use std::cmp::Ordering;

/// A type that may be converted into and compared with another type.
///
/// The type must also be comparable with itself, and follow the same 
/// order as if converting instances to `T` and comparing the results.
pub trait MyTrait<'a> : Ord {
    /// Owned type into which this type can be converted.
    type Owned;
    /// Conversion from an instance of this type to the owned type.
    fn into_owned(self) -> Self::Owned;
    ///
    fn clone_onto(&self, other: &mut Self::Owned); 
    /// Indicates that `self <= other`; used for sorting.
    fn compare(&self, other: &Self::Owned) -> Ordering;
    /// `self <= other`
    fn less_equals(&self, other: &Self::Owned) -> bool {
        self.compare(other) != Ordering::Greater
    }
    /// `self == other`
    fn equals(&self, other: &Self::Owned) -> bool {
        self.compare(other) == Ordering::Equal
    }
    /// `self < other`
    fn less_than(&self, other: &Self::Owned) -> bool {
        self.compare(other) == Ordering::Less
    }
    /// Borrows an owned instance as onesself.
    fn borrow_as(other: &'a Self::Owned) -> Self; 
}

impl<'a, T: Ord+ToOwned+?Sized> MyTrait<'a> for &'a T {
    type Owned = T::Owned;
    fn into_owned(self) -> Self::Owned { self.to_owned() }
    fn clone_onto(&self, other: &mut Self::Owned) { <T as ToOwned>::clone_into(self, other) }
    fn compare(&self, other: &Self::Owned) -> Ordering { self.cmp(&other.borrow()) }
    fn borrow_as(other: &'a Self::Owned) -> Self {
        other.borrow()
    }
}

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor {

    /// Key by which updates are indexed.
    type Key<'a>: Copy + Clone + MyTrait<'a, Owned = Self::KeyOwned>;
    /// Owned version of the above.
    type KeyOwned: Ord + Clone;
    /// Values associated with keys.
    type Val<'a>: Copy + Clone + MyTrait<'a> + for<'b> PartialOrd<Self::Val<'b>>;
    /// Timestamps associated with updates
    type Time: Timestamp + Lattice + Ord + Clone;
    /// Associated update.
    type Diff: Semigroup + ?Sized;

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
    fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a>;
    /// A reference to the current value. Asserts if invalid.
    fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a>;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> {
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
    fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>);
    /// Convenience method to get access by reference to an owned key.
    fn seek_key_owned(&mut self, storage: &Self::Storage, key: &Self::KeyOwned) {
        self.seek_key(storage, MyTrait::borrow_as(key));
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);

    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec<V, F>(&mut self, from: F, storage: &Self::Storage) -> Vec<((Self::KeyOwned, V), Vec<(Self::Time, Self::Diff)>)>
    where 
        F: Fn(Self::Val<'_>) -> V,
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
                out.push(((self.key(storage).into_owned(), from(self.val(storage))), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}
