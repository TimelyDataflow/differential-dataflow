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

/// A reference type corresponding to an owned type, supporting conversion in each direction.
///
/// This trait can be implemented by a GAT, and enables owned types to be borrowed as a GAT.
/// This trait is analogous to `ToOwned`, but not as prescriptive. Specifically, it avoids the
/// requirement that the other trait implement `Borrow`, for which a borrow must result in a
/// `&'self Borrowed`, which cannot move the lifetime into a GAT borrowed type.
pub trait IntoOwned<'a> {
    /// Owned type into which this type can be converted.
    type Owned;
    /// Conversion from an instance of this type to the owned type.
    fn into_owned(self) -> Self::Owned;
    /// Clones `self` onto an existing instance of the owned type.
    fn clone_onto(&self, other: &mut Self::Owned); 
    /// Borrows an owned instance as oneself.
    fn borrow_as(owned: &'a Self::Owned) -> Self;
}

impl<'a, T: ToOwned+?Sized> IntoOwned<'a> for &'a T {
    type Owned = T::Owned;
    fn into_owned(self) -> Self::Owned { self.to_owned() }
    fn clone_onto(&self, other: &mut Self::Owned) { <T as ToOwned>::clone_into(self, other) }
    fn borrow_as(owned: &'a Self::Owned) -> Self { owned.borrow() }
}

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor {

    /// Key by which updates are indexed.
    type Key<'a>: Copy + Clone + Ord + IntoOwned<'a, Owned = Self::KeyOwned>;
    /// Owned version of the above.
    type KeyOwned: Ord + Clone;
    /// Values associated with keys.
    type Val<'a>: Copy + Clone + Ord + IntoOwned<'a> + for<'b> PartialOrd<Self::Val<'b>>;
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
