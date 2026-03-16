//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

pub mod cursor_list;

pub use self::cursor_list::CursorList;

use crate::trace::implementations::Data;

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
///
/// Types are columnar: owned types implement `Data` (which includes `Columnar`),
/// and all reference types are the corresponding `columnar::Ref<'a, T>`.
pub trait Cursor {

    /// The owned key type.
    type Key: Data;
    /// The owned val type.
    type Val: Data;
    /// The owned time type.
    type Time: Data + crate::lattice::Lattice + timely::progress::Timestamp;
    /// The owned diff type.
    type Diff: Data + crate::difference::Semigroup;

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
    fn key<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Key>;
    /// A reference to the current value. Asserts if invalid.
    fn val<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Val>;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Key>>;
    /// Returns a reference to the current value, if valid.
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>>;

    /// Applies `logic` to each pair of time and difference. Intended for mutation of the
    /// closure's scope.
    fn map_times<L: FnMut(columnar::Ref<'_, Self::Time>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &Self::Storage, logic: L);

    /// Advances the cursor to the next key.
    fn step_key(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, storage: &Self::Storage, key: columnar::Ref<'_, Self::Key>);

    /// Advances the cursor to the next value.
    fn step_val(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, storage: &Self::Storage, val: columnar::Ref<'_, Self::Val>);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);

    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec<K, IK, V, IV>(&mut self, storage: &Self::Storage, into_key: IK, into_val: IV) -> Vec<((K, V), Vec<(Self::Time, Self::Diff)>)>
    where
        IK: for<'a> Fn(columnar::Ref<'a, Self::Key>) -> K,
        IV: for<'a> Fn(columnar::Ref<'a, Self::Val>) -> V,
    {
        let mut out = Vec::new();
        self.rewind_keys(storage);
        while let Some(key) = self.get_key(storage) {
            self.rewind_vals(storage);
            while let Some(val) = self.get_val(storage) {
                let mut kv_out = Vec::new();
                self.map_times(storage, |ts, r| {
                    kv_out.push((
                        <Self::Time as columnar::Columnar>::into_owned(ts),
                        <Self::Diff as columnar::Columnar>::into_owned(r),
                    ));
                });
                out.push(((into_key(key), into_val(val)), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}
