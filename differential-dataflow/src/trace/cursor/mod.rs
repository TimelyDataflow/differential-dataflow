//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

pub mod cursor_list;

pub use self::cursor_list::CursorList;

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor {

    /// The key type.
    type Key: crate::Data;
    /// The value type.
    type Val: crate::Data;
    /// The time type.
    type Time: crate::lattice::Lattice + timely::progress::Timestamp + crate::Data;
    /// The diff type.
    type Diff: crate::difference::Semigroup + crate::Data;

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
    fn val<'a>(&self, storage: &'a Self::Storage) -> columnar::Ref<'a, Self::Val>;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<&'a Self::Key>;
    /// Returns a reference to the current value, if valid.
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>>;

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
    fn seek_val(&mut self, storage: &Self::Storage, val: columnar::Ref<'_, Self::Val>);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);

    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec<K, IK, V, IV>(&mut self, storage: &Self::Storage, into_key: IK, into_val: IV) -> Vec<((K, V), Vec<(Self::Time, Self::Diff)>)>
    where
        IK: for<'a> Fn(&'a Self::Key) -> K,
        IV: for<'a> Fn(columnar::Ref<'a, Self::Val>) -> V,
    {
        let mut out = Vec::new();
        self.rewind_keys(storage);
        while let Some(key) = self.get_key(storage) {
            self.rewind_vals(storage);
            while let Some(val) = self.get_val(storage) {
                let mut kv_out = Vec::new();
                self.map_times(storage, |ts, r| {
                    kv_out.push((ts.clone(), r.clone()));
                });
                out.push(((into_key(key), into_val(val)), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}
