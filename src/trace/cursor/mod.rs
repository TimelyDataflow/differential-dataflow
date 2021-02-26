//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

// pub mod cursor_list;
pub mod cursor_pair;
pub mod cursor_list;

pub use self::cursor_list::CursorList;

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor<K, V, T, R> {

    /// Type the cursor addresses data in.
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
    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K;
    /// A reference to the current value. Asserts if invalid.
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<&'a K> {
        if self.key_valid(storage) { Some(self.key(storage)) } else { None }
    }
    /// Returns a reference to the current value, if valid.
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<&'a V> {
        if self.val_valid(storage) { Some(self.val(storage)) } else { None }
    }

    /// Applies `logic` to each pair of time and difference. Intended for mutation of the
    /// closure's scope.
    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, logic: L);

    /// Advances the cursor to the next key.
    fn step_key(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, storage: &Self::Storage, key: &K);

    /// Advances the cursor to the next value.
    fn step_val(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, storage: &Self::Storage, val: &V);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);
}

/// Debugging and testing utilities for Cursor.
pub trait CursorDebug<K: Clone, V: Clone, T: Clone, R: Clone> : Cursor<K, V, T, R> {
    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec(&mut self, storage: &Self::Storage) -> Vec<((K, V), Vec<(T, R)>)> {
        let mut out = Vec::new();
        self.rewind_keys(storage);
        self.rewind_vals(storage);
        while self.key_valid(storage) {
            while self.val_valid(storage) {
                let mut kv_out = Vec::new();
                self.map_times(storage, |ts, r| {
                    kv_out.push((ts.clone(), r.clone()));
                });
                out.push(((self.key(storage).clone(), self.val(storage).clone()), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}

impl<C, K: Clone, V: Clone, T: Clone, R: Clone> CursorDebug<K, V, T, R> for C where C: Cursor<K, V, T, R> { }
