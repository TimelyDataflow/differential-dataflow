//! A generic cursor implementation merging multiple cursors.

use super::Cursor;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and the the indices of cursors with
/// the minimum key and minimum value. It performs no clever management of these sets otherwise.
#[derive(Debug)]
pub struct CursorList<K, V, T, R, C: Cursor<K, V, T, R>> {
    _phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    cursors: Vec<C>,
    min_key: Vec<usize>,
    min_val: Vec<usize>,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> CursorList<K, V, T, R, C> where K: Ord, V: Ord {
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(cursors: Vec<C>, storage: &[C::Storage]) -> Self {

        let mut result = CursorList {
            _phantom: ::std::marker::PhantomData,
            cursors,
            min_key: Vec::new(),
            min_val: Vec::new(),
        };

        result.minimize_keys(storage);
        result
    }

    // Initialize min_key with the indices of cursors with the minimum key.
    //
    // This method scans the current keys of each cursor, and tracks the indices
    // of cursors whose key equals the minimum valid key seen so far. As it goes,
    // if it observes an improved key it clears the current list, updates the
    // minimum key, and continues.
    //
    // Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    // in a consistent state as well.
    fn minimize_keys(&mut self, storage: &[C::Storage]) {

        self.min_key.clear();

        // Determine the index of the cursor with minimum key.
        let mut min_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            let key = cursor.get_key(&storage[index]);
            if key.is_some() {
                if min_key_opt.is_none() || key.lt(&min_key_opt) {
                    min_key_opt = key;
                    self.min_key.clear();
                }
                if key.eq(&min_key_opt) {
                    self.min_key.push(index);
                }
            }
        }

        self.minimize_vals(storage);
    }

    // Initialize min_val with the indices of minimum key cursors with the minimum value.
    //
    // This method scans the current values of cursor with minimum keys, and tracks the
    // indices of cursors whose value equals the minimum valid value seen so far. As it
    // goes, if it observes an improved value it clears the current list, updates the minimum
    // value, and continues.
    fn minimize_vals(&mut self, storage: &[C::Storage]) {

        self.min_val.clear();

        // Determine the index of the cursor with minimum value.
        let mut min_val: Option<&V> = None;
        for &index in self.min_key.iter() {
            let val = self.cursors[index].get_val(&storage[index]);
            if val.is_some() {
                if min_val.is_none() || val.lt(&min_val) {
                    min_val = val;
                    self.min_val.clear();
                }
                if val.eq(&min_val) {
                    self.min_val.push(index);
                }
            }
        }
    }
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> Cursor<K, V, T, R> for CursorList<K, V, T, R, C>
where
    K: Ord,
    V: Ord {

    type Storage = Vec<C::Storage>;

    // validation methods
    #[inline(always)]
    fn key_valid(&self, _storage: &Self::Storage) -> bool { !self.min_key.is_empty() }
    #[inline(always)]
    fn val_valid(&self, _storage: &Self::Storage) -> bool { !self.min_val.is_empty() }

    // accessors
    #[inline(always)]
    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.cursors[self.min_key[0]].key_valid(&storage[self.min_key[0]]));
        self.cursors[self.min_key[0]].key(&storage[self.min_key[0]])
    }
    #[inline(always)]
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.val_valid(storage));
        debug_assert!(self.cursors[self.min_val[0]].val_valid(&storage[self.min_val[0]]));
        self.cursors[self.min_val[0]].val(&storage[self.min_val[0]])
    }
    #[inline(always)]
    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        for &index in self.min_val.iter() {
            self.cursors[index].map_times(&storage[index], |t,d| logic(t,d));
        }
    }

    // key methods
    #[inline(always)]
    fn step_key(&mut self, storage: &Self::Storage) {
        for &index in self.min_key.iter() {
            self.cursors[index].step_key(&storage[index]);
        }
        self.minimize_keys(storage);
    }
    #[inline(always)]
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) {
        for index in 0 .. self.cursors.len() {
            self.cursors[index].seek_key(&storage[index], key);
        }
        self.minimize_keys(storage);
    }

    // value methods
    #[inline(always)]
    fn step_val(&mut self, storage: &Self::Storage) {
        for &index in self.min_val.iter() {
            self.cursors[index].step_val(&storage[index]);
        }
        self.minimize_vals(storage);
    }
    #[inline(always)]
    fn seek_val(&mut self, storage: &Self::Storage, val: &V) {
        for &index in self.min_key.iter() {
            self.cursors[index].seek_val(&storage[index], val);
        }
        self.minimize_vals(storage);
    }

    // rewinding methods
    #[inline(always)]
    fn rewind_keys(&mut self, storage: &Self::Storage) {
        for index in 0 .. self.cursors.len() {
            self.cursors[index].rewind_keys(&storage[index]);
        }
        self.minimize_keys(storage);
    }
    #[inline(always)]
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        for &index in self.min_key.iter() {
            self.cursors[index].rewind_vals(&storage[index]);
        }
        self.minimize_vals(storage);
    }
}