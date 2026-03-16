//! A generic cursor implementation merging multiple cursors.

use super::Cursor;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and the the indices of cursors with
/// the minimum key and minimum value. It performs no clever management of these sets otherwise.
#[derive(Debug)]
pub struct CursorList<C> {
    cursors: Vec<C>,
    min_key: Vec<usize>,
    min_val: Vec<usize>,
}

impl<C: Cursor> CursorList<C> {
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(cursors: Vec<C>, storage: &[C::Storage]) -> Self  {
        let mut result = CursorList {
            cursors,
            min_key: Vec::new(),
            min_val: Vec::new(),
        };

        result.minimize_keys(storage);
        result
    }

    fn minimize_keys(&mut self, storage: &[C::Storage]) {
        self.min_key.clear();

        let mut iter = self.cursors.iter().enumerate().flat_map(|(idx, cur)| cur.get_key(&storage[idx]).map(|key| (idx, key)));
        if let Some((idx, key)) = iter.next() {
            let mut min_key = key;
            self.min_key.push(idx);
            for (idx, key) in iter {
                match key.cmp(&min_key) {
                    std::cmp::Ordering::Less => {
                        self.min_key.clear();
                        self.min_key.push(idx);
                        min_key = key;
                    }
                    std::cmp::Ordering::Equal => {
                        self.min_key.push(idx);
                    }
                    std::cmp::Ordering::Greater => { }
                }
            }
        }

        self.minimize_vals(storage);
    }

    fn minimize_vals(&mut self, storage: &[C::Storage]) {
        self.min_val.clear();

        let mut iter = self.min_key.iter().cloned().flat_map(|idx| self.cursors[idx].get_val(&storage[idx]).map(|val| (idx, val)));
        if let Some((idx, val)) = iter.next() {
            let mut min_val = val;
            self.min_val.push(idx);
            for (idx, val) in iter {
                match val.cmp(&min_val) {
                    std::cmp::Ordering::Less => {
                        self.min_val.clear();
                        self.min_val.push(idx);
                        min_val = val;
                    }
                    std::cmp::Ordering::Equal => {
                        self.min_val.push(idx);
                    }
                    std::cmp::Ordering::Greater => { }
                }
            }
        }
    }
}

impl<C: Cursor> Cursor for CursorList<C> {

    type Key = C::Key;
    type Val = C::Val;
    type Time = C::Time;
    type Diff = C::Diff;
    type Storage = Vec<C::Storage>;

    #[inline]
    fn key_valid(&self, _storage: &Vec<C::Storage>) -> bool { !self.min_key.is_empty() }
    #[inline]
    fn val_valid(&self, _storage: &Vec<C::Storage>) -> bool { !self.min_val.is_empty() }

    #[inline]
    fn key<'a>(&self, storage: &'a Vec<C::Storage>) -> columnar::Ref<'a, Self::Key> {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.cursors[self.min_key[0]].key_valid(&storage[self.min_key[0]]));
        self.cursors[self.min_key[0]].key(&storage[self.min_key[0]])
    }
    #[inline]
    fn val<'a>(&self, storage: &'a Vec<C::Storage>) -> columnar::Ref<'a, Self::Val> {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.val_valid(storage));
        debug_assert!(self.cursors[self.min_val[0]].val_valid(&storage[self.min_val[0]]));
        self.cursors[self.min_val[0]].val(&storage[self.min_val[0]])
    }
    #[inline]
    fn get_key<'a>(&self, storage: &'a Vec<C::Storage>) -> Option<columnar::Ref<'a, Self::Key>> {
        self.min_key.get(0).map(|idx| self.cursors[*idx].key(&storage[*idx]))
    }
    #[inline]
    fn get_val<'a>(&self, storage: &'a Vec<C::Storage>) -> Option<columnar::Ref<'a, Self::Val>> {
        self.min_val.get(0).map(|idx| self.cursors[*idx].val(&storage[*idx]))
    }

    #[inline]
    fn map_times<L: FnMut(columnar::Ref<'_, Self::Time>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &Vec<C::Storage>, mut logic: L) {
        for &index in self.min_val.iter() {
            self.cursors[index].map_times(&storage[index], |t,d| logic(t,d));
        }
    }

    #[inline]
    fn step_key(&mut self, storage: &Vec<C::Storage>) {
        for &index in self.min_key.iter() {
            self.cursors[index].step_key(&storage[index]);
        }
        self.minimize_keys(storage);
    }
    #[inline]
    fn seek_key(&mut self, storage: &Vec<C::Storage>, key: columnar::Ref<'_, Self::Key>) {
        for (cursor, storage) in self.cursors.iter_mut().zip(storage) {
            cursor.seek_key(storage, key);
        }
        self.minimize_keys(storage);
    }

    #[inline]
    fn step_val(&mut self, storage: &Vec<C::Storage>) {
        for &index in self.min_val.iter() {
            self.cursors[index].step_val(&storage[index]);
        }
        self.minimize_vals(storage);
    }
    #[inline]
    fn seek_val(&mut self, storage: &Vec<C::Storage>, val: columnar::Ref<'_, Self::Val>) {
        for (cursor, storage) in self.cursors.iter_mut().zip(storage) {
            cursor.seek_val(storage, val);
        }
        self.minimize_vals(storage);
    }

    #[inline]
    fn rewind_keys(&mut self, storage: &Vec<C::Storage>) {
        for (cursor, storage) in self.cursors.iter_mut().zip(storage) {
            cursor.rewind_keys(storage);
        }
        self.minimize_keys(storage);
    }
    #[inline]
    fn rewind_vals(&mut self, storage: &Vec<C::Storage>) {
        for &index in self.min_key.iter() {
            self.cursors[index].rewind_vals(&storage[index]);
        }
        self.minimize_vals(storage);
    }
}
