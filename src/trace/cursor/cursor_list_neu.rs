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
	pub fn new(cursors: Vec<C>, storage: &Vec<C::Storage>) -> Self {

		let mut result = CursorList {
			_phantom: ::std::marker::PhantomData,
			cursors: cursors.into_iter().collect(),
			min_key: Vec::new(),
			min_val: Vec::new(),
		};

		result.minimize_keys(storage);
		result
	}

	// Initialize min_key with the indices of cursors with the minimum key.
	fn minimize_keys(&mut self, storage: &Vec<C::Storage>) {

		self.min_key.clear();

		// Determine the index of the cursor with minimum key.
		let mut min_key_index: Option<usize> = None;
		for (index, cursor) in self.cursors.iter().enumerate() {
			if cursor.key_valid(&storage[index]) {
				if let Some(min_index) = min_key_index {
					if cursor.key(&storage[index]).lt(self.cursors[min_index].key(&storage[min_index])) {
						min_key_index = Some(index);
					}
				}
				else {
					min_key_index = Some(index);
				}
			}
		}
		// Install each index with equal key.
		if let Some(min_index) = min_key_index {
			for (index, cursor) in self.cursors.iter().enumerate() {
				if cursor.key_valid(&storage[index]) {
					if cursor.key(&storage[index]).eq(self.cursors[min_index].key(&storage[min_index])) {
						self.min_key.push(index);
					}
				}
			}
		}

		self.minimize_vals(storage);
	}

	// Initialize min_val with the indices of minimum key cursors with the minimum value.
	fn minimize_vals(&mut self, storage: &Vec<C::Storage>) {

		self.min_val.clear();

		// Determine the index of the cursor with minimum value.
		let mut min_val_index: Option<usize> = None;
		for &index in self.min_key.iter() {
			if self.cursors[index].val_valid(&storage[index]) {
				if let Some(min_index) = min_val_index {
					if self.cursors[index].val(&storage[index]).lt(self.cursors[min_index].val(&storage[min_index])) {
						min_val_index = Some(index);
					}
				}
				else {
					min_val_index = Some(index);
				}
			}
		}
		// Install each index with equal value.
		if let Some(min_index) = min_val_index {
			for &index in self.min_key.iter() {
				if self.cursors[index].val_valid(&storage[index]) {
					if self.cursors[index].val(&storage[index]).eq(self.cursors[min_index].val(&storage[min_index])) {
						self.min_val.push(index);
					}
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
	fn key_valid(&self, _storage: &Self::Storage) -> bool { !self.min_key.is_empty() }
	fn val_valid(&self, _storage: &Self::Storage) -> bool { !self.min_val.is_empty() }

	// accessors
	fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K {
		debug_assert!(self.key_valid(storage));
		assert!(self.cursors[self.min_key[0]].key_valid(&storage[self.min_key[0]]));
		self.cursors[self.min_key[0]].key(&storage[self.min_key[0]])
	}
	fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V {
		debug_assert!(self.key_valid(storage));
		debug_assert!(self.val_valid(storage));
		assert!(self.cursors[self.min_val[0]].val_valid(&storage[self.min_val[0]]));
		self.cursors[self.min_val[0]].val(&storage[self.min_val[0]])
	}
	fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
		for &index in self.min_val.iter() {
			self.cursors[index].map_times(&storage[index], |t,d| logic(t,d));
		}
	}

	// key methods
	fn step_key(&mut self, storage: &Self::Storage) {
		for &index in self.min_key.iter() {
			self.cursors[index].step_key(&storage[index]);
		}
		self.minimize_keys(storage);
	}
	fn seek_key(&mut self, storage: &Self::Storage, key: &K) {
		for index in 0 .. self.cursors.len() {
			self.cursors[index].seek_key(&storage[index], key);
		}
		self.minimize_keys(storage);
	}

	// value methods
	fn step_val(&mut self, storage: &Self::Storage) {
		for &index in self.min_val.iter() {
			self.cursors[index].step_val(&storage[index]);
		}
		self.minimize_vals(storage);
	}
	fn seek_val(&mut self, storage: &Self::Storage, val: &V) {
		for &index in self.min_key.iter() {
			self.cursors[index].seek_val(&storage[index], val);
		}
		self.minimize_vals(storage);
	}

	// rewinding methods
	fn rewind_keys(&mut self, storage: &Self::Storage) {
		for index in 0 .. self.cursors.len() {
			self.cursors[index].rewind_keys(&storage[index]);
		}
		self.minimize_keys(storage);
	}
	fn rewind_vals(&mut self, storage: &Self::Storage) {
		for &index in self.min_key.iter() {
			self.cursors[index].rewind_vals(&storage[index]);
		}
		self.minimize_vals(storage);
	}
}