//! A generic cursor implementation merging multiple cursors.

use super::Cursor;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` keeps its cursor ordered by `key`, and cursors with the smallest key are then ordered by `val`.
///
/// At any point, the current elements (keys, vals) are determined by the number of leading equivalent keys and 
/// equivalent values in `self.cursors`. Although they are implicit in `self.cursors`, We maintain these values 
/// explicitly, in `self.equiv_keys` and `self.equiv_vals`. We also track the number of valid keys and valid 
/// values, to avoid continually re-considering cursors in invalid states.
#[derive(Debug)]
pub struct CursorList<K, V, T, R, C: Cursor<K, V, T, R>> {
	_phantom: ::std::marker::PhantomData<(K, V, T, R)>,
	cursors: Vec<(C, usize)>,	// ordered by valid keys and valid values.
	equiv_keys: usize,	// cursors[..equiv_keys] all have the same key.
	equiv_vals: usize,	// cursors[..equiv_vals] all have the same key and value.
	valid_keys: usize,	// cursors[..valid_keys] all have valid_key() true.
	valid_vals: usize,	// cursors[..valid_vals] all have valid_val() true.
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> CursorList<K, V, T, R, C> where K: Ord, V: Ord {
	/// Creates a new cursor list from pre-existing cursors.
	pub fn new(cursors: Vec<C>, storage: &Vec<C::Storage>) -> Self {
		let cursors_len = cursors.len();
		let mut result = CursorList {
			_phantom: ::std::marker::PhantomData,
			cursors: cursors.into_iter().enumerate().map(|(i,c)| (c,i)).collect(),
			equiv_keys: cursors_len,
			equiv_vals: 0,
			valid_keys: cursors_len,
			valid_vals: 0,
		};

		result.tidy_keys(storage, cursors_len);
		result
	}

	/// Re-sorts the first `valid_keys` cursors under the assumption that cursors[prefix .. valid_keys] are ordered by key.
	fn tidy_keys(&mut self, storage: &Vec<C::Storage>, prefix: usize) {

		// 0. invalidate this, because.
		self.equiv_keys = 0;

		// 1. move invalid cursors to `self.valid_vals` and decrement appropriately.
		let mut dirty = 0; 
		for index in 0 .. prefix {
			if self.cursors[index].0.key_valid(&storage[self.cursors[index].1]) {
				self.cursors.swap(dirty, index);
				dirty += 1;
			}
		}
		if prefix - dirty > 0 {	// must translate valid keys down
			for index in prefix .. self.valid_keys {
				self.cursors.swap(index, index - (prefix - dirty));
			}
		}
		self.valid_keys -= prefix - dirty;

		// 2. If disorderly values remain, .. 
		if dirty > 0 {
			// a. identify the largest value among them.
			let mut max_index = 0;
			for index in 1 .. dirty {
				if self.cursors[index].0.key(&storage[self.cursors[index].1]) > self.cursors[max_index].0.key(&storage[self.cursors[max_index].1]) {
					max_index = index;
				}
			}
			// b. determine how many of the ordered values we must involve.
			let mut beyond = dirty;
			while beyond < self.valid_keys && self.cursors[beyond].0.key(&storage[self.cursors[beyond].1]) < self.cursors[max_index].0.key(&storage[self.cursors[max_index].1]) {
				beyond += 1;
			}
			// c. sort those cursors.
			self.cursors[.. beyond].sort_by(|x,y| x.0.key(&storage[x.1]).cmp(y.0.key(&storage[y.1])));
			// println!("{:?} / {:?}", beyond, self.cursors.len());
		}
		
		// 3. count equivalent keys (if any are valid)
		if self.valid_keys > 0 {
			self.equiv_keys = 1;
			while self.equiv_keys < self.valid_keys && self.cursors[self.equiv_keys].0.key(&storage[self.cursors[self.equiv_keys].1]) == self.cursors[0].0.key(&storage[self.cursors[0].1]) {
				self.equiv_keys += 1;
			}
		}

		// 4. order equivalent keys by value.
		let to_tidy = self.equiv_keys;
		self.valid_vals = self.equiv_keys;	// <-- presumably true? 
		self.tidy_vals(storage, to_tidy);
	}
	/// Re-sorts the first `valid_vals` cursors under the assumption that cursors[prefix .. valid_vals] are ordered by value.
	fn tidy_vals(&mut self, storage: &Vec<C::Storage>, prefix: usize) {

		// 0. invalidate this, because.
		self.equiv_vals = 0;

		// 1. move invalid cursors to `self.valid_vals` and decrement appropriately.
		let mut dirty = 0; 
		for index in 0 .. prefix {
			if self.cursors[index].0.val_valid(&storage[self.cursors[index].1]) {
				self.cursors.swap(dirty, index);
				dirty += 1;
			}
		}
		if prefix - dirty > 0 {
			for index in prefix .. self.valid_vals {
				self.cursors.swap(index, index - (prefix - dirty));
			}
		}
		self.valid_vals -= prefix - dirty;

		// 2. If disorderly values remain, .. 
		if dirty > 0 {
			// a. identify the largest value among them.
			let mut max_index = 0;
			for index in 1 .. dirty {
				if self.cursors[index].0.val(&storage[self.cursors[index].1]) > self.cursors[max_index].0.val(&storage[self.cursors[max_index].1]) {
					max_index = index;
				}
			}
			// b. determine how many of the ordered values we must involve.
			let mut beyond = dirty;
			while beyond < self.valid_vals && self.cursors[beyond].0.val(&storage[self.cursors[beyond].1]) < self.cursors[max_index].0.val(&storage[self.cursors[max_index].1]) {
				beyond += 1;
			}
			// c. sort those cursors.
			self.cursors[.. beyond].sort_by(|x,y| x.0.val(&storage[x.1]).cmp(y.0.val(&storage[y.1])));
		}

		// 3. count equivalent values
		if self.valid_vals > 0 {
			self.equiv_vals = 1;
			while self.equiv_vals < self.valid_vals && self.cursors[self.equiv_vals].0.val(&storage[self.cursors[self.equiv_vals].1]) == self.cursors[0].0.val(&storage[self.cursors[0].1]) {
				self.equiv_vals += 1;
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
	fn key_valid(&self, _storage: &Self::Storage) -> bool { self.valid_keys > 0 }
	fn val_valid(&self, _storage: &Self::Storage) -> bool { self.valid_vals > 0 }

	// accessors 
	fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { 
		debug_assert!(self.key_valid(storage));
		self.cursors[0].0.key(&storage[self.cursors[0].1])
	}
	fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { 
		debug_assert!(self.key_valid(storage));
		debug_assert!(self.val_valid(storage));
		self.cursors[0].0.val(&storage[self.cursors[0].1]) 
	}
	fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
		for cursor in &mut self.cursors[.. self.equiv_vals] {
			cursor.0.map_times(&storage[cursor.1], |t,d| logic(t,d));
		}
	}

	// key methods
	fn step_key(&mut self, storage: &Self::Storage) {
		for cursor in &mut self.cursors[.. self.equiv_keys] {
			cursor.0.step_key(&storage[cursor.1]);
		}
		let to_tidy = self.equiv_keys;
		self.tidy_keys(storage, to_tidy);
	}
	fn seek_key(&mut self, storage: &Self::Storage, key: &K) {
		let mut index = 0;
		while index < self.valid_keys && self.cursors[index].0.key(&storage[self.cursors[index].1]) < &key {
			let temp = self.cursors[index].1;
			self.cursors[index].0.seek_key(&storage[temp], key);
			index += 1;
		}
		self.tidy_keys(storage, index);
	}
	
	// value methods
	fn step_val(&mut self, storage: &Self::Storage) {
		for cursor in &mut self.cursors[.. self.equiv_vals] {
			cursor.0.step_val(&storage[cursor.1]);
		}
		let to_tidy = self.equiv_vals;
		self.tidy_vals(storage, to_tidy);
	}
	fn seek_val(&mut self, storage: &Self::Storage, val: &V) {
		let mut index = 0;
		while index < self.valid_vals && self.cursors[index].0.val(&storage[self.cursors[index].1]) < &val {
			let temp = self.cursors[index].1;
			self.cursors[index].0.seek_val(&storage[temp], val);
			index += 1;
		}
		self.tidy_vals(storage, index);
	}

	// rewinding methods
	fn rewind_keys(&mut self, storage: &Self::Storage) { 
		let len = self.cursors.len();
		self.valid_keys = len;
		for cursor in &mut self.cursors[.. len] {
			cursor.0.rewind_keys(&storage[cursor.1]);
		}
		self.tidy_keys(storage, len);
	}
	fn rewind_vals(&mut self, storage: &Self::Storage) { 
		let len = self.equiv_keys;
		self.valid_vals = len;
		for cursor in &mut self.cursors[.. len] {
			cursor.0.rewind_vals(&storage[cursor.1]);
		}
		self.tidy_vals(storage, len);
	}
}