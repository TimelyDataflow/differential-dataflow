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
pub struct CursorList<K, V, T, C: Cursor<K, V, T>> {
	_phantom: ::std::marker::PhantomData<(K, V, T)>,
	cursors: Vec<C>,	// ordered by valid keys and valid values.
	equiv_keys: usize,	// cursors[..equiv_keys] all have the same key.
	equiv_vals: usize,	// cursors[..equiv_vals] all have the same key and value.
	valid_keys: usize,	// cursors[..valid_keys] all have valid_key() true.
	valid_vals: usize,	// cursors[..valid_vals] all have valid_val() true.
}

impl<K, V, T, C: Cursor<K, V, T>> CursorList<K, V, T, C> where K: Ord, V: Ord {
	/// Creates a new cursor list from pre-existing cursors.
	pub fn new(cursors: Vec<C>) -> Self {
		let cursors_len = cursors.len();
		let mut result = CursorList {
			_phantom: ::std::marker::PhantomData,
			cursors: cursors,
			equiv_keys: cursors_len,
			equiv_vals: 0,
			valid_keys: cursors_len,
			valid_vals: 0,
		};

		result.tidy_keys(cursors_len);
		result
	}

	/// Re-sorts the first `valid_keys` cursors under the assumption that cursors[prefix .. valid_keys] are ordered by key.
	fn tidy_keys(&mut self, prefix: usize) {

		// 0. invalidate this, because.
		self.equiv_keys = 0;

		// 1. move invalid cursors to `self.valid_vals` and decrement appropriately.
		let mut dirty = 0; 
		for index in 0 .. prefix {
			if self.cursors[index].key_valid() {
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
				if self.cursors[index].key() > self.cursors[max_index].key() {
					max_index = index;
				}
			}
			// b. determine how many of the ordered values we must involve.
			let mut beyond = dirty;
			while beyond < self.valid_keys && self.cursors[beyond].key() < self.cursors[max_index].key() {
				beyond += 1;
			}
			// c. sort those cursors.
			self.cursors[.. beyond].sort_by(|x,y| x.key().cmp(y.key()));
			// println!("{:?} / {:?}", beyond, self.cursors.len());
		}
		
		// 3. count equivalent keys (if any are valid)
		if self.valid_keys > 0 {
			self.equiv_keys = 1;
			while self.equiv_keys < self.valid_keys && self.cursors[self.equiv_keys].key() == self.cursors[0].key() {
				self.equiv_keys += 1;
			}
		}

		// 4. order equivalent keys by value.
		let to_tidy = self.equiv_keys;
		self.valid_vals = self.equiv_keys;	// <-- presumably true? 
		self.tidy_vals(to_tidy);
	}
	/// Re-sorts the first `valid_vals` cursors under the assumption that cursors[prefix .. valid_vals] are ordered by value.
	fn tidy_vals(&mut self, prefix: usize) {

		// 0. invalidate this, because.
		self.equiv_vals = 0;

		// 1. move invalid cursors to `self.valid_vals` and decrement appropriately.
		let mut dirty = 0; 
		for index in 0 .. prefix {
			if self.cursors[index].val_valid() {
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
				if self.cursors[index].val() > self.cursors[max_index].val() {
					max_index = index;
				}
			}
			// b. determine how many of the ordered values we must involve.
			let mut beyond = dirty;
			while beyond < self.valid_vals && self.cursors[beyond].val() < self.cursors[max_index].val() {
				beyond += 1;
			}
			// c. sort those cursors.
			self.cursors[.. beyond].sort_by(|x,y| x.val().cmp(y.val()));
		}

		// 3. count equivalent values
		if self.valid_vals > 0 {
			self.equiv_vals = 1;
			while self.equiv_vals < self.valid_vals && self.cursors[self.equiv_vals].val() == self.cursors[0].val() {
				self.equiv_vals += 1;
			}
		}
	}
}

impl<K, V, T, C: Cursor<K, V, T>> Cursor<K, V, T> for CursorList<K, V, T, C> 
where 
	K: Ord+Clone, 
	V: Ord+Clone {

	// validation methods
	fn key_valid(&self) -> bool { self.valid_keys > 0 }
	fn val_valid(&self) -> bool { self.valid_vals > 0 }

	// accessors 
	fn key(&self) -> &K { 
		debug_assert!(self.key_valid());
		self.cursors[0].key() 
	}
	fn val(&self) -> &V { 
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		self.cursors[0].val() 
	}
	fn map_times<L: FnMut(&T, isize)>(&mut self, mut logic: L) {
		for cursor in &mut self.cursors[.. self.equiv_vals] {
			cursor.map_times(|t,d| logic(t,d));
		}
	}

	// key methods
	fn step_key(&mut self) {
		for cursor in &mut self.cursors[.. self.equiv_keys] {
			cursor.step_key();
		}
		let to_tidy = self.equiv_keys;
		self.tidy_keys(to_tidy);
	}
	fn seek_key(&mut self, key: &K) {
		let mut index = 0;
		while index < self.valid_keys && self.cursors[index].key() < &key {
			self.cursors[index].seek_key(key);
			index += 1;
		}
		self.tidy_keys(index);
	}
	
	// value methods
	fn step_val(&mut self) {
		for cursor in &mut self.cursors[.. self.equiv_vals] {
			cursor.step_val();
		}
		let to_tidy = self.equiv_vals;
		self.tidy_vals(to_tidy);
	}
	fn seek_val(&mut self, val: &V) {
		let mut index = 0;
		while index < self.valid_vals && self.cursors[index].val() < &val {
			self.cursors[index].seek_val(val);
			index += 1;
		}
		self.tidy_vals(index);
	}

	// rewinding methods
	fn rewind_keys(&mut self) { 
		let len = self.cursors.len();
		for cursor in &mut self.cursors[.. len] {
			cursor.rewind_keys();
		}
		self.tidy_keys(len);
	}
	fn rewind_vals(&mut self) { 
		let len = self.equiv_keys;
		for cursor in &mut self.cursors[.. len] {
			cursor.rewind_vals();
		}
		self.tidy_vals(len);
	}
}