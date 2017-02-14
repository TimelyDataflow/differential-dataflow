
/// A cursor for navigating ordered `(key, val, time, diff)` updates.
///
/// NOTE: It is currently unclear to me whether we would like a cursor interface or
/// an iterator interface. The cursor makes some merging more efficient, and we'll
/// likely want to add the `seek` functionality anyhow, so cursor it is for now.
pub trait Cursor {
	
	/// Type of key used by cursor.
	type Key: Ord;
	/// Type of value used by cursor.
	type Val: Ord;
	/// Time of timestamp used by cursor.
	type Time: Ord;

	/// Indicates if the current key is valid.
	///
	/// A value of `false` indicates that the cursor has exhausted all keys.
	fn key_valid(&self) -> bool;
	/// Indicates if the current value is valid.
	///
	/// A value of `false` indicates that the cursor has exhausted all values for this key.
	fn val_valid(&self) -> bool;

	/// A reference to the current key. Asserts if invalid.
	fn key(&self) -> &Self::Key;
	/// A reference to the current value. Asserts if invalid.
	fn val(&self) -> &Self::Val;
	/// Applies `logic` to each pair of time and difference.
	fn map_times<L: FnMut(&Self::Time, isize)>(&self, logic: L);

	/// Advances the cursor to the next key. Indicates if the key is valid.
	fn step_key(&mut self) -> bool;
	/// Advances the cursor to the specified key. Indicates if the key is valid.
	fn seek_key(&mut self, key: &Self::Key) -> bool;
	/// Reveals the next key, if it is valid.
	fn peek_key(&self) -> Option<&Self::Key>;
	
	/// Advances the cursor to the next value. Indicates if the value is valid.
	fn step_val(&mut self) -> bool;
	/// Advances the cursor to the specified value. Indicates if the value is valid.
	fn seek_val(&mut self, val: &Self::Val) -> bool;
	/// Reveals the next value, if it is valid.
	fn peek_val(&self) -> Option<&Self::Val>;

	/// Rewinds the cursor to the first key.	
	fn rewind_keys(&mut self);
	/// Rewinds the cursor to the first value for current key.
	fn rewind_vals(&mut self);
}

/// A view over keys available through a cursor.
pub struct KeyViewer<'a, C: Cursor+'a> {
	active: bool,		// if false, we should not step in `next_key`. TODO : annoying that we test this so much.
	cursor: &'a mut C,
}

impl<'a, C: Cursor+'a> KeyViewer<'a, C> {
	/// Returns a new key viewer from a reference to a raw cursor.
	pub fn new(cursor: &'a mut C) -> Self { KeyViewer { active: false, cursor: cursor } }
	/// Returns a view over the values of the next key, if it exists.
	pub fn next_key<'b>(&'b mut self) -> Option<ValViewer<'b, C>> where 'a : 'b {
		if self.active {
			self.cursor.step_key();
		}
		else {
			self.active = true;
		}
		if self.cursor.key_valid() {
			Some(ValViewer { active: false, cursor: self.cursor })
		}
		else {
			None
		}
	}
	/// Returns a view over the values of the specified key, if it exists.
	///
	/// Note: calling `next_key` for the first time after seeking may result in a duplicate key.
	/// This sucks, but it is currently hard to know if `seek_key` advanced the cursor out of its
	/// initial state.
	pub fn seek_key<'b>(&'b mut self, key: &C::Key) -> Option<ValViewer<'b, C>> where 'a : 'b {
		// self.active = true;			// TODO : how can we know if we've started? we may repeat key.
		self.cursor.seek_key(key);
		if self.cursor.key_valid() {
			Some(ValViewer { active: false, cursor: self.cursor })
		}
		else {
			None
		}
	}
	/// Reveals the next key, if it exists.
	pub fn peek_key(&self) -> Option<&C::Key> {
		self.cursor.peek_key()
	}
	/// Rewinds the key viewer to the first key.
	pub fn rewind(&mut self) {
		self.active = false;
		self.cursor.rewind_keys();
	}
}

/// A view over values associated with one key.
pub struct ValViewer<'a, C: Cursor+'a> {
	active: bool,		// if false, we should not step in `next_val`. TODO : annoying that we test this so much.
	cursor: &'a mut C,
}

impl<'a, C: Cursor+'a> ValViewer<'a, C> {
	/// Returns a view over the times of the next value, if it exists.
	pub fn next_val<'b>(&'b mut self) -> Option<TimeViewer<'b, C>> {
		if self.active {
			self.cursor.step_val();
		}
		else {
			self.active = true;
		}
		if self.cursor.val_valid() {
			Some(TimeViewer { cursor: &*self.cursor })
		}
		else { 
			None 
		}
	}
	/// Returns a view over the times of the specified value, if it exists.
	pub fn seek_val<'b>(&'b mut self, val: &C::Val) -> Option<TimeViewer<'b, C>> {
		// self.active = true;			// TODO : how can we know if we've started? we may repeat value.
		self.cursor.seek_val(val);
		if self.cursor.val_valid() {
			Some(TimeViewer { cursor: &*self.cursor })
		}
		else { 
			None 
		}
	}
	/// Reveals the next value, if it exists.
	pub fn peek_val(&self) -> Option<&C::Val> {
		self.cursor.peek_val()
	}
	/// Rewinds the value viewer to the first value.
	pub fn rewind(&mut self) {
		self.active = false;
		self.cursor.rewind_vals();
	}
	/// The key associated with the values viewed.
	pub fn key(&self) -> &C::Key { self.cursor.key() }
}

/// A view over times and differences associated with one (key, value) pair.
pub struct TimeViewer<'a, C: Cursor+'a> {
	cursor: &'a C,
}

impl<'a, C: Cursor+'a> TimeViewer<'a, C> {
	/// The key associated with the times viewed.
	pub fn key(&self) -> &C::Key { self.cursor.key() }
	/// The value associated with the times viewed.
	pub fn val(&self) -> &C::Val { self.cursor.val() }
	/// Applies `logic` to each `(time, diff)` pair in the view.
	pub fn map<L: FnMut(&C::Time, isize)>(&self, logic: L) {
		self.cursor.map_times(logic);
	}
}

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` keeps its cursor ordered by `key`, and cursors with the smallest key are then ordered by `val`.
///
/// At any point, the current elements (keys, vals) are determined by the number of leading equivalent keys and 
/// equivalent values in `self.cursors`. Although they are implicit in `self.cursors`, We maintain these values 
/// explicitly, in `self.equiv_keys` and `self.equiv_vals`. We also track the number of valid keys and valid 
/// values, to avoid continually re-considering cursors in invalid states.
pub struct CursorList<C: Cursor> {
	cursors: Vec<C>,	// ordered by valid keys and valid values.
	equiv_keys: usize,	// cursors[..equiv_keys] all have the same key.
	equiv_vals: usize,	// cursors[..equiv_vals] all have the same key and value.
	valid_keys: usize,	// cursors[..valid_keys] all have valid_key() true.
	valid_vals: usize,	// cursors[..valid_vals] all have valid_val() true.
}

impl<C: Cursor> CursorList<C> {
	/// Creates a new cursor list from pre-existing cursors.
	pub fn new(cursors: Vec<C>) -> Self {
		let cursors_len = cursors.len();
		let mut result = CursorList {
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

impl<C: Cursor> Cursor for CursorList<C> {

	type Key = C::Key;
	type Val = C::Val;
	type Time = C::Time;

	// validation methods
	fn key_valid(&self) -> bool { self.valid_keys > 0 }
	fn val_valid(&self) -> bool { self.valid_vals > 0 }

	// accessors 
	fn key(&self) -> &Self::Key { self.cursors[0].key() }
	fn val(&self) -> &Self::Val { self.cursors[0].val() }
	fn map_times<L: FnMut(&Self::Time, isize)>(&self, mut logic: L) {
		for cursor in &self.cursors[.. self.equiv_vals] {
			cursor.map_times(|t,d| logic(t,d));
		}
	}

	// key methods
	fn step_key(&mut self) -> bool {
		for cursor in &mut self.cursors[.. self.equiv_keys] {
			cursor.step_key();
		}
		let to_tidy = self.equiv_keys;
		self.tidy_keys(to_tidy);
		self.key_valid()
	}
	fn seek_key(&mut self, key: &Self::Key) -> bool {
		let mut index = 0;
		while index < self.valid_keys && self.cursors[index].key() < key {
			self.cursors[index].seek_key(key);
			index += 1;
		}
		self.tidy_keys(index);
		self.key_valid()
	}
	fn peek_key(&self) -> Option<&Self::Key> {
		if self.key_valid() { Some(self.key()) } else { None }
	}
	
	// value methods
	fn step_val(&mut self) -> bool {
		for cursor in &mut self.cursors[.. self.equiv_vals] {
			cursor.step_val();
		}
		let to_tidy = self.equiv_vals;
		self.tidy_vals(to_tidy);
		self.val_valid()
	}
	fn seek_val(&mut self, val: &Self::Val) -> bool {
		let mut index = 0;
		while index < self.valid_vals && self.cursors[index].val() < val {
			self.cursors[index].seek_val(val);
			index += 1;
		}
		self.tidy_vals(index);
		self.val_valid()
	}
	fn peek_val(&self) -> Option<&Self::Val> {
		if self.val_valid() { Some(self.val()) } else { None }
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