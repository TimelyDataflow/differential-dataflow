//! Types wrapping cursor implementations in safer interfaces.
//! 
//! Views provide safe interfaces to cursors by only allowing navigation at appropriate levels, 
//! and by returning views of the next levels only when the data are valid.

use super::Cursor;

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