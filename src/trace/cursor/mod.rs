//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator 
//! both because it allows navigation on multiple levels (key and val), but also because it 
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

pub mod viewers;
pub mod cursor_list;
// pub mod cursor_pair;

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
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
	fn step_key(&mut self);
	/// Advances the cursor to the specified key. Indicates if the key is valid.
	fn seek_key(&mut self, key: &Self::Key);
	/// Reveals the next key, if it is valid.
	fn peek_key(&self) -> Option<&Self::Key>;
	
	/// Advances the cursor to the next value. Indicates if the value is valid.
	fn step_val(&mut self);
	/// Advances the cursor to the specified value. Indicates if the value is valid.
	fn seek_val(&mut self, val: &Self::Val);
	/// Reveals the next value, if it is valid.
	fn peek_val(&self) -> Option<&Self::Val>;

	/// Rewinds the cursor to the first key.	
	fn rewind_keys(&mut self);
	/// Rewinds the cursor to the first value for current key.
	fn rewind_vals(&mut self);
}