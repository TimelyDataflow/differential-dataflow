//! A generic cursor implementation merging pairs of different cursors.

use super::Cursor;

/// A cursor over the combined updates of two different cursors.
pub struct CursorPair<C1: Cursor, C2: Cursor<Key=C1::Key, Val=C1::Val, Time=C1::Time>> {
	cursor1: C1,
	cursor2: C2,
}

impl<C1: Cursor, C2: Cursor<Key=C1::Key, Val=C1::Val, Time=C1::Time>> CursorPair<C1, C2> {
	fn active_key1(&self) -> bool {
		!self.cursor2.key_valid() || (self.cursor1.key_valid() && self.cursor1.key() <= self.cursor2.key())
	}
	fn active_key2(&self) -> bool {
		!self.cursor1.key_valid() || (self.cursor2.key_valid() && self.cursor2.key() <= self.cursor1.key())
	}
}


impl<C1: Cursor, C2: Cursor<Key=C1::Key, Val=C1::Val, Time=C1::Time>> Cursor for CursorPair<C1, C2> {

	type Key = C1::Key;
	type Val = C1::Val;
	type Time = C1::Time;

	// validation methods
	fn key_valid(&self) -> bool { self.cursor1.key_valid() || self.cursor2.key_valid() }
	fn val_valid(&self) -> bool { unimplemented!() }

	// accessors 
	fn key(&self) -> &Self::Key { unimplemented!(); }
	fn val(&self) -> &Self::Val { unimplemented!(); }
	fn map_times<L: FnMut(&Self::Time, isize)>(&self, mut logic: L) {
		unimplemented!();
		// self.cursor1.map_times(|t,d| logic(t,d));
		// self.cursor2.map_times(|t,d| logic(t,d));
	}

	// key methods
	fn step_key(&mut self) {
		unimplemented!();
	}
	fn seek_key(&mut self, key: &Self::Key) {
		unimplemented!();
	}
	fn peek_key(&self) -> Option<&Self::Key> {
		if self.key_valid() { Some(self.key()) } else { None }
	}
	
	// value methods
	fn step_val(&mut self) {
		unimplemented!();
	}
	fn seek_val(&mut self, val: &Self::Val) {
		unimplemented!();
	}
	fn peek_val(&self) -> Option<&Self::Val> {
		if self.val_valid() { Some(self.val()) } else { None }
	}

	// rewinding methods
	fn rewind_keys(&mut self) { 
		self.cursor1.rewind_keys();
		self.cursor2.rewind_keys();
	}
	fn rewind_vals(&mut self) { 
		unimplemented!();
	}
}