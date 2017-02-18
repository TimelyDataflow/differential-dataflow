//! A trace implementation for a collection that does not change.
//! 
//! This trace avoids maintaining times and differences, as it only needs to record keys and values.
//! Unlike the `time.rs` trace, we are "assured" that the collection accumulates to positive counts,
//! and we are able to represent the frequency of each record in "unary", by the number of times it 
//! occurs.
//!
//! This is perhaps the tightest representation I can think of for otherwise arbitrary keys and vals.

/// An immutable collection of keys and values.
pub struct Spine<Key: Ord, Val: Ord> {
	batch: Option<Layer<Key, Val>>,
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Trace<Key, Val, Time> for Spine<Key, Val> {

	type Batch = Rc<Layer<Key, Val>>;
	type Cursor = LayerCursor<Key, Val, Time>;

	fn new(default: Time) -> Self { Spine { batch: None } }

	fn insert(&mut self, batch: Self::Batch) {
		self.batch = Some(batch);
	}
	fn cursor(&self) -> Self::Cursor {
		if let Some(&ref batch) = self.batch {
			batch.cursor()
		}
		else {
			panic!("no batch present; what to do?");
		}
	}
	fn advance_by(&mut self, frontier: &[Time]) { /* totally ignored */ }
}

/// Represents an immutable collection of updates at the base time.
pub struct Layer<Key: Ord, Val: Ord> {
	keys: Vec<Key>,
	offs: Vec<usize>,
	vals: Vec<Val>,
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Batch<Key, Val, Time> for Layer<Key, Val> {

	type Builder = LayerBuilder<Key, Val>;
	type Cursor = LayerCursor<Key, Val, Time>;

	fn cursor(&self) -> Self::Cursor;
	fn len(&self) -> usize;
}

/// Supports navigation through a single layer.
pub struct LayerCursor<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	time: Time,
	layer: Rc<Layer<Key, Val>>,
	key_off: (usize, usize),
	val_off: (usize, usize),
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Cursor<Key, Val, Time> for LayerCursor<Key, Val, Time> {

	type Key = Key;
	type Val = Val;
	type Time = Time;

	fn key_valid(&self) -> bool { self.key_off.0 < self.key_off.1 }
	fn val_valid(&self) -> bool { self.val_off.0 < self.val_off.1 }

	fn key(&self) -> &Self::Key {
		debug_assert!(self.key_valid());
		&self.layer.keys[self.key_off.0]
	}
	fn val(&self) -> &Self::Val {
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		&self.layer.vals[self.val_off.0];
	}
	fn map_times<L: FnMut(&Self::Time, isize)>(&self, logic: L) {
		logic(&self.time, 1);
	}

	fn step_key(&mut self) {
		self.key_off.0 += 1;
		if self.key_off.0 < self.key_off.1 {
			self.val_off.0 = self.layer.offs[self.key_off.0-1];
			self.val_off.1 = self.layer.offs[self.key_off.0];
		}
		else {
			self.key_off.0 = self.key_off.1;
			self.val_off.0 = self.val_off.1;
		}
	}
	fn seek_key(&mut self, key: &Self::Key) {
		self.key_off.0 += advance(&self.layer.keys[self.key_off.0 ..], |k| k.lt(key));
		if self.key_off.0 < self.key_off.1 {
			self.val_off.0 = self.layer.offs[self.key_off.0-1];
			self.val_off.1 = self.layer.offs[self.key_off.0];
		}
		else {
			self.key_off.0 = self.key_off.1;
			self.val_off.0 = self.val_off.1;
		}
	}
	fn peek_key(&self) -> Option<&Self::Key> {
		if self.key_valid() { Some(self.key()) } else { None }
	}
	
	fn step_val(&mut self) {
		self.val_off.0 += 1;
		if self.val_off.0 >= self.val_off.1 {
			self.val_off.0 = self.val_off.1;
		}
	}
	fn seek_val(&mut self, val: &Self::Val) {
		self.val_off += advance(&self.layer.vals[self.val_off.0 .. self.val_off.1], |v| v.lt(val));
	}
	fn peek_val(&self) -> Option<&Self::Val> {
		if self.val_valid() { Some(self.val()) } else { None }
	}

	fn rewind_keys(&mut self) {
		self.key_off.0 = 0;
		self.val_off.0 = 0;
		if self.key_off.1 > 0 {
			self.val_off.1 = self.layer.offs[0];
		}
		else {
			self.val_off.1 = 0;
		}
	}
	fn rewind_vals(&mut self) {
		self.val_off.0 = if self.key_off.0 == 0 { 0 } else { self.layer.offs[self.key_off.0-1] };
	}

}