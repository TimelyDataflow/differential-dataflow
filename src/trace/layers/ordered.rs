//! Implementation using ordered keys and exponential search.

use std::rc::Rc;
use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder};
use owning_ref::OwningRef;

/// A level of the trie, with keys and offsets into a lower layer.
///
/// In this representation, the values for `keys[i]` are found at `vals[offs[i] .. offs[i+1]]`.
#[derive(Debug)]
pub struct OrderedLayer<K: Ord, L> {
	/// The keys of the layer.
	pub keys: Vec<K>,
	/// The offsets associate with each key.
	///
	/// The bounds for `keys[i]` are `(offs[i], offs[i+1]`). The offset array is guaranteed to be one
	/// element longer than the keys array, ensuring that these accesses do not panic.
	pub offs: Vec<usize>,
	/// The ranges of values associated with the keys.
	pub vals: L,
}

impl<B, K: Ord+Clone, L: Trie<B>> Trie<B> for OrderedLayer<K, L> {
	type Item = (K, L::Item);
	type Cursor = OrderedCursor<B, K, L::Cursor>;
	type MergeBuilder = OrderedBuilder<B, K, L::MergeBuilder>;
	type TupleBuilder = OrderedBuilder<B, K, L::TupleBuilder>;

	fn keys(&self) -> usize { self.keys.len() }
	fn tuples(&self) -> usize { self.vals.tuples() }
	fn cursor_from(&self, owned_self: OwningRef<Rc<B>, Self>, lower: usize, upper: usize) -> Self::Cursor {
		// let child_lower = if lower == 0 { 0 } else { self.offs[lower-1] };
		// let child_upper = self.offs[lower];

		if lower < upper {

		debug_assert!(self.offs.len() > lower + 1);

			let child_lower = self.offs[lower];
			let child_upper = self.offs[lower + 1];
			OrderedCursor {
				keys: owned_self.clone().map(|x| &x.keys),
				offs: owned_self.clone().map(|x| &x.offs),
				bounds: (lower, upper),
				child: self.vals.cursor_from(owned_self.map(|x| &x.vals), child_lower, child_upper),
				pos: lower,
			}		
		}
		else {
			OrderedCursor {
				keys: owned_self.clone().map(|x| &x.keys),
				offs: owned_self.clone().map(|x| &x.offs),
				bounds: (0, 0),
				child: self.vals.cursor_from(owned_self.map(|x| &x.vals), 0, 0),
				pos: 0,
			}	
		}
	}
}

/// Assembles a layer of this 
pub struct OrderedBuilder<B, K: Ord, L> {
	/// Keys
	pub keys: Vec<K>,
	/// Offsets
	pub offs: Vec<usize>,
	/// The next layer down
	pub vals: L,
	_b: ::std::marker::PhantomData<B>,
}

impl<B, K: Ord+Clone, L: Builder<B>> Builder<B> for OrderedBuilder<B, K, L> {
	type Trie = OrderedLayer<K, L::Trie>; 
	fn boundary(&mut self) -> usize { 
		self.offs[self.keys.len()] = self.vals.boundary();
		self.keys.len()
	}
	fn done(mut self) -> Self::Trie {
		if self.keys.len() > 0 && self.offs[self.keys.len()] == 0 {
			self.offs[self.keys.len()] = self.vals.boundary();
		}
		OrderedLayer {
			keys: self.keys,
			offs: self.offs,
			vals: self.vals.done(),
		}
	}
}

impl<B, K: Ord+Clone, L: MergeBuilder<B>> MergeBuilder<B> for OrderedBuilder<B, K, L> {
	fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
		let mut offs = Vec::with_capacity(other1.keys() + other2.keys() + 1);
		offs.push(0);
		OrderedBuilder {
			keys: Vec::with_capacity(other1.keys() + other2.keys()),
			offs: offs,
			vals: L::with_capacity(&other1.vals, &other2.vals),
			_b: ::std::marker::PhantomData,
		}
	}
	fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {

		if lower < upper {
			let other_basis = other.offs[lower];
			let self_basis = self.offs.last().map(|&x| x).unwrap_or(0);

			self.keys.extend_from_slice(&other.keys[lower .. upper]);
			for index in lower .. upper {
				self.offs.push((other.offs[index + 1] + self_basis) - other_basis);
			}
			self.vals.copy_range(&other.vals, other_basis, other.offs[upper]);
		}
		else {
			panic!("{}: lower !< upper: {}", lower, upper);
		}
	}
	fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize {
		let (trie1, mut lower1, upper1) = other1;
		let (trie2, mut lower2, upper2) = other2;

		self.keys.reserve((upper1 - lower1) + (upper2 - lower2));

		// while both mergees are still active
		while lower1 < upper1 && lower2 < upper2 {

			match trie1.keys[lower1].cmp(&trie2.keys[lower2]) {
				::std::cmp::Ordering::Less => {
					// determine how far we can advance lower1 until we reach/pass lower2
					let step = 1 + advance(&trie1.keys[(1+lower1)..upper1], |x| x < &trie2.keys[lower2]);
					self.copy_range(trie1, lower1, lower1 + step);
					lower1 += step;
				}
				::std::cmp::Ordering::Equal => {
					let lower = self.vals.boundary();
					// record vals_length so we can tell if anything was pushed.
					let upper = self.vals.push_merge(
						(&trie1.vals, trie1.offs[lower1], trie1.offs[lower1+1]), 
						(&trie2.vals, trie2.offs[lower2], trie2.offs[lower2+1])
					);
					if upper > lower {
						self.keys.push(trie1.keys[lower1].clone());
						self.offs.push(upper);
					}

					lower1 += 1;
					lower2 += 1;
				} 
				::std::cmp::Ordering::Greater => {
					// determine how far we can advance lower2 until we reach/pass lower1
					let step = 1 + advance(&trie2.keys[(1+lower2)..upper2], |x| x < &trie1.keys[lower1]);
					self.copy_range(trie2, lower2, lower2 + step);
					lower2 += step;
				}
			}
		}

		if lower1 < upper1 { self.copy_range(trie1, lower1, upper1); }
		if lower2 < upper2 { self.copy_range(trie2, lower2, upper2); }

		self.keys.len()
	}
}

impl<B, K: Ord+Clone, L: TupleBuilder<B>> TupleBuilder<B> for OrderedBuilder<B, K, L> {

	type Item = (K, L::Item);
	fn new() -> Self { OrderedBuilder { keys: Vec::new(), offs: vec![0], vals: L::new(), _b: ::std::marker::PhantomData } }
	fn with_capacity(cap: usize) -> Self { 
		let mut offs = Vec::with_capacity(cap + 1);
		offs.push(0);
		OrderedBuilder{ 
			keys: Vec::with_capacity(cap), 
			offs: offs, 
			vals: L::with_capacity(cap),
			_b: ::std::marker::PhantomData,
		}
	}
	#[inline(always)]
	fn push_tuple(&mut self, (key, val): (K, L::Item)) {

		// if first element, prior element finish, or different element, need to push and maybe punctuate.
		if self.keys.len() == 0 || self.offs[self.keys.len()] != 0 || self.keys[self.keys.len()-1] != key {
			if self.keys.len() > 0 && self.offs[self.keys.len()] == 0 {
				self.offs[self.keys.len()] = self.vals.boundary();
			}
			self.keys.push(key);
			self.offs.push(0);		// <-- indicates "unfinished".
		}
		self.vals.push_tuple(val);
	}
}

/// A cursor with a child cursor that is updated as we move.
#[derive(Debug)]
pub struct OrderedCursor<B, K: Ord, L: Cursor> {
	keys: OwningRef<Rc<B>, Vec<K>>,
	offs: OwningRef<Rc<B>, Vec<usize>>,
	pos: usize,
	bounds: (usize, usize),
	/// The cursor for the trie layer below this one.
	pub child: L,
}

impl<B, K: Ord, L: Cursor> Cursor for OrderedCursor<B, K, L> {
	type Key = K;
	fn key(&self) -> &Self::Key { &self.keys[self.pos] }
	fn step(&mut self) {
		self.pos += 1; 
		if self.valid() {
			self.child.reposition(self.offs[self.pos], self.offs[self.pos + 1]);
		}
		else {
			self.pos = self.bounds.1;
		}
	}
	fn seek(&mut self, key: &Self::Key) {
		self.pos += advance(&self.keys[self.pos .. self.bounds.1], |k| k.lt(key));
		if self.valid() {
			self.child.reposition(self.offs[self.pos], self.offs[self.pos + 1]);
		}
	}
	// fn size(&self) -> usize { self.bounds.1 - self.bounds.0 }
	fn valid(&self) -> bool { self.pos < self.bounds.1 }
	fn rewind(&mut self) {
		self.pos = self.bounds.0;
		if self.valid() {
			self.child.reposition(self.offs[self.pos], self.offs[self.pos + 1]);
		}
	}
	fn reposition(&mut self, lower: usize, upper: usize) {
		self.pos = lower;
		self.bounds = (lower, upper);
		if self.valid() {
			self.child.reposition(self.offs[self.pos], self.offs[self.pos + 1]);
		}
	}
}

/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to 
/// count the number of elements in time logarithmic in the result.
#[inline(never)]
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

	// start with no advance
	let mut index = 0;
	if index < slice.len() && function(&slice[index]) {

		// advance in exponentially growing steps.
		let mut step = 1;
		while index + step < slice.len() && function(&slice[index + step]) {
			index += step;
			step = step << 1;
		}

		// advance in exponentially shrinking steps.
		step = step >> 1;
		while step > 0 {
			if index + step < slice.len() && function(&slice[index + step]) {
				index += step;
			}
			step = step >> 1;
		}

		index += 1;
	}	

	index
}
