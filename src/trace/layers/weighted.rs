//! Implementation using ordered keys and exponential search.

use std::rc::Rc;
use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder};

/// A layer with sorted keys and integer weights.
#[derive(Debug)]
pub struct WeightedLayer<K: Ord> {
	/// Keys.
	pub keys: Rc<Vec<K>>,
	/// Weights.
	pub wgts: Rc<Vec<isize>>,
}

impl<K: Ord+Clone> Trie for WeightedLayer<K> {
	type Item = (K, isize);
	type Cursor = WeightedCursor<K>;
	type MergeBuilder = WeightedBuilder<K>;
	type TupleBuilder = WeightedBuilder<K>;
	fn keys(&self) -> usize { self.keys.len() }
	fn tuples(&self) -> usize { self.keys() }
	fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor {	
		WeightedCursor {
			keys: self.keys.clone(),
			wgts: self.wgts.clone(),
			bounds: (lower, upper),
			pos: lower,
		}
	}
}

/// A builder for a weighted layer.
pub struct WeightedBuilder<K: Ord> {
	is_new: bool,
	/// Keys.
	pub keys: Vec<K>,
	/// Weights.
	pub wgts: Vec<isize>,
}

impl<K: Ord+Clone> Builder for WeightedBuilder<K> {
	type Trie = WeightedLayer<K>; 
	fn boundary(&mut self) -> usize { 
		self.is_new = true; 
		self.keys.len() 
	}
	fn done(self) -> Self::Trie {
		WeightedLayer {
			keys: Rc::new(self.keys),
			wgts: Rc::new(self.wgts),
		}
	}
}

impl<K: Ord+Clone> MergeBuilder for WeightedBuilder<K> {
	fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
		WeightedBuilder {
			is_new: false,
			keys: Vec::with_capacity(other1.keys() + other2.keys()),
			wgts: Vec::with_capacity(other1.keys() + other2.keys()),
		}
	}
	fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
		// println!("copying range: {:?} -> {:?}", lower, upper);
		self.keys.extend_from_slice(&other.keys[lower .. upper]);
		self.wgts.extend_from_slice(&other.wgts[lower .. upper]);
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
					let sum = trie1.wgts[lower1] + trie2.wgts[lower2];
					if sum != 0 {
						self.keys.push(trie1.keys[lower1].clone());
						self.wgts.push(sum);
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

impl<K: Ord+Clone> TupleBuilder for WeightedBuilder<K> {

	type Item = (K, isize);
	fn new() -> Self { 
		WeightedBuilder { 
			is_new: false,
			keys: Vec::new(), 
			wgts: Vec::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self {
		WeightedBuilder {
			is_new: false,
			keys: Vec::with_capacity(cap),
			wgts: Vec::with_capacity(cap),
		}
	}
	fn push_tuple(&mut self, tuple: (K, isize)) {
		if self.is_new || self.keys.last().map(|x| x != &tuple.0).unwrap_or(true) {
			self.keys.push(tuple.0);
			self.wgts.push(tuple.1);
			self.is_new = false;
		}
		else {
			self.wgts[self.keys.len()-1] += tuple.1;
			if self.wgts[self.keys.len()-1] == 0 {
				self.keys.pop();
				self.wgts.pop();
			}
		}
	}
}

/// A cursor with a child cursor that is updated as we move.
pub struct WeightedCursor<K: Ord> {
	keys: Rc<Vec<K>>,
	wgts: Rc<Vec<isize>>,
	pos: usize,
	bounds: (usize, usize),
}

impl<K: Ord> WeightedCursor<K> {
	/// Recovers the weight of the item.
	pub fn weight(&self) -> isize { self.wgts[self.bounds.0] }
}

impl<K: Ord> Cursor for WeightedCursor<K> {
	type Key = K;
	fn key(&self) -> &Self::Key { &self.keys[self.pos] }
	fn step(&mut self) {
		self.pos += 1;
		if !self.valid() {
			self.pos = self.bounds.1;
		}
	}
	fn seek(&mut self, key: &Self::Key) {
		self.pos += advance(&self.keys[self.pos .. self.bounds.1], |k| k.lt(key));
	}
	// fn size(&self) -> usize { self.bounds.1 - self.bounds.0 }
	fn valid(&self) -> bool { self.pos < self.bounds.1 }
	fn rewind(&mut self) {
		self.pos = self.bounds.0;
	}
	fn reposition(&mut self, lower: usize, upper: usize) {
		self.pos = lower;
		self.bounds = (lower, upper);
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