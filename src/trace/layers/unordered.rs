//! Implementation using ordered keys and exponential search.

use std::rc::Rc;

use owning_ref::{OwningRef, Erased};

use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder};

/// A layer of unordered values. 
#[derive(Debug)]
pub struct UnorderedLayer<K> {
	/// Unordered values.
	pub vals: Vec<K>,
}

impl<K: Clone> Trie for UnorderedLayer<K> {
	type Item = K;
	type Cursor = UnorderedCursor<K>;
	type MergeBuilder = UnorderedBuilder<K>;
	type TupleBuilder = UnorderedBuilder<K>;
	fn keys(&self) -> usize { self.vals.len() }
	fn tuples(&self) -> usize { <UnorderedLayer<K> as Trie>::keys(&self) }
	fn cursor_from(&self, owned_self: OwningRef<Rc<Erased>, Self>, lower: usize, upper: usize) -> Self::Cursor {	
		// println!("unordered: {} .. {}", lower, upper);
		UnorderedCursor {
			vals: owned_self.map(|x| &x.vals[..]),
			bounds: (lower, upper),
			pos: lower,
		}
	}
}

/// A builder for unordered values.
pub struct UnorderedBuilder<K> {
	/// Unordered values.
	pub vals: Vec<K>,
}

impl<K: Clone> Builder for UnorderedBuilder<K> {
	type Trie = UnorderedLayer<K>; 
	fn boundary(&mut self) -> usize { self.vals.len() } 
	fn done(self) -> Self::Trie { UnorderedLayer { vals: self.vals } }
}

impl<K: Clone> MergeBuilder for UnorderedBuilder<K> {
	fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
		UnorderedBuilder {
			vals: Vec::with_capacity(<UnorderedLayer<K> as Trie>::keys(other1) + <UnorderedLayer<K> as Trie>::keys(other2)),
		}
	}
	fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
		self.vals.extend_from_slice(&other.vals[lower .. upper]);
	}
	fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize {
		<UnorderedBuilder<K> as MergeBuilder>::copy_range(self, &other1.0, other1.1, other1.2);
		<UnorderedBuilder<K> as MergeBuilder>::copy_range(self, &other2.0, other2.1, other2.2);
		self.vals.len()
	}
}

impl<K: Clone> TupleBuilder for UnorderedBuilder<K> {
	type Item = K;
	fn new() -> Self { UnorderedBuilder { vals: Vec::new() } }
	fn with_capacity(cap: usize) -> Self { UnorderedBuilder { vals: Vec::with_capacity(cap) } }
	#[inline(always)] fn push_tuple(&mut self, tuple: K) { self.vals.push(tuple) }
}

/// A cursor for walking through an unordered sequence of values.
///
/// This cursor does not support `seek`, though I'm not certain how to expose this.
#[derive(Debug)]
pub struct UnorderedCursor<K> {
	vals: OwningRef<Rc<Erased>, [K]>,
	pos: usize,
	bounds: (usize, usize),
}

impl<K: Clone> Cursor for UnorderedCursor<K> {
	type Key = K;
	fn key(&self) -> &Self::Key { &self.vals[self.pos] }
	fn step(&mut self) {
		self.pos += 1; 
		if !self.valid() {
			self.pos = self.bounds.1;
		}
	}
	fn seek(&mut self, _key: &Self::Key) {
		panic!("seeking in an UnorderedCursor");
	}
	fn valid(&self) -> bool { self.pos < self.bounds.1 }
	fn rewind(&mut self) {
		self.pos = self.bounds.0;
	}
	fn reposition(&mut self, lower: usize, upper: usize) {
		self.pos = lower;
		self.bounds = (lower, upper);
	}
}
