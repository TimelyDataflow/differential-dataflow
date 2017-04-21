//! Trace and batch implementations based on Robin Hood hashing.
//!
//! The types and type aliases in this module start with either 
//! 
//! * `HashVal`: Collections whose data have the form `(key, val)` where `key` is hash-ordered.
//! * `HashKey`: Collections whose data have the form `key` where `key` is hash-ordered.
//!
//! Although `HashVal` is more general than `HashKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

use std::rc::Rc;

use ::Diff;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::hashed::{HashedLayer, HashedBuilder, HashedCursor};
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::unordered::{UnorderedLayer, UnorderedBuilder, UnorderedCursor};

use lattice::Lattice;
use trace::{Batch, Builder, Cursor};
use trace::description::Description;

use super::spine::Spine;
use super::batcher::RadixBatcher;

/// A trace implementation using a spine of hash-map batches.
pub type HashValSpine<K, V, T, R> = Spine<K, V, T, R, HashValBatch<K, V, T, R>>;
/// A trace implementation for empty values using a spine of hash-map batches.
pub type HashKeySpine<K, T, R> = Spine<K, (), T, R, HashKeyBatch<K, T, R>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct HashValBatch<K: HashOrdered, V: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: Rc<HashedLayer<K, OrderedLayer<V, UnorderedLayer<(T, R)>>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> Batch<K, V, T, R> for HashValBatch<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Clone+Ord, T: Lattice+Ord+Clone+Default, R: Diff {
	type Batcher = RadixBatcher<K, V, T, R, Self>;
	type Builder = HashValBuilder<K, V, T, R>;
	type Cursor = HashValCursor<K, V, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		HashValCursor { cursor: self.layer.cursor() } 
	}
	fn len(&self) -> usize { self.layer.tuples() }
	fn description(&self) -> &Description<T> { &self.desc }
	fn merge(&self, other: &Self) -> Self {

		// Things are horribly wrong if this is not true.
		assert!(self.desc.upper() == other.desc.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.le(t1))) {
			other.desc.since()
		}
		else {
			self.desc.since()
		};
		
		HashValBatch {
			layer: Rc::new(self.layer.merge(&other.layer)),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		}
	}
}

impl<K: HashOrdered, V: Ord, T: Lattice+Ord+Clone, R> Clone for HashValBatch<K, V, T, R> {
	fn clone(&self) -> Self {
		HashValBatch {
			layer: self.layer.clone(),
			desc: self.desc.clone(),
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct HashValCursor<K: Clone+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy> {
	cursor: HashedCursor<K, OrderedCursor<V, UnorderedCursor<(T, R)>>>,
}

impl<K: Clone+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy> Cursor<K, V, T, R> for HashValCursor<K, V, T, R> {
	fn key(&self) -> &K { &self.cursor.key() }
	fn val(&self) -> &V { &self.cursor.child.key() }
	fn map_times<L: FnMut(&T, R)>(&mut self, mut logic: L) {
		self.cursor.child.child.rewind();
		while self.cursor.child.child.valid() {
			logic(&self.cursor.child.child.key().0, self.cursor.child.child.key().1);
			self.cursor.child.child.step();
		}
	}
	fn key_valid(&self) -> bool { self.cursor.valid() }
	fn val_valid(&self) -> bool { self.cursor.child.valid() }
	fn step_key(&mut self){ self.cursor.step(); }
	fn seek_key(&mut self, key: &K) { self.cursor.seek(key); }
	fn step_val(&mut self) { self.cursor.child.step(); }
	fn seek_val(&mut self, val: &V) { self.cursor.child.seek(val); }
	fn rewind_keys(&mut self) { self.cursor.rewind(); }
	fn rewind_vals(&mut self) { self.cursor.child.rewind(); }
}


/// A builder for creating layers from unsorted update tuples.
pub struct HashValBuilder<K: HashOrdered, V: Ord, T: Ord, R: Diff> {
	builder: HashedBuilder<K, OrderedBuilder<V, UnorderedBuilder<(T, R)>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, HashValBatch<K, V, T, R>> for HashValBuilder<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Diff {

	fn new() -> Self { 
		HashValBuilder { 
			builder: HashedBuilder::<K, OrderedBuilder<V, UnorderedBuilder<(T, R)>>>::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self { 
		HashValBuilder { 
			builder: HashedBuilder::<K, OrderedBuilder<V, UnorderedBuilder<(T, R)>>>::with_capacity(cap) 
		} 
	}

	#[inline(always)]
	fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
		self.builder.push_tuple((key, (val, (time, diff))));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> HashValBatch<K, V, T, R> {
		HashValBatch {
			layer: Rc::new(self.builder.done()),
			desc: Description::new(lower, upper, since)
		}
	}
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct HashKeyBatch<K: HashOrdered, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: Rc<HashedLayer<K, UnorderedLayer<(T, R)>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, T, R> Batch<K, (), T, R> for HashKeyBatch<K, T, R> 
where K: Clone+Default+HashOrdered, T: Lattice+Ord+Clone+Default, R: Diff {
	type Batcher = RadixBatcher<K, (), T, R, Self>;
	type Builder = HashKeyBuilder<K, T, R>;
	type Cursor = HashKeyCursor<K, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		HashKeyCursor { empty: (), valid: true, cursor: self.layer.cursor() } 
	}
	fn len(&self) -> usize { self.layer.tuples() }
	fn description(&self) -> &Description<T> { &self.desc }
	fn merge(&self, other: &Self) -> Self {

		// Things are horribly wrong if this is not true.
		assert!(self.desc.upper() == other.desc.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.le(t1))) {
			other.desc.since()
		}
		else {
			self.desc.since()
		};
		
		HashKeyBatch {
			layer: Rc::new(self.layer.merge(&other.layer)),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		}
	}
}

impl<K: HashOrdered, T: Lattice+Ord+Clone, R> Clone for HashKeyBatch<K, T, R> {
	fn clone(&self) -> Self {
		HashKeyBatch {
			layer: self.layer.clone(),
			desc: self.desc.clone(),
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct HashKeyCursor<K: Clone+HashOrdered, T: Lattice+Ord+Clone, R: Copy> {
	valid: bool,
	empty: (),
	cursor: HashedCursor<K, UnorderedCursor<(T, R)>>,
}

impl<K: Clone+HashOrdered, T: Lattice+Ord+Clone, R: Copy> Cursor<K, (), T, R> for HashKeyCursor<K, T, R> {
	fn key(&self) -> &K { &self.cursor.key() }
	fn val(&self) -> &() { &self.empty }
	fn map_times<L: FnMut(&T, R)>(&mut self, mut logic: L) {
		self.cursor.child.rewind();
		while self.cursor.child.valid() {
			logic(&self.cursor.child.key().0, self.cursor.child.key().1);
			self.cursor.child.step();
		}
	}
	fn key_valid(&self) -> bool { self.cursor.valid() }
	fn val_valid(&self) -> bool { self.valid }
	fn step_key(&mut self){ self.cursor.step(); self.valid = true; }
	fn seek_key(&mut self, key: &K) { self.cursor.seek(key); self.valid = true; }
	fn step_val(&mut self) { self.valid = false; }
	fn seek_val(&mut self, _val: &()) { }
	fn rewind_keys(&mut self) { self.cursor.rewind(); self.valid = true; }
	fn rewind_vals(&mut self) { self.valid = true; }
}


/// A builder for creating layers from unsorted update tuples.
pub struct HashKeyBuilder<K: HashOrdered, T: Ord, R: Diff> {
	builder: HashedBuilder<K, UnorderedBuilder<(T, R)>>,
}

impl<K, T, R> Builder<K, (), T, R, HashKeyBatch<K, T, R>> for HashKeyBuilder<K, T, R> 
where K: Clone+Default+HashOrdered, T: Lattice+Ord+Clone+Default, R: Diff {

	fn new() -> Self { 
		HashKeyBuilder { 
			builder: HashedBuilder::<K, UnorderedBuilder<(T, R)>>::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self { 
		HashKeyBuilder { 
			builder: HashedBuilder::<K, UnorderedBuilder<(T, R)>>::with_capacity(cap) 
		} 
	}

	#[inline(always)]
	fn push(&mut self, (key, _, time, diff): (K, (), T, R)) {
		self.builder.push_tuple((key, (time, diff)));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> HashKeyBatch<K, T, R> {
		HashKeyBatch {
			layer: Rc::new(self.builder.done()),
			desc: Description::new(lower, upper, since)
		}
	}
}

