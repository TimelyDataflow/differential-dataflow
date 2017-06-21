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
use owning_ref::OwningRef;

use ::Diff;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::hashed::{HashedLayer, HashedBuilder, HashedCursor};
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::unordered::{UnorderedLayer, UnorderedBuilder, UnorderedCursor};

use lattice::Lattice;
use trace::{Batch, BatchReader, Builder, Cursor};
use trace::description::Description;

use super::spine::Spine;
use super::batcher::RadixBatcher;

/// A trace implementation using a spine of hash-map batches.
pub type HashValSpine<K, V, T, R> = Spine<K, V, T, R, Rc<HashValBatch<K, V, T, R>>>;
/// A trace implementation for empty values using a spine of hash-map batches.
pub type HashKeySpine<K, T, R> = Spine<K, (), T, R, Rc<HashKeyBatch<K, T, R>>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct HashValBatch<K: HashOrdered, V: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: HashedLayer<K, OrderedLayer<V, UnorderedLayer<(T, R)>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> BatchReader<K, V, T, R> for Rc<HashValBatch<K, V, T, R>>
where K: Clone+Default+HashOrdered, V: Clone+Ord, T: Lattice+Ord+Clone+Default, R: Diff {
	type Cursor = HashValCursor<K, V, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		HashValCursor {
			cursor: self.layer.cursor(OwningRef::new(self.clone()).map(|x| &x.layer)),
		}
	}
	fn len(&self) -> usize { <HashedLayer<K, OrderedLayer<V, UnorderedLayer<(T, R)>>> as Trie<HashValBatch<K, V, T, R>>>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R> Batch<K, V, T, R> for Rc<HashValBatch<K, V, T, R>>
where K: Clone+Default+HashOrdered, V: Clone+Ord, T: Lattice+Ord+Clone+Default, R: Diff {
	type Batcher = RadixBatcher<K, V, T, R, Self>;
	type Builder = HashValBuilder<K, V, T, R>;
	fn merge(&self, other: &Self) -> Self {

		// Things are horribly wrong if this is not true.
		assert!(self.desc.upper() == other.desc.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.less_equal(t1))) {
			other.desc.since()
		}
		else {
			self.desc.since()
		};
		
		Rc::new(HashValBatch {
			layer: <HashedLayer<K, OrderedLayer<V, UnorderedLayer<(T, R)>>> as Trie<HashValBatch<K, V, T, R>>>::merge(&self.layer, &other.layer),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		})
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct HashValCursor<K: Clone+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy> {
	cursor: HashedCursor<HashValBatch<K, V, T, R>, K, OrderedCursor<HashValBatch<K, V, T, R>, V, UnorderedCursor<HashValBatch<K, V, T, R>, (T, R)>>>,
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
pub struct HashValBuilder<K: HashOrdered, V: Ord, T: Ord+Lattice, R: Diff> {
	builder: HashedBuilder<HashValBatch<K, V, T, R>, K, OrderedBuilder<HashValBatch<K, V, T, R>, V, UnorderedBuilder<(T, R)>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, Rc<HashValBatch<K, V, T, R>>> for HashValBuilder<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Diff {

	fn new() -> Self { 
		HashValBuilder { 
			builder: HashedBuilder::<HashValBatch<K, V, T, R>, K, OrderedBuilder<HashValBatch<K, V, T, R>, V, UnorderedBuilder<(T, R)>>>::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self { 
		HashValBuilder { 
			builder: HashedBuilder::<HashValBatch<K, V, T, R>, K, OrderedBuilder<HashValBatch<K, V, T, R>, V, UnorderedBuilder<(T, R)>>>::with_capacity(cap) 
		} 
	}

	#[inline(always)]
	fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
		self.builder.push_tuple((key, (val, (time, diff))));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<HashValBatch<K, V, T, R>> {
		Rc::new(HashValBatch {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		})
	}
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct HashKeyBatch<K: HashOrdered, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: HashedLayer<K, UnorderedLayer<(T, R)>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, T, R> BatchReader<K, (), T, R> for Rc<HashKeyBatch<K, T, R>>
where K: Clone+Default+HashOrdered, T: Lattice+Ord+Clone+Default, R: Diff {
	type Cursor = HashKeyCursor<K, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		HashKeyCursor {
			empty: (),
			valid: true,
			cursor: self.layer.cursor(OwningRef::new(self.clone()).map(|x| &x.layer)),
		} 
	}
	fn len(&self) -> usize { <HashedLayer<K, UnorderedLayer<(T, R)>> as Trie<HashKeyBatch<K, T, R>>>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, T, R> Batch<K, (), T, R> for Rc<HashKeyBatch<K, T, R>>
where K: Clone+Default+HashOrdered, T: Lattice+Ord+Clone+Default, R: Diff {
	type Batcher = RadixBatcher<K, (), T, R, Self>;
	type Builder = HashKeyBuilder<K, T, R>;
	fn merge(&self, other: &Self) -> Self {

		// Things are horribly wrong if this is not true.
		assert!(self.desc.upper() == other.desc.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.less_equal(t1))) {
			other.desc.since()
		}
		else {
			self.desc.since()
		};
		
		Rc::new(HashKeyBatch {
			layer: <HashedLayer<K, UnorderedLayer<(T, R)>> as Trie<HashKeyBatch<K, T, R>>>::merge(&self.layer, &other.layer),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		})
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct HashKeyCursor<K: Clone+HashOrdered, T: Lattice+Ord+Clone, R: Copy> {
	valid: bool,
	empty: (),
	cursor: HashedCursor<HashKeyBatch<K, T, R>, K, UnorderedCursor<HashKeyBatch<K, T, R>, (T, R)>>,
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
pub struct HashKeyBuilder<K: HashOrdered, T: Ord+Lattice, R: Diff> {
	builder: HashedBuilder<HashKeyBatch<K, T, R>, K, UnorderedBuilder<(T, R)>>,
}

impl<K, T, R> Builder<K, (), T, R, Rc<HashKeyBatch<K, T, R>>> for HashKeyBuilder<K, T, R> 
where K: Clone+Default+HashOrdered, T: Lattice+Ord+Clone+Default, R: Diff {

	fn new() -> Self { 
		HashKeyBuilder { 
			builder: HashedBuilder::<HashKeyBatch<K, T, R>, K, UnorderedBuilder<(T, R)>>::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self { 
		HashKeyBuilder { 
			builder: HashedBuilder::<HashKeyBatch<K, T, R>, K, UnorderedBuilder<(T, R)>>::with_capacity(cap) 
		} 
	}

	#[inline(always)]
	fn push(&mut self, (key, _, time, diff): (K, (), T, R)) {
		self.builder.push_tuple((key, (time, diff)));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<HashKeyBatch<K, T, R>> {
		Rc::new(HashKeyBatch {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		})
	}
}

