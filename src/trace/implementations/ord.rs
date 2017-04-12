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

use ::Ring;
use hashable::Hashable;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::unordered::{UnorderedLayer, UnorderedBuilder, UnorderedCursor};

use lattice::Lattice;
use trace::{Batch, Builder, Cursor};
use trace::description::Description;

use super::spine::Spine;
use super::batcher::RadixBatcher;

/// A trace implementation using a spine of hash-map batches.
pub type OrdValSpine<K, V, T, R> = Spine<K, V, T, R, OrdValBatch<K, V, T, R>>;
/// A trace implementation for empty values using a spine of hash-map batches.
pub type OrdKeySpine<K, T, R> = Spine<K, (), T, R, OrdKeyBatch<K, T, R>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct OrdValBatch<K: Ord+Hashable, V: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: Rc<OrderedLayer<K, OrderedLayer<V, UnorderedLayer<(T, R)>>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> Batch<K, V, T, R> for OrdValBatch<K, V, T, R> 
where K: Ord+Clone+Hashable, V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring {
	type Batcher = RadixBatcher<K, V, T, R, Self>;
	type Builder = OrdValBuilder<K, V, T, R>;
	type Cursor = OrdValCursor<K, V, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		OrdValCursor { cursor: self.layer.cursor() } 
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
		
		OrdValBatch {
			layer: Rc::new(self.layer.merge(&other.layer)),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		}
	}
}

impl<K: Ord+Hashable, V: Ord, T: Lattice+Ord+Clone, R> Clone for OrdValBatch<K, V, T, R> {
	fn clone(&self) -> Self {
		OrdValBatch {
			layer: self.layer.clone(),
			desc: self.desc.clone(),
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdValCursor<K: Ord+Clone+Hashable, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy> {
	cursor: OrderedCursor<K, OrderedCursor<V, UnorderedCursor<(T, R)>>>,
}

impl<K: Ord+Clone+Hashable, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy> Cursor<K, V, T, R> for OrdValCursor<K, V, T, R> {
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
pub struct OrdValBuilder<K: Ord+Hashable, V: Ord, T: Ord, R: Ring> {
	builder: OrderedBuilder<K, OrderedBuilder<V, UnorderedBuilder<(T, R)>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, OrdValBatch<K, V, T, R>> for OrdValBuilder<K, V, T, R> 
where K: Ord+Clone+Hashable, V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring {

	fn new() -> Self { 
		OrdValBuilder { 
			builder: OrderedBuilder::<K, OrderedBuilder<V, UnorderedBuilder<(T, R)>>>::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self { 
		OrdValBuilder { 
			builder: OrderedBuilder::<K, OrderedBuilder<V, UnorderedBuilder<(T, R)>>>::with_capacity(cap) 
		} 
	}

	#[inline(always)]
	fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
		self.builder.push_tuple((key, (val, (time, diff))));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> OrdValBatch<K, V, T, R> {
		OrdValBatch {
			layer: Rc::new(self.builder.done()),
			desc: Description::new(lower, upper, since)
		}
	}
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct OrdKeyBatch<K: Ord+Hashable, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: Rc<OrderedLayer<K, UnorderedLayer<(T, R)>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, T, R> Batch<K, (), T, R> for OrdKeyBatch<K, T, R> 
where K: Ord+Clone+Hashable, T: Lattice+Ord+Clone, R: Ring {
	type Batcher = RadixBatcher<K, (), T, R, Self>;
	type Builder = OrdKeyBuilder<K, T, R>;
	type Cursor = OrdKeyCursor<K, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		OrdKeyCursor { empty: (), valid: true, cursor: self.layer.cursor() } 
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
		
		OrdKeyBatch {
			layer: Rc::new(self.layer.merge(&other.layer)),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		}
	}
}

impl<K: Ord+Hashable, T: Lattice+Ord+Clone, R> Clone for OrdKeyBatch<K, T, R> {
	fn clone(&self) -> Self {
		OrdKeyBatch {
			layer: self.layer.clone(),
			desc: self.desc.clone(),
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<K: Ord+Clone+Hashable, T: Lattice+Ord+Clone, R: Copy> {
	valid: bool,
	empty: (),
	cursor: OrderedCursor<K, UnorderedCursor<(T, R)>>,
}

impl<K: Ord+Clone+Hashable, T: Lattice+Ord+Clone, R: Copy> Cursor<K, (), T, R> for OrdKeyCursor<K, T, R> {
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
pub struct OrdKeyBuilder<K: Ord, T: Ord, R: Ring> {
	builder: OrderedBuilder<K, UnorderedBuilder<(T, R)>>,
}

impl<K, T, R> Builder<K, (), T, R, OrdKeyBatch<K, T, R>> for OrdKeyBuilder<K, T, R> 
where K: Ord+Clone+Hashable, T: Lattice+Ord+Clone, R: Ring {

	fn new() -> Self { 
		OrdKeyBuilder { 
			builder: OrderedBuilder::<K, UnorderedBuilder<(T, R)>>::new() 
		} 
	}

	fn with_capacity(cap: usize) -> Self {
		OrdKeyBuilder { 
			builder: OrderedBuilder::<K, UnorderedBuilder<(T, R)>>::with_capacity(cap) 
		} 
	}

	#[inline(always)]
	fn push(&mut self, (key, _, time, diff): (K, (), T, R)) {
		self.builder.push_tuple((key, (time, diff)));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> OrdKeyBatch<K, T, R> {
		OrdKeyBatch {
			layer: Rc::new(self.builder.done()),
			desc: Description::new(lower, upper, since)
		}
	}
}

