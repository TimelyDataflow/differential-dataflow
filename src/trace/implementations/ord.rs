//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with either
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key` is ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is ordered.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

use std::rc::Rc;

use ::difference::Monoid;
use lattice::Lattice;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder};
use trace::{Batch, BatchReader, Builder, Merger, Cursor};
use trace::description::Description;

use trace::layers::MergeBuilder;

// use super::spine::Spine;
use super::spine_fueled::Spine;
use super::merge_batcher::MergeBatcher;

use abomonation::abomonated::Abomonated;

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>>;

/// A trace implementation using a spine of abomonated ordered lists.
pub type OrdValSpineAbom<K, V, T, R> = Spine<K, V, T, R, Rc<Abomonated<OrdValBatch<K, V, T, R>, Vec<u8>>>>;

/// A trace implementation for empty values using a spine of ordered lists.
pub type OrdKeySpine<K, T, R> = Spine<K, (), T, R, Rc<OrdKeyBatch<K, T, R>>>;

/// A trace implementation for empty values using a spine of abomonated ordered lists.
pub type OrdKeySpineAbom<K, T, R> = Spine<K, (), T, R, Rc<Abomonated<OrdKeyBatch<K, T, R>, Vec<u8>>>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug, Abomonation)]
pub struct OrdValBatch<K: Ord, V: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> BatchReader<K, V, T, R> for OrdValBatch<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid {
	type Cursor = OrdValCursor<V, T, R>;
	fn cursor(&self) -> Self::Cursor { OrdValCursor { cursor: self.layer.cursor() } }
	fn len(&self) -> usize { <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R> Batch<K, V, T, R> for OrdValBatch<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Monoid {
	type Batcher = MergeBatcher<K, V, T, R, Self>;
	type Builder = OrdValBuilder<K, V, T, R>;
	type Merger = OrdValMerger<K, V, T, R>;

	fn begin_merge(&self, other: &Self) -> Self::Merger {
		OrdValMerger::new(self, other)
	}
}

impl<K, V, T, R> OrdValBatch<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Monoid {
	fn advance_builder_from(layer: &mut OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>, frontier: &[T], key_pos: usize) {

		let key_start = key_pos;
		let val_start = layer.offs[key_pos];
		let time_start = layer.vals.offs[val_start];

		// We have unique ownership of the batch, and can advance times in place.
		// We must still sort, collapse, and remove empty updates.

		// We will zip throught the time leaves, calling advance on each,
		//    then zip through the value layer, sorting and collapsing each,
		//    then zip through the key layer, collapsing each .. ?

		// 1. For each (time, diff) pair, advance the time.
		for i in time_start .. layer.vals.vals.vals.len() {
			layer.vals.vals.vals[i].0 = layer.vals.vals.vals[i].0.advance_by(frontier);
		}

		// 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
		//    This may leave `val` with an empty range; filtering happens in step 3.
		let mut write_position = time_start;
		for i in val_start .. layer.vals.keys.len() {

			// NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be changed.
			//     we will change batch.layer.vals.offs[i] in this iteration, from `write_position`'s
			//     initial value.

			let lower = layer.vals.offs[i];
			let upper = layer.vals.offs[i+1];

			layer.vals.offs[i] = write_position;

			let updates = &mut layer.vals.vals.vals[..];

			// sort the range by the times (ignore the diffs; they will collapse).
			updates[lower .. upper].sort_by(|x,y| x.0.cmp(&y.0));

			for index in lower .. (upper - 1) {
				if updates[index].0 == updates[index+1].0 {
					let prev = ::std::mem::replace(&mut updates[index].1, R::zero());
					updates[index+1].1 += prev;
				}
			}

			for index in lower .. upper {
				if !updates[index].1.is_zero() {
					updates.swap(write_position, index);
					write_position += 1;
				}
			}
		}
		layer.vals.vals.vals.truncate(write_position);
		layer.vals.offs[layer.vals.keys.len()] = write_position;

		// 3. For each `(key, off)` pair, (values already sorted), filter vals, and rewrite `off`.
		//    This may leave `key` with an empty range. Filtering happens in step 4.
		let mut write_position = val_start;
		for i in key_start .. layer.keys.len() {

			// NB: batch.layer.offs[i+1] must remain as is for the next iteration.
			//     instead, we update batch.layer.offs[i]

			let lower = layer.offs[i];
			let upper = layer.offs[i+1];

			layer.offs[i] = write_position;

			// values should already be sorted, but some might now be empty.
			for index in lower .. upper {
				let val_lower = layer.vals.offs[index];
				let val_upper = layer.vals.offs[index+1];
				if val_lower < val_upper {
					layer.vals.keys.swap(write_position, index);
					layer.vals.offs[write_position+1] = layer.vals.offs[index+1];
					write_position += 1;
				}
			}
			// batch.layer.offs[i+1] = write_position;
		}
		layer.vals.keys.truncate(write_position);
		layer.vals.offs.truncate(write_position + 1);
		layer.offs[layer.keys.len()] = write_position;

		// 4. Remove empty keys.
		let mut write_position = key_start;
		for i in key_start .. layer.keys.len() {

			let lower = layer.offs[i];
			let upper = layer.offs[i+1];

			if lower < upper {
				layer.keys.swap(write_position, i);
				// batch.layer.offs updated via `dedup` below; keeps me sane.
				write_position += 1;
			}
		}
		layer.offs.dedup();
		layer.keys.truncate(write_position);
		layer.offs.truncate(write_position+1);
	}
}

/// State for an in-progress merge.
pub struct OrdValMerger<K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Monoid> {
	// first batch, and position therein.
	lower1: usize,
	upper1: usize,
	// second batch, and position therein.
	lower2: usize,
	upper2: usize,
	// result that we are currently assembling.
	result: <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::MergeBuilder,
	description: Description<T>,
}

impl<K, V, T, R> Merger<K, V, T, R, OrdValBatch<K, V, T, R>> for OrdValMerger<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Monoid {
	fn new(batch1: &OrdValBatch<K, V, T, R>, batch2: &OrdValBatch<K, V, T, R>) -> Self {

		assert!(batch1.upper() == batch2.lower());

		let since = if batch1.description().since().iter().all(|t1| batch2.description().since().iter().any(|t2| t2.less_equal(t1))) {
			batch2.description().since()
		}
		else {
			batch1.description().since()
		};

		let description = Description::new(batch1.lower(), batch2.upper(), since);

		OrdValMerger {
			lower1: 0,
			upper1: batch1.layer.keys(),
			lower2: 0,
			upper2: batch2.layer.keys(),
			result: <<OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
			description: description,
		}
	}
	fn done(self) -> OrdValBatch<K, V, T, R> {

		assert!(self.lower1 == self.upper1);
		assert!(self.lower2 == self.upper2);

		OrdValBatch {
			layer: self.result.done(),
			desc: self.description,
		}
	}
	fn work(&mut self, source1: &OrdValBatch<K,V,T,R>, source2: &OrdValBatch<K,V,T,R>, frontier: &Option<Vec<T>>, fuel: &mut usize) {

		let starting_updates = self.result.vals.vals.vals.len();
		let mut effort = 0;

		let initial_key_pos = self.result.keys.len();

		// while both mergees are still active
		while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
			self.result.merge_step((&source1.layer, &mut self.lower1, self.upper1), (&source2.layer, &mut self.lower2, self.upper2));
			effort = self.result.vals.vals.vals.len() - starting_updates;
		}

		if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
			// these are just copies, so let's bite the bullet and just do them.
			if self.lower1 < self.upper1 { self.result.copy_range(&source1.layer, self.lower1, self.upper1); self.lower1 = self.upper1; }
			if self.lower2 < self.upper2 { self.result.copy_range(&source2.layer, self.lower2, self.upper2); self.lower2 = self.upper2; }
		}

		effort = self.result.vals.vals.vals.len() - starting_updates;

		// if we are supplied a frontier, we should compact.
		if let Some(frontier) = frontier.as_ref() {
			OrdValBatch::advance_builder_from(&mut self.result, frontier, initial_key_pos)
		}

		if effort >= *fuel { *fuel = 0; }
		else 			   { *fuel -= effort; }
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdValCursor<V: Ord+Clone, T: Lattice+Ord+Clone, R: Monoid> {
	cursor: OrderedCursor<OrderedLayer<V, OrderedLeaf<T, R>>>,
}

impl<K, V, T, R> Cursor<K, V, T, R> for OrdValCursor<V, T, R>
where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Monoid {

	type Storage = OrdValBatch<K, V, T, R>;

	fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
	fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { &self.cursor.child.key(&storage.layer.vals) }
	fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
		self.cursor.child.child.rewind(&storage.layer.vals.vals);
		while self.cursor.child.child.valid(&storage.layer.vals.vals) {
			logic(&self.cursor.child.child.key(&storage.layer.vals.vals).0, self.cursor.child.child.key(&storage.layer.vals.vals).1.clone());
			self.cursor.child.child.step(&storage.layer.vals.vals);
		}
	}
	fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
	fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.child.valid(&storage.layer.vals) }
	fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); }
	fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek(&storage.layer, key); }
	fn step_val(&mut self, storage: &Self::Storage) { self.cursor.child.step(&storage.layer.vals); }
	fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.child.seek(&storage.layer.vals, val); }
	fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); }
	fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.child.rewind(&storage.layer.vals); }
}


/// A builder for creating layers from unsorted update tuples.
pub struct OrdValBuilder<K: Ord, V: Ord, T: Ord+Lattice, R: Monoid> {
	builder: OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, OrdValBatch<K, V, T, R>> for OrdValBuilder<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Monoid {

	fn new() -> Self {
		OrdValBuilder {
			builder: OrderedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>::new()
		}
	}
	fn with_capacity(cap: usize) -> Self {
		OrdValBuilder {
			builder: <OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>> as TupleBuilder>::with_capacity(cap)
		}
	}

	#[inline(always)]
	fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
		self.builder.push_tuple((key, (val, (time, diff))));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> OrdValBatch<K, V, T, R> {
		OrdValBatch {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		}
	}
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug, Abomonation)]
pub struct OrdKeyBatch<K: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: OrderedLayer<K, OrderedLeaf<T, R>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, T, R> BatchReader<K, (), T, R> for OrdKeyBatch<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid {
	type Cursor = OrdKeyCursor<T, R>;
	fn cursor(&self) -> Self::Cursor {
		OrdKeyCursor {
			empty: (),
			valid: true,
			cursor: self.layer.cursor(),
		}
	}
	fn len(&self) -> usize { <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, T, R> Batch<K, (), T, R> for OrdKeyBatch<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid {
	type Batcher = MergeBatcher<K, (), T, R, Self>;
	type Builder = OrdKeyBuilder<K, T, R>;
	type Merger = OrdKeyMerger<K, T, R>;

	fn begin_merge(&self, other: &Self) -> Self::Merger {
		OrdKeyMerger::new(self, other)
	}
}

impl<K, T, R> OrdKeyBatch<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid {
	fn advance_builder_from(layer: &mut OrderedBuilder<K, OrderedLeafBuilder<T, R>>, frontier: &[T], key_pos: usize) {

		let key_start = key_pos;
		let time_start = layer.offs[key_pos];

		// We will zip through the time leaves, calling advance on each,
		//    then zip through the value layer, sorting and collapsing each,
		//    then zip through the key layer, collapsing each .. ?

		// 1. For each (time, diff) pair, advance the time.
		for i in time_start .. layer.vals.vals.len() {
			layer.vals.vals[i].0 = layer.vals.vals[i].0.advance_by(frontier);
		}
		// for time_diff in self.layer.vals.vals.iter_mut() {
		// 	time_diff.0 = time_diff.0.advance_by(frontier);
		// }

		// 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
		//    This may leave `val` with an empty range; filtering happens in step 3.
		let mut write_position = time_start;
		for i in key_start .. layer.keys.len() {

			// NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be changed.
			//     we will change batch.layer.vals.offs[i] in this iteration, from `write_position`'s
			//     initial value.

			let lower = layer.offs[i];
			let upper = layer.offs[i+1];

			layer.offs[i] = write_position;

			let updates = &mut layer.vals.vals[..];

			// sort the range by the times (ignore the diffs; they will collapse).
			updates[lower .. upper].sort_by(|x,y| x.0.cmp(&y.0));

			for index in lower .. (upper - 1) {
				if updates[index].0 == updates[index+1].0 {
					let prev = ::std::mem::replace(&mut updates[index].1, R::zero());
					updates[index + 1].1 += prev;
				}
			}

			for index in lower .. upper {
				if !updates[index].1.is_zero() {
					updates.swap(write_position, index);
					write_position += 1;
				}
			}
		}
		layer.vals.vals.truncate(write_position);
		layer.offs[layer.keys.len()] = write_position;

		// 4. Remove empty keys.
		let mut write_position = key_start;
		for i in key_start .. layer.keys.len() {

			let lower = layer.offs[i];
			let upper = layer.offs[i+1];

			if lower < upper {
				layer.keys.swap(write_position, i);
				// batch.layer.offs updated via `dedup` below; keeps me sane.
				write_position += 1;
			}
		}
		layer.offs.dedup();
		layer.keys.truncate(write_position);
		layer.offs.truncate(write_position+1);
	}
}

/// State for an in-progress merge.
pub struct OrdKeyMerger<K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid> {
	// first batch, and position therein.
	lower1: usize,
	upper1: usize,
	// second batch, and position therein.
	lower2: usize,
	upper2: usize,
	// result that we are currently assembling.
	result: <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::MergeBuilder,
	description: Description<T>,
}

impl<K, T, R> Merger<K, (), T, R, OrdKeyBatch<K, T, R>> for OrdKeyMerger<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid {
	fn new(batch1: &OrdKeyBatch<K, T, R>, batch2: &OrdKeyBatch<K, T, R>) -> Self {

		assert!(batch1.upper() == batch2.lower());

		let since = if batch1.description().since().iter().all(|t1| batch2.description().since().iter().any(|t2| t2.less_equal(t1))) {
			batch2.description().since()
		}
		else {
			batch1.description().since()
		};

		let description = Description::new(batch1.lower(), batch2.upper(), since);

		OrdKeyMerger {
			lower1: 0,
			upper1: batch1.layer.keys(),
			lower2: 0,
			upper2: batch2.layer.keys(),
			result: <<OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
			description: description,
		}
	}
	fn done(self) -> OrdKeyBatch<K, T, R> {

		assert!(self.lower1 == self.upper1);
		assert!(self.lower2 == self.upper2);

		OrdKeyBatch {
			layer: self.result.done(),
			desc: self.description,
		}
	}
	fn work(&mut self, source1: &OrdKeyBatch<K,T,R>, source2: &OrdKeyBatch<K,T,R>, frontier: &Option<Vec<T>>, fuel: &mut usize) {

		let starting_updates = self.result.vals.vals.len();
		let mut effort = 0;

		let initial_key_pos = self.result.keys.len();

		// while both mergees are still active
		while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
			self.result.merge_step((&source1.layer, &mut self.lower1, self.upper1), (&source2.layer, &mut self.lower2, self.upper2));
			effort = self.result.vals.vals.len() - starting_updates;
		}

		if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
			// these are just copies, so let's bite the bullet and just do them.
			if self.lower1 < self.upper1 { self.result.copy_range(&source1.layer, self.lower1, self.upper1); self.lower1 = self.upper1; }
			if self.lower2 < self.upper2 { self.result.copy_range(&source2.layer, self.lower2, self.upper2); self.lower2 = self.upper2; }
		}

		effort = self.result.vals.vals.len() - starting_updates;

		if let Some(frontier) = frontier.as_ref() {
			OrdKeyBatch::advance_builder_from(&mut self.result, frontier, initial_key_pos);
		}

		if effort >= *fuel { *fuel = 0; }
		else 			   { *fuel -= effort; }
	}
}


/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<T: Lattice+Ord+Clone, R: Monoid> {
	valid: bool,
	empty: (),
	cursor: OrderedCursor<OrderedLeaf<T, R>>,
}

impl<K: Ord+Clone, T: Lattice+Ord+Clone, R: Monoid> Cursor<K, (), T, R> for OrdKeyCursor<T, R> {

	type Storage = OrdKeyBatch<K, T, R>;

	fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
	fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () { unsafe { ::std::mem::transmute(&self.empty) } }
	fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
		self.cursor.child.rewind(&storage.layer.vals);
		while self.cursor.child.valid(&storage.layer.vals) {
			logic(&self.cursor.child.key(&storage.layer.vals).0, self.cursor.child.key(&storage.layer.vals).1.clone());
			self.cursor.child.step(&storage.layer.vals);
		}
	}
	fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
	fn val_valid(&self, _storage: &Self::Storage) -> bool { self.valid }
	fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); self.valid = true; }
	fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek(&storage.layer, key); self.valid = true; }
	fn step_val(&mut self, _storage: &Self::Storage) { self.valid = false; }
	fn seek_val(&mut self, _storage: &Self::Storage, _val: &()) { }
	fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); self.valid = true; }
	fn rewind_vals(&mut self, _storage: &Self::Storage) { self.valid = true; }
}


/// A builder for creating layers from unsorted update tuples.
pub struct OrdKeyBuilder<K: Ord, T: Ord+Lattice, R: Monoid> {
	builder: OrderedBuilder<K, OrderedLeafBuilder<T, R>>,
}

impl<K, T, R> Builder<K, (), T, R, OrdKeyBatch<K, T, R>> for OrdKeyBuilder<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Monoid {

	fn new() -> Self {
		OrdKeyBuilder {
			builder: OrderedBuilder::<K, OrderedLeafBuilder<T, R>>::new()
		}
	}

	fn with_capacity(cap: usize) -> Self {
		OrdKeyBuilder {
			builder: <OrderedBuilder<K, OrderedLeafBuilder<T, R>> as TupleBuilder>::with_capacity(cap)
		}
	}

	#[inline(always)]
	fn push(&mut self, (key, _, time, diff): (K, (), T, R)) {
		self.builder.push_tuple((key, (time, diff)));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> OrdKeyBatch<K, T, R> {
		OrdKeyBatch {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		}
	}
}

