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

use ::Diff;
use lattice::Lattice;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder};
use trace::{Batch, BatchReader, Builder, Merger, Cursor};
use trace::description::Description;

use trace::layers::MergeBuilder;

use super::spine::Spine;
// use super::spine_fueled::Spine;
use super::merge_batcher::MergeBatcher;

/// A trace implementation using a spine of hash-map batches.
pub type OrdValSpine<K, V, T, R> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>>;
/// A trace implementation for empty values using a spine of hash-map batches.
pub type OrdKeySpine<K, T, R> = Spine<K, (), T, R, Rc<OrdKeyBatch<K, T, R>>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct OrdValBatch<K: Ord, V: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> BatchReader<K, V, T, R> for Rc<OrdValBatch<K, V, T, R>>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	type Cursor = OrdValCursor<V, T, R>;
	fn cursor(&self) -> (Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage) { 
		let cursor = OrdValCursor {
			cursor: self.layer.cursor()
		};

		(cursor, self.clone())
	}
	fn len(&self) -> usize { <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R> Batch<K, V, T, R> for Rc<OrdValBatch<K, V, T, R>>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
	type Batcher = MergeBatcher<K, V, T, R, Self>;
	type Builder = OrdValBuilder<K, V, T, R>;
	type Merger = OrdValMerger<K, V, T, R>;
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
		
		Rc::new(OrdValBatch {
			layer: <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::merge(&self.layer, &other.layer),  //self.layer.merge(&other.layer),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		})
	}
	fn begin_merge(&self, other: &Self) -> Self::Merger {
		OrdValMerger::new(self, other)
	}

	fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {

		let mut advanced = false;

		// let test = true;
		// let clone = if test { Some(self.advance_ref(frontier)) } else { None };

		if let Some(batch) = Rc::get_mut(self) {

			// We have unique ownership of the batch, and can advance times in place. 
			// We must still sort, collapse, and remove empty updates.

			// We will zip throught the time leaves, calling advance on each, 
			//    then zip through the value layer, sorting and collapsing each,
			//    then zip through the key layer, collapsing each .. ?

			// 1. For each (time, diff) pair, advance the time.
			for time_diff in &mut batch.layer.vals.vals.vals {
				time_diff.0 = time_diff.0.advance_by(frontier);
			}

			// 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
			//    This may leave `val` with an empty range; filtering happens in step 3.
			let mut write_position = 0;
			for i in 0 .. batch.layer.vals.keys.len() {

				// NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be changed.
				//     we will change batch.layer.vals.offs[i] in this iteration, from `write_position`'s
				//     initial value.

				let lower = batch.layer.vals.offs[i];
				let upper = batch.layer.vals.offs[i+1];

				batch.layer.vals.offs[i] = write_position;

				let updates = &mut batch.layer.vals.vals.vals[..];

				// sort the range by the times (ignore the diffs; they will collapse).
				updates[lower .. upper].sort_by(|x,y| x.0.cmp(&y.0));

				for index in lower .. (upper - 1) {
					if updates[index].0 == updates[index+1].0 {
						updates[index+1].1 = updates[index+1].1 + updates[index].1;
						updates[index].1 = R::zero();
					}
				}

				for index in lower .. upper {
					if !updates[index].1.is_zero() {
						updates.swap(write_position, index);
						write_position += 1;
					}
				}
			}
			batch.layer.vals.vals.vals.truncate(write_position);
			batch.layer.vals.offs[batch.layer.vals.keys.len()] = write_position;

			// 3. For each `(key, off)` pair, (values already sorted), filter vals, and rewrite `off`.
			//    This may leave `key` with an empty range. Filtering happens in step 4.
			let mut write_position = 0;
			for i in 0 .. batch.layer.keys.len() {

				// NB: batch.layer.offs[i+1] must remain as is for the next iteration. 
				//     instead, we update batch.layer.offs[i]

				let lower = batch.layer.offs[i];
				let upper = batch.layer.offs[i+1];

				batch.layer.offs[i] = write_position;

				// values should already be sorted, but some might now be empty.
				for index in lower .. upper {
					let val_lower = batch.layer.vals.offs[index];
					let val_upper = batch.layer.vals.offs[index+1];
					if val_lower < val_upper {
						batch.layer.vals.keys.swap(write_position, index);
						batch.layer.vals.offs[write_position+1] = batch.layer.vals.offs[index+1];
						write_position += 1;
					}
				}
				// batch.layer.offs[i+1] = write_position;
			}
			batch.layer.vals.keys.truncate(write_position);
			batch.layer.vals.offs.truncate(write_position + 1);
			batch.layer.offs[batch.layer.keys.len()] = write_position;

			// 4. Remove empty keys.
			let mut write_position = 0;
			for i in 0 .. batch.layer.keys.len() {

				let lower = batch.layer.offs[i];
				let upper = batch.layer.offs[i+1];

				if lower < upper {
					batch.layer.keys.swap(write_position, i);
					// batch.layer.offs updated via `dedup` below; keeps me sane.
					write_position += 1;
				}
			}
			batch.layer.offs.dedup();
			batch.layer.keys.truncate(write_position);
			batch.layer.offs.truncate(write_position+1);

			// if let Some(clone) = clone {
			// 	if !clone.layer.eq(&batch.layer) {
			// 		panic!("OrdValBatch::advance_mut: error");
			// 	}
			// }

			advanced = true;
		}

		if !advanced {
			*self = self.advance_ref(frontier);
		}
	}
}

/// State for an in-progress merge.
pub struct OrdValMerger<K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff> {
	// first batch, and position therein.
	batch1: Rc<OrdValBatch<K, V, T, R>>,
	lower1: usize,
	upper1: usize,
	// second batch, and position therein.
	batch2: Rc<OrdValBatch<K, V, T, R>>,
	lower2: usize,
	upper2: usize,
	// result that we are currently assembling.
	result: <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::MergeBuilder,
}

impl<K, V, T, R> OrdValMerger<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
	fn new(batch1: &Rc<OrdValBatch<K, V, T, R>>, batch2: &Rc<OrdValBatch<K, V, T, R>>) -> Self {
		OrdValMerger {
			batch1: batch1.clone(),
			lower1: 0,
			upper1: batch1.layer.keys(),
			batch2: batch2.clone(),
			lower2: 0,
			upper2: batch2.layer.keys(),
			result: <<OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
		}
	}
}

impl<K, V, T, R> Merger<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>> for OrdValMerger<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
	fn done(self) -> Rc<OrdValBatch<K, V, T, R>> {

		assert!(self.lower1 == self.upper1);
		assert!(self.lower2 == self.upper2);

		// Things are horribly wrong if this is not true.
		assert!(self.batch1.upper() == self.batch2.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.batch1.description().since().iter().all(|t1| self.batch2.description().since().iter().any(|t2| t2.less_equal(t1))) {
			self.batch2.description().since()
		}
		else {
			self.batch1.description().since()
		};

		let result1 = self.result.done();
		// let result2 = <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::merge(&self.batch1.layer, &self.batch2.layer);
		// assert!(result1 == result2);

		Rc::new(OrdValBatch {
			layer: result1,
			desc: Description::new(self.batch1.lower(), self.batch2.upper(), since),
		})
	}
	fn work(&mut self, fuel: &mut usize) {

		let starting_updates = self.result.vals.vals.vals.len();
		let mut effort = 0;

		// while both mergees are still active
		while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
			self.result.merge_step((&self.batch1.layer, &mut self.lower1, self.upper1), (&self.batch2.layer, &mut self.lower2, self.upper2));
			effort = self.result.vals.vals.vals.len() - starting_updates;
		}

		if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
			// these are just copies, so let's bite the bullet and just do them.
			if self.lower1 < self.upper1 { self.result.copy_range(&self.batch1.layer, self.lower1, self.upper1); self.lower1 = self.upper1; }
			if self.lower2 < self.upper2 { self.result.copy_range(&self.batch2.layer, self.lower2, self.upper2); self.lower2 = self.upper2; }
		}

		effort = self.result.vals.vals.vals.len() - starting_updates;

		if effort >= *fuel { *fuel = 0; }
		else 			   { *fuel -= effort; }
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdValCursor<V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff> {
	cursor: OrderedCursor<OrderedLayer<V, OrderedLeaf<T, R>>>,
}

impl<K, V, T, R> Cursor<K, V, T, R> for OrdValCursor<V, T, R> 
where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {

	type Storage = Rc<OrdValBatch<K, V, T, R>>;

	fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
	fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { &self.cursor.child.key(&storage.layer.vals) }
	fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
		self.cursor.child.child.rewind(&storage.layer.vals.vals);
		while self.cursor.child.child.valid(&storage.layer.vals.vals) {
			logic(&self.cursor.child.child.key(&storage.layer.vals.vals).0, self.cursor.child.child.key(&storage.layer.vals.vals).1);
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
pub struct OrdValBuilder<K: Ord, V: Ord, T: Ord+Lattice, R: Diff> {
	builder: OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>> for OrdValBuilder<K, V, T, R> 
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {

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
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<OrdValBatch<K, V, T, R>> {
		Rc::new(OrdValBatch {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		})
	}
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct OrdKeyBatch<K: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: OrderedLayer<K, OrderedLeaf<T, R>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, T, R> BatchReader<K, (), T, R> for Rc<OrdKeyBatch<K, T, R>>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	type Cursor = OrdKeyCursor<T, R>;
	fn cursor(&self) -> (Self::Cursor, <Self::Cursor as Cursor<K, (), T, R>>::Storage) { 
		let cursor = OrdKeyCursor {
			empty: (),
			valid: true,
			cursor: self.layer.cursor(),
		};
		(cursor, self.clone())
	}
	fn len(&self) -> usize { <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, T, R> Batch<K, (), T, R> for Rc<OrdKeyBatch<K, T, R>>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	type Batcher = MergeBatcher<K, (), T, R, Self>;
	type Builder = OrdKeyBuilder<K, T, R>;
	type Merger = OrdKeyMerger<K, T, R>;
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
		
		Rc::new(OrdKeyBatch {
			layer: <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::merge(&self.layer, &other.layer),
			desc: Description::new(self.desc.lower(), other.desc.upper(), since),
		})
	}
	fn begin_merge(&self, other: &Self) -> Self::Merger {
		OrdKeyMerger::new(self, other)
	}

	// TODO: The following looks good to me, but causes a perf reduction in Eintopf when I uncomment it.
	//       This could be for many reasons, including never getting the benefits and me being wrong about
	//       how much things cost. Until that gets sorted out, let's just admire the code rather than use it.

	fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {
	
		let mut advanced = false;
	
		// let test = true;
		// let clone = if test { Some(self.advance_ref(frontier)) } else { None };
	
		if let Some(batch) = Rc::get_mut(self) {
	
			// We have unique ownership of the batch, and can advance times in place. 
			// We must still sort, collapse, and remove empty updates.
	
			// We will zip through the time leaves, calling advance on each, 
			//    then zip through the value layer, sorting and collapsing each,
			//    then zip through the key layer, collapsing each .. ?
	
			// 1. For each (time, diff) pair, advance the time.
			for time_diff in &mut batch.layer.vals.vals {
				time_diff.0 = time_diff.0.advance_by(frontier);
			}
	
			// 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
			//    This may leave `val` with an empty range; filtering happens in step 3.
			let mut write_position = 0;
			for i in 0 .. batch.layer.keys.len() {
	
				// NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be changed.
				//     we will change batch.layer.vals.offs[i] in this iteration, from `write_position`'s
				//     initial value.
	
				let lower = batch.layer.offs[i];
				let upper = batch.layer.offs[i+1];
	
				batch.layer.offs[i] = write_position;
	
				let updates = &mut batch.layer.vals.vals[..];

				// sort the range by the times (ignore the diffs; they will collapse).
				updates[lower .. upper].sort_by(|x,y| x.0.cmp(&y.0));
	
				for index in lower .. (upper - 1) {
					if updates[index].0 == updates[index+1].0 {
						updates[index+1].1 = updates[index].1 + updates[index+1].1;
						updates[index].1 = R::zero();
					}
				}
	
				for index in lower .. upper {
					if !updates[index].1.is_zero() {
						updates.swap(write_position, index);
						write_position += 1;
					}
				}
			}
			batch.layer.vals.vals.truncate(write_position);
			batch.layer.offs[batch.layer.keys.len()] = write_position;
	
			// 4. Remove empty keys.
			let mut write_position = 0;
			for i in 0 .. batch.layer.keys.len() {
	
				let lower = batch.layer.offs[i];
				let upper = batch.layer.offs[i+1];
	
				if lower < upper {
					batch.layer.keys.swap(write_position, i);
					// batch.layer.offs updated via `dedup` below; keeps me sane.
					write_position += 1;
				}
			}
			batch.layer.offs.dedup();
			batch.layer.keys.truncate(write_position);
			batch.layer.offs.truncate(write_position+1);
	
			// if let Some(clone) = clone {
			// 	if !clone.layer.eq(&batch.layer) {
			// 		panic!("OrdKeyBatch::advance_mut: error");
			// 	}
			// }
	
			advanced = true;
		}
	
		if !advanced {
			*self = self.advance_ref(frontier);
		}
	}
}

/// State for an in-progress merge.
pub struct OrdKeyMerger<K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff> {
	// first batch, and position therein.
	batch1: Rc<OrdKeyBatch<K, T, R>>,
	lower1: usize,
	upper1: usize,
	// second batch, and position therein.
	batch2: Rc<OrdKeyBatch<K, T, R>>,
	lower2: usize,
	upper2: usize,
	// result that we are currently assembling.
	result: <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::MergeBuilder,
}

impl<K, T, R> OrdKeyMerger<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	fn new(batch1: &Rc<OrdKeyBatch<K, T, R>>, batch2: &Rc<OrdKeyBatch<K, T, R>>) -> Self {
		OrdKeyMerger {
			batch1: batch1.clone(),
			lower1: 0,
			upper1: batch1.layer.keys(),
			batch2: batch2.clone(),
			lower2: 0,
			upper2: batch2.layer.keys(),
			result: <<OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
		}
	}
}

impl<K, T, R> Merger<K, (), T, R, Rc<OrdKeyBatch<K, T, R>>> for OrdKeyMerger<K, T, R>
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	fn done(self) -> Rc<OrdKeyBatch<K, T, R>> {

		assert!(self.lower1 == self.upper1);
		assert!(self.lower2 == self.upper2);

		// Things are horribly wrong if this is not true.
		assert!(self.batch1.upper() == self.batch2.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.batch1.description().since().iter().all(|t1| self.batch2.description().since().iter().any(|t2| t2.less_equal(t1))) {
			self.batch2.description().since()
		}
		else {
			self.batch1.description().since()
		};

		let result1 = self.result.done();
		// let result2 = <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::merge(&self.batch1.layer, &self.batch2.layer);
		// assert!(result1 == result2);

		Rc::new(OrdKeyBatch {
			layer: result1,
			desc: Description::new(self.batch1.lower(), self.batch2.upper(), since),
		})
	}
	fn work(&mut self, fuel: &mut usize) {

		let starting_updates = self.result.vals.vals.len();
		let mut effort = 0;

		// while both mergees are still active
		while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
			self.result.merge_step((&self.batch1.layer, &mut self.lower1, self.upper1), (&self.batch2.layer, &mut self.lower2, self.upper2));
			effort = self.result.vals.vals.len() - starting_updates;
		}

		if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
			// these are just copies, so let's bite the bullet and just do them.
			if self.lower1 < self.upper1 { self.result.copy_range(&self.batch1.layer, self.lower1, self.upper1); self.lower1 = self.upper1; }
			if self.lower2 < self.upper2 { self.result.copy_range(&self.batch2.layer, self.lower2, self.upper2); self.lower2 = self.upper2; }
		}

		effort = self.result.vals.vals.len() - starting_updates;

		if effort >= *fuel { *fuel = 0; }
		else 			   { *fuel -= effort; }
	}
}


/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<T: Lattice+Ord+Clone, R: Diff> {
	valid: bool,
	empty: (),
	cursor: OrderedCursor<OrderedLeaf<T, R>>,
}

impl<K: Ord+Clone, T: Lattice+Ord+Clone, R: Diff> Cursor<K, (), T, R> for OrdKeyCursor<T, R> {

	type Storage = Rc<OrdKeyBatch<K, T, R>>;

	fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
	fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () { unsafe { ::std::mem::transmute(&self.empty) } }
	fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
		self.cursor.child.rewind(&storage.layer.vals);
		while self.cursor.child.valid(&storage.layer.vals) {
			logic(&self.cursor.child.key(&storage.layer.vals).0, self.cursor.child.key(&storage.layer.vals).1);
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
pub struct OrdKeyBuilder<K: Ord, T: Ord+Lattice, R: Diff> {
	builder: OrderedBuilder<K, OrderedLeafBuilder<T, R>>,
}

impl<K, T, R> Builder<K, (), T, R, Rc<OrdKeyBatch<K, T, R>>> for OrdKeyBuilder<K, T, R> 
where K: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {

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
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<OrdKeyBatch<K, T, R>> {
		Rc::new(OrdKeyBatch {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		})
	}
}

