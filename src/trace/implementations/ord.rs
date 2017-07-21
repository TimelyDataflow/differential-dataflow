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
use owning_ref::OwningRef;

use ::Diff;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder, OrderedLeafCursor};

use lattice::Lattice;
use trace::{Batch, BatchReader, Builder, Cursor};
use trace::description::Description;

use super::spine::Spine;
use super::batcher::RadixBatcher;

/// A trace implementation using a spine of hash-map batches.
pub type OrdValSpine<K, V, T, R> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>>;
/// A trace implementation for empty values using a spine of hash-map batches.
pub type OrdKeySpine<K, T, R> = Spine<K, (), T, R, Rc<OrdKeyBatch<K, T, R>>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct OrdValBatch<K: Ord+HashOrdered, V: Ord, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> BatchReader<K, V, T, R> for Rc<OrdValBatch<K, V, T, R>>
where K: Ord+Clone+HashOrdered+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	type Cursor = OrdValCursor<K, V, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		OrdValCursor {
			cursor: self.layer.cursor(OwningRef::new(self.clone()).map(|x| &x.layer).erase_owner())
		}
	}
	fn len(&self) -> usize { <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R> Batch<K, V, T, R> for Rc<OrdValBatch<K, V, T, R>>
where K: Ord+Clone+HashOrdered+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
	type Batcher = RadixBatcher<K, V, T, R, Self>;
	type Builder = OrdValBuilder<K, V, T, R>;
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

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdValCursor<K: Ord+Clone+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy> {
	cursor: OrderedCursor<K, OrderedCursor<V, OrderedLeafCursor<T, R>>>,
}

impl<K, V, T, R> Cursor<K, V, T, R> for OrdValCursor<K, V, T, R> 
where K: Ord+Clone+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone, R: Copy {
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
pub struct OrdValBuilder<K: Ord+HashOrdered, V: Ord, T: Ord+Lattice, R: Diff> {
	builder: OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>> for OrdValBuilder<K, V, T, R> 
where K: Ord+Clone+HashOrdered+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {

	fn new() -> Self { 
		OrdValBuilder { 
			builder: OrderedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>::new() 
		} 
	}
	fn with_capacity(cap: usize) -> Self { 
		OrdValBuilder { 
			builder: OrderedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>::with_capacity(cap) 
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
pub struct OrdKeyBatch<K: Ord+HashOrdered, T: Lattice, R> {
	/// Where all the dataz is.
	pub layer: OrderedLayer<K, OrderedLeaf<T, R>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, T, R> BatchReader<K, (), T, R> for Rc<OrdKeyBatch<K, T, R>>
where K: Ord+Clone+HashOrdered+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	type Cursor = OrdKeyCursor<K, T, R>;
	fn cursor(&self) -> Self::Cursor { 
		OrdKeyCursor {
			empty: (),
			valid: true,
			cursor: self.layer.cursor(OwningRef::new(self.clone()).map(|x| &x.layer).erase_owner()),
		} 
	}
	fn len(&self) -> usize { <OrderedLayer<K, OrderedLeaf<T, R>> as Trie>::tuples(&self.layer) }
	fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, T, R> Batch<K, (), T, R> for Rc<OrdKeyBatch<K, T, R>>
where K: Ord+Clone+HashOrdered+'static, T: Lattice+Ord+Clone+'static, R: Diff {
	type Batcher = RadixBatcher<K, (), T, R, Self>;
	type Builder = OrdKeyBuilder<K, T, R>;
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

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<K: Ord+Clone+HashOrdered, T: Lattice+Ord+Clone, R: Copy> {
	valid: bool,
	empty: (),
	cursor: OrderedCursor<K, OrderedLeafCursor<T, R>>,
}

impl<K: Ord+Clone+HashOrdered, T: Lattice+Ord+Clone, R: Copy> Cursor<K, (), T, R> for OrdKeyCursor<K, T, R> {
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
pub struct OrdKeyBuilder<K: Ord+HashOrdered, T: Ord+Lattice, R: Diff> {
	builder: OrderedBuilder<K, OrderedLeafBuilder<T, R>>,
}

impl<K, T, R> Builder<K, (), T, R, Rc<OrdKeyBatch<K, T, R>>> for OrdKeyBuilder<K, T, R> 
where K: Ord+Clone+HashOrdered+'static, T: Lattice+Ord+Clone+'static, R: Diff {

	fn new() -> Self { 
		OrdKeyBuilder { 
			builder: OrderedBuilder::<K, OrderedLeafBuilder<T, R>>::new() 
		} 
	}

	fn with_capacity(cap: usize) -> Self {
		OrdKeyBuilder { 
			builder: OrderedBuilder::<K, OrderedLeafBuilder<T, R>>::with_capacity(cap) 
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

