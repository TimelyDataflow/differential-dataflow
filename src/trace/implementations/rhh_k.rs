//! A trie-structured representation of update tuples with hashable keys. 
//! 
//! One goal of this representation is to allow multiple kinds of types of hashable keys, including
//! keys that implement `Hash`, keys whose hashes have been computed and are stashed with the key, and
//! integers keys which are promised to be random enough to be used as the hashes themselves.
use std::rc::Rc;
use std::fmt::Debug;

use timely::progress::frontier::Antichain;
use timely_sort::{MSBRadixSorter, RadixSorterBase};


use ::Ring;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::hashed::{HashedLayer, HashedBuilder, HashedCursor};
use trace::layers::unordered::{UnorderedLayer, UnorderedBuilder, UnorderedCursor};

use lattice::Lattice;
use trace::{Batch, Batcher, Builder, Cursor, Trace};
use trace::consolidate;
use trace::description::Description;
use trace::cursor::cursor_list::CursorList;

type RHHBuilder<Key, Time, R> = HashedBuilder<Key, UnorderedBuilder<(Time, R)>>;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
#[derive(Debug)]
pub struct Spine<Key: HashOrdered, Time: Lattice+Ord+Debug, R: Ring> {
	advance_frontier: Vec<Time>,			// Times after which the trace must accumulate correctly.
	through_frontier: Vec<Time>,			// Times after which the trace must be able to subset its inputs.
	merging: Vec<Rc<Layer<Key, Time, R>>>,	// Several possibly shared collections of updates.
	pending: Vec<Rc<Layer<Key, Time, R>>>,	// Layers at times in advance of `frontier`.
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and layer types.
impl<Key, Time, R> Trace<Key, (), Time, R> for Spine<Key, Time, R> 
where 
	Key: Clone+Default+HashOrdered+'static,
	Time: Lattice+Ord+Clone+Default+Debug+'static,
	R: Ring,
{

	type Batch = Rc<Layer<Key, Time, R>>;
	type Cursor = CursorList<Key, (), Time, R, LayerCursor<Key, Time, R>>;

	fn new() -> Self {
		Spine { 
			advance_frontier: vec![<Time as Lattice>::min()],
			through_frontier: vec![<Time as Lattice>::min()],
			merging: Vec::new(),
			pending: Vec::new(),
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {

		// we can ignore degenerate layers (TODO: learn where they come from; suppress them?)
		if layer.desc.lower() != layer.desc.upper() {
			self.pending.push(layer);
			self.consider_merges();
		}
		else {
			// degenerate layers had best be empty.
			assert!(layer.len() == 0);
		}
	}
	fn cursor_through(&self, upper: &[Time]) -> Option<Self::Cursor> {

		// Check that `upper` is greater or equal to `self.through_frontier`.
		// Otherwise, the cut could be in `self.merging` and it is user error anyhow.
		let upper_valid = upper.iter().all(|t1| self.through_frontier.iter().any(|t2| t2.le(t1)));

		// Find the position of the batch with upper limit `upper`. It is possible we 
		// find nothing (e.g. when `upper` is `&[]`, or when `self.pending` is empty),
		// in which case we must test that the last upper limit is less or equal to `upper`. 
		let count = self.pending.iter()
								.position(|batch| batch.desc.upper() == upper)
								.unwrap_or(self.pending.len());

		// The count is valid if either pending is empty, or the identified location's upper limit is not after `upper`.
		let count_valid = self.pending.len() == 0 || upper.iter().all(|t1| self.pending[count].desc.upper().iter().any(|t2| t2.le(t1)));

		// We can return a cursor if `upper` is valid, and if `count` reflects a valid cut point.
		if upper_valid && count_valid {

			let mut cursors = Vec::new();
			for layer in &self.merging[..] {
				if layer.len() > 0 {
					cursors.push(layer.cursor());
				}
			}
			for batch in &self.pending[..count] {
				if batch.len() > 0 {
					cursors.push(batch.cursor());
				}
			}
			Some(CursorList::new(cursors))

		}
		else {
			None
		}

	}
	fn advance_by(&mut self, frontier: &[Time]) {
		self.advance_frontier = frontier.to_vec();
	}
	fn distinguish_since(&mut self, frontier: &[Time]) {
		self.through_frontier = frontier.to_vec();
		self.consider_merges();
	}
}

impl<Key, Time, R> Spine<Key, Time, R> 
where 
	Key: Clone+Default+HashOrdered+'static,
	Time: Lattice+Ord+Clone+Default+Debug+'static,
	R: Ring,
{
	// Migrate data from `self.pending` into `self.merging`.
	fn consider_merges(&mut self) {

		// TODO: We could consider merging in batches here, rather than in sequence. Little is known...
		while self.pending.len() > 0 && self.through_frontier.iter().all(|t1| self.pending[0].description().upper().iter().any(|t2| t2.le(t1))) {

			let layer = self.pending.remove(0);

			// while last two elements exist, both less than layer.len()
			while self.merging.len() >= 2 && self.merging[self.merging.len() - 2].len() < layer.len() {
				let layer1 = self.merging.pop().unwrap();
				let layer2 = self.merging.pop().unwrap();
				let result = layer2.merge(&layer1).unwrap();
				self.merging.push(result);
			}

			self.merging.push(layer);

		    while self.merging.len() >= 2 && self.merging[self.merging.len() - 2].len() < 2 * self.merging[self.merging.len() - 1].len() {
				let layer1 = self.merging.pop().unwrap();
				let layer2 = self.merging.pop().unwrap();
				let mut result = layer2.merge(&layer1).unwrap();

				// if we just merged the last layer, `advance_by` it.
				if self.merging.len() == 0 {
					result = Rc::new(Layer::<Key, Time, R>::advance_by(&result, &self.advance_frontier[..]));
				}

				self.merging.push(result);
			}
		}
	}
}

/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct Layer<Key: HashOrdered, Time: Lattice+Ord, R: Ring> {
	/// Where all the dataz is.
	pub layer: HashedLayer<Key, UnorderedLayer<(Time, R)>>,
	/// Description of the update times this layer represents.
	pub desc: Description<Time>,
}

impl<Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default, R: Ring> Batch<Key, (), Time, R> for Rc<Layer<Key, Time, R>> {
	type Batcher = LayerBatcher<Key, Time, R>;
	type Builder = LayerBuilder<Key, Time, R>;
	type Cursor = LayerCursor<Key, Time, R>;
	fn cursor(&self) -> Self::Cursor { 
		LayerCursor { empty: (), valid: true, cursor: self.layer.cursor() } 
	}
	fn len(&self) -> usize { self.layer.tuples() }
	fn description(&self) -> &Description<Time> { &self.desc }
	fn merge(&self, other: &Self) -> Option<Self> {

		// Things are horribly wrong if this is not true.
		if other.desc.upper() == self.desc.lower() {

			// one of self.desc.since or other.desc.since needs to be not behind the other...
			let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.le(t1))) {
				other.desc.since()
			}
			else {
				self.desc.since()
			};
			
			Some(Rc::new(Layer {
				layer: self.layer.merge(&other.layer),
				desc: Description::new(other.desc.lower(), self.desc.upper(), since),
			}))
		}
		else { None }
	}
}

impl<Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default+Debug, R: Ring> Layer<Key, Time, R> {

	/// Advances times in `layer` and consolidates differences for like times.
	///
	/// TODO: This method could be defined on `&mut self`, exploiting in-place mutation
	/// to avoid allocation and building headaches. It is implemented on the `Rc` variant
	/// to get access to `cursor()`, and in principle to allow a progressive implementation. 
	#[inline(never)]
	pub fn advance_by(layer: &Rc<Self>, frontier: &[Time]) -> Self { 

		// TODO: This is almost certainly too much `with_capacity`.
		// TODO: We should design and implement an "in-order builder", which takes cues from key and val
		// structure, rather than having to infer them from tuples.
		// TODO: We should understand whether in-place mutation is appropriate, or too gross. At the moment,
		// this could be a general method defined on any implementor of `trace::Cursor`.
		let mut builder = <RHHBuilder<Key, Time, R> as TupleBuilder>::with_capacity(layer.len());

		if layer.len() > 0 {
			let mut times = Vec::new();
			let mut cursor = layer.cursor();

			while cursor.key_valid() {
				if cursor.val_valid() {
					cursor.map_times(|time: &Time, diff| times.push((time.advance_by(frontier).unwrap(), diff)));
					consolidate(&mut times, 0);
					for (time, diff) in times.drain(..) {
						builder.push_tuple((cursor.key().clone(), (time, diff)));
					}
				}
				cursor.step_key();
			}
		}

		Layer { 
			layer: builder.done(), 
			desc: Description::new(layer.desc.lower(), layer.desc.upper(), frontier),
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct LayerCursor<Key: Clone+HashOrdered, Time: Lattice+Ord+Clone, R: Copy> {
	valid: bool,
	empty: (),
	cursor: HashedCursor<Key, UnorderedCursor<(Time, R)>>,
}


impl<Key: Clone+HashOrdered, Time: Lattice+Ord+Clone, R: Copy> Cursor<Key, (), Time, R> for LayerCursor<Key, Time, R> {
	fn key(&self) -> &Key { &self.cursor.key() }
	fn val(&self) -> &() { &self.empty }
	fn map_times<L: FnMut(&Time, R)>(&mut self, mut logic: L) {
		self.cursor.child.rewind();
		while self.cursor.child.valid() {
			logic(&self.cursor.child.key().0, self.cursor.child.key().1);
			self.cursor.child.step();
		}
	}
	fn key_valid(&self) -> bool { self.cursor.valid() }
	fn val_valid(&self) -> bool { self.valid }
	fn step_key(&mut self){ self.cursor.step(); self.valid = true; }
	fn seek_key(&mut self, key: &Key) { self.cursor.seek(key); self.valid = true; }
	fn step_val(&mut self) { self.valid = false; }
	fn seek_val(&mut self, _val: &()) { }
	fn rewind_keys(&mut self) { self.cursor.rewind(); self.valid = true; }
	fn rewind_vals(&mut self) { self.valid = true; }
}


/// A builder for creating layers from unsorted update tuples.
pub struct LayerBatcher<K, T: PartialOrd, R: Ring> {
	// where we stash records we don't know what to do with yet.
    buffers: Vec<Vec<((K, ()), T, R)>>,
    sorted: usize,
    sorter: MSBRadixSorter<((K, ()), T, R)>,
    stash: Vec<Vec<((K, ()), T, R)>>,
    lower: Vec<T>,
    frontier: Antichain<T>,
}

impl<Key, Time: PartialOrd, R: Ring> LayerBatcher<Key, Time, R>
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {

	// Provides an allocated buffer, either from stash or through allocation.
	fn empty(&mut self) -> Vec<((Key, ()), Time, R)> {
		self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10))
	}

	// Compacts the representation of data in self.buffer and self.buffers.
	// This could, in principle, move data into a trace, because it is even more 
	// compact in that form, and easier to merge as compared to re-sorting.
	#[inline(never)]
	fn compact(&mut self) {

		let mut sum1 = R::zero();
		for batch in &self.buffers {
			for element in batch.iter() {
				sum1 = sum1 + element.2;
			}
		}

		self.sorter.sort_and(&mut self.buffers, &|x: &((Key, ()),Time,R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));

		let mut sum2 = R::zero();
		for batch in &self.buffers {
			for element in batch.iter() {
				sum2 = sum2 + element.2;
			}
		}

		assert!(sum1 == sum2);

		self.sorter.rebalance(&mut self.stash, 256);
		self.sorted = self.buffers.len();
		self.stash.clear();	// <-- too aggressive?
	}

	#[inline(never)]
	fn segment(&mut self, upper: &[Time]) -> Vec<Vec<((Key,()), Time, R)>> {

		let mut to_keep = Vec::new();	// updates that are not yet ready.
		let mut to_seal = Vec::new();	// updates that are ready to go.

		let mut to_keep_tail = self.empty();
		let mut to_seal_tail = self.empty();

		// swing through each buffer, each element, and partition
		for mut buffer in self.buffers.drain(..) {
			for ((key, _), time, diff) in buffer.drain(..) {
				if !upper.iter().any(|t| t.le(&time)) {
					if to_seal_tail.len() == to_seal_tail.capacity() {
						if to_seal_tail.len() > 0 {
							to_seal.push(to_seal_tail);
						}
						to_seal_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
					}
					to_seal_tail.push(((key, ()), time, diff));
				}
				else {
					if to_keep_tail.len() == to_keep_tail.capacity() {
						if to_keep_tail.len() > 0 {
							to_keep.push(to_keep_tail);
						}
						to_keep_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
					}
					to_keep_tail.push(((key, ()), time, diff));
				}
			}
			self.stash.push(buffer);
		}

		if to_keep_tail.len() > 0 { to_keep.push(to_keep_tail); }
		if to_seal_tail.len() > 0 { to_seal.push(to_seal_tail); }

		self.buffers = to_keep;
		to_seal
	}
}

impl<Key, Time, R: Ring> Batcher<Key, (), Time, R, Rc<Layer<Key, Time, R>>> for LayerBatcher<Key, Time, R> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {
	fn new() -> Self { 
		LayerBatcher { 
			buffers: Vec::new(),
			sorter: MSBRadixSorter::new(),
			sorted: 0,
			stash: Vec::new(),
			frontier: Antichain::new(),
			lower: vec![Time::min()],
		} 
	}

	#[inline(never)]
	fn push_batch(&mut self, batch: &mut Vec<((Key, ()), Time, R)>) {

		// If we have spare capacity, copy contents rather than appending list.
		if self.buffers.last().map(|buf| buf.len() + batch.len() <= buf.capacity()) == Some(true) {
			self.buffers.last_mut().map(|buf| buf.extend(batch.drain(..)));
		}
		else {
			self.buffers.push(::std::mem::replace(batch, Vec::new()));
		}

		// If we have accepted a lot of data since our last compaction, compact again!
		if self.buffers.len() > ::std::cmp::max(2 * self.sorted, 1_000) {
			self.compact();
		}
	}

	// Sealing a batch means finding those updates with times not greater or equal to any time 
	// in `upper`. All updates must have time greater or equal to the previously used `upper`,
	// which we call `lower`, by assumption that after sealing a batcher we receive no more 
	// updates with times not greater or equal to `upper`.
	#[inline(never)]
	fn seal(&mut self, upper: &[Time]) -> Rc<Layer<Key, Time, R>> {

		// TODO: We filter and then consolidate; we could consolidate and then filter, for general
		//       health of compact state. Would mean that repeated `seal` calls wouldn't have to 
		//       re-sort the data if we tracked a dirty bit. Maybe wouldn't be all that helpful, 
		//       if we aren't running a surplus of data (if the optimistic compaction isn't helpful).
		//
		// Until timely dataflow gets multiple capabilities per message, we will probably want to
		// consider sealing multiple batches at once, as we will get multiple requests with nearly
		// the same `upper`, as we retire a few capabilities in sequence. Food for thought, anyhow.
		//
		// Our goal here is to partition stashed updates into "those to keep" and "those to sort"
		// as efficiently as possible. In particular, if we can avoid lot of allocations, re-using
		// the allocations we already have, I would be delighted.

		// Extract data we plan to seal and ship.
		let mut to_seal = self.segment(upper);

		let mut sum1 = R::zero();
		for batch in &self.buffers {
			for element in batch.iter() {
				sum1 = sum1 + element.2;
			}
		}

		// Sort the data; this uses top-down MSB radix sort with an early exit to consolidate_vec.
		self.sorter.sort_and(&mut to_seal, &|x: &((Key,()), Time, R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));		

		let mut sum2 = R::zero();
		for batch in &self.buffers {
			for element in batch.iter() {
				sum2 = sum2 + element.2;
			}
		}

		assert!(sum1 == sum2);

		// Create a new layer from the consolidated updates.
		let count = to_seal.iter().map(|x| x.len()).sum();
		let mut builder = LayerBuilder::with_capacity(count);
		for buffer in to_seal.iter_mut() {
			for ((key, _), time, diff) in buffer.drain(..) {
				debug_assert!(!diff.is_zero());
				builder.push((key, (), time, diff));
			}
		}

		// Recycle the consumed buffers, if appropriate.
		self.sorter.rebalance(&mut to_seal, 256);

		// Return the finished layer with its bounds.
		let result = builder.done(&self.lower[..], upper, &self.lower[..]);
		self.lower = upper.to_vec();
		result
	}

	fn frontier(&mut self) -> &[Time] {
		self.frontier = Antichain::new();
		for buffer in &self.buffers {
			for &(_, ref time, _) in buffer {
				self.frontier.insert(time.clone());
			}
		}
		self.frontier.elements()
	}
}

/// A builder for creating layers from unsorted update tuples.
pub struct LayerBuilder<Key: HashOrdered, Time: Ord, R: Ring> {
	builder: RHHBuilder<Key, Time, R>,
}

impl<Key, Time, R> Builder<Key, (), Time, R, Rc<Layer<Key, Time, R>>> for LayerBuilder<Key, Time, R> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default, R: Ring {

	fn new() -> Self { LayerBuilder { builder: RHHBuilder::new() } }
	fn push(&mut self, (key, _, time, diff): (Key, (), Time, R)) {
		self.builder.push_tuple((key, (time, diff)));
	}

	#[inline(never)]
	fn done(self, lower: &[Time], upper: &[Time], since: &[Time]) -> Rc<Layer<Key, Time, R>> {
		Rc::new(Layer {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, since)
		})
	}
}

impl<Key, Time, R> LayerBuilder<Key, Time, R> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default, R: Ring {
	fn with_capacity(cap: usize) -> Self { LayerBuilder { builder: RHHBuilder::with_capacity(cap) } }
}

/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
#[inline(always)]
pub fn consolidate_vec<D: Ord+Clone, T:Ord+Clone, R: Ring>(slice: &mut Vec<(D, T, R)>) {
	slice.sort_by(|x,y| (&x.0,&x.1).cmp(&(&y.0,&y.1)));
	for index in 1 .. slice.len() {
		if slice[index].0 == slice[index - 1].0 && slice[index].1 == slice[index - 1].1 {
			slice[index].2 = slice[index].2 + slice[index - 1].2;
			slice[index - 1].2 = R::zero();
		}
	}
	slice.retain(|x| !x.2.is_zero());
}