//! A trie-structured representation of update tuples with hashable keys. 
//! 
//! One goal of this representation is to allow multiple kinds of types of hashable keys, including
//! keys that implement `Hash`, keys whose hashes have been computed and are stashed with the key, and
//! integers keys which are promised to be random enough to be used as the hashes themselves.
use std::rc::Rc;
use std::fmt::Debug;
use std::mem::replace;

use timely::progress::frontier::Antichain;
use timely_sort::{Unsigned, LSBRadixSorter};


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
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Time, R>>>	// Several possibly shared collections of updates.
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

	fn new(default: Time) -> Self {
		Spine { 
			frontier: vec![default],
			layers: Vec::new(),
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {

		// while last two elements exist, both less than layer.len()
		while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.len() {
			let layer1 = self.layers.pop().unwrap();
			let layer2 = self.layers.pop().unwrap();
			let result = Rc::new(Layer::merge(&layer1, &layer2));
			self.layers.push(result);
		}

		self.layers.push(layer);

	    while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
			let layer1 = self.layers.pop().unwrap();
			let layer2 = self.layers.pop().unwrap();
			let mut result = Rc::new(layer1.merge(&layer2));

			// if we just merged the last layer, `advance_by` it.
			if self.layers.len() == 0 {
				result = Rc::new(Layer::<Key, Time, R>::advance_by(&result, &self.frontier[..]));
			}

			self.layers.push(result);
		}

	}
	fn cursor(&self) -> Self::Cursor {
		let mut cursors = Vec::new();
		for layer in &self.layers[..] {
			if layer.len() > 0 {
				cursors.push(layer.cursor());
			}
		}

		CursorList::new(cursors)
	}
	fn advance_by(&mut self, frontier: &[Time]) {
		self.frontier = frontier.to_vec();
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
}

impl<Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default, R: Ring> Layer<Key, Time, R> {

	/// Conducts a full merge, right away. Times not advanced.
	pub fn merge(&self, other: &Self) -> Self {

		// This may not be true if we leave gaps, so we try hard not to do that.
		assert!(other.desc.upper() == self.desc.lower());

		// one of self.desc.since or other.desc.since needs to be not behind the other...
		let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.le(t1))) {
			other.desc.since()
		}
		else {
			self.desc.since()
		};
		
		Layer {
			layer: self.layer.merge(&other.layer),
			desc: Description::new(other.desc.lower(), self.desc.upper(), since),
		}
	}
	/// Advances times in `layer` and consolidates differences for like times.
	///
	/// TODO: This method could be defined on `&mut self`, exploiting in-place mutation
	/// to avoid allocation and building headaches. It is implemented on the `Rc` variant
	/// to get access to `cursor()`, and in principle to allow a progressive implementation. 
	pub fn advance_by(layer: &Rc<Self>, frontier: &[Time]) -> Self { 

		// TODO: This is almost certainly too much `with_capacity`.
		// TODO: We should design and implement an "in-orded builder", which takes cues from key and val
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
						let key_ref: &Key = cursor.key();
						let key_clone: Key = key_ref.clone();
						builder.push_tuple((key_clone, (time, diff)));
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
    buffer: Vec<(K, T, R)>,
    buffers: Vec<Vec<(K, T, R)>>,

    sorted: usize,
    sorter: LSBRadixSorter<(K, T, R)>,
    stash: Vec<Vec<(K, T, R)>>,
    stage: Vec<((K, T), R)>,

    lower: Vec<T>,

    /// lower bound of contained updates.
    frontier: Antichain<T>,
}

impl<Key, Time: PartialOrd, R: Ring> LayerBatcher<Key, Time, R>
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {
	fn empty(&mut self) -> Vec<(Key, Time, R)> {
		self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10))
	}

	// Compacts the representation of data in self.buffer and self.buffers.
	// This could, in principle, move data into a trace, because it is even more 
	// compact in that form, and easier to merge as compared to re-sorting.
	fn compact(&mut self) {

		let mut buffers = ::std::mem::replace(&mut self.buffers, Vec::new());
		self.sorter.sort(&mut buffers, &|x| x.0.hashed());

		// set this so that we avoid re-tripping sorting.
		self.sorted = self.buffers.len();

		// consolidate the contents, writing them back to their buffers.
		let mut current_hash = 0;
		for mut buffer in buffers.drain(..) {
			for (key, time, diff) in buffer.drain(..) {
	    		if key.hashed().as_u64() != current_hash {
	    			current_hash = key.hashed().as_u64();
					consolidate(&mut self.stage, 0);
					for ((key, time), diff) in self.stage.drain(..) {

						// *we* know push doesn't use `self.stage`, but ...
						self.buffer.push((key, time, diff));
						if self.buffer.len() == (1 << 10) {
							let empty = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
							self.buffers.push(::std::mem::replace(&mut self.buffer, empty));
						}

					}
	    		}
	    		self.stage.push(((key, time), diff));				
			}
			self.stash.push(buffer)
		}
		consolidate(&mut self.stage, 0);
		for ((key, time), diff) in self.stage.drain(..) {

			// *we* know push doesn't use `self.stage`, but ...
			self.buffer.push((key, time, diff));
			if self.buffer.len() == (1 << 10) {
				let empty = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
				self.buffers.push(::std::mem::replace(&mut self.buffer, empty));
			}

		}

		// update self.sorted to reflect compacted data.
		self.sorted = self.buffers.len();
		self.stash.clear();
	}

}

impl<Key, Time, R: Ring> Batcher<Key, (), Time, R, Rc<Layer<Key, Time, R>>> for LayerBatcher<Key, Time, R> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {
	fn new() -> Self { 
		LayerBatcher { 
			buffer: Vec::with_capacity(1 << 10), 
			buffers: Vec::new(),
			sorter: LSBRadixSorter::new(),
			sorted: 0,
			stash: Vec::new(),
			stage: Vec::new(),
			frontier: Antichain::new(),
			lower: vec![Time::min()],
		} 
	}

	fn push(&mut self, (key, _, time, diff): (Key, (), Time, R)) {

		// each pushed update should be in the future of the current lower bound.
		debug_assert!(self.lower.iter().any(|t| t.le(&time)));

		self.buffer.push((key, time, diff));
		if self.buffer.len() == (1 << 10) {
			let empty = self.empty();
			self.buffers.push(::std::mem::replace(&mut self.buffer, empty));

			// if it has been a while since we have compacted, let's do so now.
			if self.buffers.len() > ::std::cmp::max(2 * self.sorted, 1_000) {
				self.compact();
			}
		}
	}

	// TODO: Consider sorting everything, which would allow cancelation of any updates.
	#[inline(never)]
	fn seal(&mut self, upper: &[Time]) -> Rc<Layer<Key, Time, R>> {

		// Sealing a batch means finding those updates with times greater or equal to some element
		// of `lower`, but not greater or equal to any element of `upper`. 
		//
		// Until timely dataflow gets multiple capabilities per message, we will probably want to
		// consider sealing multiple batches at once, as we will get multiple requests with nearly
		// the same `upper`, as we retire a few capabilities in sequence. Food for thought, anyhow.
		//
		// Our goal here is to partition stashed updates into "those to keep" and "those to sort"
		// as efficiently as possible. In particular, if we can avoid lot of allocations, re-using
		// the allocations we already have, I would be delighted.

		// move the tail onto self.buffers.
		if self.buffer.len() > 0 {
			let empty = self.empty();
			let tail = replace(&mut self.buffer, empty);
			self.buffers.push(tail);
		}

		// partition data into data we keep and data we seal and ship.
		let mut to_keep = Vec::new();	// updates that are not yet ready.
		let mut to_seal = Vec::new();	// updates that are ready to go.

		let mut to_keep_tail = self.empty();
		let mut to_seal_tail = self.empty();

		// swing through each buffer, each element, and partition
		for mut buffer in self.buffers.drain(..) {
			for (key, time, diff) in buffer.drain(..) {
				if !upper.iter().any(|t| t.le(&time)) {
					if to_seal_tail.len() == to_seal_tail.capacity() {
						if to_seal_tail.len() > 0 {
							to_seal.push(to_seal_tail);
						}
						to_seal_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
					}
					to_seal_tail.push((key, time, diff));
				}
				else {
					if to_keep_tail.len() == to_keep_tail.capacity() {
						if to_keep_tail.len() > 0 {
							to_keep.push(to_keep_tail);
						}
						to_keep_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
					}
					to_keep_tail.push((key, time, diff));
				}
			}
			self.stash.push(buffer);
		}

		// retain stuff we keep.
		self.buffers = to_keep;
		self.buffer = to_keep_tail;

		// now we sort `to_seal`. do this differently based on its length.
		if to_seal_tail.len() > 0 { to_seal.push(to_seal_tail); }
		if to_seal.len() > 1 {
			// sort the data; will probably trigger many allocations.
			self.sorter.sort(&mut to_seal, &|x| x.0.hashed());

			let mut builder = LayerBuilder::new();
			let mut current_hash = 0;
			for buffer in to_seal.iter_mut() {
				for (key, time, diff) in buffer.drain(..) {
	        		if key.hashed().as_u64() != current_hash {
	        			current_hash = key.hashed().as_u64();
						consolidate(&mut self.stage, 0);
						for ((key, time), diff) in self.stage.drain(..) {
							builder.push((key, (), time, diff));
						}
	        		}
	        		self.stage.push(((key, time), diff));				
				}
			}
			self.sorter.recycle(to_seal);
			consolidate(&mut self.stage, 0);
			for ((key, time), diff) in self.stage.drain(..) {
				builder.push((key, (), time, diff));
			}

			// 3. Return the finished layer with its bounds.
			let result = builder.done(&self.lower[..], upper);
			self.lower = upper.to_vec();
			result
		}
		else {
			let mut data = to_seal.pop().unwrap_or_else(|| self.empty());
			for (key, time, diff) in data.drain(..) {
				self.stage.push(((key, time), diff));
			}
			consolidate(&mut self.stage, 0);

			let mut builder = LayerBuilder::new();
			for ((key, time), diff) in self.stage.drain(..) {
				builder.push((key, (), time, diff));
			}
			let result = builder.done(&self.lower[..], upper);
			self.lower = upper.to_vec();
			result
		}
	}

	fn frontier(&mut self) -> &[Time] {

		self.frontier = Antichain::new();

		for buffer in &self.buffers {
			for &(_, ref time, _) in buffer {
				self.frontier.insert(time.clone());
			}
		}
		for &(_, ref time, _) in &self.buffer {
			self.frontier.insert(time.clone());
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
	fn done(self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Time, R>> {
		Rc::new(Layer {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, lower)
		})
	}
}