//! A trie-structured representation of update tuples with hashable keys. 
//! 
//! One goal of this representation is to allow multiple kinds of types of hashable keys, including
//! keys that implement `Hash`, keys whose hashes have been computed and are stashed with the key, and
//! integers keys which are promised to be random enough to be used as the hashes themselves.
use std::rc::Rc;
use std::mem::replace;

use timely::progress::frontier::Antichain;
use timely_sort::{Unsigned, LSBRadixSorter};

use ::Ring;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::hashed::{HashedLayer, HashedBuilder, HashedCursor};
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::unordered::{UnorderedLayer, UnorderedBuilder, UnorderedCursor};

use lattice::Lattice;
use trace::{Batch, Batcher, Builder, Cursor, Trace};
use trace::consolidate;
use trace::description::Description;
use trace::cursor::cursor_list::CursorList;

type RHHBuilder<Key, Val, Time, R> = HashedBuilder<Key, OrderedBuilder<Val, UnorderedBuilder<(Time, R)>>>;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
#[derive(Debug)]
pub struct Spine<Key: HashOrdered, Val: Ord, Time: Lattice+Ord, R: Ring> {
	frontier: Vec<Time>,						// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Val, Time, R>>>,	// Several possibly shared collections of updates.
	done: bool,
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
impl<Key, Val, Time, R> Trace<Key, Val, Time, R> for Spine<Key, Val, Time, R> 
where 
	Key: Clone+Default+HashOrdered+'static,
	Val: Ord+Clone+'static, 
	Time: Lattice+Ord+Clone+Default+'static,
	R: Ring,
{

	type Batch = Rc<Layer<Key, Val, Time, R>>;
	type Cursor = CursorList<Key, Val, Time, R, LayerCursor<Key, Val, Time, R>>;

	fn new(default: Time) -> Self {
		Spine { 
			frontier: vec![default],
			layers: Vec::new(),
			done: false,
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {
		assert!(!self.done);

		// while last two elements exist, both less than layer.len()
		while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.len() {
			let layer1 = self.layers.pop().unwrap();
			let layer2 = self.layers.pop().unwrap();
			let result = Rc::new(Layer::merge(&layer1, &layer2));
			if result.len() > 0 {
				self.layers.push(result);
			}
		}

		// assert that the interval added is contiguous (that we aren't missing anything).
		self.layers.push(layer);

	    while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
			let layer1 = self.layers.pop().unwrap();
			let layer2 = self.layers.pop().unwrap();
			let mut result = Rc::new(layer1.merge(&layer2));

			// if we just merged the last layer, `advance_by` it.
			if self.layers.len() == 0 {
				result = Rc::new(Layer::<Key, Val, Time, R>::advance_by(&result, &self.frontier[..]));
			}

			self.layers.push(result);
		}
	
	}
	fn cursor(&self) -> Self::Cursor {
		assert!(!self.done);
		let mut cursors = Vec::new();
		for layer in &self.layers[..] {
			if layer.len() > 0 {
				cursors.push(LayerCursor { cursor: layer.layer.cursor() } );
			}
		}

		CursorList::new(cursors)
	}
	fn advance_by(&mut self, frontier: &[Time]) {
		self.frontier = frontier.to_vec();
		if self.frontier.len() == 0 {
			self.layers.clear();
			self.done = true;
		}
	}
}


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct Layer<Key: HashOrdered, Val: Ord, Time: Lattice+Ord, R: Ring> {
	/// Where all the dataz is.
	pub layer: HashedLayer<Key, OrderedLayer<Val, UnorderedLayer<(Time, R)>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<Time>,
}

impl<Key: Clone+Default+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone+Default, R: Ring> Batch<Key, Val, Time, R> for Rc<Layer<Key, Val, Time, R>> {
	type Batcher = LayerBatcher<Key, Val, Time, R>;
	type Builder = LayerBuilder<Key, Val, Time, R>;
	type Cursor = LayerCursor<Key, Val, Time, R>;
	fn cursor(&self) -> Self::Cursor {  LayerCursor { cursor: self.layer.cursor() } }
	fn len(&self) -> usize { self.layer.tuples() }
}

impl<Key: Clone+Default+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone+Default, R: Ring> Layer<Key, Val, Time, R> {

	/// Conducts a full merge, right away. Times not advanced.
	pub fn merge(&self, other: &Self) -> Self {

		// // this may not be true if we leave gaps; a weaker statement would be "<=".
		// assert!(other.desc.upper() == self.desc.lower());

		// each element of self.desc.lower must be in the future of some element of other.desc.upper
		for time in self.desc.lower() {
			assert!(other.desc.upper().iter().any(|t| t.le(time)));
		}

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
		// TODO: We should design and implement an "in-order builder", which takes cues from key and val
		// structure, rather than having to re-infer them from tuples.
		// TODO: We should understand whether in-place mutation is appropriate, or too gross. At the moment,
		// this could be a general method defined on any implementor of `trace::Cursor`.
		let mut builder = <RHHBuilder<Key, Val, Time, R> as TupleBuilder>::with_capacity(layer.len());

		if layer.len() > 0 {
			let mut times = Vec::new();
			let mut cursor = layer.cursor();

			while cursor.key_valid() {
				while cursor.val_valid() {
					cursor.map_times(|time: &Time, diff| times.push((time.advance_by(frontier).unwrap(), diff)));
					consolidate(&mut times, 0);
					for (time, diff) in times.drain(..) {
						let key_ref: &Key = cursor.key();
						let key_clone: Key = key_ref.clone();
						let val_ref: &Val = cursor.val();
						let val_clone: Val = val_ref.clone();
						builder.push_tuple((key_clone, (val_clone, (time, diff))));
					}
					cursor.step_val()
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
pub struct LayerCursor<Key: Clone+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone, R: Copy> {
	cursor: HashedCursor<Key, OrderedCursor<Val, UnorderedCursor<(Time, R)>>>,
}


impl<Key: Clone+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone, R: Copy> Cursor<Key, Val, Time, R> for LayerCursor<Key, Val, Time, R> {
	fn key(&self) -> &Key { &self.cursor.key() }
	fn val(&self) -> &Val { self.cursor.child.key() }
	fn map_times<L: FnMut(&Time, R)>(&mut self, mut logic: L) {
		self.cursor.child.child.rewind();
		while self.cursor.child.child.valid() {
			logic(&self.cursor.child.child.key().0, self.cursor.child.child.key().1);
			self.cursor.child.child.step();
		}
	}
	fn key_valid(&self) -> bool { self.cursor.valid() }
	fn val_valid(&self) -> bool { self.cursor.child.valid() }
	fn step_key(&mut self){ self.cursor.step(); }
	fn seek_key(&mut self, key: &Key) { self.cursor.seek(key); }
	fn step_val(&mut self) { self.cursor.child.step(); }
	fn seek_val(&mut self, val: &Val) { self.cursor.child.seek(val); }
	fn rewind_keys(&mut self) { self.cursor.rewind(); }
	fn rewind_vals(&mut self) { self.cursor.child.rewind(); }
}


/// A builder for creating layers from unsorted update tuples.
pub struct LayerBatcher<K, V, T: PartialOrd, R> {
	// where we stash records we don't know what to do with yet.
    buffer: Vec<((K, V), T, R)>,
    buffers: Vec<Vec<((K, V), T, R)>>,

    sorter: LSBRadixSorter<((K, V), T, R)>,
    stash: Vec<Vec<((K, V), T, R)>>,
    stage: Vec<((K, V, T), R)>,

    /// lower bound of contained updates.
    frontier: Antichain<T>,
}

impl<Key, Val, Time: PartialOrd, R> LayerBatcher<Key, Val, Time, R> {
	fn empty(&mut self) -> Vec<((Key, Val), Time, R)> {
		self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10))
	}
}

impl<Key, Val, Time, R> Batcher<Key, Val, Time, R, Rc<Layer<Key, Val, Time, R>>> for LayerBatcher<Key, Val, Time, R> 
where Key: Clone+Default+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone+Default, R: Ring {
	fn new() -> Self { 
		LayerBatcher { 
			buffer: Vec::with_capacity(1 << 10), 
			buffers: Vec::new(),
			sorter: LSBRadixSorter::new(),
			stash: Vec::new(),
			stage: Vec::new(),
			frontier: Antichain::new(),
		} 
	}
	fn push(&mut self, (key, val, time, diff): (Key, Val, Time, R)) {
		self.buffer.push(((key, val), time, diff));
		if self.buffer.len() == (1 << 10) {
			let empty = self.empty();
			self.buffers.push(::std::mem::replace(&mut self.buffer, empty));
		}
	}

	// TODO: Consider sorting everything, which would allow cancelation of any updates.
	#[inline(never)]
	fn seal(&mut self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Val, Time, R>> {

		// 1. Scan all of self.buffers and self.buffer to move appropriate updates to self.sorter.
		if self.buffers.len() > 0 {
	    	if self.buffer.len() > 0 {
	    		let empty = self.empty();
				self.buffers.push(replace(&mut self.buffer, empty));
	    	}

	    	let mut buffers = replace(&mut self.buffers, Vec::new());
	    	for mut buffer in buffers.drain(..) {
	    		for ((key, val), time, diff) in buffer.drain(..) {
					if lower.iter().any(|t| t.le(&time)) && !upper.iter().any(|t| t.le(&time)) {
						self.sorter.push(((key, val), time, diff), &|x| (x.0).0.hashed());
					}
					else {
						if self.buffer.len() == (1 << 10) {
							let empty = self.empty();
							self.buffers.push(replace(&mut self.buffer, empty));
						}
						// frontier.insert(time.clone());
						self.buffer.push(((key, val), time, diff));
					}        			
	    		}
	    		self.stash.push(buffer);
	    	}
	    	replace(&mut self.stash, Vec::new());
	    	// self.sorter.recycle(replace(&mut self.stash, Vec::new()));

			// 2. Finish up sorting, then drain the contents into `builder`, consolidating as we go.
			let mut builder = LayerBuilder::new();
			let mut sorted = self.sorter.finish(&|x| (x.0).0.hashed());
			let mut current_hash = 0;
			for buffer in sorted.iter_mut() {
				for ((key, val), time, diff) in buffer.drain(..) {
	        		if key.hashed().as_u64() != current_hash {
	        			current_hash = key.hashed().as_u64();
						consolidate(&mut self.stage, 0);
						for ((key, val, time), diff) in self.stage.drain(..) {
							builder.push((key, val, time, diff));
						}
	        		}
	        		self.stage.push(((key, val, time), diff));				
				}
			}
			// self.sorter.recycle(sorted);
			consolidate(&mut self.stage, 0);
			for ((key, val, time), diff) in self.stage.drain(..) {
				builder.push((key, val, time, diff));
			}

			// 3. Return the finished layer with its bounds.
			builder.done(lower, upper)
		}
		else {
			let mut stash = self.empty();

			for ((key, val), time, diff) in self.buffer.drain(..) {
				if lower.iter().any(|t| t.le(&time)) && !upper.iter().any(|t| t.le(&time)) {
					self.stage.push(((key, val, time), diff));
				}
				else {	
					stash.push(((key, val), time, diff));
				}
			}

			self.stash.push(replace(&mut self.buffer, stash));

			consolidate(&mut self.stage, 0);
			let mut builder = LayerBuilder::new();
			for ((key, val, time), diff) in self.stage.drain(..) {
				builder.push((key, val, time, diff));
			}

			builder.done(lower, upper)
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
pub struct LayerBuilder<Key: HashOrdered, Val: Ord, Time: Ord, R> {
	builder: RHHBuilder<Key, Val, Time, R>,
}

impl<Key, Val, Time, R> Builder<Key, Val, Time, R, Rc<Layer<Key, Val, Time, R>>> for LayerBuilder<Key, Val, Time, R> 
where Key: Clone+Default+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone+Default, R: Ring {

	fn new() -> Self { LayerBuilder { builder: RHHBuilder::new() } }
	fn push(&mut self, (key, val, time, diff): (Key, Val, Time, R)) {
		self.builder.push_tuple((key, (val, (time, diff))));
	}

	#[inline(never)]
	fn done(self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Val, Time, R>> {
		Rc::new(Layer {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, lower)
		})
	}
}