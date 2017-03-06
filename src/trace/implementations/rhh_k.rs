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

type RHHBuilder<Key, Time> = HashedBuilder<Key, UnorderedBuilder<(Time, isize)>>;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
#[derive(Debug)]
pub struct Spine<Key: HashOrdered, Time: Lattice+Ord+Debug> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Time>>>	// Several possibly shared collections of updates.
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and layer types.
impl<Key, Time> Trace<Key, (), Time> for Spine<Key, Time> 
where 
	Key: Clone+Default+HashOrdered+'static,
	Time: Lattice+Ord+Clone+Default+Debug+'static,
{

	type Batch = Rc<Layer<Key, Time>>;
	type Cursor = CursorList<Key, (), Time, LayerCursor<Key, Time>>;

	fn new(default: Time) -> Self {
		Spine { 
			frontier: vec![default],
			layers: Vec::new(),
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {
		if layer.layer.keys() > 0 {
			// while last two elements exist, both less than layer.len()
			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.len() {
				let layer1 = self.layers.pop().unwrap();
				let layer2 = self.layers.pop().unwrap();
				let result = Rc::new(Layer::merge(&layer1, &layer2));
				if result.len() > 0 {
					self.layers.push(result);
				}
			}

			if layer.len() > 0 {
				self.layers.push(layer);
			}

		    while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
				let layer1 = self.layers.pop().unwrap();
				let layer2 = self.layers.pop().unwrap();
				let mut result = Rc::new(layer1.merge(&layer2));

				// if we just merged the last layer, `advance_by` it.
				if self.layers.len() == 0 {
					result = Rc::new(Layer::<Key, Time>::advance_by(&result, &self.frontier[..]));
				}

				if result.len() > 0 {
					self.layers.push(result);
				}
			}
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
pub struct Layer<Key: HashOrdered, Time: Lattice+Ord> {
	/// Where all the dataz is.
	pub layer: HashedLayer<Key, UnorderedLayer<(Time, isize)>>,
	/// Description of the update times this layer represents.
	pub desc: Description<Time>,
}

impl<Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default> Batch<Key, (), Time> for Rc<Layer<Key, Time>> {
	type Batcher = LayerBatcher<Key, Time>;
	type Builder = LayerBuilder<Key, Time>;
	type Cursor = LayerCursor<Key, Time>;
	fn cursor(&self) -> Self::Cursor { 
		LayerCursor { empty: (), valid: true, cursor: self.layer.cursor() } 
	}
	fn len(&self) -> usize { self.layer.tuples() }
}

impl<Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default> Layer<Key, Time> {

	/// Conducts a full merge, right away. Times not advanced.
	pub fn merge(&self, other: &Self) -> Self {
		Layer {
			layer: self.layer.merge(&other.layer),
			desc: Description::new(&[], &[], &[]),
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
		let mut builder = <RHHBuilder<Key, Time> as TupleBuilder>::with_capacity(layer.len());

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
pub struct LayerCursor<Key: Clone+HashOrdered, Time: Lattice+Ord+Clone> {
	valid: bool,
	empty: (),
	cursor: HashedCursor<Key, UnorderedCursor<(Time, isize)>>,
}


impl<Key: Clone+HashOrdered, Time: Lattice+Ord+Clone> Cursor<Key, (), Time> for LayerCursor<Key, Time> {
	fn key(&self) -> &Key { &self.cursor.key() }
	fn val(&self) -> &() { &self.empty }
	fn map_times<L: FnMut(&Time, isize)>(&mut self, mut logic: L) {
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
pub struct LayerBatcher<K, T: PartialOrd> {
	// where we stash records we don't know what to do with yet.
    buffer: Vec<(K, T, isize)>,
    buffers: Vec<Vec<(K, T, isize)>>,

    sorter: LSBRadixSorter<(K, T, isize)>,
    stash: Vec<Vec<(K, T, isize)>>,
    stage: Vec<((K, T), isize)>,

    /// lower bound of contained updates.
    frontier: Antichain<T>,
}

impl<Key, Time: PartialOrd> LayerBatcher<Key, Time> {
	fn empty(&mut self) -> Vec<(Key, Time, isize)> {
		self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10))
	}
}

impl<Key, Time> Batcher<Key, (), Time, Rc<Layer<Key, Time>>> for LayerBatcher<Key, Time> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {
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
	fn push(&mut self, (key, _, time, diff): (Key, (), Time, isize)) {
		self.buffer.push((key, time, diff));
		if self.buffer.len() == (1 << 10) {
			let empty = self.empty();
			self.buffers.push(::std::mem::replace(&mut self.buffer, empty));
		}
	}
	// TODO: Consider sorting everything, which would allow cancelation of any updates.
	fn seal(&mut self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Time>> {

		// 1. Scan all of self.buffers and self.buffer to move appropriate updates to self.sorter.
    	if self.buffer.len() > 0 {
			let empty = self.empty();
			self.buffers.push(replace(&mut self.buffer, empty));
    	}

    	// let mut frontier = Antichain::new();
    	let buffers = replace(&mut self.buffers, Vec::new());
    	for mut buffer in buffers.into_iter() {
    		for (key, time, diff) in buffer.drain(..) {
				if lower.iter().any(|t| t.le(&time)) && !upper.iter().any(|t| t.le(&time)) {
					self.sorter.push((key, time, diff), &|x| x.0.hashed());
				}
				else {
					if self.buffer.len() == (1 << 10) {
						let empty = self.empty();
						self.buffers.push(replace(&mut self.buffer, empty));
					}
					// frontier.insert(time.clone());
					self.buffer.push((key, time, diff));
				}        			
    		}
    		self.stash.push(buffer);
    	}

    	// self.frontier = frontier;

		// 2. Finish up sorting, then drain the contents into `builder`, consolidating as we go.
		let mut builder = LayerBuilder::new();

		let mut sorted = self.sorter.finish(&|x| x.0.hashed());
		let mut current_hash = 0;
		for mut buffer in sorted.drain(..) {
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
			// TODO: Do something with `buffer`?
		}
		consolidate(&mut self.stage, 0);
		for ((key, time), diff) in self.stage.drain(..) {
			builder.push((key, (), time, diff));
		}

		// 3. Return the finished layer with its bounds.
		builder.done(lower, upper)
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
pub struct LayerBuilder<Key: HashOrdered, Time: Ord> {
	builder: RHHBuilder<Key, Time>,
}

impl<Key, Time> Builder<Key, (), Time, Rc<Layer<Key, Time>>> for LayerBuilder<Key, Time> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {

	fn new() -> Self { LayerBuilder { builder: RHHBuilder::new() } }
	fn push(&mut self, (key, _, time, diff): (Key, (), Time, isize)) {
		self.builder.push_tuple((key, (time, diff)));
	}

	#[inline(never)]
	fn done(self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Time>> {
		Rc::new(Layer {
			layer: self.builder.done(),
			desc: Description::new(lower, upper, lower)
		})
	}
}