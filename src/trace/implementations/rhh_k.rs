//! A trie-structured representation of update tuples with hashable keys. 
//! 
//! One goal of this representation is to allow multiple kinds of types of hashable keys, including
//! keys that implement `Hash`, keys whose hashes have been computed and are stashed with the key, and
//! integers keys which are promised to be random enough to be used as the hashes themselves.
use std::rc::Rc;
// use std::hash::Hash;
// use std::borrow::Borrow;

use timely_sort::{Unsigned};

// use ::Hashable;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::hashed::{HashedLayer, HashedBuilder, HashedCursor};
use trace::layers::unordered::{UnorderedLayer, UnorderedBuilder, UnorderedCursor};

use lattice::Lattice;
use trace::{Batch, Builder, Cursor, Trace};
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
pub struct Spine<Key: HashOrdered, Time: Lattice+Ord> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Time>>>	// Several possibly shared collections of updates.
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and layer types.
impl<Key, Time> Trace<Key, (), Time> for Spine<Key, Time> 
where 
	Key: Clone+Default+HashOrdered+'static,
	Time: Lattice+Ord+Clone+Default+'static,
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
pub struct LayerBuilder<K: HashOrdered, T: Ord> {
	time: T,
	// TODO : Have this build `Layer`s and merge, instead of re-sorting sorted data.
    // buffer: Vec<((K, T), isize)>,
    buffer: Vec<(K, isize)>,
}

impl<Key, Time> Builder<Key, (), Time, Rc<Layer<Key, Time>>> for LayerBuilder<Key, Time> 
where Key: Clone+Default+HashOrdered, Time: Lattice+Ord+Clone+Default {
	fn new() -> Self { LayerBuilder { time: Default::default(), buffer: Vec::new() } }
	fn push(&mut self, (key, _, time, diff): (Key, (), Time, isize)) {
		self.time = time;
		self.buffer.push((key, diff));
		// self.buffer.push(((key, time), diff));
	}
	#[inline(never)]
	fn done(mut self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Time>> {

		let print = self.buffer.len() > 1000000;

        let timer = ::std::time::Instant::now();

        // radix sort larger batches. could radix sort smaller batches, with a different implementation.
        if self.buffer.len() > 1_000 {

        	let mut sorter = ::timely_sort::LSBSWCRadixSorter::new();
        	for update in self.buffer.drain(..) {
        		sorter.push(update, &|x| x.0.hashed());
        	}
        	let mut sorted = sorter.finish(&|x| x.0.hashed());

        	let mut current_hash = 0;
        	let mut current_pos = 0;
        	for (key, diff) in sorted.drain(..).flat_map(|batch| batch.into_iter()) {
        		if key.hashed().as_u64() != current_hash {
        			current_hash = key.hashed().as_u64();
					consolidate(&mut self.buffer, current_pos);
        			current_pos = self.buffer.len();
        		}
        		self.buffer.push((key,diff));
        	}
			// consolidate(&mut self.buffer, current_pos);
			consolidate(&mut self.buffer, current_pos);
        }
        else {
        	// TODO : Perhaps this *needs* to be a custom sort using hash-code, for types which implement
        	// `Hashed` but whose `Ord` does not respect it?
        	consolidate(&mut self.buffer, 0);
	    }
		if print { println!("collapse: {:?}", timer.elapsed()); }


        let timer = ::std::time::Instant::now();
		let mut builder = <RHHBuilder<Key, Time> as TupleBuilder>::with_capacity(self.buffer.len());
        // TODO : We should be able to build a `Layer` directly here, without the weird indirection of `push_tuple`.
        // We can compute the number of keys, (key,val)s, allocate exact amounts, and then drop everything in place.
        // This is not unethical, because we have full knowledge of the collection trace structure here.
		for (key,diff) in self.buffer.drain(..) {
			builder.push_tuple((key, (self.time.clone(), diff)));
		}
		let layer = builder.done();
		if print { println!("building: {:?}", timer.elapsed()); }

		Rc::new(Layer {
			layer: layer,
			desc: Description::new(lower, upper, lower)
		})
	}
}