//! A trie-structured representation of update tuples with hashable keys. 
//! 
//! One goal of this representation is to allow multiple kinds of types of hashable keys, including
//! keys that implement `Hash`, keys whose hashes have been computed and are stashed with the key, and
//! integers keys which are promised to be random enough to be used as the hashes themselves.
use std::rc::Rc;
// use std::mem::replace;

use timely::progress::frontier::Antichain;
use timely_sort::{MSBRadixSorter, RadixSorterBase};

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
	layers: Vec<Layer<Key, Val, Time, R>>,	// Several possibly shared collections of updates.
	done: bool,
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
impl<K, V, T, R> Trace<K, V, T, R> for Spine<K, V, T, R> 
where 
	K: Clone+Default+HashOrdered+'static,
	V: Ord+Clone+'static, 
	T: Lattice+Ord+Clone+Default+'static,
	R: Ring,
{

	type Batch = Layer<K, V, T, R>;
	type Cursor = CursorList<K, V, T, R, LayerCursor<K, V, T, R>>;

	fn new() -> Self {
		Spine { 
			frontier: vec![<T as Lattice>::min()],
			layers: Vec::new(),
			done: false,
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {
		assert!(!self.done);

		// we can ignore degenerate layers.
		if layer.desc.lower() != layer.desc.upper() {

			// while last two elements exist, both less than layer.len()
			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.len() {
				let layer1 = self.layers.pop().unwrap();
				let layer2 = self.layers.pop().unwrap();
				let result = layer1.merge(&layer2).unwrap();
				self.layers.push(result);
			}

			self.layers.push(layer);

			// Repeatedly merge elements that do not exhibit geometric increase in size.
			// TODO: We could try and plan merges, as we can see how many we will do. We might benefit
			//       from noticing that we will do an advance_by, for example, though I'm not entirely
			//       sure how, yet.
		    while self.layers.len() >= 2 
		    	&& self.layers[self.layers.len()-2].len() < 2 * self.layers[self.layers.len()-1].len() {
				let layer1 = self.layers.pop().unwrap();
				let layer2 = self.layers.pop().unwrap();
				let mut result = layer1.merge(&layer2).unwrap();

				// if we just merged the last layer, `advance_by` it.
				if self.layers.len() == 0 {
					result = Layer::<K, V, T, R>::advance_by(&result, &self.frontier[..]);
				}

				self.layers.push(result);
			}
		}
		else {
			assert!(layer.len() == 0);
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
	fn cursor_through(&self, frontier: &[T]) -> Option<Self::Cursor> {
		unimplemented!()
	}
	fn advance_by(&mut self, frontier: &[T]) {
		self.frontier = frontier.to_vec();
		if self.frontier.len() == 0 {
			self.layers.clear();
			self.done = true;
		}
	}
	fn distinguish_since(&mut self, frontier: &[T]) {
		unimplemented!()
	}
}


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct Layer<K: HashOrdered, V: Ord, T: Lattice+Ord, R: Ring> {
	/// Where all the dataz is.
	pub layer: Rc<HashedLayer<K, OrderedLayer<V, UnorderedLayer<(T, R)>>>>,
	/// Description of the update times this layer represents.
	pub desc: Description<T>,
}

impl<K, V, T, R> Batch<K, V, T, R> for Layer<K, V, T, R>
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Ring {
	type Batcher = LayerBatcher<K, V, T, R>;
	type Builder = LayerBuilder<K, V, T, R>;
	type Cursor = LayerCursor<K, V, T, R>;
	fn cursor(&self) -> Self::Cursor {  LayerCursor { cursor: self.layer.cursor() } }
	fn len(&self) -> usize { self.layer.tuples() }
	fn description(&self) -> &Description<T> { &self.desc }
	fn merge(&self, other: &Self) -> Option<Self> {

		if self.desc.upper() == other.desc.lower() {

			// one of self.desc.since or other.desc.since needs to be not behind the other...
			let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.le(t1))) {
				other.desc.since()
			}
			else {
				self.desc.since()
			};

			Some(Layer {
				layer: Rc::new(self.layer.merge(&other.layer)),
				desc: Description::new(self.desc.lower(), other.desc.upper(), since),
			})	
		}
		else { None }
	}
}

impl<K: HashOrdered, V: Ord, T: Lattice+Ord+Clone, R: Ring> Clone for Layer<K, V, T, R> {
	fn clone(&self) -> Self {
		Layer {
			layer: self.layer.clone(),
			desc: self.desc.clone(),
		}
	}
}

impl<K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Ring> Layer<K, V, T, R> {


	/// Advances times in `layer` and consolidates differences for like times.
	//
	// TODO: This method could be defined on `&mut self`, exploiting in-place mutation
	//       to avoid allocation and building headaches. It is implemented on the `Rc` variant
	//       to get access to `cursor()`, and in principle to allow a progressive implementation. 
	pub fn advance_by(layer: &Self, frontier: &[T]) -> Self { 

		// TODO: This is almost certainly too much `with_capacity`.
		// TODO: We should design and implement an "in-order builder", which takes cues from key and val
		//       structure, rather than having to re-infer them from streams of tuples.
		// TODO: We should understand whether in-place mutation is appropriate, or too gross. At the moment,
		//       this could be a general method defined on any implementor of `trace::Cursor`. On the other
		//       hand, it would be more efficient to do in place.
		let mut builder = <RHHBuilder<K, V, T, R> as TupleBuilder>::with_capacity(layer.len());

		if layer.len() > 0 {
			let mut times = Vec::new();
			let mut cursor = layer.cursor();

			while cursor.key_valid() {
				while cursor.val_valid() {
					cursor.map_times(|time: &T, diff| times.push((time.advance_by(frontier).unwrap(), diff)));
					consolidate(&mut times, 0);
					for (time, diff) in times.drain(..) {
						builder.push_tuple((cursor.key().clone(), (cursor.val().clone(), (time, diff))));
					}
					cursor.step_val()
				}
				cursor.step_key();
			}
		}

		Layer { 
			layer: Rc::new(builder.done()),
			desc: Description::new(layer.desc.lower(), layer.desc.upper(), frontier),
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct LayerCursor<Key: Clone+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone, R: Copy> {
	cursor: HashedCursor<Key, OrderedCursor<Val, UnorderedCursor<(Time, R)>>>,
}

impl<Key, Val, Time, R> Cursor<Key, Val, Time, R> for LayerCursor<Key, Val, Time, R> 
where Key: Clone+HashOrdered, Val: Ord+Clone, Time: Lattice+Ord+Clone, R: Copy {
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
    buffers: Vec<Vec<((K, V), T, R)>>,
    sorted: usize,
    sorter: MSBRadixSorter<((K, V), T, R)>,
    stash: Vec<Vec<((K, V), T, R)>>,
    lower: Vec<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, R> LayerBatcher<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Ring {

	// Provides an allocated buffer, either from stash or through allocation.	
	fn empty(&mut self) -> Vec<((K, V), T, R)> {
		self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10))
	}

	/// Compacts the representation of data in self.buffer and self.buffers.
	#[inline(never)]
	fn compact(&mut self) {

		// TODO: We should probably track compacted and disordered data separately, to avoid continually
		//       recompacting compaced data. It shouldn't be more than a 2x saving, but that is not small.

		// TODO: We could also manage compacted data in a trace, which may eventually be a good idea for
		//       when we have clever application-dependent compaction (e.g. extracting times from updates).
		//       This could also be helpful in sizing the batches, rather than totally guessing. It would
		self.sorter.sort_and(&mut self.buffers, &|x: &((K, V),T,R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));
		self.sorter.rebalance(&mut self.stash, 256);
		self.sorted = self.buffers.len();
		self.stash.clear();	// <-- too aggressive?
	}

	/// Extracts and returns batches of updates at times not greater or equal to elements of `upper`.
	#[inline(never)]
	fn segment(&mut self, upper: &[T]) -> Vec<Vec<((K, V), T, R)>> {

		// partition data into data we keep and data we seal and ship.
		let mut to_keep = Vec::new();	// updates that are not yet ready.
		let mut to_seal = Vec::new();	// updates that are ready to go.

		let mut to_keep_tail = self.empty();
		let mut to_seal_tail = self.empty();

		// swing through each buffer, each element, and partition
		for mut buffer in self.buffers.drain(..) {
			for ((key, val), time, diff) in buffer.drain(..) {
				if !upper.iter().any(|t| t.le(&time)) {
					if to_seal_tail.len() == to_seal_tail.capacity() {
						if to_seal_tail.len() > 0 {
							to_seal.push(to_seal_tail);
						}
						to_seal_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
					}
					to_seal_tail.push(((key, val), time, diff));
				}
				else {
					if to_keep_tail.len() == to_keep_tail.capacity() {
						if to_keep_tail.len() > 0 {
							to_keep.push(to_keep_tail);
						}
						to_keep_tail = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1 << 10));
					}
					to_keep_tail.push(((key, val), time, diff));
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

impl<K, V, T, R> Batcher<K, V, T, R, Layer<K, V, T, R>> for LayerBatcher<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Ring {
	fn new() -> Self { 
		LayerBatcher { 
			buffers: Vec::new(),
			sorter: MSBRadixSorter::new(),
			sorted: 0,
			stash: Vec::new(),
			frontier: Antichain::new(),
			lower: vec![T::min()]
		} 
	}

	fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>) {

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

	#[inline(never)]
	fn seal(&mut self, upper: &[T]) -> Layer<K, V, T, R> {

		// TODO: Consider sorting everything, which would allow cancelation of any updates.

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

		let mut to_seal = self.segment(upper);

		// Sort the data; this uses top-down MSB radix sort with an early exit to consolidate_vec.
		self.sorter.sort_and(&mut to_seal, &|x: &((K,V),T,R)| (x.0).0.hashed(), |slice| consolidate_vec(slice));		

		// Create a new layer from the consolidated updates.
		let count = to_seal.iter().map(|x| x.len()).sum();
		let mut builder = LayerBuilder::with_capacity(count);
		for buffer in to_seal.iter_mut() {
			for ((key, val), time, diff) in buffer.drain(..) {
				debug_assert!(!diff.is_zero());
				builder.push((key, val, time, diff));
			}
		}

		// Recycle the consumed buffers, if appropriate.
		self.sorter.rebalance(&mut to_seal, 256);

		// Return the finished layer with its bounds.
		let result = builder.done(&self.lower[..], upper, &self.lower[..]);
		self.lower = upper.to_vec();
		result
	}

	fn frontier(&mut self) -> &[T] {
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
pub struct LayerBuilder<K: HashOrdered, V: Ord, T: Ord, R> {
	builder: RHHBuilder<K, V, T, R>,
}

impl<K, V, T, R> Builder<K, V, T, R, Layer<K, V, T, R>> for LayerBuilder<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Ring {

	fn new() -> Self { LayerBuilder { builder: RHHBuilder::new() } }
	#[inline(always)]
	fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
		self.builder.push_tuple((key, (val, (time, diff))));
	}

	#[inline(never)]
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Layer<K, V, T, R> {
		Layer {
			layer: Rc::new(self.builder.done()),
			desc: Description::new(lower, upper, since)
		}
	}
}

impl<K, V, T, R> LayerBuilder<K, V, T, R> 
where K: Clone+Default+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone+Default, R: Ring {
	fn with_capacity(cap: usize) -> Self { LayerBuilder { builder: RHHBuilder::with_capacity(cap) } }
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
#[inline(always)]
pub fn consolidate_vec<D: Ord+Clone, T:Ord+Clone, R: Ring>(slice: &mut Vec<(D, T, R)>) {
	slice.sort_by(|x,y| (&x.0,&x.1).cmp(&(&y.0, &y.1)));
	for index in 1 .. slice.len() {
		if slice[index].0 == slice[index - 1].0 && slice[index].1 == slice[index - 1].1 {
			slice[index].2 = slice[index].2 + slice[index - 1].2;
			slice[index - 1].2 = R::zero();
		}
	}
	slice.retain(|x| !x.2.is_zero());
}