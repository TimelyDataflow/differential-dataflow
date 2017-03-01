//! A trie-structured representation of update tuples. 
//!
//! The trie representation separates out the key, val, and time components of update tuples into distinct
//! arrays. This representation maintains one copy of each key, and one copy of each value for each key. 
//! 
//! One of the intents of the trie representation is that it can provide multiple `Trace` implementations: 
//! the trie can have the standard (key, val) interpretation, but it can also act as a trace whose keys are
//! (key, val) and whose values are (), which can be useful for set operations (e.g. `distinct`) on the 
//! collection. These other implementations are currently commented out as they confound type inference.

use std::rc::Rc;
use std::cmp::Ordering;
use std::fmt::Debug;

use lattice::Lattice;
use trace::{Batch, Builder, Cursor, Trace, consolidate};
use trace::description::Description;
use trace::cursor::cursor_list::CursorList;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
pub struct Spine<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Val, Time>>>	// Several possibly shared collections of updates.
}

impl<K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+Debug+'static> Trace<K, V, T> for Spine<K, V, T> {

	type Batch = Rc<Layer<K, V, T>>;
	type Cursor = CursorList<K, V, T, LayerCursor<K, V, T>>;

	fn new(default: T) -> Self {
		Spine { 
			frontier: vec![default],
			layers: Vec::new(),
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {
		if layer.times.len() > 0 {
			// while last two elements exist, both less than layer.len()
			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.times.len() {
				let layer1 = self.layers.pop().unwrap();
				let layer2 = self.layers.pop().unwrap();
				let result = Rc::new(Layer::merge(&layer1, &layer2, &self.frontier[..]));
				self.layers.push(result);
			}

			self.layers.push(layer);

			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
				let layer1 = self.layers.pop().unwrap();
				let layer2 = self.layers.pop().unwrap();
				let result = Rc::new(Layer::merge(&layer1, &layer2, &self.frontier[..]));
				self.layers.push(result);
			}
		}
	}
	fn cursor(&self) -> Self::Cursor {
		let mut cursors = Vec::new();
		for layer in &self.layers[..] {
			cursors.push(layer.cursor());
		}

		CursorList::new(cursors)
	}
	fn advance_by(&mut self, frontier: &[T]) {
		self.frontier = frontier.to_vec();
	}
}

/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct Layer<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	/// Sorted list of keys with at least one update tuple.
	pub keys: Vec<Key>,
	/// Offsets in `self.vals` where the corresponding key *stops*.
	pub offs: Vec<usize>,
	/// Values and offsets in `self.times`.
	pub vals: Vec<(Val, usize)>,
	/// Times and differences, in correspondence with update tuples.
	pub times: Vec<(Time, isize)>,
	/// Description of the update times this layer represents.
	pub desc: Description<Time>,
}

impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> Batch<Key, Val, Time> for Rc<Layer<Key, Val, Time>> {

	type Builder = LayerBuilder<(Key, Val, Time)>;
	type Cursor = LayerCursor<Key, Val, Time>;
	fn cursor(&self) -> Self::Cursor { LayerCursor::new(self.clone()) }
	fn len(&self) -> usize { self.times.len() }
}

impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> Layer<Key, Val, Time> {
	/// Constructs a new Layer from sorted data.
	pub fn new<I: Iterator<Item=(Key,Val,Time,isize)>>(mut data: I, lower: &[Time], upper: &[Time]) -> Layer<Key, Val, Time> {

		let mut keys = Vec::new();
		let mut offs = Vec::new();
		let mut vals = Vec::new();
		let mut times = Vec::with_capacity(data.size_hint().0);

		if let Some((key, val, time, diff)) = data.next() {

			keys.push(key);
			offs.push(1);
			vals.push((val, 1));
			times.push((time, diff));

			for (key, val, time, diff) in data {

				times.push((time, diff));
				if key != keys[keys.len()-1] {
					vals.push((val, times.len()));
					keys.push(key);
					offs.push(vals.len())
				}
				else if val != vals[vals.len()-1].0 {
					vals.push((val, times.len()));
				}

				let vals_len = vals.len();
				vals[vals_len - 1].1 = times.len();
				let keys_len = keys.len();
				offs[keys_len-1] = vals.len();
			}
		}

		Layer {
			keys: keys,
			offs: offs,
			vals: vals,
			times: times,
			desc: Description::new(lower, upper, lower),
		}
	}

	/// Returns an empty new layer.
	pub fn empty() -> Layer<Key, Val, Time> {
		Layer::new(Vec::new().into_iter(), &[], &[])
	}

	/// Extends a layer from a key in another layer.
	fn extend_from(&mut self, layer: &Self, key_off: usize, frontier: &[Time]) {

		let lower = if key_off == 0 { 0 } else { layer.offs[key_off-1] };
		let upper = layer.offs[key_off];

		let vals_len = self.vals.len();

		for val_index in lower .. upper {

			let times_len = self.times.len();
			let lower = if val_index == 0 { 0 } else { layer.vals[val_index-1].1 };
			let upper = layer.vals[val_index].1;

			for &(ref time, diff) in &layer.times[lower .. upper] {
				self.times.push((time.advance_by(frontier).unwrap(), diff));
			}
			consolidate(&mut self.times, times_len);

			if self.times.len() > times_len {
				self.vals.push((layer.vals[val_index].0.clone(), self.times.len()));
			}
		}

		if self.vals.len() > vals_len {
			self.keys.push(layer.keys[key_off].clone());
			self.offs.push(self.vals.len());
		}
	}

	/// Conducts a full merge, right away. Times advanced per `frontier`.
	pub fn merge(layer1: &Self, layer2: &Self, frontier: &[Time]) -> Self {

		// TODO : Figure out if lower, upper are just unions or something.
		// NOTE : The should be, as we only merge contiguous time slices.
		let mut result = Layer::new(Vec::new().into_iter(), &[], &[]);

		result.keys = Vec::with_capacity(layer1.keys.len() + layer2.keys.len());
		result.offs = Vec::with_capacity(layer1.offs.len() + layer2.offs.len());
		result.vals = Vec::with_capacity(layer1.vals.len() + layer2.vals.len());
		result.times = Vec::with_capacity(layer1.times.len() + layer2.times.len());

		let mut key1_off = 0;
		let mut key2_off = 0;

		while key1_off < layer1.keys.len() && key2_off < layer2.keys.len() {

			match layer1.keys[key1_off].cmp(&layer2.keys[key2_off]) {
				Ordering::Less		=> {
					result.extend_from(layer1, key1_off, frontier);
					key1_off += 1;
				},
				Ordering::Equal  	=> {

					let vals_len = result.vals.len();

					let mut lower1 = if key1_off == 0 { 0 } else { layer1.offs[key1_off-1] };
					let upper1 = layer1.offs[key1_off];

					let mut lower2 = if key2_off == 0 { 0 } else { layer2.offs[key2_off-1] };
					let upper2 = layer2.offs[key2_off];

					while lower1 < upper1 && lower2 < upper2 {

						let times_len = result.times.len();

						let old_lower1 = lower1;
						let old_lower2 = lower2;

						if layer1.vals[old_lower1].0 <= layer2.vals[old_lower2].0 {
							let t_lower = if lower1 == 0 { 0 } else { layer1.vals[lower1 - 1].1 };
							let t_upper = layer1.vals[lower1].1;
							for &(ref time, diff) in &layer1.times[t_lower .. t_upper] {
								result.times.push((time.advance_by(frontier).unwrap(), diff));
							}
							lower1 += 1;
						}

						if layer2.vals[old_lower2].0 <= layer1.vals[old_lower1].0 {
							let t_lower = if lower2 == 0 { 0 } else { layer2.vals[lower2 - 1].1 };
							let t_upper = layer2.vals[lower2].1;
							for &(ref time, diff) in &layer2.times[t_lower .. t_upper] {
								result.times.push((time.advance_by(frontier).unwrap(), diff));
							}		
							lower2 += 1					
						}

						consolidate(&mut result.times, times_len);

						if result.times.len() > times_len {
							if lower1 > old_lower1 {	
								result.vals.push((layer1.vals[old_lower1].0.clone(), result.times.len()));
							}
							else {
								result.vals.push((layer2.vals[old_lower2].0.clone(), result.times.len()));								
							}
						}
					}

					while lower1 < upper1 {
						let times_len = result.times.len();

						let t_lower = if lower1 == 0 { 0 } else { layer1.vals[lower1 - 1].1 };
						let t_upper = layer1.vals[lower1].1;
						for &(ref time, diff) in &layer1.times[t_lower .. t_upper] {
							result.times.push((time.advance_by(frontier).unwrap(), diff));
						}

						consolidate(&mut result.times, times_len);

						if result.times.len() > times_len {
							result.vals.push((layer1.vals[lower1].0.clone(), result.times.len()));
						}

						lower1 += 1;
					}

					while lower2 < upper2 {
						let times_len = result.times.len();

						let t_lower = if lower2 == 0 { 0 } else { layer2.vals[lower2 - 1].1 };
						let t_upper = layer2.vals[lower2].1;
						for &(ref time, diff) in &layer2.times[t_lower .. t_upper] {
							result.times.push((time.advance_by(frontier).unwrap(), diff));
						}

						consolidate(&mut result.times, times_len);

						if result.times.len() > times_len {
							result.vals.push((layer2.vals[lower2].0.clone(), result.times.len()));
						}

						lower2 += 1;
					}

					// push key if vals pushed.
					if result.vals.len() > vals_len {
						result.keys.push(layer1.keys[key1_off].clone());
						result.offs.push(result.vals.len());
					}

					key1_off += 1;
					key2_off += 1;
				},
				Ordering::Greater	=> {
					result.extend_from(layer2, key2_off, frontier);
					key2_off += 1;
				},
			}
		}

		while key1_off < layer1.keys.len() {
			result.extend_from(layer1, key1_off, frontier);
			key1_off += 1;
		}

		while key2_off < layer2.keys.len() {
			result.extend_from(layer2, key2_off, frontier);
			key2_off += 1;
		}

		result
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct LayerCursor<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	key_slice: (usize, usize),
	val_slice: (usize, usize),
	layer: Rc<Layer<Key, Val, Time>>,
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> LayerCursor<Key, Val, Time> {
	/// Creates a new LayerCursor from non-empty layer data. 
	pub fn new(layer: Rc<Layer<Key, Val, Time>>) -> LayerCursor<Key, Val, Time> {
		LayerCursor {
			key_slice: (0, layer.keys.len()),
			val_slice: (0, if layer.keys.len() > 0 { layer.offs[0] } else { 0 }),
			layer: layer,
		}
	}
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Cursor<Key, Val, Time> for LayerCursor<Key, Val, Time> {

	fn key(&self) -> &Key { 
		debug_assert!(self.key_valid());
		&self.layer.keys[self.key_slice.0]
	}
	fn val(&self) -> &Val { 
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		&self.layer.vals[self.val_slice.0].0
	}
	fn map_times<L: FnMut(&Time, isize)>(&mut self, mut logic: L) {
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		let lower = if self.val_slice.0 == 0 { 0 } else { self.layer.vals[self.val_slice.0 - 1].1 };
		let upper = self.layer.vals[self.val_slice.0].1;
		for &(ref time, diff) in &self.layer.times[lower .. upper] {
			logic(time, diff);
		}
	}

	fn key_valid(&self) -> bool { self.key_slice.0 < self.key_slice.1 }
	fn val_valid(&self) -> bool { self.val_slice.0 < self.val_slice.1 }

	fn step_key(&mut self){
		self.key_slice.0 += 1;
		if self.key_valid() {
			let lower = if self.key_slice.0 > 0 { self.layer.offs[self.key_slice.0-1] } else { 0 };
			let upper = self.layer.offs[self.key_slice.0];
			self.val_slice = (lower, upper);
		}
		else {
			self.key_slice.0 = self.key_slice.1;	// step back so we don't overflow.
			self.val_slice = (self.layer.times.len(), self.layer.times.len());
		}
	}

	fn seek_key(&mut self, key: &Key) {
		let step = advance(&self.layer.keys[self.key_slice.0 .. self.key_slice.1], |k| k.lt(key));
		self.key_slice.0 += step;
		if self.key_valid() {
			let lower = if self.key_slice.0 > 0 { self.layer.offs[self.key_slice.0-1] } else { 0 };
			let upper = self.layer.offs[self.key_slice.0];
			self.val_slice = (lower, upper);
		}
		else {
			self.key_slice.0 = self.key_slice.1;
			self.val_slice = (self.layer.times.len(), self.layer.times.len());
		}
	}

	fn step_val(&mut self) {
		self.val_slice.0 += 1;
		if !self.val_valid() {
			self.val_slice.0 = self.val_slice.1;
		}
	}
	fn seek_val(&mut self, val: &Val) {
		let step = advance(&self.layer.vals[self.val_slice.0 .. self.val_slice.1], |v| v.0.lt(val));
		self.val_slice.0 += step;
		if !self.val_valid() {
			self.val_slice.0 = self.val_slice.1; 
		}
	}

	fn rewind_keys(&mut self) {
		*self = LayerCursor::new(self.layer.clone())
	}

	fn rewind_vals(&mut self) {
		debug_assert!(self.key_valid());
		let lower = if self.key_slice.0 > 0 { self.layer.offs[self.key_slice.0-1] } else { 0 };
		let upper = self.layer.offs[self.key_slice.0];
		self.val_slice = (lower, upper);
	}
}

/// A builder for creating layers from unsorted update tuples.
pub struct LayerBuilder<T: Ord> {
    sorted: usize,
    buffer: Vec<(T, isize)>,
}

impl<T: Ord> LayerBuilder<T> {
    /// Allocates a new batch compacter.
    pub fn new() -> LayerBuilder<T> {
        LayerBuilder {
            sorted: 0,
            buffer: Vec::new(),
        }
    }

    /// Adds an element to the batch compacter.
    pub fn push(&mut self, element: (T, isize)) {
        self.buffer.push(element);
        // if self.buffer.len() > ::std::cmp::max(self.sorted * 2, 1024) {
        //     self.buffer.sort();
        //     for index in 1 .. self.buffer.len() {
        //         if self.buffer[index].0 == self.buffer[index-1].0 {
        //             self.buffer[index].1 += self.buffer[index-1].1;g
        //             self.buffer[index-1].1 = 0;
        //         }
        //     }
        //     self.buffer.retain(|x| x.1 != 0);
        //     self.sorted = self.buffer.len();
        // }
    }
    /// Finishes compaction, returns results.
    pub fn done(mut self) -> Vec<(T, isize)> {
        if self.buffer.len() > self.sorted {
            self.buffer.sort();
            for index in 1 .. self.buffer.len() {
                if self.buffer[index].0 == self.buffer[index-1].0 {
                    self.buffer[index].1 += self.buffer[index-1].1;
                    self.buffer[index-1].1 = 0;
                }
            }
            self.buffer.retain(|x| x.1 != 0);
            self.sorted = self.buffer.len();
        }
        self.buffer
    }
}

impl<Key, Val, Time> Builder<Key, Val, Time, Rc<Layer<Key, Val, Time>>> for LayerBuilder<(Key, Val, Time)> 
where Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone {
	fn new() -> Self { LayerBuilder::new() }
	fn push(&mut self, (key, val, time, diff): (Key, Val, Time, isize)) {
		self.push(((key, val, time), diff));
	}
	fn done(self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Val, Time>> {
		Rc::new(Layer::new(self.done().into_iter().map(|((k,v,t),d)| (k,v,t,d)), lower, upper))
	}
}

/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to 
/// count the number of elements in time logarithmic in the result.
#[inline(never)]
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {

        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step = step << 1;
        }

        // advance in exponentially shrinking steps.
        step = step >> 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step = step >> 1;
        }

        index += 1;
    }   

    index
}