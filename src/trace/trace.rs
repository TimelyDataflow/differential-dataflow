//! A navigable collection of (key, val, time, isize) updates.
//! 
//! A `Trace` supports searching by key, within which one can search by val, 

use std::rc::Rc;
use lattice::Lattice;

// A trace is made up of several layers, each of which are immutable collections of updates.
//
// The layers are Rc'd to exploit their immutability through more aggressive sharing.
#[allow(dead_code)]
pub struct Trace<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<LayerMerge<Key, Val, Time>>	// Several possibly shared collections of updates.
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Trace<Key, Val, Time> {

	/// Creates a new empty trace
	#[allow(dead_code)]
	pub fn new(default: Time) -> Self { 
		Trace { 
			frontier: vec![default],
			layers: Vec::new(),
		} 
	}

	/// Returns a wholly owned cursor to navigate the trace.
	#[allow(dead_code)]
	pub fn cursor(&self) -> TraceCursor<Key, Val, Time> {
		TraceCursor::new(&self.layers[..])
	}

	/// Inserts a new layer layer, merging to maintain few layers.
	#[allow(dead_code)]
	pub fn insert(&mut self, layer: Rc<Layer<Key, Val, Time>>) {
		let units = layer.times.len();
		for layer in &mut self.layers {
			if let LayerMerge::Merging(_,_,ref mut merge) = *layer {
				merge.work(units);
			}
		}

		// unimplemented!();
		self.layers.push(LayerMerge::Finished(layer));
	}

	#[allow(dead_code)]
	pub fn advance_frontier(&mut self, _frontier: &[Time]) {
		unimplemented!();
	}
}

pub enum LayerMerge<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	Merging(Rc<Layer<Key, Val, Time>>, Rc<Layer<Key, Val, Time>>, Merge<Key, Val, Time>),
	Finished(Rc<Layer<Key, Val, Time>>),
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> LayerMerge<Key, Val, Time> {
	fn work(&mut self, units: usize) {
		if let LayerMerge::Merging(_,_,ref mut merge) = *self {
			merge.work(units);
		}
	}
}

pub struct Merge<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	source1: Rc<Layer<Key, Val, Time>>,
	source2: Rc<Layer<Key, Val, Time>>,
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Merge<Key, Val, Time> {
	// merges at least `units` updates.
	fn work(&mut self, units: usize) {
		unimplemented!();
	}
}

// A layer organizes data first by key, then by val, and finally by time.
#[allow(dead_code)]
pub struct Layer<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	keys: Vec<(Key, usize)>,		// key and offset in self.vals of final element.
	vals: Vec<(Val, usize)>,		// val and offset in self.times of final element.
	times: Vec<(Time, isize)>,		// time and corresponding difference.
	desc: Description<Time>,		// describes contents of the layer (from when, mainly).
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord+Clone> Layer<Key, Val, Time> {
	/// Constructs a new Layer from sorted data.
	#[allow(dead_code)]
	pub fn new<I: Iterator<Item=(Key,Val,Time,isize)>>(mut data: I, lower: &[Time], upper: &[Time]) -> Layer<Key, Val, Time> {

		let mut keys = Vec::new();
		let mut vals = Vec::new();
		let mut times = Vec::new();

		if let Some((key, val, time, diff)) = data.next() {

			keys.push((key, 1));
			vals.push((val, 1));
			times.push((time, diff));

			for (key, val, time, diff) in data {

				times.push((time, diff));
				if key != keys[keys.len()-1].0 {
					vals.push((val, times.len()));
					keys.push((key, vals.len()));
				}
				else if val != vals[vals.len()-1].0 {
					vals.push((val, times.len()));
				}

				let vals_len = vals.len();
				vals[vals_len - 1].1 = times.len();
				let keys_len = keys.len();
				keys[keys_len-1].1 = vals.len();
			}
		}

		Layer {
			keys: keys,
			vals: vals,
			times: times,
			desc: Description {
				lower: lower.to_vec(),
				upper: upper.to_vec(),
				since: lower.to_vec(),
			}
		}
	}
}

/// Describes an interval of partially ordered times.
///
/// A `Description` indicates a set of partially ordered times, and a moment at which they are
/// observed. The `lower` and `upper` frontiers bound the times contained within, and the `since`
/// frontier indicates a moment at which the times were observed. If `since` is strictly in 
/// advance of `lower`, the contained times may be "advanced" to times which appear equivalent to
/// any time after `since`.
#[allow(dead_code)]
struct Description<Time: Lattice+Ord> {
	lower: Vec<Time>,	// lower frontier of contained updates.
	upper: Vec<Time>,	// upper frontier of contained updates.
	since: Vec<Time>,	// frontier used for update compaction.
}

/// A cursor allowing navigation of a trace.
#[allow(dead_code)]
pub struct TraceCursor<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	layers: Vec<LayerCursor<Key, Val, Time>>,	// cursors for each layer.
	dirty: usize,								// number of consumed layers.
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> TraceCursor<Key, Val, Time> {

	#[allow(dead_code)]
	fn new(layers: &[LayerMerge<Key, Val, Time>]) -> TraceCursor<Key, Val, Time> {
		let mut cursors = Vec::new();
		for layer in layers {
			match layer {
				&LayerMerge::Merging(ref layer1, ref layer2, _) => {					
					cursors.push(LayerCursor::new(layer1.clone()));
					cursors.push(LayerCursor::new(layer2.clone())); 
				}
				&LayerMerge::Finished(ref layer) => { 
					cursors.push(LayerCursor::new(layer.clone())); 
				}
			}
		}
		TraceCursor { layers: cursors, dirty: 0 }
	}

	/// Advances the cursor to the next key, providing a view if the key exists.
	#[allow(dead_code)]
	pub fn next_key<'a>(&'a mut self) -> Option<KeyView<'a, Key, Val, Time>> {
		for index in 0 .. self.dirty { self.layers[index].step_key(); }
		self.tidy_keys();

		if self.layers.len() > 0 { 
			self.dirty = 1 + self.layers[1..].iter().take_while(|x| x.key() == self.layers[0].key()).count();
			Some(KeyView::new(&mut self.layers[0 .. self.dirty])) 
		} 
		else { 
			None 
		}
	}

	/// Seeks forward for the key, providing a view if the key is found.
	/// 
	/// This takes time logarithmic in the distance to the key, using a galloping search.
	/// The existence of the key indicates that data exist, however, and that the value 
	/// cursor will return some values, but updates across multiple layers may cancel with 
	/// each other.
	#[allow(dead_code)]
	pub fn seek_key<'a>(&'a mut self, key: &Key) -> Option<KeyView<'a, Key, Val, Time>> {

		let mut dirty = 0;
		while dirty < self.layers.len() && self.layers[dirty].key() < key {
			self.layers[dirty].seek_key(key);
			dirty += 1;
		}
		assert!(dirty >= self.dirty);
		self.dirty = dirty;
		self.tidy_keys();

		if self.layers.len() > 0 && self.layers[0].key() == key {
			self.dirty = 1 + self.layers[1..].iter().take_while(|x| x.key() == self.layers[0].key()).count();
			Some(KeyView::new(&mut self.layers[0 .. self.dirty]))
		}
		else {
			None
		}
	}

	pub fn peek_key(&mut self) -> Option<&Key> {
		self.tidy_keys();

		if self.layers.len() > 0 {
			Some(&self.layers[0].key())
		}
		else {
			None
		}
	}

	/// Called after the first `self.dirty` layer cursor have been "dirtied".
	///
	/// Each time we advance the key, either through `next_key` or `seek_key`, we first
	/// advance any cursor that has been previously used, and perhaps more in the case of 
	/// `seek_key`. Having done so, we need to re-introduce the invariant that `self.layers`
	/// contains only layers with valid keys, and is sorted by those keys.
	#[allow(dead_code)]
	fn tidy_keys(&mut self) {
		let mut valid = 0; 
		while valid < self.dirty {
			if self.layers[valid].key_valid() { 
				valid += 1; 
			}
			else { 
				self.layers.remove(valid); 	// linear work, but not too common.
				self.dirty -= 1;
			}
		}

		if self.dirty > 0 {
			let mut max_index = 0;
			for index in 1 .. self.dirty {
				if self.layers[index].key() > self.layers[max_index].key() {
					max_index = index;
				}
			}

			let further = self.layers[self.dirty ..].iter().take_while(|x| x.key() < self.layers[max_index].key()).count();
			self.layers[0 .. (self.dirty + further)].sort_by(|x,y| x.key().cmp(&y.key()));
			self.dirty = 0;
		}
	}
}

/// Wraps a `TraceCursor` and provides access to the values of the current key.
#[allow(dead_code)]
pub struct KeyView<'a, Key: Ord+'a, Val: Ord+'a, Time: Lattice+Ord+'a> {
	layers: &'a mut [LayerCursor<Key, Val, Time>],	// reference to source data.
	dirty: usize,									// number of consumed values.
}

impl<'a, Key: Ord+'a, Val: Ord+'a, Time: Lattice+Ord+'a> KeyView<'a, Key, Val, Time> {

	#[allow(dead_code)]
	fn new(layers: &'a mut [LayerCursor<Key, Val, Time>]) -> KeyView<'a, Key, Val, Time> {
		layers.sort_by(|x,y| x.val().cmp(&y.val()));
		KeyView {
			layers: layers,
			dirty: 0,
		}
	}

	/// Returns the key associated with the `KeyView`.
	#[allow(dead_code)]
	pub fn key(&self) -> &Key { self.layers[0].key() }

	/// Advances the key view to the next value, returns a view if it exists.
	#[allow(dead_code)]
	pub fn next_val(&mut self) -> Option<ValView<Key, Val, Time>> {
		for index in 0 .. self.dirty { self.layers[index].step_val(); }
		self.tidy_vals();

		if self.layers.len() > 0 { 
			self.dirty = 1 + self.layers[1..].iter().take_while(|x| x.val() == self.layers[0].val()).count();
			Some(ValView::new(&mut self.layers[0 .. self.dirty]))
		} 
		else { 
			None 
		}
	}
	/// Advances the key view to the sought value, returns a view if it exists.
	#[allow(dead_code)]
	pub fn seek_val<'b>(&'b mut self, val: &Val) -> Option<ValView<'b, Key, Val, Time>> { 
		let mut dirty = 0;
		while dirty < self.layers.len() && self.layers[dirty].val() < val {
			self.layers[dirty].seek_val(val);
			dirty += 1;
		}
		assert!(dirty >= self.dirty);
		self.dirty = dirty;
		self.tidy_vals();

		if self.layers.len() > 0 && self.layers[0].val() == val {
			self.dirty = 1 + self.layers[1..].iter().take_while(|x| x.val() == self.layers[0].val()).count();
			Some(ValView::new(&mut self.layers[0 .. self.dirty]))
		}
		else {
			None
		}
	}

	#[allow(dead_code)]
	fn tidy_vals(&mut self) {
		let mut valid = 0; 
		while valid < self.dirty {
			if self.layers[valid].val_valid() { 
				valid += 1; 
			}
			else { 
				// self.layers.remove(valid); 	// linear work, but not too common.
				for i in valid .. (self.layers.len() - 1) {
					self.layers.swap(i, i + 1);
				}
				let new_len = self.layers.len() - 1;
				// intent of next line is `self.layers = &mut self.layers[..new_len]`.
				self.layers = &mut ::std::mem::replace(&mut self.layers, &mut [])[.. new_len];
				self.dirty -= 1;
			}
		}

		if self.dirty > 0 {
			let mut max_index = 0;
			for index in 1 .. self.dirty {
				if self.layers[index].val() > self.layers[max_index].val() {
					max_index = index;
				}
			}

			let mut range = self.dirty;
			while range < self.layers.len() && self.layers[range].val() < self.layers[max_index].val() {
				range += 1;
			}

			self.layers[0 .. range].sort_by(|x,y| x.val().cmp(&y.val()));
			self.dirty = 0;
		}
	}
}

/// Wrapper around a fixed value, allows mapping across time diffs.
#[allow(dead_code)]
pub struct ValView<'a, Key: Ord+'a, Val: Ord+'a, Time: Lattice+Ord+'a> {
	layers: &'a mut [LayerCursor<Key, Val, Time>],	// reference to source data.
}

impl<'a, Key: Ord+'a, Val: Ord+'a, Time: Lattice+Ord+'a> ValView<'a, Key, Val, Time> {

	// constructs a new trace from a 
	#[allow(dead_code)]
	fn new(layers: &'a mut [LayerCursor<Key, Val, Time>]) -> ValView<'a, Key, Val, Time> {
		ValView {
			layers: layers,
		}
	} 

	/// Returns the value associated with the `ValView`.
	#[allow(dead_code)]
	pub fn val(&self) -> &Val { self.layers[0].val() }

	/// Applies `logic` to each time associated with the value.
	#[allow(dead_code)]
	pub fn map<L: FnMut(&(Time, isize))>(&self, mut logic: L) {
		for layer in self.layers.iter() {
			for times in layer.times() {
				logic(times)
			}
		}
	}
}

/// A cursor for navigating a single layer.
#[allow(dead_code)]
struct LayerCursor<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	key_slice: (usize, usize),
	val_slice: (usize, usize),
	layer: Rc<Layer<Key, Val, Time>>,
}

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> LayerCursor<Key, Val, Time> {

	// creates a new LayerCursor from non-empty layer data. 
	#[allow(dead_code)]
	fn new(layer: Rc<Layer<Key, Val, Time>>) -> LayerCursor<Key, Val, Time> {
		LayerCursor {
			key_slice: (0, layer.keys.len()),
			val_slice: (0, if layer.keys.len() > 0 { layer.keys[0].1 } else { 0 }),
			layer: layer,
		}
	}

	#[allow(dead_code)]
	fn key(&self) -> &Key { 
		debug_assert!(self.key_valid());
		&self.layer.keys[self.key_slice.0].0
	 }
	#[allow(dead_code)]
	fn val(&self) -> &Val { 
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		&self.layer.vals[self.val_slice.0].0
	}
	#[allow(dead_code)]
	fn times(&self) -> &[(Time, isize)] {
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		let lower = if self.val_slice.0 == 0 { 0 } else { self.layer.vals[self.val_slice.0 - 1].1 };
		let upper = self.layer.vals[self.val_slice.0].1;
		&self.layer.times[lower .. upper]
	}

	#[allow(dead_code)]
	fn key_valid(&self) -> bool { self.key_slice.0 < self.key_slice.1 }
	#[allow(dead_code)]
	fn val_valid(&self) -> bool { self.val_slice.0 < self.val_slice.1 }

	// advances key slice, updates val slice, and returns whether cursor is valid.
	#[allow(dead_code)]
	fn step_key(&mut self) -> bool {
		self.key_slice.0 += 1;
		if self.key_valid() {
			self.val_slice = (self.val_slice.1, self.layer.keys[self.key_slice.0].1);
			true
		}
		else {
			self.key_slice.0 = self.key_slice.1;	// step back so we don't overflow.
			self.val_slice = (0, 0);
			false
		}
	}

	// advances key slice, updates val slice, and returns whether cursor is valid.
	#[allow(dead_code)]
	fn seek_key(&mut self, key: &Key) -> bool {
		let step = advance(&self.layer.keys[self.key_slice.0 .. self.key_slice.1], |k| k.0.lt(key));
		self.key_slice.0 += step;
		if self.key_valid() {
			self.val_slice = (self.val_slice.1, self.layer.keys[self.key_slice.0].1);
			true			
		}
		else {
			self.key_slice.0 = self.key_slice.1;
			self.val_slice = (0, 0);
			false
		}
	}

	#[allow(dead_code)]
	fn step_val(&mut self) -> bool {
		self.val_slice.0 += 1;
		if self.val_valid() {
			true
		}
		else {
			self.val_slice.0 = self.val_slice.1;
			false
		}
	}
	#[allow(dead_code)]
	fn seek_val(&mut self, val: &Val) -> bool {
		let step = advance(&self.layer.vals[self.val_slice.0 .. self.val_slice.1], |v| v.0.lt(val));
		self.val_slice.0 += step;
		if self.val_valid() {
			true
		}
		else {
			self.val_slice.0 = self.val_slice.1; 
			false
		}
	}
}


/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to 
/// count the number of elements in time logarithmic in the result.
// #[inline(never)]
#[allow(dead_code)]
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
