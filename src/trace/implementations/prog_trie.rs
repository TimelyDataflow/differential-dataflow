
use std::rc::Rc;
use std::cmp::Ordering;

use lattice::Lattice;
use trace::{Batch, Builder, Cursor, Trace, consolidate};
use trace::cursor::CursorList;

// A trace is made up of several layers, each of which are immutable collections of updates.
pub struct Spine<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<LayerMerge<Key, Val, Time>>>	// Several possibly shared collections of updates.
}


impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> Trace<Key, Val, Time> for Spine<Key, Val, Time> {

	/// The type of an immutable collection of updates.
	type Batch = Rc<Layer<Key, Val, Time>>;
	/// The type used to enumerate the collections contents.
	type Cursor = CursorList<LayerCursor<Key, Val, Time>>;

	/// Allocates a new empty trace.
	fn new(default: Time) -> Self {
		Spine { 
			frontier: vec![default],
			layers: Vec::new(),
		} 		
	}

	/// Introduces a batch of updates to the trace.
	///
	/// Note: this does not perform progressive merging; that code is around somewhere though.
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
	/// Acquires a cursor to the collection's contents.
	fn cursor(&self) -> Self::Cursor {
		let mut cursors = Vec::new();
		for layer in &self.layers[..] {
			cursors.push(layer.cursor());
		}

		CursorList::new(cursors)
	}
	/// Advances the frontier of times the collection must respond to.
	fn advance_by(&mut self, frontier: &[Time]) {
		self.frontier = frontier.to_vec();
	}
}


/// An immutable collection of update tuples, from a contiguous interval of logical times.
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
		let mut times = Vec::new();

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
			desc: Description {
				lower: lower.to_vec(),
				upper: upper.to_vec(),
				since: lower.to_vec(),
			}
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


/// Layer wrapper for either layers and layer merges in progress.
pub enum LayerMerge<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	/// A merge in progress, contain references to the two source layers and the merge state.
	Merging(Rc<Layer<Key, Val, Time>>, Rc<Layer<Key, Val, Time>>, Merge<Key, Val, Time>),
	/// A single layer of a merge that was completed or never started.
	Finished(Rc<Layer<Key, Val, Time>>),
}

impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> LayerMerge<Key, Val, Time> {

	/// Creates a new merge object for a pair of layers.
	fn merge(layer1: Rc<Layer<Key, Val, Time>>, layer2: Rc<Layer<Key, Val, Time>>, frontier: &[Time]) -> Self {
		// bold to assert, but let's see why not.
		assert!(layer1.desc.upper == layer2.desc.lower);
		LayerMerge::Merging(layer1.clone(), layer2.clone(), Merge {
			source1: LayerCursor::new(layer1),
			source2: LayerCursor::new(layer2),
			result: Layer::empty(),
			frontier: frontier.to_vec(),
			worked: 0,
			target: 0,
		})
	}

	/// Replaces a `Finished` variant with a `Merging` variant using `self` and `other`.
	pub fn merge_with(&mut self, other: Rc<Layer<Key, Val, Time>>, frontier: &[Time]) {
		// unimplemented!();
		let this = ::std::mem::replace(self, LayerMerge::Finished(Rc::new(Layer::empty())));
		if let LayerMerge::Finished(layer) = this {
			*self = LayerMerge::merge(layer, other, frontier);
		}
		else {
			panic!("started merge into un-finished merge");
		}
	}

	/// Merges at least `units` tuples; returns "left over" effort.
	pub fn work(&mut self, units: usize) -> usize {
		let result = if let LayerMerge::Merging(_,_,ref mut merge) = *self {
			merge.work(units)
		}
		else {
			None
		};

		if let Some((result, remaining)) = result {
			*self = LayerMerge::Finished(Rc::new(result));
			remaining
		}
		else {
			0
		}
	}

	/// Number of tuples, or sum of source tuples for merges in progress.
	///
	/// The length may decrease when a merge completes and cancelation or consolidation occurs.
	pub fn len(&self) -> usize {
		match self {
			&LayerMerge::Merging(ref x, ref y,_) => x.times.len() + y.times.len(),
			&LayerMerge::Finished(ref x) => x.times.len(),
		}
	}
}

/// A merge in progress.
///
/// This structure represents the state for a merge in progress, used by progressive merging.
pub struct Merge<Key: Ord, Val: Ord, Time: Lattice+Ord> {
	source1: LayerCursor<Key, Val, Time>,
	source2: LayerCursor<Key, Val, Time>,
	result: Layer<Key, Val, Time>,
	frontier: Vec<Time>,
	worked: usize,
	target: usize,
}

impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> Merge<Key, Val, Time> {

	/// Merges at least `units` source tuples. Returns the layer and "left over" effort when the merge completes.
	fn work(&mut self, units: usize) -> Option<(Layer<Key, Val, Time>, usize)> {

		self.target += units;
		while self.source1.key_valid() && self.source2.key_valid() && self.worked < self.target {

			// note where we start to account for amont of work done.
			let times_pos = (self.source1.val_slice.0, self.source2.val_slice.0);

			// compare keys to determine what work to do next.
			match self.source1.key().cmp(&self.source2.key()) {
				Ordering::Less => 		{
					self.result.extend_from(&self.source1.layer, self.source1.key_slice.0, &self.frontier[..]);
					self.source1.step_key();
				},
				Ordering::Equal => 		{

					let vals_len = self.result.vals.len();

					// we will do all the work, rather than monitor self.worked.
					while self.source1.val_valid() && self.source2.val_valid() {

						let times_len = self.result.times.len();

						let result = &mut self.result;
						let frontier = &self.frontier[..];

						match self.source1.val().cmp(&self.source2.val()) {
							Ordering::Less => 		{
								self.source1.map_times(|time, diff| {
									result.times.push((time.advance_by(frontier).unwrap(), diff));
								});
								consolidate(&mut result.times, times_len);

								// if we pushed times, push the value.
								if result.times.len() > times_len {
									result.vals.push((self.source1.val().clone(), result.times.len()));
								}

								self.source1.step_val();
							},
							Ordering::Equal => 		{

								self.source1.map_times(|time, diff| {
									result.times.push((time.advance_by(frontier).unwrap(), diff));
								});
								self.source2.map_times(|time, diff| {
									result.times.push((time.advance_by(frontier).unwrap(), diff));
								});

								consolidate(&mut result.times, times_len);

								// if we pushed times, push the value.
								if result.times.len() > times_len {
									result.vals.push((self.source1.val().clone(), result.times.len()));
								}

								self.source1.step_val();
								self.source2.step_val();
							},
							Ordering::Greater => 	{

								self.source2.map_times(|time, diff| {
									result.times.push((time.advance_by(&frontier).unwrap(), diff));
								});
								consolidate(&mut result.times, times_len);

								// if we pushed times, push the value.
								if result.times.len() > times_len {
									result.vals.push((self.source2.val().clone(), result.times.len()));
								}

								self.source2.step_val();
							},
						}
					}

					while self.source1.val_valid() {
						let times_len = self.result.times.len();
						let result = &mut self.result;
						let frontier = &self.frontier[..];
						self.source1.map_times(|time, diff| {
							result.times.push((time.advance_by(frontier).unwrap(), diff));
						});

						consolidate(&mut result.times, times_len);

						// if we pushed times, push the value.
						if result.times.len() > times_len {
							result.vals.push((self.source1.val().clone(), result.times.len()));
						}

						self.source1.step_val();
					}

					while self.source2.val_valid() {
						let times_len = self.result.times.len();
						let result = &mut self.result;
						let frontier = &self.frontier[..];
						self.source2.map_times(|time, diff| {
							result.times.push((time.advance_by(frontier).unwrap(), diff));
						});

						consolidate(&mut result.times, times_len);

						// if we pushed times, push the value.
						if result.times.len() > times_len {
							result.vals.push((self.source2.val().clone(), result.times.len()));
						}

						self.source2.step_val();
					}

					// if we pushed values, push the key.
					if self.result.vals.len() > vals_len {
						self.result.keys.push(self.source1.key().clone());
						self.result.offs.push(self.result.vals.len());
					}

					self.source1.step_key();
					self.source2.step_key();
				},
				Ordering::Greater =>	{
					self.result.extend_from(&self.source2.layer, self.source2.key_slice.0, &self.frontier[..]);
					self.source2.step_key();		
				},
			}

			self.worked += self.source1.val_slice.0 - times_pos.0;
			self.worked += self.source2.val_slice.0 - times_pos.1;
		}

		// if self.source2 drained, get to draining self.source1.
		while self.source1.key_valid() && self.worked < self.target {
			let time_pos = self.source1.val_slice.0;
			self.result.extend_from(&self.source1.layer, self.source1.key_slice.0, &self.frontier[..]);
			self.worked += self.source1.val_slice.0 - time_pos;
			self.source1.step_key();
		}

		// if self.source1 drained, get to draining self.source2.
		while self.source2.key_valid() && self.worked < self.target {
			let time_pos = self.source2.val_slice.0;
			self.result.extend_from(&self.source2.layer, self.source2.key_slice.0, &self.frontier[..]);
			self.worked += self.source2.val_slice.0 - time_pos;
			self.source2.step_key();
		}

		// we are done! finish things up
		if !self.source1.key_valid() && !self.source2.key_valid() {

			// println!("\nmerge complete");
			// println!("layer1: {:?}", self.source1.layer);
			// println!("layer2: {:?}", self.source2.layer);
			// println!("result: {:?}", self.result);

			let empty_layer = Layer::new(Vec::new().into_iter(), &[], &[]);
			let surplus = if self.target > self.worked { self.target - self.worked + 1 } else { 1 };
			Some((::std::mem::replace(&mut self.result, empty_layer), surplus))
		}
		else {
			None
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
pub struct Description<Time> {
	/// lower frontier of contained updates.
	lower: Vec<Time>,
	/// upper frontier of contained updates.
	upper: Vec<Time>,
	/// frontier used for update compaction.
	since: Vec<Time>,
}

/// A cursor for navigating a single layer.
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

impl<Key: Ord, Val: Ord, Time: Lattice+Ord> Cursor for LayerCursor<Key, Val, Time> {

	type Key = Key;
	type Val = Val;
	type Time = Time;

	fn key(&self) -> &Key { 
		debug_assert!(self.key_valid());
		&self.layer.keys[self.key_slice.0]
	}
	fn val(&self) -> &Val { 
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		&self.layer.vals[self.val_slice.0].0
	}
	fn map_times<L: FnMut(&Time, isize)>(&self, mut logic: L) {
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
	fn peek_key(&self) -> Option<&Self::Key> {
		if self.key_valid() { Some(self.key()) } else { None }
	}

	fn step_val(&mut self) {
		self.val_slice.0 += 1;
		if !self.val_valid() {
			self.val_slice.0 = self.val_slice.1;
		}
	}
	fn seek_val(&mut self, val: &Self::Val) {
		let step = advance(&self.layer.vals[self.val_slice.0 .. self.val_slice.1], |v| v.0.lt(val));
		self.val_slice.0 += step;
		if !self.val_valid() {
			self.val_slice.0 = self.val_slice.1; 
		}
	}
	fn peek_val(&self) -> Option<&Self::Val> {
		if self.val_valid() { Some(self.val()) } else { None }
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
        //             self.buffer[index].1 += self.buffer[index-1].1;
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

impl<Key: Ord, Val: Ord, Time: Ord> Builder<Key, Val, Time> for LayerBuilder<(Key, Val, Time)> {
	fn new() -> Self { LayerBuilder::new() }
	fn push(&mut self, (key, val, time, diff): (Key, Val, Time, isize)) {
		self.push(((key, val, time), diff));
	}
}

impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> Into<Rc<Layer<Key,Val,Time>>> for LayerBuilder<(Key, Val, Time)> {
	fn into(self) -> Rc<Layer<Key,Val,Time>> {
		Rc::new(Layer::new(self.done().into_iter().map(|((k,v,t),d)| (k,v,t,d)), &[], &[]))
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


// impl<Key: Ord+Clone, Val: Ord+Clone, Time: Lattice+Ord+Clone> Spine<Key, Val, Time> {
// 	/// Inserts a new layer layer, merging to maintain few layers.
// 	pub fn _prog_insert(&mut self, layer: Rc<Layer<Key, Val, Time>>) {

// 		// TODO : we want to support empty layers to allow descriptions to advance.
// 		if layer.times.len() > 0 {

// 			let effort = 4 * layer.times.len();

// 			// apply effort to existing merges.
// 			// when a merge finishes, consider merging it with the preceding layer.
// 			let mut index = 0;
// 			while index < self.layers.len() {

// 				let mut units = self.layers[index].work(effort);
// 				if units > 0 {

// 					if self.layers[index].len() == 0 {
// 						self.layers.remove(index);
// 					}
// 					else {
// 						while units > 0 && index > 0 && index < self.layers.len() && self.layers[index-1].len() < 2 * self.layers[index].len() {

// 							if let LayerMerge::Finished(layer) = self.layers.remove(index) {
// 								self.layers[index-1].merge_with(layer, &self.frontier[..]);
// 							}

// 							index -= 1;
// 							units = self.layers[index].work(units);
// 							if self.layers[index].len() == 0 {
// 								self.layers.remove(index);
// 							}
// 						}
// 					}
// 				}
// 				else {
// 					index += 1;
// 				}
// 			}

// 			// while last two elements exist, both less than layer.len()
// 			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.times.len() {
// 				match (self.layers.pop(), self.layers.pop()) {
// 					(Some(LayerMerge::Finished(layer1)), Some(LayerMerge::Finished(layer2))) => {
// 						let merge = Layer::merge(&*layer1, &*layer2, &self.frontier[..]);
// 						if merge.times.len() > 0 {
// 							self.layers.push(LayerMerge::Finished(Rc::new(merge)));
// 						}
// 					},
// 					_ => { panic!("found unfinished merge; debug!"); }
// 				}
// 			}

// 			// now that `layer` is smallest, push it on.
// 			self.layers.push(LayerMerge::Finished(layer));

// 			// if we need to start a merge, let's do that.
// 			if self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
// 				if let Some(LayerMerge::Finished(layer)) = self.layers.pop() {
// 					let layers_len = self.layers.len();
// 					self.layers[layers_len - 1].merge_with(layer, &self.frontier[..]);
// 				}
// 				else {
// 					panic!("found unfinished merge; debug!");
// 				}
// 			}
// 		}
// 	}
// }