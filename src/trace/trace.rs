//! A navigable collection of (key, val, time, isize) updates.
//! 
//! A `Trace` supports searching by key, within which one can search by val, 

use std::rc::Rc;
use std::fmt::Debug;
use std::cmp::Ordering;
use lattice::Lattice;


// A trace is made up of several layers, each of which are immutable collections of updates.
//
// The layers are Rc'd to exploit their immutability through more aggressive sharing.
#[derive(Debug)]
pub struct Trace<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<LayerMerge<Key, Val, Time>>	// Several possibly shared collections of updates.
}

impl<Key: Ord+Debug+Clone, Val: Ord+Debug+Clone, Time: Lattice+Ord+Debug+Clone> Trace<Key, Val, Time> {

	/// Creates a new empty trace
	pub fn new(default: Time) -> Self { 
		Trace { 
			frontier: vec![default],
			layers: Vec::new(),
		} 
	}

	/// Returns a wholly owned cursor to navigate the trace.
	pub fn cursor(&self) -> TraceCursor<Key, Val, Time> {
		TraceCursor::new(&self.layers[..])
	}

	pub fn insert(&mut self, layer: Rc<Layer<Key, Val, Time>>) {

		if layer.times.len() > 0 {
			// while last two elements exist, both less than layer.len()
			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.times.len() {
				match (self.layers.pop(), self.layers.pop()) {
					(Some(LayerMerge::Finished(layer1)), Some(LayerMerge::Finished(layer2))) => {
						let merge = Layer::merge(&*layer1, &*layer2, &self.frontier[..]);
						if merge.times.len() > 0 {
							self.layers.push(LayerMerge::Finished(Rc::new(merge)));
						}
					},
					_ => { panic!("found unfinished merge; debug!"); }
				}
			}

			self.layers.push(LayerMerge::Finished(layer));

			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
				if let Some(LayerMerge::Finished(layer1)) = self.layers.pop() {
					if let Some(LayerMerge::Finished(layer2)) = self.layers.pop() {
						let merge = Layer::merge(&*layer1, &*layer2, &self.frontier[..]);
						if merge.times.len() > 0 {
							self.layers.push(LayerMerge::Finished(Rc::new(merge)));
						}
					} else { panic!("wtf"); }
				} else { panic!("wtf"); }
			}
		}
	}

	/// Inserts a new layer layer, merging to maintain few layers.
	pub fn _prog_insert(&mut self, layer: Rc<Layer<Key, Val, Time>>) {

		// TODO : we want to support empty layers to allow descriptions to advance.
		if layer.times.len() > 0 {

			let effort = 8 * layer.times.len();

			// apply effort to existing merges.
			// when a merge finishes, consider merging it with the preceding layer.
			let mut index = 0;
			while index < self.layers.len() {

				let mut units = self.layers[index].work(effort);
				if units > 0 {

					if self.layers[index].len() == 0 {
						self.layers.remove(index);
					}
					else {
						while units > 0 && index > 0 && index < self.layers.len() && self.layers[index-1].len() < 2 * self.layers[index].len() {

							// we have just completed a merge, and we have a preceding small-enough layer.
							let layer1 = ::std::mem::replace(&mut self.layers[index-1], LayerMerge::Finished(Rc::new(Layer::empty())));
							let layer2 = self.layers.remove(index);

							match (layer1, layer2) {
								(LayerMerge::Finished(layer1), LayerMerge::Finished(layer2)) => {
									self.layers[index-1] = LayerMerge::merge(layer1, layer2, &self.frontier[..]);
								},
								_ => { panic!("uh oh!"); }
							}

							index -= 1;
							units = self.layers[index].work(units);
							if self.layers[index].len() == 0 {
								self.layers.remove(index);
							}
						}
					}
				}
				else {
					index += 1;
				}
			}

			// while last two elements exist, both less than layer.len()
			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.times.len() {
				match (self.layers.pop(), self.layers.pop()) {
					(Some(LayerMerge::Finished(layer1)), Some(LayerMerge::Finished(layer2))) => {
						let merge = Layer::merge(&*layer1, &*layer2, &self.frontier[..]);
						if merge.times.len() > 0 {
							self.layers.push(LayerMerge::Finished(Rc::new(merge)));
						}
					},
					_ => { panic!("found unfinished merge; debug!"); }
				}
			}

			// now that `layer` is smallest, push it on.
			self.layers.push(LayerMerge::Finished(layer));

			// if we need to start a merge, let's do that.
			if self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
				match (self.layers.pop(), self.layers.pop()) {
					(Some(LayerMerge::Finished(layer1)), Some(LayerMerge::Finished(layer2))) => {
						self.layers.push(LayerMerge::merge(layer1, layer2, &self.frontier[..]));
					},
					_ => { panic!("found unfinished merge; debug!"); }
				}
			}
		}
	}

	/// Advances the frontier of the trace, allowing compaction of like times.
	pub fn advance_frontier(&mut self, frontier: &[Time]) {
		self.frontier = frontier.to_vec();
	}
}

/// Layer wrapper for either layers and layer merges in progress.
#[derive(Debug)]
pub enum LayerMerge<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
	/// A merge in progress, contain references to the two source layers and the merge state.
	Merging(Rc<Layer<Key, Val, Time>>, Rc<Layer<Key, Val, Time>>, Merge<Key, Val, Time>),
	/// A single layer of a merge that was completed or never started.
	Finished(Rc<Layer<Key, Val, Time>>),
}

impl<Key: Ord+Debug+Clone, Val: Ord+Debug+Clone, Time: Lattice+Ord+Debug+Clone> LayerMerge<Key, Val, Time> {

	/// Creates a new merge object for a pair of layers.
	fn merge(layer1: Rc<Layer<Key, Val, Time>>, layer2: Rc<Layer<Key, Val, Time>>, frontier: &[Time]) -> Self {

		// bold to assert, but let's see why not.
		assert_eq!(layer1.desc.upper, layer2.desc.lower);

		LayerMerge::Merging(layer1.clone(), layer2.clone(), Merge {
			source1: LayerCursor::new(layer1),
			source2: LayerCursor::new(layer2),
			result: Layer::empty(),
			frontier: frontier.to_vec(),
			worked: 0,
			target: 0,
		})

	}

	/// Merges at least `units` tuples; returns "left over" effort.
	fn work(&mut self, units: usize) -> usize {
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
	fn len(&self) -> usize {
		match self {
			&LayerMerge::Merging(ref x, ref y,_) => x.times.len() + y.times.len(),
			&LayerMerge::Finished(ref x) => x.times.len(),
		}
	}
}

/// A merge in progress.
///
/// This structure represents the state for a merge in progress, used by progressive merging.
#[derive(Debug)]
pub struct Merge<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
	source1: LayerCursor<Key, Val, Time>,
	source2: LayerCursor<Key, Val, Time>,
	result: Layer<Key, Val, Time>,
	frontier: Vec<Time>,
	worked: usize,
	target: usize,
}

impl<Key: Ord+Debug+Clone, Val: Ord+Debug+Clone, Time: Lattice+Ord+Debug+Clone> Merge<Key, Val, Time> {

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

						match self.source1.val().cmp(&self.source2.val()) {
							Ordering::Less => 		{

								for &(ref time, diff) in self.source1.times() {
									self.result.times.push((time.advance_by(&self.frontier).unwrap(), diff));
								}
								consolidate(&mut self.result.times, times_len);

								// if we pushed times, push the value.
								if self.result.times.len() > times_len {
									self.result.vals.push((self.source1.val().clone(), self.result.times.len()));
								}

								self.source1.step_val();
							},
							Ordering::Equal => 		{

								for &(ref time, diff) in self.source1.times() {
									self.result.times.push((time.advance_by(&self.frontier[..]).unwrap(), diff));
								}
								for &(ref time, diff) in self.source2.times() {
									self.result.times.push((time.advance_by(&self.frontier[..]).unwrap(), diff));
								}
								consolidate(&mut self.result.times, times_len);

								// if we pushed times, push the value.
								if self.result.times.len() > times_len {
									self.result.vals.push((self.source1.val().clone(), self.result.times.len()));
								}

								self.source1.step_val();
								self.source2.step_val();
							},
							Ordering::Greater => 	{

								for &(ref time, diff) in self.source2.times() {
									self.result.times.push((time.advance_by(&self.frontier[..]).unwrap(), diff));
								}
								consolidate(&mut self.result.times, times_len);

								// if we pushed times, push the value.
								if self.result.times.len() > times_len {
									self.result.vals.push((self.source2.val().clone(), self.result.times.len()));
								}

								self.source2.step_val();
							},
						}
					}

					while self.source1.val_valid() {
						let times_len = self.result.times.len();
						for &(ref time, diff) in self.source1.times() {
							self.result.times.push((time.advance_by(&self.frontier).unwrap(), diff));
						}
						consolidate(&mut self.result.times, times_len);

						// if we pushed times, push the value.
						if self.result.times.len() > times_len {
							self.result.vals.push((self.source1.val().clone(), self.result.times.len()));
						}

						self.source1.step_val();
					}

					while self.source2.val_valid() {
						let times_len = self.result.times.len();
						for &(ref time, diff) in self.source2.times() {
							self.result.times.push((time.advance_by(&self.frontier).unwrap(), diff));
						}
						consolidate(&mut self.result.times, times_len);

						// if we pushed times, push the value.
						if self.result.times.len() > times_len {
							self.result.vals.push((self.source2.val().clone(), self.result.times.len()));
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

/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct Layer<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
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

impl<Key: Ord+Debug+Clone, Val: Ord+Debug+Clone, Time: Lattice+Ord+Debug+Clone> Layer<Key, Val, Time> {
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

/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate<T: Ord+Clone>(vec: &mut Vec<(T, isize)>, off: usize) {
	vec[off..].sort_by(|x,y| x.0.cmp(&y.0));
	for index in (off + 1) .. vec.len() {
		if vec[index].0 == vec[index - 1].0 {
			vec[index].1 += vec[index - 1].1;
			vec[index - 1].1 = 0;
		}
	}
	let mut cursor = off;
	for index in off .. vec.len() {
		if vec[index].1 != 0 {
			vec[cursor] = vec[index].clone();
			cursor += 1;
		}
	}
	vec.truncate(cursor);
}

/// Describes an interval of partially Ord+Debugered times.
///
/// A `Description` indicates a set of partially ordered times, and a moment at which they are
/// observed. The `lower` and `upper` frontiers bound the times contained within, and the `since`
/// frontier indicates a moment at which the times were observed. If `since` is strictly in 
/// advance of `lower`, the contained times may be "advanced" to times which appear equivalent to
/// any time after `since`.
#[derive(Debug)]
pub struct Description<Time: Lattice+Ord+Debug> {
	/// lower frontier of contained updates.
	lower: Vec<Time>,
	/// upper frontier of contained updates.
	upper: Vec<Time>,
	/// frontier used for update compaction.
	since: Vec<Time>,
}

/// A cursor allowing navigation of a trace.
pub struct TraceCursor<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
	layers: Vec<LayerCursor<Key, Val, Time>>,	// cursors for each layer.
	dirty: usize,								// number of consumed layers.
}

impl<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> TraceCursor<Key, Val, Time> {
	/// Constructs new cursor from slice of layers.
	fn new(layers: &[LayerMerge<Key, Val, Time>]) -> TraceCursor<Key, Val, Time> {
		let mut cursors = Vec::new();
		for layer in layers {
			match layer {
				&LayerMerge::Merging(ref layer1, ref layer2, _) => {					
					cursors.push(LayerCursor::new(layer1.clone()));
					cursors.push(LayerCursor::new(layer2.clone())); 
				}
				&LayerMerge::Finished(ref layer) => { 
					assert!(layer.times.len() > 0);
					cursors.push(LayerCursor::new(layer.clone())); 
				}
			}
		}

		cursors.sort_by(|x,y| x.key().cmp(&y.key()));
		TraceCursor { layers: cursors, dirty: 0 }
	}

	/// Advances the cursor to the next key, providing a view if the key exists.
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

	/// Reveals the key that would be revealed by `self.next_key()`.
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
	#[inline(never)]
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
#[derive(Debug)]
pub struct KeyView<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> {
	layers: &'a mut [LayerCursor<Key, Val, Time>],	// reference to source data.
	dirty: usize,									// number of consumed values.
}

impl<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> KeyView<'a, Key, Val, Time> {
	/// Allocates a new layer from a mutable layer cursor slice.
	fn new(layers: &'a mut [LayerCursor<Key, Val, Time>]) -> KeyView<'a, Key, Val, Time> {
		layers.sort_by(|x,y| x.val().cmp(&y.val()));
		KeyView {
			layers: layers,
			dirty: 0,
		}
	}

	/// Returns the key associated with the `KeyView`.
	pub fn key(&self) -> &Key { self.layers[0].key() }

	/// Advances the key view to the next value, returns a view if it exists.
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
	/// Reveals the value that would be returned by `self.step_val()`.
	pub fn peek_val(&mut self) -> Option<&Val> {
		self.tidy_vals();
		if self.layers.len() > 0 {
			Some(&self.layers[0].val())
		}
		else {
			None
		}
	}

	/// Removes completed cursors, and re-sorts invalidated cursors.
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
#[derive(Debug)]
pub struct ValView<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> {
	layers: &'a mut [LayerCursor<Key, Val, Time>],	// reference to source data.
}

impl<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> ValView<'a, Key, Val, Time> {

	// constructs a new trace from a 
	fn new(layers: &'a mut [LayerCursor<Key, Val, Time>]) -> ValView<'a, Key, Val, Time> {
		ValView {
			layers: layers,
		}
	} 

	/// Returns the value associated with the `ValView`.
	pub fn val(&self) -> &Val { self.layers[0].val() }

	/// Applies `logic` to each time associated with the value.
	pub fn map<L: FnMut(&(Time, isize))>(&self, mut logic: L) {
		for layer in self.layers.iter() {
			for times in layer.times() {
				logic(times)
			}
		}
	}
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct LayerCursor<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
	key_slice: (usize, usize),
	val_slice: (usize, usize),
	layer: Rc<Layer<Key, Val, Time>>,
}

impl<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> LayerCursor<Key, Val, Time> {

	/// Creates a new LayerCursor from non-empty layer data. 
	pub fn new(layer: Rc<Layer<Key, Val, Time>>) -> LayerCursor<Key, Val, Time> {
		LayerCursor {
			key_slice: (0, layer.keys.len()),
			val_slice: (0, if layer.keys.len() > 0 { layer.offs[0] } else { 0 }),
			layer: layer,
		}
	}

	/// A reference to the current key.
	pub fn key(&self) -> &Key { 
		debug_assert!(self.key_valid());
		&self.layer.keys[self.key_slice.0]
	}
	/// A reference to the current val.
	pub fn val(&self) -> &Val { 
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		&self.layer.vals[self.val_slice.0].0
	}
	/// A reference to the slice of times for the current val.
	pub fn times(&self) -> &[(Time, isize)] {
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		let lower = if self.val_slice.0 == 0 { 0 } else { self.layer.vals[self.val_slice.0 - 1].1 };
		let upper = self.layer.vals[self.val_slice.0].1;
		&self.layer.times[lower .. upper]
	}

	/// Indicates if the current key is valid.
	pub fn key_valid(&self) -> bool { self.key_slice.0 < self.key_slice.1 }
	/// Indicates if the current val is valid.
	pub fn val_valid(&self) -> bool { self.val_slice.0 < self.val_slice.1 }

	/// Advances key slice, updates val slice, and returns whether cursor is valid.
	pub fn step_key(&mut self) -> bool {
		self.key_slice.0 += 1;
		if self.key_valid() {
			let lower = if self.key_slice.0 > 0 { self.layer.offs[self.key_slice.0-1] } else { 0 };
			let upper = self.layer.offs[self.key_slice.0];
			self.val_slice = (lower, upper);
			true
		}
		else {
			self.key_slice.0 = self.key_slice.1;	// step back so we don't overflow.
			self.val_slice = (self.layer.times.len(), self.layer.times.len());
			false
		}
	}

	/// Advances key slice, updates val slice, and returns whether cursor is valid.
	pub fn seek_key(&mut self, key: &Key) -> bool {
		let step = advance(&self.layer.keys[self.key_slice.0 .. self.key_slice.1], |k| k.lt(key));
		self.key_slice.0 += step;
		if self.key_valid() {
			let lower = if self.key_slice.0 > 0 { self.layer.offs[self.key_slice.0-1] } else { 0 };
			let upper = self.layer.offs[self.key_slice.0];
			self.val_slice = (lower, upper);
			true			
		}
		else {
			self.key_slice.0 = self.key_slice.1;
			self.val_slice = (self.layer.times.len(), self.layer.times.len());
			false
		}
	}

	/// Advances val slice, and returns whether the cursor is valid.
	pub fn step_val(&mut self) -> bool {
		self.val_slice.0 += 1;
		if self.val_valid() {
			true
		}
		else {
			self.val_slice.0 = self.val_slice.1;
			false
		}
	}
	/// Advances val slice, and returns whether the cursor is valid.
	pub fn seek_val(&mut self, val: &Val) -> bool {
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

	/// rewinds the value cursor to 
	pub fn rewind_val(&mut self) {
		debug_assert!(self.key_valid());
		let lower = if self.key_slice.0 > 0 { self.layer.offs[self.key_slice.0-1] } else { 0 };
		let upper = self.layer.offs[self.key_slice.0];
		self.val_slice = (lower, upper);
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
