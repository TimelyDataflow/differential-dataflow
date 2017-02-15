//! A trie-structured representation of un-keyed update tuples.

use std::rc::Rc;
use std::cmp::Ordering;

use lattice::Lattice;
use trace::{Batch, Builder, Cursor, Trace, consolidate};
use trace::description::Description;
use trace::cursor::cursor_list::CursorList;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
pub struct Spine<Key: Ord, Time: Lattice+Ord> {
	frontier: Vec<Time>,				// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Time>>>	// Several possibly shared collections of updates.
}

impl<Key: Ord+Clone, Time: Lattice+Ord+Clone> Trace<Key, (), Time> for Spine<Key, Time> {

	type Batch = Rc<Layer<Key, Time>>;
	type Cursor = CursorList<LayerCursor<Key, Time>>;

	fn new(default: Time) -> Self {
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
	fn advance_by(&mut self, frontier: &[Time]) {
		self.frontier = frontier.to_vec();
	}
}

/// An immutable collection of update tuples, from a contiguous interval of logical times.
pub struct Layer<Key: Ord, Time: Lattice+Ord> {
	/// Sorted list of keys with at least one update tuple.
	pub keys: Vec<Key>,
	/// Offsets in `self.vals` where the corresponding key *stops*.
	pub offs: Vec<usize>,
	/// Times and differences, in correspondence with update tuples.
	pub times: Vec<(Time, isize)>,
	/// Description of the update times this layer represents.
	pub desc: Description<Time>,
}

impl<Key: Ord+Clone, Time: Lattice+Ord+Clone> Batch<Key, (), Time> for Rc<Layer<Key, Time>> {
	type Builder = LayerBuilder<(Key, Time)>;
	type Cursor = LayerCursor<Key, Time>;
	fn cursor(&self) -> Self::Cursor { LayerCursor::new(self.clone()) }
	fn len(&self) -> usize { self.times.len() }
}


impl<Key: Ord+Clone, Time: Lattice+Ord+Clone> Layer<Key, Time> {
	/// Constructs a new Layer from sorted data.
	pub fn new<I: Iterator<Item=(Key,Time,isize)>>(mut data: I, lower: &[Time], upper: &[Time]) -> Layer<Key, Time> {

		let mut keys = Vec::new();
		let mut offs = Vec::new();
		let mut times = Vec::new();

		if let Some((key, time, diff)) = data.next() {

			keys.push(key);
			offs.push(1);
			times.push((time, diff));

			for (key, time, diff) in data {
				times.push((time, diff));
				if key != keys[keys.len()-1] {
					keys.push(key);
					offs.push(times.len())
				}
				
				offs[keys.len()-1] = times.len();
			}
		}

		Layer {
			keys: keys,
			offs: offs,
			times: times,
			desc: Description::new(lower, upper, lower),
		}
	}

	/// Returns an empty new layer.
	pub fn empty() -> Layer<Key, Time> {
		Layer::new(Vec::new().into_iter(), &[], &[])
	}

	/// Conducts a full merge, right away. Times advanced per `frontier`.
	pub fn merge(layer1: &Rc<Self>, layer2: &Rc<Self>, frontier: &[Time]) -> Self {

		// TODO : Figure out if lower, upper are just unions or something.
		// NOTE : The should be, as we only merge contiguous time slices.
		let mut result = Layer::new(Vec::new().into_iter(), &[], &[]);

		result.keys = Vec::with_capacity(layer1.keys.len() + layer2.keys.len());
		result.offs = Vec::with_capacity(layer1.offs.len() + layer2.offs.len());
		result.times = Vec::with_capacity(layer1.times.len() + layer2.times.len());

		let mut cursor1 = layer1.cursor();
		let mut cursor2 = layer2.cursor();

		while cursor1.key_valid() && cursor2.key_valid() {

			match cursor1.key().cmp(cursor2.key()) {
				Ordering::Less => {
					let times_len = result.times.len();
					cursor1.map_times(|time, diff| {
						result.times.push((time.advance_by(frontier).unwrap(), diff));
					});
					consolidate(&mut result.times, times_len);
					if result.times.len() > times_len {
						result.keys.push(cursor1.key().clone());
						result.offs.push(result.times.len());
					}
					cursor1.step_key();
				},
				Ordering::Equal => {
					let times_len = result.times.len();
					cursor1.map_times(|time, diff| {
						result.times.push((time.advance_by(frontier).unwrap(), diff));
					});
					cursor2.map_times(|time, diff| {
						result.times.push((time.advance_by(frontier).unwrap(), diff));
					});
					consolidate(&mut result.times, times_len);
					if result.times.len() > times_len {
						result.keys.push(cursor1.key().clone());
						result.offs.push(result.times.len());
					}
					cursor1.step_key();
					cursor2.step_key();
				},
				Ordering::Greater => {
					let times_len = result.times.len();
					cursor2.map_times(|time, diff| {
						result.times.push((time.advance_by(frontier).unwrap(), diff));
					});
					consolidate(&mut result.times, times_len);
					if result.times.len() > times_len {
						result.keys.push(cursor2.key().clone());
						result.offs.push(result.times.len());
					}
					cursor2.step_key();
				},
			}
		}

		while cursor1.key_valid() {
			let times_len = result.times.len();
			cursor1.map_times(|time, diff| {
				result.times.push((time.advance_by(frontier).unwrap(), diff));
			});
			consolidate(&mut result.times, times_len);
			if result.times.len() > times_len {
				result.keys.push(cursor1.key().clone());
				result.offs.push(result.times.len());
			}
			cursor1.step_key();
		}

		while cursor2.key_valid() {
			let times_len = result.times.len();
			cursor2.map_times(|time, diff| {
				result.times.push((time.advance_by(frontier).unwrap(), diff));
			});
			consolidate(&mut result.times, times_len);
			if result.times.len() > times_len {
				result.keys.push(cursor2.key().clone());
				result.offs.push(result.times.len());
			}
			cursor2.step_key();
		}

		result
	}
}

/// A cursor for navigating a single layer.
pub struct LayerCursor<Key: Ord, Time: Lattice+Ord> {
	key_pos: usize,
	val_seen: bool,
	val: (),
	layer: Rc<Layer<Key, Time>>,
}

impl<Key: Ord, Time: Lattice+Ord> LayerCursor<Key, Time> {
	/// Creates a new LayerCursor from non-empty layer data. 
	pub fn new(layer: Rc<Layer<Key, Time>>) -> LayerCursor<Key, Time> {
		LayerCursor {
			key_pos: 0,
			val_seen: false,
			val: (),
			layer: layer,
		}
	}
}

impl<Key: Ord, Time: Lattice+Ord> Cursor for LayerCursor<Key, Time> {

	type Key = Key;
	type Val = ();
	type Time = Time;

	fn key(&self) -> &Key { 
		debug_assert!(self.key_valid());
		&self.layer.keys[self.key_pos]
	}
	fn val(&self) -> &() { 
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		&self.val
	}
	fn map_times<L: FnMut(&Time, isize)>(&self, mut logic: L) {
		debug_assert!(self.key_valid());
		debug_assert!(self.val_valid());
		let lower = if self.key_pos == 0 { 0 } else { self.layer.offs[self.key_pos - 1] };
		let upper = self.layer.offs[self.key_pos];
		for &(ref time, diff) in &self.layer.times[lower .. upper] {
			logic(time, diff);
		}
	}

	fn key_valid(&self) -> bool { self.key_pos < self.layer.keys.len() }
	fn val_valid(&self) -> bool { !self.val_seen }

	fn step_key(&mut self){
		self.key_pos += 1;
		if self.key_valid() {
			self.val_seen = false;
		}
		else {
			self.key_pos = self.layer.keys.len();
			self.val_seen = true;
		}
	}

	fn seek_key(&mut self, key: &Key) {
		let step = advance(&self.layer.keys[self.key_pos ..], |k| k.lt(key));
		self.key_pos += step;
		if self.key_valid() {
			self.val_seen = false;
		}
		else {
			self.key_pos = self.layer.keys.len();
			self.val_seen = true;
		}
	}
	fn peek_key(&self) -> Option<&Self::Key> {
		if self.key_valid() { Some(self.key()) } else { None }
	}

	fn step_val(&mut self) {
		self.val_seen = true;
	}
	fn seek_val(&mut self, _: &Self::Val) {
		// I think this does nothing.
	}
	fn peek_val(&self) -> Option<&Self::Val> {
		if self.val_valid() { Some(self.val()) } else { None }
	}

	fn rewind_keys(&mut self) {
		*self = LayerCursor::new(self.layer.clone())
	}

	fn rewind_vals(&mut self) {
		debug_assert!(self.key_valid());
		self.val_seen = false;
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

impl<Key, Time> Builder<Key, (), Time, Rc<Layer<Key, Time>>> for LayerBuilder<(Key, Time)> 
where Key: Ord+Clone, Time: Lattice+Ord+Clone {
	fn new() -> Self { LayerBuilder::new() }
	fn push(&mut self, (key, _, time, diff): (Key, (), Time, isize)) {
		self.push(((key, time), diff));
	}
	fn done(self, lower: &[Time], upper: &[Time]) -> Rc<Layer<Key, Time>> {
		Rc::new(Layer::new(self.done().into_iter().map(|((k,t),d)| (k,t,d)), lower, upper))
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