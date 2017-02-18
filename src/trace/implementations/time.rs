//! A trace implementation for updates with relatively few distinct times.
//!
//! This implementation is similar to the initial differential dataflow implementation, 
//! except it lacks an index from keys into the batches. On the other hand, it does give
//! the more efficient representation, where we avoid repeating times all over the place.
//!
//! This implementation keeps separate batches for each distinct time, and may keep multiple
//! batches for equivalent times until it finds reason to merge them (e.g. two of similar
//! size). Ideally, the number of batches to merge at run time should be at most linear in 
//! the numbers of distinct times, and logarithmic in something else (inserts for each time?)

use ::Delta;

/// An append only collection of updates, optimized for relatively few distinct times.
///
/// This implementation maintains separate allocations for each distinct time, and may keep
/// multiple allocations for the same time if merging would be disproportionately expensive.
pub struct Spine<Key, Val, Time> {
	// ideally we keep this sorted by (time, size), to easily notice merge options.
	// we keep a second `Time` to mutate in response to `advance_by`. 
	layers: Vec<(Time, Layer<Key, Val, Time>)>,
	// the current frontier.
	frontier: Vec<Time>,
	/// number of inserted tuples size last reorganization.
	effort: usize,
}

impl<Key: Ord, Val: Ord, Time: Ord>  Trace<Key, Val, Time> for Spine<Key, Val, Time> {

	type Batch = Rc<Layer<Key, Val, Time>>;
	type Cursor = CursorList<LayerCursor<Key, Val, Time>>;

	fn new(default: Time) -> Self {
		Spine {
			layers: Vec::new(),
			frontier: vec![default],
		}
	}
	fn insert(&mut self, batch: Self::Batch) {
		self.effort += batch.len();
		self.layers.push(batch);
		
		// think about advancing times, re-sorting, merging?
		if self.effort > self.layers.len() {
			for entry in self.layers.iter_mut() {
				entry.0 = entry.0.advance_by(&self.frontier[..]);
			}

			// sort layers first by time, then by length. merge options emerge.
			self.layers.sort_by(|x,y| (x.0, x.1.len()).cmp(&(y.0, y.1.len())));

			let mut cursor = 0;
			for i in 1 .. self.layers.len() {
				if self.layers[i].0 == self.layers[cursor].0 && self.layers[i].len() < self.layers[cursor].len() * 2 {
					// merge layers[i] and layers[cursor] into position cursor.
					self.layers[cursor] = Layer::merge(&self.layers[i], &self.layers[cursor], &self.frontier[..]);
				}
				else {
					cursor += 1;
				}
			}

			self.effort = 0;
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

/// An immutable collection of updates with a common time.
#[derive(Clone)]
pub struct Layer<Key, Val, Time> {
	time: Time,
	keys: Vec<Key>,
	offs: Vec<usize>,
	vals: Vec<(Val, Delta)>,
}

pub struct LayerCursor<Key, Val, Time> {

}

pub struct LayerBuilder<Key, Val, Time> {
	time: Time,

}