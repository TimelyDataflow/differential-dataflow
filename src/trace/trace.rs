//! A navigable collection of (key, val, time, isize) updates.
//! 
//! A `Trace` supports searching by key, within which one can search by val, 

use std::rc::Rc;
use std::fmt::Debug;

use lattice::Lattice;
use trace::layer::{Layer, LayerMerge, LayerCursor};
use trace::trace_trait::{KeyCursor, ValCursor, TimeCursor};
use trace::cursor::Cursor;

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

	/// Inserts a new layer into the trace.
	pub fn insert(&mut self, layer: Rc<Layer<Key, Val, Time>>) {

		if layer.times.len() > 0 {
			// while last two elements exist, both less than layer.len()
			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < layer.times.len() {
				if let Some(LayerMerge::Finished(layer)) = self.layers.pop() {
					let len = self.layers.len();
					self.layers[len-1].merge_with(layer, &self.frontier);
					if self.layers[len-1].len() == 0 {
						self.layers.pop();
					}
				}
				else {
					panic!("trying to merge unfinished merge into other layer");
				}
			}

			self.layers.push(LayerMerge::Finished(layer));

			while self.layers.len() >= 2 && self.layers[self.layers.len() - 2].len() < 2 * self.layers[self.layers.len() - 1].len() {
				if let Some(LayerMerge::Finished(layer)) = self.layers.pop() {
					let len = self.layers.len();
					self.layers[len-1].merge_with(layer, &self.frontier);
					if self.layers[len-1].len() == 0 {
						self.layers.pop();
					}
				}
				else {
					panic!("trying to merge unfinished merge into other layer");
				}
			}
		}
	}

	/// Inserts a new layer layer, merging to maintain few layers.
	pub fn _prog_insert(&mut self, layer: Rc<Layer<Key, Val, Time>>) {

		// TODO : we want to support empty layers to allow descriptions to advance.
		if layer.times.len() > 0 {

			let effort = 4 * layer.times.len();

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

							if let LayerMerge::Finished(layer) = self.layers.remove(index) {
								self.layers[index-1].merge_with(layer, &self.frontier[..]);
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
				if let Some(LayerMerge::Finished(layer)) = self.layers.pop() {
					let layers_len = self.layers.len();
					self.layers[layers_len - 1].merge_with(layer, &self.frontier[..]);
				}
				else {
					panic!("found unfinished merge; debug!");
				}
			}
		}
	}

	/// Advances the frontier of the trace, allowing compaction of like times.
	pub fn advance_frontier(&mut self, frontier: &[Time]) {
		self.frontier = frontier.to_vec();
	}
}




/// A cursor allowing navigation of a trace.
///
/// A trace cursor supports stepping through keys (`step_key()`), seeking *forward* to specific keys 
/// (`seek_key(&key)`) and examining the next key you would receive by stepping (`peek_key()`).
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

impl<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> KeyCursor<'a, Key, Val, Time> for TraceCursor<Key, Val, Time> {

	type ValCursor = KeyView<'a, Key, Val, Time>;

	/// Advances the cursor to the next key, providing a view if the key exists.
	fn next_key(&'a mut self) -> Option<KeyView<'a, Key, Val, Time>> {
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
	fn seek_key(&'a mut self, key: &Key) -> Option<KeyView<'a, Key, Val, Time>> {

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
	fn peek_key(&mut self) -> Option<&Key> {
		self.tidy_keys();
		if self.layers.len() > 0 {
			Some(&self.layers[0].key())
		}
		else {
			None
		}
	}
}

/// A cursor for the values associated with a single key.
///
/// The cursor can step through values (`step_val()`), seek *forward* to specific values (`seek_val(&val)`),
/// and peek at the next available value (`peek_val()`).
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

impl<'a, 'b, Key, Val, Time> ValCursor<'b, Key, Val, Time> for KeyView<'a, Key, Val, Time> 
	where 'a : 'b,
		  Key: Ord+Debug+'a, 
		  Val: Ord+Debug+'a,
		  Time: Lattice+Ord+Debug+'a {

	type TimeCursor = ValView<'b, Key, Val, Time>;

	/// Returns the key associated with the `KeyView`.
	fn key(&self) -> &Key { self.layers[0].key() }

	/// Advances the key view to the next value, returns a view if it exists.
	fn next_val(&'b mut self) -> Option<ValView<'b, Key, Val, Time>> {

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
	fn seek_val(&'b mut self, val: &Val) -> Option<ValView<'b, Key, Val, Time>> { 
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
	fn peek_val(&mut self) -> Option<&Val> {
		self.tidy_vals();
		if self.layers.len() > 0 {
			Some(&self.layers[0].val())
		}
		else {
			None
		}
	}

}

/// A handle to the `(time, diff)` pairs for a single `(key, val)` pair.
#[derive(Debug)]
pub struct ValView<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> {
	layers: &'a mut [LayerCursor<Key, Val, Time>],	// reference to source data.
}

impl<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> ValView<'a, Key, Val, Time> {
	fn new(layers: &'a mut [LayerCursor<Key, Val, Time>]) -> ValView<'a, Key, Val, Time> {
		ValView {
			layers: layers,
		}
	} 
}

impl<'a, Key: Ord+Debug+'a, Val: Ord+Debug+'a, Time: Lattice+Ord+Debug+'a> TimeCursor<Val, Time> for ValView<'a, Key, Val, Time> {
	fn val(&self) -> &Val { self.layers[0].val() }
	fn map<L: FnMut(&Time, isize)>(&self, mut logic: L) {
		for layer in self.layers.iter() {
			layer.map_times(|time, diff| logic(time, diff));
		}
	}

}


