//! A navigable collection of (key, val, time, isize) updates.
//! 
//! A `Trace` supports searching by key, within which one can search by val, 

use std::rc::Rc;
use std::fmt::Debug;

use lattice::Lattice;
use trace::layer::{Layer, LayerCursor};
use trace::cursor::CursorList;
use trace::{Trace, Batch};

// A trace is made up of several layers, each of which are immutable collections of updates.
#[derive(Debug)]
pub struct LayerTrace<Key: Ord+Debug, Val: Ord+Debug, Time: Lattice+Ord+Debug> {
	frontier: Vec<Time>,					// Times after which the times in the traces must be distinguishable.
	layers: Vec<Rc<Layer<Key, Val, Time>>>	// Several possibly shared collections of updates.
}


impl<Key: Ord+Debug+Clone, Val: Ord+Debug+Clone, Time: Lattice+Ord+Debug+Clone> Trace<Key, Val, Time> for LayerTrace<Key, Val, Time> {

	/// The type of an immutable collection of updates.
	type Batch = Rc<Layer<Key, Val, Time>>;
	/// The type used to enumerate the collections contents.
	type Cursor = CursorList<LayerCursor<Key, Val, Time>>;

	/// Allocates a new empty trace.
	fn new(default: Time) -> Self {
		LayerTrace { 
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

// impl<Key: Ord+Debug+Clone, Val: Ord+Debug+Clone, Time: Lattice+Ord+Debug+Clone> LayerTrace<Key, Val, Time> {
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