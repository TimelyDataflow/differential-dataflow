//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging 
//! immutable batches of updates. It is generic with respect to the batch type, and can be 
//! instantiated for any implementor of `trace::Batch`.

use ::Ring;
use lattice::Lattice;
use trace::{Batch, Builder, Cursor, Trace};
use trace::consolidate;
use trace::cursor::cursor_list::CursorList;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
#[derive(Debug)]
pub struct Spine<K, V, T: Lattice+Ord, R: Ring, B: Batch<K, V, T, R>> {
	phantom: ::std::marker::PhantomData<(K, V, R)>,
	advance_frontier: Vec<T>,	// Times after which the trace must accumulate correctly.
	through_frontier: Vec<T>,	// Times after which the trace must be able to subset its inputs.
	merging: Vec<B>,			// Several possibly shared collections of updates.
	pending: Vec<B>,			// Layers at times in advance of `frontier`.
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and layer types.
impl<K, V, T, R, B> Trace<K, V, T, R> for Spine<K, V, T, R, B> 
where 
	K: Ord+Clone,	// TODO: Why is clone required? (cursorlist?)
	V: Ord+Clone,
	// K: Clone+Default+HashOrdered+'static,
	// Time: Lattice+Ord+Clone+Default+Debug+'static,
	T: Lattice+Ord+Clone,  // Clone needed for `advance_frontier` and friends.
	R: Ring,
	B: Batch<K, V, T, R>+Clone+'static,
{

	type Batch = B;
	type Cursor = CursorList<K, V, T, R, <B as Batch<K, V, T, R>>::Cursor>;

	fn new() -> Self {
		Spine { 
			phantom: ::std::marker::PhantomData,
			advance_frontier: vec![<T as Lattice>::min()],
			through_frontier: vec![<T as Lattice>::min()],
			merging: Vec::new(),
			pending: Vec::new(),
		} 		
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, layer: Self::Batch) {

		// we can ignore degenerate layers (TODO: learn where they come from; suppress them?)
		if layer.description().lower() != layer.description().upper() {
			self.pending.push(layer);
			self.consider_merges();
		}
		else {
			// degenerate layers had best be empty.
			assert!(layer.len() == 0);
		}
	}
	fn cursor_through(&self, upper: &[T]) -> Option<Self::Cursor> {

		// THIS IS ALL WRONG AT THE MOMENT (AT LEAST, THE COUNT_VALID STUFF).
		unimplemented!();

		// Check that `upper` is greater or equal to `self.through_frontier`.
		// Otherwise, the cut could be in `self.merging` and it is user error anyhow.
		let upper_valid = upper.iter().all(|t1| self.through_frontier.iter().any(|t2| t2.le(t1)));

		// Find the position of the batch with upper limit `upper`. It is possible we 
		// find nothing (e.g. when `upper` is `&[]`, or when `self.pending` is empty),
		// in which case we must test that the last upper limit is less or equal to `upper`. 
		let count = self.pending.iter()
								.position(|batch| batch.description().upper() == upper)
								.unwrap_or(self.pending.len());

		// The count is valid if either pending is empty, or the identified location's upper limit is not after `upper`.
		let count_valid = self.pending.len() == 0 || upper.iter().all(|t1| self.pending[count-1].description().upper().iter().any(|t2| t2.le(t1)));

		// We can return a cursor if `upper` is valid, and if `count` reflects a valid cut point.
		if upper_valid && count_valid {
			let mut cursors = Vec::new();
			cursors.extend(self.merging.iter().filter(|b| b.len() > 0).map(|b| b.cursor()));
			cursors.extend(self.pending[..count].iter().filter(|b| b.len() > 0).map(|b| b.cursor()));
			Some(CursorList::new(cursors))
		}
		else {
			panic!("failed in `cursor_through`: {:?}, {:?}", upper_valid, count_valid);
			None
		}
	}
	fn advance_by(&mut self, frontier: &[T]) {
		self.advance_frontier = frontier.to_vec();
	}
	fn distinguish_since(&mut self, frontier: &[T]) {
		self.through_frontier = frontier.to_vec();
		self.consider_merges();
	}
}

impl<K, V, T, R, B> Spine<K, V, T, R, B> 
where 
	K: Clone,				// Clone required by advance
	V: Clone,				// Clone required by advance
	// Key: Clone+Default+HashOrdered+'static,
	T: Lattice+Ord+Clone,	// Clone required by `consolidate`.
	R: Ring,
	B: Batch<K, V, T, R>,
{
	// Migrate data from `self.pending` into `self.merging`.
	fn consider_merges(&mut self) {

		// TODO: We could consider merging in batches here, rather than in sequence. Little is known...
		while self.pending.len() > 0 && self.through_frontier.iter().all(|t1| self.pending[0].description().upper().iter().any(|t2| t2.le(t1))) {

			let batch = self.pending.remove(0);

			// while last two elements exist, both less than layer.len()
			while self.merging.len() >= 2 && self.merging[self.merging.len() - 2].len() < batch.len() {
				let layer1 = self.merging.pop().unwrap();
				let layer2 = self.merging.pop().unwrap();
				let result = layer2.merge(&layer1).unwrap();
				self.merging.push(result);
			}

			self.merging.push(batch);

		    while self.merging.len() >= 2 && self.merging[self.merging.len() - 2].len() < 2 * self.merging[self.merging.len() - 1].len() {
				let layer1 = self.merging.pop().unwrap();
				let layer2 = self.merging.pop().unwrap();
				let mut result = layer2.merge(&layer1).unwrap();

				// if we just merged the last layer, `advance_by` it.
				if self.merging.len() == 0 {
					result = Self::advance_by(&result, &self.advance_frontier[..]);
				}

				self.merging.push(result);
			}
		}
	}

	/// Advances times in `layer` and consolidates differences for like times.
	///
	/// TODO: This method could be defined on `&mut self`, exploiting in-place mutation
	/// to avoid allocation and building headaches. It is implemented on the `Rc` variant
	/// to get access to `cursor()`, and in principle to allow a progressive implementation. 
	#[inline(never)]
	fn advance_by(layer: &B, frontier: &[T]) -> B { 

		// TODO: This is almost certainly too much `with_capacity`.
		// TODO: We should design and implement an "in-order builder", which takes cues from key and val
		// structure, rather than having to infer them from tuples.
		// TODO: We should understand whether in-place mutation is appropriate, or too gross. At the moment,
		// this could be a general method defined on any implementor of `trace::Cursor`.
		let mut builder = <B as Batch<K, V, T, R>>::Builder::new();

		let mut times = Vec::new();
		let mut cursor = layer.cursor();

		while cursor.key_valid() {
			while cursor.val_valid() {
				cursor.map_times(|time: &T, diff| times.push((time.advance_by(frontier).unwrap(), diff)));
				consolidate(&mut times, 0);
				for (time, diff) in times.drain(..) {
					builder.push((cursor.key().clone(), cursor.val().clone(), time, diff));
				}
			}
			cursor.step_key();
		}

		builder.done(layer.description().lower(), layer.description().upper(), frontier)
	}
}
