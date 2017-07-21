//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging 
//! immutable batches of updates. It is generic with respect to the batch type, and can be 
//! instantiated for any implementor of `trace::Batch`.

use ::Diff;
use lattice::Lattice;
use trace::{Batch, BatchReader, Trace, TraceReader};
use trace::cursor::cursor_list::CursorList;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections. 
#[derive(Debug)]
pub struct Spine<K, V, T: Lattice+Ord, R: Diff, B: Batch<K, V, T, R>> {
	phantom: ::std::marker::PhantomData<(K, V, R)>,
	advance_frontier: Vec<T>,	// Times after which the trace must accumulate correctly.
	through_frontier: Vec<T>,	// Times after which the trace must be able to subset its inputs.
	merging: Vec<B>,			// Several possibly shared collections of updates.
	pending: Vec<B>,			// Batches at times in advance of `frontier`.
}

impl<K, V, T, R, B> TraceReader<K, V, T, R> for Spine<K, V, T, R, B> 
where 
	K: Ord+Clone,			// Clone is required by `batch::advance_*` (in-place could remove).
	V: Ord+Clone,			// Clone is required by `batch::advance_*` (in-place could remove).
	T: Lattice+Ord+Clone,	// Clone is required by `advance_by` and `batch::advance_*`.
	R: Diff,
	B: Batch<K, V, T, R>+Clone+'static,
{
	type Batch = B;
	type Cursor = CursorList<K, V, T, R, <B as BatchReader<K, V, T, R>>::Cursor>;

	fn cursor_through(&mut self, upper: &[T]) -> Option<Self::Cursor> {

		// we shouldn't grab a cursor into a closed trace, right?
		assert!(self.advance_frontier.len() > 0);

		// Check that `upper` is greater or equal to `self.through_frontier`.
		// Otherwise, the cut could be in `self.merging` and it is user error anyhow.
		if upper.iter().all(|t1| self.through_frontier.iter().any(|t2| t2.less_equal(t1))) {

			let mut cursors = Vec::new();
			cursors.extend(self.merging.iter().filter(|b| b.len() > 0).map(|b| b.cursor()));
			for batch in &self.pending {
				let include_lower = upper.iter().all(|t1| batch.lower().iter().any(|t2| t2.less_equal(t1)));
				let include_upper = upper.iter().all(|t1| batch.upper().iter().any(|t2| t2.less_equal(t1)));

				if include_lower != include_upper && upper != batch.lower() {
					panic!("`cursor_through`: `upper` straddles batch");
					// return None;
				}

				// include pending batches 
				if include_upper {
					cursors.push(batch.cursor());
				}
			}
			Some(CursorList::new(cursors))
		}
		else {
			None
		}
	}
	fn advance_by(&mut self, frontier: &[T]) {
		self.advance_frontier = frontier.to_vec();
		if self.advance_frontier.len() == 0 {
			self.pending.clear();
			self.merging.clear();
		}
	}
	fn advance_frontier(&mut self) -> &[T] { &self.advance_frontier[..] }
	fn distinguish_since(&mut self, frontier: &[T]) {
		self.through_frontier = frontier.to_vec();
		self.consider_merges();
	}
	fn distinguish_frontier(&mut self) -> &[T] { &self.through_frontier[..] }

	fn map_batches<F: FnMut(&Self::Batch)>(&mut self, mut f: F) {
		for batch in self.merging.iter() {
			f(batch);
		}
		for batch in self.pending.iter() {
			f(batch);
		}
	}
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and batch types.
impl<K, V, T, R, B> Trace<K, V, T, R> for Spine<K, V, T, R, B> 
where 
	K: Ord+Clone,			// Clone is required by `batch::advance_*` (in-place could remove).
	V: Ord+Clone,			// Clone is required by `batch::advance_*` (in-place could remove).
	T: Lattice+Ord+Clone,	// Clone is required by `advance_by` and `batch::advance_*`.
	R: Diff,
	B: Batch<K, V, T, R>+Clone+'static,
{

	fn new() -> Self {
		Spine { 
			phantom: ::std::marker::PhantomData,
			advance_frontier: vec![<T as Lattice>::minimum()],
			through_frontier: vec![<T as Lattice>::minimum()],
			merging: Vec::new(),
			pending: Vec::new(),
		}
	}
	// Note: this does not perform progressive merging; that code is around somewhere though.
	fn insert(&mut self, batch: Self::Batch) {

		// we can ignore degenerate batches (TODO: learn where they come from; suppress them?)
		if batch.lower() != batch.upper() {
			self.pending.push(batch);
			self.consider_merges();
		}
		else {
			// degenerate batches had best be empty.
			assert!(batch.len() == 0);
		}
	}
}

impl<K, V, T, R, B> Spine<K, V, T, R, B> 
where 
	K: Ord+Clone,			// Clone is required by `advance_mut`.
	V: Ord+Clone,			// Clone is required by `advance_mut`.
	T: Lattice+Ord+Clone,	// Clone is required by `advance_mut`.
	R: Diff,
	B: Batch<K, V, T, R>,
{
	// Migrate data from `self.pending` into `self.merging`.
	#[inline(never)]
	fn consider_merges(&mut self) {

		// TODO: We could consider merging in batches here, rather than in sequence. 
		//       Little is currently known about whether this is important ...
		while self.pending.len() > 0 && 
		      self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1))) 
		{
			// this could be a VecDeque, if we ever notice this.
			let batch = self.pending.remove(0);

			// while last two elements exist, both less than batch.len()
			while self.merging.len() >= 2 && self.merging[self.merging.len() - 2].len() < batch.len() {
				let batch1 = self.merging.pop().unwrap();
				let batch2 = self.merging.pop().unwrap();
				let result = batch2.merge(&batch1);
				self.merging.push(result);
			}

			self.merging.push(batch);

			// `len` exists only to narrow while condition.
			let mut len = self.merging.len();
			while len >= 2 && self.merging[len - 2].len() < 2 * self.merging[len - 1].len() {

				let mut batch1 = self.merging.pop().unwrap();
				let mut batch2 = self.merging.pop().unwrap();

				// advance inputs, rather than outputs.
				if self.merging.len() == 0 {
					batch1.advance_mut(&self.advance_frontier[..]);
					batch2.advance_mut(&self.advance_frontier[..]);
				}

				let result = batch2.merge(&batch1);

				self.merging.push(result);
				len = self.merging.len();
			}
		}
	}
}
