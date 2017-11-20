//! Traits and datastructures representing a collection trace.
//!
//! A collection trace is a set of updates of the form `(key, val, time, diff)`, which determine the contents
//! of a collection at given times by accumulating updates whose time field is less or equal to the target field.
//!
//! The `Trace` trait describes those types and methods that a data structure must implement to be viewed as a
//! collection trace. This trait allows operator implementations to be generic with respect to the type of trace,
//! and allows various data structures to be interpretable as multiple different types of trace.

pub mod cursor;
pub mod description;
pub mod implementations;
pub mod layers;
pub mod wrappers;

use ::Diff;
use ::lattice::Lattice;
pub use self::cursor::Cursor;
pub use self::description::Description;

// 	The traces and batch and cursors want the flexibility to appear as if they manage certain types of keys and 
// 	values and such, while perhaps using other representations, I'm thinking mostly of wrappers around the keys
// 	and vals that change the `Ord` implementation, or stash hash codes, or the like.
// 	
// 	This complicates what requirements we make so that the trace is still usable by someone who knows only about
// 	the base key and value types. For example, the complex types should likely dereference to the simpler types,
//	so that the user can make sense of the result as if they were given references to the simpler types. At the 
//  same time, the collection should be formable from base types (perhaps we need an `Into` or `From` constraint)
//  and we should, somehow, be able to take a reference to the simple types to compare against the more complex 
//  types. This second one is also like an `Into` or `From` constraint, except that we start with a reference and 
//  really don't need anything more complex than a reference, but we can't form an owned copy of the complex type 
//  without cloning it. 
//
//  We could just start by cloning things. Worry about wrapping references later on.

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which extends this trait with further methods
/// to update the contents of the trace. These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the underlying trace and cause work to happen).
pub trait TraceReader<Key, Val, Time, R> {

	/// The type of an immutable collection of updates.
	type Batch: BatchReader<Key, Val, Time, R>+Clone+'static;

	/// The type used to enumerate the collections contents.
	type Cursor: Cursor<Key, Val, Time, R>;

	/// Provides a cursor over updates contained in the trace.
	fn cursor(&mut self) -> (Self::Cursor, <Self::Cursor as Cursor<Key, Val, Time, R>>::Storage) {
		if let Some(cursor) = self.cursor_through(&[]) {
			cursor
		}
		else {
			panic!("unable to acquire complete cursor for trace; is it closed?");
		}
	}

	/// Acquires a cursor to the restriction of the collection's contents to updates at times not greater or 
	/// equal to an element of `upper`.
	///
	/// This method is expected to work if called with an `upper` that (i) was an observed bound in batches from
	/// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
	/// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return a cursor. This
	/// should allow `upper` such as `&[]` as used by `self.cursor()`, though it is difficult to imagine other uses.
	fn cursor_through(&mut self, upper: &[Time]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Key, Val, Time, R>>::Storage)>;

	/// Advances the frontier of times the collection must be correctly accumulable through.
	///
	/// Practically, this allows the trace to advance times in updates it maintains as long as the advanced times 
	/// still compare equivalently to any times greater or equal to some element of `frontier`. Times not greater
	/// or equal to some element of `frontier` may no longer correctly accumulate, so do not advance a trace unless
	/// you are quite sure you no longer require the distinction.
	fn advance_by(&mut self, frontier: &[Time]);

	/// Reports the frontier from which all time comparisions should be accurate.
	///
	/// Times that are not greater or equal to some element of the advance frontier may accumulate inaccurately as
	/// the trace may have lost the ability to distinguish between such times. Accumulations are only guaranteed to
	/// be accurate from the frontier onwards.
	fn advance_frontier(&mut self) -> &[Time];

	/// Advances the frontier that may be used in `cursor_through`.
	///
	/// Practically, this allows the trace to merge batches whose upper frontier comes before `frontier`. The trace
	/// is likely to be annoyed or confused if you use a frontier other than one observed as an upper bound of an 
	/// actual batch. This doesn't seem likely to be a problem, but get in touch if it is.
	///
	/// Calling `distinguish_since(&[])` indicates that all batches may be merged at any point, which essentially 
	/// disables the use of `cursor_through` with any parameter other than `&[]`, which is the behavior of `cursor`.
	fn distinguish_since(&mut self, frontier: &[Time]);

	/// Reports the frontier from which the collection may be subsetted.
	///
	/// The semantics are less elegant here, but the underlying trace will not merge batches in advance of this 
	/// frontier, which ensures that operators can extract the subset of the trace at batch boundaries from this
	/// frontier onward. These boundaries may be used in `cursor_through`, whereas boundaries not in advance of 
	/// this frontier are not guaranteed to return a cursor.
	fn distinguish_frontier(&mut self) -> &[Time];

	/// Maps some logic across the batches the collection manages.
	///
	/// This is currently used only to extract historical data to prime late-starting operators who want to reproduce
	/// the stream of batches moving past the trace. It could also be a fine basis for a default implementation of the
	/// cursor methods, as they (by default) just move through batches accumulating cursors into a cursor list.
	fn map_batches<F: FnMut(&Self::Batch)>(&mut self, f: F);

}

/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace must pretend to look like a collection of `(Key, Val, Time, isize)` tuples, but is permitted
/// to introduce new types `KeyRef`, `ValRef`, and `TimeRef` which can be dereference to the types above.
///
/// The trace must be constructable from, and navigable by the `Key`, `Val`, `Time` types, but does not need
/// to return them.
pub trait Trace<Key, Val, Time, R> : TraceReader<Key, Val, Time, R> where <Self as TraceReader<Key, Val, Time, R>>::Batch: Batch<Key, Val, Time, R> {

	/// Allocates a new empty trace.
	fn new() -> Self;

	/// Introduces a batch of updates to the trace.
	///
	/// Batches describe the time intervals they contain, and they should be added to the trace in contiguous
	/// intervals. If a batch arrives with a lower bound that does not equal the upper bound of the most recent
	/// addition, the trace will add an empty batch. It is an error to then try to populate that region of time.
	///
	/// This restriction could be relaxed, especially if we discover ways in which batch interval order could 
	/// commute. For now, the trace should complain, to the extent that it cares about contiguous intervals.
	fn insert(&mut self, batch: Self::Batch);
}

/// A batch of updates whose contents may be read.
///
/// This is a restricted interface to batches of updates, which support the reading of the batch's contents,
/// but do not expose ways to construct the batches. This trait is appropriate for views of the batch, and is
/// especially useful for views derived from other sources in ways that prevent the construction of batches
/// from the type of data in the view (for example, filtered views, or views with extended time coordinates).
pub trait BatchReader<K, V, T, R> where Self: ::std::marker::Sized 
{
	/// The type used to enumerate the batch's contents.
	type Cursor: Cursor<K, V, T, R, Storage=Self>;
	/// Acquires a cursor to the batch's contents.
	fn cursor(&self) -> Self::Cursor;
	/// The number of updates in the batch.
	fn len(&self) -> usize;
	/// Describes the times of the updates in the batch.
	fn description(&self) -> &Description<T>;

	/// All times in the batch are greater or equal to an element of `lower`.
	fn lower(&self) -> &[T] { self.description().lower() }
	/// All times in the batch are not greater or equal to any element of `upper`.
	fn upper(&self) -> &[T] { self.description().upper() }
}

/// An immutable collection of updates.
pub trait Batch<K, V, T, R> : BatchReader<K, V, T, R> where Self: ::std::marker::Sized {
	/// A type used to assemble batches from disordered updates.
	type Batcher: Batcher<K, V, T, R, Self>;
	/// A type used to assemble batches from ordered update sequences.
	type Builder: Builder<K, V, T, R, Self>;
	/// A type used to progressively merge batches.
	type Merger: Merger<K, V, T, R, Self>;

	/// Merges two consecutive batches.
	///
	/// Panics if `self.upper()` does not equal `other.lower()`. This is almost certainly a logic bug,
	/// as the resulting batch does not have a contiguous description. If you would like to put an empty
	/// interval between the two, you can create an empty interval and do two merges.
	fn merge(&self, other: &Self) -> Self;

	/// Initiates the merging of consecutive batches.
	///
	/// The result of this method can be exercised to eventually produce the same result
	/// that a call to `self.merge(other)` would produce, but it can be done in a measured
	/// fashion. This can help to avoid latency spikes where a large merge needs to happen.
	fn begin_merge(&self, other: &Self) -> Self::Merger;

	/// Advance times to `frontier` creating a new batch.
	fn advance_ref(&self, frontier: &[T]) -> Self where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {

		assert!(frontier.len() > 0);

		// TODO: This is almost certainly too much `with_capacity`.
		let mut builder = Self::Builder::with_capacity(self.len());

		let mut times = Vec::new();
		let mut cursor = self.cursor();

		while cursor.key_valid(self) {
			while cursor.val_valid(self) {
				cursor.map_times(self, |time: &T, diff| times.push((time.advance_by(frontier), diff)));
				consolidate(&mut times, 0);
				for (time, diff) in times.drain(..) {
					builder.push((cursor.key(self).clone(), cursor.val(self).clone(), time, diff));
				}
				cursor.step_val(self);
			}
			cursor.step_key(self);
		}

		builder.done(self.description().lower(), self.description().upper(), frontier)
	}
	/// Advance times to `frontier` updating this batch.
	///
	/// This method gives batches the ability to collapse in-place when possible, and is the common 
	/// entry point to advance batches. Most types of batches do have shared state, but `advance` is 
	/// commonly invoked just after a batch is formed from a merge and when there is a unique owner 
	/// of the shared state. 
	#[inline(never)]
	fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {
		*self = self.advance_ref(frontier);
	}
}

/// Functionality for collecting and batching updates.
pub trait Batcher<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Allocates a new empty batcher.
	fn new() -> Self; 
	/// Adds an unordered batch of elements to the batcher.
	fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>);
	/// Returns all updates not greater or equal to an element of `upper`.
	fn seal(&mut self, upper: &[T]) -> Output;
	/// Returns the lower envelope of contained update times.
	fn frontier(&mut self) -> &[T];
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Allocates an empty builder.
	fn new() -> Self;
	/// Allocates an empty builder with some capacity.
	fn with_capacity(cap: usize) -> Self;
	/// Adds an element to the batch.
	fn push(&mut self, element: (K, V, T, R));
	/// Adds an ordered sequence of elements to the batch.
	fn extend<I: Iterator<Item=(K,V,T,R)>>(&mut self, iter: I) {
		for item in iter { self.push(item); }
	}
	/// Completes building and returns the batch.
	fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Output;
}

/// Represents a merge in progress.
pub trait Merger<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Perform some amount of work, decrementing `fuel`.
	///
	/// If `fuel` is non-zero after the call, the merging is complete and
	/// one should call `done` to extract the merged results.
	fn work(&mut self, source1: &Output, source2: &Output, fuel: &mut usize);
	/// Extracts merged results.
	///
	/// This method should only be called after `work` has been called and
	/// has not brought `fuel` to zero. Otherwise, the merge is still in 
	/// progress.
	fn done(self) -> Output;
}


/// Blanket implementations for reference counted batches.
pub mod rc_blanket_impls {

	use std::rc::Rc;

	use ::Diff;
	use ::lattice::Lattice;
	use super::{Batch, BatchReader, Batcher, Builder, Merger, Cursor, Description};

	impl<K, V, T, R, B: BatchReader<K,V,T,R>> BatchReader<K,V,T,R> for Rc<B> {

		/// The type used to enumerate the batch's contents.
		type Cursor = RcBatchCursor<K, V, T, R, B>;
		/// Acquires a cursor to the batch's contents.
		fn cursor(&self) -> Self::Cursor {
			RcBatchCursor::new((&**self).cursor())
		}

		/// The number of updates in the batch.
		fn len(&self) -> usize { (&**self).len() }
		/// Describes the times of the updates in the batch.
		fn description(&self) -> &Description<T> { (&**self).description() }
	}

	/// Wrapper to provide cursor to nested scope.
	pub struct RcBatchCursor<K, V, T, R, B: BatchReader<K, V, T, R>> {
	    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
	    cursor: B::Cursor,
	}

	impl<K, V, T, R, B: BatchReader<K, V, T, R>> RcBatchCursor<K, V, T, R, B> {
	    fn new(cursor: B::Cursor) -> Self {
	        RcBatchCursor {
	            cursor: cursor,
	            phantom: ::std::marker::PhantomData,
	        }
	    }
	}

	impl<K, V, T, R, B: BatchReader<K, V, T, R>> Cursor<K, V, T, R> for RcBatchCursor<K, V, T, R, B> {

	    type Storage = Rc<B>;

	    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
	    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

	    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
	    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

	    #[inline(always)]
	    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, logic: L) { 
	    	self.cursor.map_times(storage, logic)
	    }

	    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
	    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }
	    
	    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
	    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

	    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
	    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
	}



	/// An immutable collection of updates.
	impl<K,V,T,R,B: Batch<K,V,T,R>> Batch<K, V, T, R> for Rc<B> {

		type Batcher = RcBatcher<K, V, T, R, B>;
		type Builder = RcBuilder<K, V, T, R, B>;
		type Merger = RcMerger<K, V, T, R, B>;

		fn merge(&self, other: &Self) -> Self { Rc::new(B::merge(self, other)) }

		fn begin_merge(&self, other: &Self) -> Self::Merger { RcMerger { merger: B::begin_merge(self, other) } }

		fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {
			let mut updated = false;
			if let Some(batch) = Rc::get_mut(self) {
				batch.advance_mut(frontier);
				updated = true;
			}

			if !updated {
				*self = self.advance_ref(frontier);
			}
		}
	}

	/// Wrapper type for batching reference counted batches.
	pub struct RcBatcher<K,V,T,R,B:Batch<K,V,T,R>> { batcher: B::Batcher }

	/// Functionality for collecting and batching updates.
	impl<K,V,T,R,B:Batch<K,V,T,R>> Batcher<K, V, T, R, Rc<B>> for RcBatcher<K,V,T,R,B> {
		fn new() -> Self { RcBatcher { batcher: <B::Batcher as Batcher<K,V,T,R,B>>::new() } }
		fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>) { self.batcher.push_batch(batch) }
		fn seal(&mut self, upper: &[T]) -> Rc<B> { Rc::new(self.batcher.seal(upper)) }
		fn frontier(&mut self) -> &[T] { self.batcher.frontier() }
	}

	/// Wrapper type for building reference counted batches.
	pub struct RcBuilder<K,V,T,R,B:Batch<K,V,T,R>> { builder: B::Builder }

	/// Functionality for building batches from ordered update sequences.
	impl<K,V,T,R,B:Batch<K,V,T,R>> Builder<K, V, T, R, Rc<B>> for RcBuilder<K,V,T,R,B> {
		fn new() -> Self { RcBuilder { builder: <B::Builder as Builder<K,V,T,R,B>>::new() } }
		fn with_capacity(cap: usize) -> Self { RcBuilder { builder: <B::Builder as Builder<K,V,T,R,B>>::with_capacity(cap) } }
		fn push(&mut self, element: (K, V, T, R)) { self.builder.push(element) }
		fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<B> { Rc::new(self.builder.done(lower, upper, since)) }
	}

	/// Wrapper type for merging reference counted batches.
	pub struct RcMerger<K,V,T,R,B:Batch<K,V,T,R>> { merger: B::Merger }

	/// Represents a merge in progress.
	impl<K,V,T,R,B:Batch<K,V,T,R>> Merger<K, V, T, R, Rc<B>> for RcMerger<K,V,T,R,B> {
		fn work(&mut self, source1: &Rc<B>, source2: &Rc<B>, fuel: &mut usize) { self.merger.work(source1, source2, fuel) }
		fn done(self) -> Rc<B> { Rc::new(self.merger.done()) }
	}
}






/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate<T: Ord+Clone, R: Diff>(vec: &mut Vec<(T, R)>, off: usize) {
	consolidate_by(vec, off, |x,y| x.cmp(&y));
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate_by<T: Eq+Clone, L: Fn(&T, &T)->::std::cmp::Ordering, R: Diff>(vec: &mut Vec<(T, R)>, off: usize, cmp: L) {
	vec[off..].sort_by(|x,y| cmp(&x.0, &y.0));
	for index in (off + 1) .. vec.len() {
		if vec[index].0 == vec[index - 1].0 {
			vec[index].1 = vec[index].1 + vec[index - 1].1;
			vec[index - 1].1 = R::zero();
		}
	}
	let mut cursor = off;
	for index in off .. vec.len() {
		if !vec[index].1.is_zero() {
			vec.swap(cursor, index);
			// vec[cursor] = vec[index].clone();
			cursor += 1;
		}
	}
	vec.truncate(cursor);
}