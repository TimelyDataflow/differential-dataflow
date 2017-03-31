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

use ::Ring;
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



/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace must pretend to look like a collection of `(Key, Val, Time, isize)` tuples, but is permitted
/// to introduce new types `KeyRef`, `ValRef`, and `TimeRef` which can be dereference to the types above.
///
/// The trace must be constructable from, and navigable by the `Key`, `Val`, `Time` types, but does not need
/// to return them.
pub trait Trace<Key, Val, Time, R> {

	/// The type of an immutable collection of updates.
	type Batch: Batch<Key, Val, Time, R>+Clone+'static;
	/// The type used to enumerate the collections contents.
	type Cursor: Cursor<Key, Val, Time, R>;

	/// Allocates a new empty trace.
	fn new(default: Time) -> Self;
	/// Introduces a batch of updates to the trace.
	///
	/// Batches describe the time intervals they contain, and they should be added to the trace in contiguous
	/// intervals. If a batch arrives with a lower bound that does not equal the upper bound of the most recent
	/// addition, the trace will add an empty batch. It is an error to then try to populate that region of time.
	///
	/// This restriction could be relaxed, especially if we discover ways in which batch interval order could 
	/// commute. For now, the trace should complain, to the extent that it cares about contiguous intervals.
	fn insert(&mut self, batch: Self::Batch);
	/// Acquires a cursor to the collection's contents.
	fn cursor(&self) -> Self::Cursor;
	/// Advances the frontier of times the collection must respond to.
	fn advance_by(&mut self, frontier: &[Time]);
}

/// An immutable collection of updates.
pub trait Batch<K, V, T, R> where Self: ::std::marker::Sized {
	/// A type used to assemble batches from disordered updates.
	type Batcher: Batcher<K, V, T, R, Self>;
	/// A type used to assemble batches from ordered update sequences.
	type Builder: Builder<K, V, T, R, Self>;
	/// The type used to enumerate the batch's contents.
	type Cursor: Cursor<K, V, T, R>;
	/// Acquires a cursor to the batch's contents.
	fn cursor(&self) -> Self::Cursor;
	/// The number of updates in the batch.
	fn len(&self) -> usize;
	/// Describes the times of the updates in the batch.
	fn description(&self) -> &Description<T>;
}

/// Functionality for collecting and batching updates.
pub trait Batcher<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Allocates a new empty batcher.
	fn new() -> Self; 
	/// Adds an element to the batcher.
	fn push(&mut self, element: (K, V, T, R));
	/// Adds an unordered sequence of elements to the batcher.
	fn extend<I: Iterator<Item=(K,V,T,R)>>(&mut self, iter: I) {
		for item in iter {
			self.push(item);
		}
	}
	/// Returns all updates not greater or equal to an element of `upper`.
	fn seal(&mut self, upper: &[T]) -> Output;
	/// Returns the lower envelope of contained update times.
	fn frontier(&mut self) -> &[T];
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Allocates an empty builder.
	fn new() -> Self;
	/// Adds an element to the batch.
	fn push(&mut self, element: (K, V, T, R));
	/// Adds an ordered sequence of elements to the batch.
	fn extend<I: Iterator<Item=(K,V,T,R)>>(&mut self, iter: I) {
		for item in iter { self.push(item); }
	}
	/// Completes building and returns the batch.
	fn done(self, lower: &[T], upper: &[T]) -> Output;
}

/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate<T: Ord+Clone, R: Ring>(vec: &mut Vec<(T, R)>, off: usize) {
	consolidate_by(vec, off, |x,y| x.cmp(&y));
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate_by<T: Eq+Clone, L: Fn(&T, &T)->::std::cmp::Ordering, R: Ring>(vec: &mut Vec<(T, R)>, off: usize, cmp: L) {
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
			vec[cursor] = vec[index].clone();
			cursor += 1;
		}
	}
	vec.truncate(cursor);
}