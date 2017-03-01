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

pub use self::cursor::Cursor;

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

pub trait Trace<Key, Val, Time> {

	/// The type of an immutable collection of updates.
	type Batch: Batch<Key, Val, Time>+Clone+'static;
	/// The type used to enumerate the collections contents.
	type Cursor: Cursor<Key, Val, Time>;

	/// Allocates a new empty trace.
	fn new(default: Time) -> Self;
	/// Introduces a batch of updates to the trace.
	fn insert(&mut self, batch: Self::Batch);
	/// Acquires a cursor to the collection's contents.
	fn cursor(&self) -> Self::Cursor;
	/// Advances the frontier of times the collection must respond to.
	fn advance_by(&mut self, frontier: &[Time]);
}


/// An immutable collection of updates.
pub trait Batch<K, V, T> where Self: ::std::marker::Sized {

	/// The type used to assemble a batch.
	///
	/// The `Self::Builder` type must support conversion into a batch, and so can always
	/// be used to create new batches. Other types may also be used to build batches, but
	/// this type must be defined to provide a default builder type.
	type Builder: Builder<K, V, T, Self>;
	/// The type used to enumerate the batch's contents.
	type Cursor: Cursor<K, V, T>;
	/// Acquires a cursor to the batch's contents.
	fn cursor(&self) -> Self::Cursor;
	/// The number of updates in the batch.
	fn len(&self) -> usize;
}

/// A type which builds up a batch, but is not navigable until done building.
pub trait Builder<K, V, T, Output: Batch<K, V, T>> {
	/// Allocates a new empty builder.
	fn new() -> Self; 
	/// Adds a new element to the batch.
	fn push(&mut self, element: (K, V, T, isize));
	/// Adds an unordered sequence of new elements to the batch.
	fn extend<I: Iterator<Item=(K,V,T,isize)>>(&mut self, iter: I) {
		for item in iter {
			self.push(item);
		}
	}
	/// Completes building and returns the batch.
	fn done(self, lower: &[T], upper: &[T]) -> Output;
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate<T: Ord+Clone>(vec: &mut Vec<(T, isize)>, off: usize) {
	consolidate_by(vec, off, |x,y| x.cmp(&y));
	// vec[off..].sort_by(|x,y| x.0.cmp(&y.0));
	// for index in (off + 1) .. vec.len() {
	// 	if vec[index].0 == vec[index - 1].0 {
	// 		vec[index].1 += vec[index - 1].1;
	// 		vec[index - 1].1 = 0;
	// 	}
	// }
	// let mut cursor = off;
	// for index in off .. vec.len() {
	// 	if vec[index].1 != 0 {
	// 		vec[cursor] = vec[index].clone();
	// 		cursor += 1;
	// 	}
	// }
	// vec.truncate(cursor);
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate_by<T: Eq+Clone, L: Fn(&T, &T)->::std::cmp::Ordering>(vec: &mut Vec<(T, isize)>, off: usize, cmp: L) {
	vec[off..].sort_by(|x,y| cmp(&x.0, &y.0));
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