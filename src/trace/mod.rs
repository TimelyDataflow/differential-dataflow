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

pub use self::implementations::trie::{Spine, Layer, LayerCursor};
pub use self::cursor::Cursor;
pub use self::cursor::viewers::{ KeyViewer, ValViewer, TimeViewer };

/// An append-only collection of `(key, val, time, diff)` tuples.
pub trait Trace<Key: Ord, Val: Ord, Time: Ord> {
	/// The type of an immutable collection of updates.
	type Batch: Batch<Key, Val, Time>;
	/// The type used to enumerate the collections contents.
	type Cursor: Cursor<Key = Key, Val = Val, Time = Time>;

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
pub trait Batch<Key: Ord, Val: Ord, Time: Ord> where Self: ::std::marker::Sized {
	/// The type used to assemble a batch.
	///
	/// The `Self::Builder` type must support conversion into a batch, and so can always
	/// be used to create new batches. Other types may also be used to build batches, but
	/// this type must be defined to provide a default builder type.
	type Builder: Builder<Key, Val, Time, Self>;
	/// The type used to enumerate the batch's contents.
	type Cursor: Cursor<Key = Key, Val = Val, Time = Time>;
	/// Acquires a cursor to the batch's contents.
	fn cursor(&self) -> Self::Cursor;
	/// The number of updates in the batch.
	fn len(&self) -> usize;
}

/// A type which builds up a batch, but is not navigable until done building.
pub trait Builder<Key, Val, Time, Output> {
	/// Allocates a new empty builder.
	fn new() -> Self; 
	/// Adds a new element to the batch.
	fn push(&mut self, element: (Key, Val, Time, isize));
	/// Completes building and returns the batch.
	fn done(self) -> Output;
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