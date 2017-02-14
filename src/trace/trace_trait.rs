//! A trait definition for a collection trace.
//!
//! In addition to storing data, a collection trace must support several modes of interacting with
//! the data, and several data structures used to represent parts of this data.

use trace::cursor::Cursor;

/// A collection of `(key, val, time, diff)` tuples.
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
	type Builder: Builder<Key, Val, Time>+Into<Self>;
	/// The type used to enumerate the batch's contents.
	type Cursor: Cursor<Key = Key, Val = Val, Time = Time>;
	/// Acquires a cursor to the batch's contents.
	fn cursor(&self) -> Self::Cursor;
	/// The number of updates in the batch.
	fn len(&self) -> usize;
}

/// A type which builds up a batch, but is not navigable until done building.
pub trait Builder<Key, Val, Time> {
	/// Allocates a new empty builder.
	fn new() -> Self; 
	/// Adds a new element to the batch.
	fn push(&mut self, element: (Key, Val, Time, isize));
}