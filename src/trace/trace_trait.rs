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
	fn new() -> Self;
	/// Introduces a batch of updates to the trace.
	fn insert(&mut self, batch: Self::Batch);
	/// Acquires a cursor to the collection's contents.
	fn cursor(&self) -> Self::Cursor;
	/// Advances the frontier of times the collection must respond to.
	fn advance(&mut self, frontier: &[Time]);
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





/// A type supporting navigation of update tuples by key.
pub trait KeyCursor<'a, Key, Val, Time> {
	/// Type supporting navigation by value; returned for each key.
	type ValCursor: ValCursor<'a, Key, Val, Time>;
	/// Advances cursor to next key, if it exists.
	fn next_key(&'a mut self) -> Option<Self::ValCursor>;
	/// Advances cursor to specific key, if it exists.
	fn seek_key(&'a mut self, key: &Key) -> Option<Self::ValCursor>;
	/// Reveals the next key, if it exists.
	fn peek_key(&mut self) -> Option<&Key>;
	// /// Returns cursor for the previous key's values. (TODO : what happens at first?)
	// fn redo_key(&'a mut self) -> Option<Self::ValCursor>;
}

/// A type supporting navigation of update tuples by value.
pub trait ValCursor<'a, Key, Val, Time> {
	/// A type abstracting the `(Time, isize)` pairs of the value.
	type TimeCursor: TimeCursor<Val, Time>;
	/// Advances cursor to the next value, if it exists.
	fn next_val(&'a mut self) -> Option<Self::TimeCursor>;
	/// Advances cursor to a specific value, if it exists.
	fn seek_val(&'a mut self, key: &Val) -> Option<Self::TimeCursor>;
	/// Reveals the next value, if it exists.
	fn peek_val(&mut self) -> Option<&Val>;
	/// Returns the key these values correspond to.
	fn key(&self) -> &Key;
}

/// A type representing unordered `(Time, isize)` pairs.
pub trait TimeCursor<Val, Time> {
	/// Applies `logic` to each `(time, diff)` tuple.
	fn map<L: FnMut(&Time, isize)>(&self, logic: L);
	/// Returns the value these updates correspond to.
	fn val(&self) -> &Val;
}



