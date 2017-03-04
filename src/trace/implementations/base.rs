//! A trace implementation for updates with times equivalent to the least time.

/// An immutable collection of updates with times equivalent to the least time.
pub struct Layer<Key, Val, Time> {
	time: Time,			// <-- so that we have an instance to hand as a reference.
	keys: Vec<Key>,
	offs: Vec<usize>,
	vals: Vec<Val>,
}