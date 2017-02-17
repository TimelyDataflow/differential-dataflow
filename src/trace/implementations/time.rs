//! A trace implementation for updates with a common time.

use ::Delta;

/// An immutable collection of updates with a common time.
pub struct Layer<Key, Val, Time> {
	time: Time,
	keys: Vec<Key>,
	offs: Vec<usize>,
	vals: Vec<(Val, Delta)>,
}