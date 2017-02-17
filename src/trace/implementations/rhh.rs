//! A Trace implementation where keys are stored as in a robin hood hashing scheme.
//!
//! Robin Hood hashing maps each element to some intended location, and the elements are then
//! stored at this location or one afterwards. When resolving conflicts, priority is given to 
//! the element "further from home". We can lay out such a hash map by sorting by hash value, 
//! and then inserting each element in the first free location from their intended location.
//!
//! Ideally, Robin Hood hashing leads to very short search sequences, and it can be very simple
//! if we only deal with static collections (as we do in each layer). The upside should be that
//! `seek_key` becomes quite fast, directly leaping to the desired location in memory at the 
//! cost of a few hash computations (once for the target key, and for each other key we encouter 
//! as we search for the target.

pub struct Layer<Key, Val, Time> {
	keys: Vec<Option<(Key, usize)>>, 	// <-- would like `Option<(Key, NonZero<usize>)>`
	vals: Vec<(Val, usize)>,
	times: Vec<(Time, isize)>,
}