//! Implementation using ordered keys with hashes and robin hood hashing.

use std::default::Default;

use timely_sort::Unsigned;

use ::hashable::{Hashable, HashOrdered};
use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder};

const MINIMUM_SHIFT : usize = 4;
const BLOAT_FACTOR : f64 = 1.1;

// I would like the trie entries to look like (Key, usize), where a usize equal to the
// previous entry indicates that the location is empty. This would let us always use the
// prior location to determine lower bounds, rather than double up upper and lower bounds
// in Entry.
//
// It might also be good to optimistically build the hash map in place. We can do this by
// upper bounding the number of keys, allocating and placing as if this many, and then 
// drawing down the allocation and placements if many keys collided or cancelled. 

/// A level of the trie, with keys and offsets into a lower layer.
///
/// If keys[i].1 == 0 then entry i should
/// be ignored. This is our version of `Option<(K, usize)>`, which comes at the cost
/// of requiring `K: Default` to populate empty keys.
///
/// Each region of this layer is an independent immutable RHH map, whose size should
/// equal something like `(1 << i) + i` for some value of `i`. The first `(1 << i)` 
/// elements are where we expect to find keys, and the remaining `i` are for spill-over
/// due to collisions near the end of the first region.
///
/// We might do something like "if X or fewer elements, just use an ordered list".

#[derive(Debug)]
pub struct HashedLayer<K: HashOrdered, L> {
	/// Keys and offsets for the keys.
	pub keys: Vec<Entry<K>>,	// track upper and lower bounds, because trickery is hard.
	/// A lower layer containing ranges of values.
	pub vals: L,
}

impl<K: HashOrdered, L> HashedLayer<K, L> {
	fn _entry_valid(&self, index: usize) -> bool { self.keys[index].is_some() }
	fn lower(&self, index: usize) -> usize { self.keys[index].get_lower() }
	fn upper(&self, index: usize) -> usize { self.keys[index].get_upper() }
}

impl<K: Clone+HashOrdered+Default, L: Trie> Trie for HashedLayer<K, L> {
	type Item = (K, L::Item);
	type Cursor = HashedCursor<L>;
	type MergeBuilder = HashedBuilder<K, L::MergeBuilder>;
	type TupleBuilder = HashedBuilder<K, L::TupleBuilder>;

	fn keys(&self) -> usize { self.keys.len() }
	fn tuples(&self) -> usize { self.vals.tuples() }
	fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor {

		if lower < upper {

			let mut shift = 0; 
			while upper - lower >= (1 << shift) {
				shift += 1;
			}
			shift -= 1;

			let mut pos = lower;	// set self.pos to something valid.
			while pos < upper && !self.keys[pos].is_some() {
				pos += 1;
			}

			HashedCursor {
				shift: shift,
				bounds: (lower, upper),
				pos: pos,
				// keys: owned_self.clone().map(|x| &x.keys[..]),
				child: self.vals.cursor_from(self.keys[pos].get_lower(), self.keys[pos].get_upper())
			}
		}
		else {
			HashedCursor {
				shift: 0,
				bounds: (0, 0),
				pos: 0,
				// keys: owned_self.clone().map(|x| &x.keys[..]), // &self.keys,
				child: self.vals.cursor_from(0, 0),
			}
		}
	}
}

/// An entry in hash tables.
#[derive(Debug, Clone)]
pub struct Entry<K: HashOrdered> {
	/// The contained key.
	key: K,
	lower1: u32,
	upper1: u32,
}

impl<K: HashOrdered> Entry<K> {
	fn new(key: K, lower: usize, upper: usize) -> Self {
		Entry {
			key: key, 
			lower1: lower as u32,
			upper1: upper as u32,
		}
	}
	// fn for_cmp(&self) -> (K::Output, &K) { (self.key.hashed(), &self.key) }
	fn is_some(&self) -> bool { self.upper1 != 0 }
	fn empty() -> Self where K: Default { Self::new(Default::default(), 0, 0) }
	fn get_lower(&self) -> usize { self.lower1 as usize}
	fn get_upper(&self) -> usize { self.upper1 as usize}
	fn _set_lower(&mut self, x: usize) { self.lower1 = x as u32; }
	fn set_upper(&mut self, x: usize) { self.upper1 = x as u32; }
}

/// Assembles a layer of this 
pub struct HashedBuilder<K: HashOrdered, L> {
	temp: Vec<Entry<K>>,		// staging for building; densely packed here and then re-laid out in self.keys.
	/// Entries in the hash map.
	pub keys: Vec<Entry<K>>,	// keys and offs co-located because we expect to find the right answers fast.
	/// A builder for the layer below.
	pub vals: L,
}

impl<K: HashOrdered+Clone+Default, L> HashedBuilder<K, L> {

	#[inline(always)]
	fn _lower(&self, index: usize) -> usize {
		self.keys[index].get_lower()
	}
	#[inline(always)]
	fn _upper(&self, index: usize) -> usize {
		self.keys[index].get_upper()
	}
}

impl<K: HashOrdered+Clone+Default, L: Builder> Builder for HashedBuilder<K, L> {
	type Trie = HashedLayer<K, L::Trie>;


	/// Looks at the contents of self.temp and extends self.keys appropriately.
	///
	/// This is where the "hash map" structure is produced. Up until this point, all (key, usize) pairs were 
	/// committed to self.temp, where they awaited layout. That now happens here.
	fn boundary(&mut self) -> usize {

		/// self.temp *should* be sorted by (hash, key); let's check!
		debug_assert!((1 .. self.temp.len()).all(|i| self.temp[i-1].key < self.temp[i].key));

		let boundary = self.vals.boundary();

		if self.temp.len() > 0 {

			// push doesn't know the length at the end; must write it
			if !self.temp[self.temp.len()-1].is_some() {
				let pos = self.temp.len()-1;
				self.temp[pos].set_upper(boundary);
			}

			// having densely packed everything, we now want to extend the allocation and rewrite the contents 
			// so that their spacing is in line with how robin hood hashing works.
			let lower = self.keys.len();
			if self.temp.len() < (1 << MINIMUM_SHIFT) {
				self.keys.extend(self.temp.drain(..));
			}
			else {
				let target = (BLOAT_FACTOR * (self.temp.len() as f64)) as u64;
				let mut shift = MINIMUM_SHIFT;
				while  (1 << shift) < target {
					shift += 1;
				}

				self.keys.reserve(1 << shift);

				// now going to start pushing things in to self.keys
				let mut cursor: usize = 0;	// <-- current write pos in self.keys.
				for entry in self.temp.drain(..) {
					// acquire top `shift` bits from `key.hashed()`
					let target = (entry.key.hashed().as_u64() >> ((<K as Hashable>::Output::bytes() * 8) - shift)) as usize;
					debug_assert!(target < (1 << shift));

					while cursor < target {
						// filling with bogus stuff
						self.keys.push(Entry::empty());
						cursor += 1;
					}
					self.keys.push(entry);
					cursor += 1;
				}

				// fill out the space, if not full.
				while cursor < (1 << shift) {
					self.keys.push(Entry::empty());
					cursor += 1;
				}

				// assert that we haven't doubled the allocation (would confuse the "what is shift?" logic)
				assert!((self.keys.len() - lower) < (2 << shift));
			}
		}

		self.keys.len()
	}

	#[inline(never)]
	fn done(mut self) -> Self::Trie {
		self.boundary();
		self.keys.shrink_to_fit();
		let vals = self.vals.done();

		if vals.tuples() > 0 { 
			assert!(self.keys.len() > 0);
		}

		HashedLayer {
			keys: self.keys,
			vals: vals,
		}
	}
}

impl<K: HashOrdered+Clone+Default, L: MergeBuilder> MergeBuilder for HashedBuilder<K, L> {

	fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
		HashedBuilder {
			temp: Vec::new(),
			keys: Vec::with_capacity(other1.keys() + other2.keys()),
			vals: L::with_capacity(&other1.vals, &other2.vals),
		}
	}
	/// Copies fully formed ranges (note plural) of keys from another trie.
	///
	/// While the ranges are fully formed, the offsets in them are relative to the other trie, and
	/// must be corrected. These keys must be moved immediately to self.keys, as there is no info
	/// about boundaries between them, and we are unable to lay out the info any differently.
	fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
		if lower < upper {
			let other_basis = other.lower(lower);	// from where in `other` the offsets do start.
			let self_basis = self.vals.boundary();	// from where in `self` the offsets must start.
			for index in lower .. upper {
				let other_entry = &other.keys[index];
				let new_entry = if other_entry.is_some() {
					Entry::new(
						other_entry.key.clone(), 
						(other_entry.get_lower() + self_basis) - other_basis,
						(other_entry.get_upper() + self_basis) - other_basis,
					)
				}
				else { Entry::empty() };
				self.keys.push(new_entry);
			}
			self.vals.copy_range(&other.vals, other.lower(lower), other.upper(upper-1));
			self.boundary();	// <-- perhaps unnecessary, but ...
		}	
	}
	fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize {
		
		// just rebinding names to clarify code.
		let (trie1, mut lower1, upper1) = other1;
		let (trie2, mut lower2, upper2) = other2;

		debug_assert!(upper1 <= trie1.keys.len());
		debug_assert!(upper2 <= trie2.keys.len());

		self.temp.reserve((upper1 - lower1) + (upper2 - lower2));

		while lower1 < trie1.keys.len() && !trie1.keys[lower1].is_some() { lower1 += 1; }
		while lower2 < trie2.keys.len() && !trie2.keys[lower2].is_some() { lower2 += 1; }

		// while both mergees are still active
		while lower1 < upper1 && lower2 < upper2 {

			debug_assert!(trie1.keys[lower1].is_some());
			debug_assert!(trie2.keys[lower2].is_some());

			match trie1.keys[lower1].key.cmp(&trie2.keys[lower2].key) {
				::std::cmp::Ordering::Less => {
					lower1 += self.push_while_less(trie1, lower1, upper1, &trie2.keys[lower2].key);
				}
				::std::cmp::Ordering::Equal => {
					let lower = self.vals.boundary();
					let upper = self.vals.push_merge(
						(&trie1.vals, trie1.lower(lower1), trie1.upper(lower1)), 
						(&trie2.vals, trie2.lower(lower2), trie2.upper(lower2))
					);
					if upper > lower {
						self.temp.push(Entry::new(trie1.keys[lower1].key.clone(), lower, upper));
					}

					lower1 += 1;
					lower2 += 1;
					while lower1 < trie1.keys.len() && !trie1.keys[lower1].is_some() { lower1 += 1; }
					while lower2 < trie2.keys.len() && !trie2.keys[lower2].is_some() { lower2 += 1; }
				} 
				::std::cmp::Ordering::Greater => {
					lower2 += self.push_while_less(trie2, lower2, upper2, &trie1.keys[lower1].key);
				}
			}
		}

		if lower1 < upper1 { self.push_all(trie1, lower1, upper1); }
		if lower2 < upper2 { self.push_all(trie2, lower2, upper2); }

		self.boundary()
	}
}


impl<K: HashOrdered+Clone+Default, L: TupleBuilder> TupleBuilder for HashedBuilder<K, L> {

	type Item = (K, L::Item);
	fn new() -> Self { HashedBuilder { temp: Vec::new(), keys: Vec::new(), vals: L::new() } }
	fn with_capacity(cap: usize) -> Self { 
		HashedBuilder { 
			temp: Vec::with_capacity(cap), 
			keys: Vec::with_capacity(cap), 
			vals: L::with_capacity(cap),
		} 
	}
	#[inline(always)]
	fn push_tuple(&mut self, (key, val): (K, L::Item)) {

		// we build up self.temp, and rely on self.boundary() to drain self.temp.
		let temp_len = self.temp.len();
		if temp_len == 0 || self.temp[temp_len-1].key != key {
			if temp_len > 0 { debug_assert!(self.temp[temp_len-1].key < key); }
			let boundary = self.vals.boundary();
			if temp_len > 0 {
				self.temp[temp_len-1].set_upper(boundary);
			}
			self.temp.push(Entry::new(key, boundary, 0));	// this should be fixed by boundary?
		}

		self.vals.push_tuple(val);
	}
}

impl<K: HashOrdered+Clone+Default, L: MergeBuilder> HashedBuilder<K, L> {
	/// Moves other stuff into self.temp. Returns number of element consumed.
	fn push_while_less(&mut self, other: &HashedLayer<K, L::Trie>, lower: usize, upper: usize, vs: &K) -> usize {

		let other_basis = other.lower(lower);	// from where in `other` the offsets do start.
		let self_basis = self.vals.boundary();	// from where in `self` the offsets must start.

		let mut bound = 0;	// tracks largest value of upper
		let mut index = lower;
		// let vs_hashed = vs.hashed();

		// stop if overrun, or if we find a valid element >= our target.
		while index < upper && !(other.keys[index].is_some() && &other.keys[index].key >= vs) {
			if other.upper(index) != 0 { 
				if bound < other.upper(index) { bound = other.upper(index); }
				debug_assert!(other.lower(index) < other.upper(index));
				let lower = (other.lower(index) + self_basis) - other_basis;
				let upper = (other.upper(index) + self_basis) - other_basis;
				self.temp.push(Entry::new(other.keys[index].key.clone(), lower, upper));
			}
			index += 1;
		}
		debug_assert!(bound > 0);
		self.vals.copy_range(&other.vals, other.lower(lower), bound);
	
		index - lower
	}

	fn push_all(&mut self, other: &HashedLayer<K, L::Trie>, lower: usize, upper: usize) {

		debug_assert!(lower < upper);
		debug_assert!(upper <= other.keys.len());

		let other_basis = other.lower(lower);	// from where in `other` the offsets do start.
		let self_basis = self.vals.boundary();	// from where in `self` the offsets must start.

		let mut bound = 0;	// tracks largest value of upper
		for index in lower .. upper {
			if other.upper(index) != 0 { 
				if bound < other.upper(index) { bound = other.upper(index); }
				let lower = (other.lower(index) + self_basis) - other_basis;
				let upper = (other.upper(index) + self_basis) - other_basis;
				self.temp.push(Entry::new(other.keys[index].key.clone(), lower, upper));
			}
		}
		debug_assert!(bound > 0);
		self.vals.copy_range(&other.vals, other.lower(lower), bound);
	}

}




/// A cursor with a child cursor that is updated as we move.
#[derive(Debug)]
pub struct HashedCursor<L: Trie> {
	shift: usize,			// amount by which to shift hashes.
	bounds: (usize, usize),	// bounds of slice of self.keys.
	pos: usize,				// <-- current cursor position.

	/// A cursor for the layer below this one.
	pub child: L::Cursor,
}

impl<K: HashOrdered, L: Trie> Cursor<HashedLayer<K, L>> for HashedCursor<L> {
	type Key = K;
	fn key<'a>(&self, storage: &'a HashedLayer<K, L>) -> &'a Self::Key { &storage.keys[self.pos].key }
	fn step(&mut self, storage: &HashedLayer<K, L>) {

		// look for next valid entry
		self.pos += 1;
		while self.pos < self.bounds.1 && !storage.keys[self.pos].is_some() {
			self.pos += 1;
		}

		if self.valid(storage) {
			let child_lower = storage.keys[self.pos].get_lower();
			let child_upper = storage.keys[self.pos].get_upper();
			self.child.reposition(&storage.vals, child_lower, child_upper);
		}
		else {
			self.pos = self.bounds.1;
		}
	}
	#[inline(never)]
	fn seek(&mut self, storage: &HashedLayer<K, L>, key: &Self::Key) {
		// leap to where the key *should* be, or at least be soon after.
		// let key_hash = key.hashed();
		
		// only update position if shift is large. otherwise leave it alone.
		if self.shift >= MINIMUM_SHIFT {
			let target = (key.hashed().as_u64() >> ((K::Output::bytes() * 8) - self.shift)) as usize;
			self.pos = target;
		}

		// scan forward until we find a valid entry >= (key_hash, key)
		while self.pos < self.bounds.1 && (!storage.keys[self.pos].is_some() || &storage.keys[self.pos].key < key)  {
			self.pos += 1;
		}

		// self.pos should now either 
		//	(i) have self.pos == self.bounds.1 (and be invalid) or 
		//	(ii) point at a valid entry with (entry_hash, entry) >= (key_hash, key).
		if self.valid(storage) {
			self.child.reposition(&storage.vals, storage.keys[self.pos].get_lower(), storage.keys[self.pos].get_upper());
		}
	}
	fn valid(&self, _storage: &HashedLayer<K, L>) -> bool { self.pos < self.bounds.1 }
	fn rewind(&mut self, storage: &HashedLayer<K, L>) {
		self.pos = self.bounds.0;
		if self.valid(storage) {
			self.child.reposition(&storage.vals, storage.keys[self.pos].get_lower(), storage.keys[self.pos].get_upper());			
		}
	}
	fn reposition(&mut self, storage: &HashedLayer<K, L>, lower: usize, upper: usize) {

		// sort out what the shift is.
		// should be just before the first power of two strictly containing (lower, upper].
		self.shift = 0; 
		while upper - lower >= (1 << self.shift) {
			self.shift += 1;
		}
		self.shift -= 1;

		self.bounds = (lower, upper);

		self.pos = lower;	// set self.pos to something valid.
		while self.pos < self.bounds.1 && !storage.keys[self.pos].is_some() {
			self.pos += 1;
		}

		if self.valid(storage) {
			self.child.reposition(&storage.vals, storage.keys[self.pos].get_lower(), storage.keys[self.pos].get_upper());			
		}
	}
}
