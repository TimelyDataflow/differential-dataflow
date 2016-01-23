//! Accumulator of `(key, val, wgt)` triples based on sorting and run-length encoding.
//!
//! Differential dataflow operators receive large numbers of `(key, val, wgt)` triples, and must
//! group these records by `key`, and then accumulate the `wgt`s of equal `val` values, discarding
//! those whose weight accumulates to zero.
//!
//! The grouped and accumulated form can be much more compact than a list of triples, by removing
//! the repetition of `key`s, accumulating multiple `wgt`s together, and discarding `val`s whose
//! weight accumulation is zero. Because we are space-sensitive, we would like to be able to
//! maintain received data compactly, rather than have to accumulate the large list of triples and
//! sort it only once complete.
//!
//! This module provides a `Accumulator` structure capable of receiving `(key, val, wgt)` triples and
//! which compacts them as they arrive, maintaining no more than 1.5x the space required for the
//! list of values. It can be configured to have a minimum capacity, so if there is enough space
//! the `Accumulator` structure will only accumulate elements in a list, then sort and coalesce, doing
//! exactly what we would have done in the simple case. As memory gets tighter, it behaves more
//! responsiby.

use iterators::coalesce::Coalesce;

use std::fmt::Debug;

use timely_sort::Unsigned;

/// A compressed representation of the accumulation of `(key, val, wgt)` triples.
// TODO : RLE where a run of two of the same elements means a value in a second array.
// TODO : this would probably improve compressed representations of small sets (those without much
// TODO : key repetition). Compressing these better means we can go longer before merging, which
// TODO : should make most everything else better too.
#[derive(Debug)]
pub struct Compact<K, V> {
    /// An ordered list of the distinct keys.
    pub keys: Vec<K>,
    /// Counts for each key indicating the number of corresponding values in `self.vals`.
    ///
    /// The list is maintained separately in the interest of eventually having run-length coding
    /// treat non-repetitions better.
    pub cnts: Vec<u32>,
    /// A list of values, ordered within each key group.
    pub vals: Vec<(V, i32)>,
}

impl<K: Ord+Debug, V: Ord> Compact<K, V> {
    /// Constructs a new `Compact` with indicated initial capacities.
    ///
    /// Most operations with `Compact` eventually shrink the amount of memory to fit whatever they
    /// have used, so the main concern here is to avoid grossly over-allocating. Typically, these
    /// structs are created in a transient compaction step and not maintained open, meaning we can
    /// afford to be a bit sloppy.
    pub fn new(k: usize, v: usize) -> Compact<K, V> {
        Compact {
            keys: Vec::with_capacity(k),
            cnts: Vec::with_capacity(k),
            vals: Vec::with_capacity(v),
            // wgts: Vec::with_capacity(w),
        }
    }
    /// Reports the size in bytes, used elsewhere to determine how much space we should use for
    /// buffering uncompressed elements.
    pub fn size(&self) -> usize {
        self.keys.len() * ::std::mem::size_of::<K>() +
        self.cnts.len() * 4 +
        self.vals.len() * ::std::mem::size_of::<(V,i32)>()
    }

    /// Populates the `Compact` from an iterator of ordered `(key, val, wgt)` triples.
    ///
    /// The `Compact` does not know about the ordering, only that it should look for repetitions of
    /// in the sequences of `key` and `wgt`.
    // #[inline(never)]
    pub fn extend<I: Iterator<Item=((K, V), i32)>>(&mut self, mut iterator: I) {

        // populate a new `Compact` with merged, coalesced data.
        if let Some(((mut old_key, val), wgt)) = iterator.next() {

            let mut key_cnt = 1;

            // always stash the val
            self.vals.push((val, wgt));

            for ((key, val), wgt) in iterator {

                // always stash the val
                self.vals.push((val,wgt));

                // if the key has changed, stash the key
                if old_key != key {
                    self.keys.push(old_key);
                    self.cnts.push(key_cnt);
                    old_key = key;
                    key_cnt = 0;
                }

                key_cnt += 1;
            }

            self.keys.push(old_key);
            self.cnts.push(key_cnt);
        }
    }

    pub fn extend_by(&mut self, buffer: &mut Vec<((K, V), i32)>) {

        // coalesce things
        let mut cursor = 0;
        for index in 1 .. buffer.len() {
            if buffer[cursor].0 == buffer[index].0 {
                buffer[cursor].1 += buffer[index].1;
            }
            else {
                if buffer[cursor].1 != 0 {
                    cursor += 1;
                }
                buffer.swap(cursor, index);
            }
        }
        if buffer[cursor].1 != 0 {
            cursor += 1;
        }
        buffer.truncate(cursor);

        let mut iter = buffer.drain(..);
        if let Some(((key1,val1),wgt1)) = iter.next() {

            let mut prev_len = self.vals.len();

            self.keys.push(key1);
            self.vals.push((val1, wgt1));

            for ((key, val), wgt) in iter {

                // if the key has changed, stash the key
                if self.keys[self.keys.len() - 1] != key {
                    self.keys.push(key);
                    self.cnts.push((self.vals.len() - prev_len) as u32);
                    prev_len = self.vals.len();
                }

                // always stash the val
                self.vals.push((val,wgt));
            }

            self.cnts.push((self.vals.len() - prev_len) as u32);
        }
    }

    // #[inline(never)]
    pub fn from_radix<U: Unsigned+Default, F: Fn(&K)->U>(source: &mut Vec<Vec<((K,V),i32)>>, function: &F) -> Option<Compact<K,V>> {

        let mut size = 0;
        for list in source.iter() {
            size += list.len();
        }

        let mut result = Compact::new(size,size);
        let mut buffer = vec![];

        let mut current = Default::default();
        
        for ((key, val), wgt) in source.iter_mut().flat_map(|x| x.drain(..)) {
            let hash = function(&key);
            if buffer.len() > 0 && hash != current {
                // if hash < current { println!("  radix sort error? {} < {}", hash, current); }
                buffer.sort_by(|x: &((K,V),i32),y: &((K,V),i32)| x.0.cmp(&y.0));
        
                // result.extend(buffer.drain(..).coalesce());
                result.extend_by(&mut buffer);
            }
            buffer.push(((key,val),wgt));
            current = hash;
        }
        
        if buffer.len() > 0 {
            // hsort_by(&mut buffer, &|x: &((K,V),i32)| &x.0);
            buffer.sort_by(|x: &((K,V),i32),y: &((K,V),i32)| x.0.cmp(&y.0));
            result.extend(buffer.drain(..).coalesce());
        }

        if result.vals.len() > 0 {
            result.keys.shrink_to_fit();
            result.cnts.shrink_to_fit();
            result.vals.shrink_to_fit();

            Some(result)
        }
        else {
            None
        }
    }

    pub fn session<'a>(&'a mut self) -> CompactSession<'a, K, V> {
        CompactSession::new(self)
    }

    pub fn push<I: Iterator<Item=(V, i32)>>(&mut self, key: K, iterator: I) {
        let mut session = self.session();
        for (val, wgt) in iterator {
            session.push(val, wgt);
        }
        session.done(key);
    }
}

pub struct CompactSession<'a, K: 'a, V: 'a> {
    compact: &'a mut Compact<K, V>,
    len: usize,
}

impl<'a, K: 'a, V: 'a> CompactSession<'a, K, V> {
    pub fn new(compact: &'a mut Compact<K, V>) -> CompactSession<'a, K, V> {
        let len = compact.vals.len();
        CompactSession {
            compact: compact,
            len: len,
        }
    }
    #[inline]
    pub fn push(&mut self, val: V, wgt: i32) {
        self.compact.vals.push((val,wgt));
    }
    pub fn done(self, key: K) {
        if self.compact.vals.len() > self.len {
            self.compact.keys.push(key);
            self.compact.cnts.push((self.compact.vals.len() - self.len) as u32);
        }
    }
}
