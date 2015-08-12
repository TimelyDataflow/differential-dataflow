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

// use std::fmt::Debug;

use iterators::merge::Merge;
use iterators::coalesce::Coalesce;

pub struct RadixAccumulator<K, V> {
    accumulators: Vec<Accumulator<K, V>>,
}

impl<K: Ord, V: Ord> RadixAccumulator<K, V> {
    pub fn push<F: Fn(&K)->u64>(&mut self, key: K, val: V, wgt: i32, func: &F) {
        self.accumulators[(func(&key) as usize) % 256].push(key, val, wgt, func);
    }
    pub fn done<F: Fn(&K)->u64>(self, func: &F) -> Vec<Option<Compact<K, V>>> {
        self.accumulators.into_iter().map(|x| x.done(func)).collect()
    }
}

/// Maintains a list of `(key, val, wgt)` triples in a compressed form.
pub struct Accumulator<K, V> {
    sorted: Vec<Compact<K, V>>,
    staged: Vec<((K, V), i32)>,
}

impl<K: Ord, V: Ord> Accumulator<K,V> {
    /// Constructs a new `Accumulator` with a default capacity of zero.
    pub fn new() -> Accumulator<K,V> {
        Accumulator {
            sorted: Vec::new(),
            staged: Vec::new(),
        }
    }
    /// Constructs a new `Accumulator` with a supplied capacity.
    pub fn with_capacity(size: usize) -> Accumulator<K, V> {
        Accumulator {
            sorted: Vec::new(),
            staged: Vec::with_capacity(size),
        }
    }
    /// Finalizes compression and returns a single `Compact` compressed accumulation.
    pub fn done<F: Fn(&K)->u64>(mut self, func: &F) -> Option<Compact<K, V>> {
        self.compress(func);
        self.merge(func);
        self.sorted.pop()
    }
    /// Adds a new element to the accumulation.
    #[inline]
    pub fn push<F: Fn(&K)->u64>(&mut self, key: K, val: V, wgt: i32, func: &F) {
        self.staged.push(((key, val), wgt));

        // TODO : smarter tests exist based on sizes of K, V, etc.
        if self.staged.len() == self.staged.capacity() {
            self.compress(func)
        }
    }

    #[inline(never)]
    fn sort_staged<F: Fn(&K)->u64>(&mut self, func: &F) {
        self.staged.sort_by(|&((ref k1, ref v1), _),&((ref k2, ref v2), _)| (func(k1), k1, v1).cmp(&(func(k2), k2, v2)));
    }

    fn compress<F: Fn(&K)->u64>(&mut self, func: &F) {
        let len = self.staged.len(); // number of elements we are compressing

        self.sort_staged(func);
        let mut next = Compact::new(0,0,0);
        next.from(::std::mem::replace(&mut self.staged, Vec::new()).into_iter().coalesce());
        let now = next.size() / ::std::mem::size_of::<((K, V), i32)>();
        self.sorted.push(next);

        // println!("compressed from {} to {}", len, now);

        // if we are getting close, might as well merge
        if now + 1024 > len {
            self.merge(func);
            if self.sorted.len() > 0 && self.sorted[0].size() / ::std::mem::size_of::<((K, V), i32)>() > self.staged.capacity() {
                self.staged = Vec::with_capacity(self.sorted[0].size() / ::std::mem::size_of::<((K, V), i32)>());
            }
            println!("merged; new capacity: {}", self.staged.capacity());
        }
        else {
            self.staged = Vec::with_capacity(len - now)
        }
    }

    #[inline(never)]
    fn merge<F: Fn(&K)->u64>(&mut self, func: &F) {
        let result = Compact::merge(::std::mem::replace(&mut self.sorted, Vec::new()), func);
        if result.size() > 0 {
            self.sorted.push(result);
        }
    }
}

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
    pub vals: Vec<V>,
    /// A list of `(wgt,cnt)` pairs indicating a weight and its number of repetitions.
    ///
    /// As with `keys` above, this may have its encoding change, especially given that we know the
    /// type of the sequence and can avoid a second array. If the format changes, the type must
    /// also change, so it shouldn't silently break anything.
    pub wgts: Vec<(i32,u32)>,
}

impl<K: Ord, V: Ord> Compact<K, V> {
    /// Constructs a new `Compact` with indicated initial capacities.
    ///
    /// Most operations with `Compact` eventually shrink the amount of memory to fit whatever they
    /// have used, so the main concern here is to avoid grossly over-allocating. Typically, these
    /// structs are created in a transient compaction step and not maintained open, meaning we can
    /// afford to be a bit sloppy.
    pub fn new(k: usize, v: usize, w: usize) -> Compact<K, V> {
        Compact {
            keys: Vec::with_capacity(k),
            cnts: Vec::with_capacity(k),
            vals: Vec::with_capacity(v),
            wgts: Vec::with_capacity(w),
        }
    }
    /// Reports the size in bytes, used elsewhere to determine how much space we should use for
    /// buffering uncompressed elements.
    pub fn size(&self) -> usize {
        self.keys.len() * ::std::mem::size_of::<K>() +
        self.cnts.len() * 4 +
        self.vals.len() * ::std::mem::size_of::<V>() +
        self.wgts.len() * 8
    }

    /// Populates the `Compact` from an iterator of ordered `(key, val, wgt)` triples.
    ///
    /// The `Compact` does not know about the ordering, only that it should look for repetitions of
    /// in the sequences of `key` and `wgt`.
    pub fn from<I: Iterator<Item=((K, V), i32)>>(&mut self, mut iterator: I) {
        self.keys.clear();
        self.cnts.clear();
        self.vals.clear();
        self.wgts.clear();

        // populate a new `Compact` with merged, coalesced data.
        if let Some(((mut old_key, val), mut old_wgt)) = iterator.next() {
            let mut key_cnt = 1;
            let mut wgt_cnt = 1;

            // always stash the val
            self.vals.push(val);

            for ((key, val), wgt) in iterator {

                // always stash the val
                self.vals.push(val);

                // if the key or weight has changed, stash the weight.
                if old_key != key || old_wgt != wgt {
                    // stash wgt, using run-length encoding
                    self.wgts.push((old_wgt, wgt_cnt));
                    old_wgt = wgt;
                    wgt_cnt = 0;
                }

                wgt_cnt += 1;

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
            self.wgts.push((old_wgt, wgt_cnt));
        }

        self.keys.shrink_to_fit();
        self.cnts.shrink_to_fit();
        self.vals.shrink_to_fit();
        self.wgts.shrink_to_fit();
    }

    /// Merges a set of `Compact` into a new single `Compact`.
    ///
    /// This method uses the `Ord` property of `K` and `V` to merge the elements into a single
    /// `Compact` whose `keys` and `wgts` are ideally further compressed. It is likely that this
    /// method will need to be expanded to take a function ordering `K`, so that the radix-based
    /// sorting and ordering used by the ultimate consumers can be implemented.
    pub fn merge<F: Fn(&K)->u64>(columns: Vec<Compact<K, V>>, func: &F) -> Compact<K, V> {

        // storage for our results. hard to estimate size, other than the max of what we have now.
        let mut result = Compact::new(0,0,0);

        let mut keyvals = vec![];

        for column in columns {
            let mut keycnt = column.keys.into_iter().zip(column.cnts.into_iter());
            if let Some((key, cnt)) = keycnt.next() {
                keyvals.push(((func(&key), key, cnt), keycnt, column.vals.into_iter().zip(column.wgts.into_iter().flat_map(|(w,c)| ::std::iter::repeat(w).take(c as usize)))));
            }
        }

        keyvals.sort_by(|&(ref kx,_,_),&(ref ky,_,_)| kx.cmp(&ky));

        while keyvals.len() > 0 {

            // determine the prefix of equivalently small keys
            let mut min_keys = 1;
            while min_keys < keyvals.len() && (keyvals[min_keys].0).0 == (keyvals[0].0).0 {
                min_keys += 1;
            }

            // this will tell us if we added any values after coalescing
            let len = result.vals.len();

            // borrow the first few elements of keyvals, taking the right number of (val,wgt) pairs
            // and feeding them in to a merge and coalesce.
            {
                let mut iter = keyvals[0..min_keys]
                                   .iter_mut()
                                   .map(|&mut ((_,_,c), _, ref mut v)| v.by_ref().take(c as usize))
                                   .merge()
                                   .coalesce();

                // easy pattern to stash a wgt for RLE
                if let Some((val, mut old_wgt)) = iter.next() {
                    result.vals.push(val);
                    let mut wgt_cnt = 1;

                    for (val, wgt) in iter {
                        result.vals.push(val);
                        if wgt != old_wgt {
                            result.wgts.push((old_wgt, wgt_cnt));
                            old_wgt = wgt;
                            wgt_cnt = 0;
                        }
                        wgt_cnt += 1;
                    }
                    result.wgts.push((old_wgt, wgt_cnt));
                }
            }

            // pop the keys from each participating iterator,
            // recover a key to push into result.keys...
            let mut key = None;
            for i in 0..min_keys {
                let index = min_keys - i - 1;
                key = Some(
                    // if we can advance, do so but worry about maintaining order.
                    if let Some((key, cnt)) = keyvals[index].1.next() {
                        let result = ::std::mem::replace(&mut keyvals[index].0, (func(&key),key,cnt)).1;

                        // walk forward from next, swapping out-of-order keys.
                        let mut next = index;
                        while next + 1 < keyvals.len() && keyvals[next].0 > keyvals[next+1].0 {
                            keyvals.swap(next, next+1);
                            next += 1;
                        }

                        result
                    }
                    else {
                        // if we have no more elements, remove the iterators.
                        (keyvals.remove(index).0).1
                    }
                );

            }

            // if we added at least one val then stash the key and count.
            if result.vals.len() > len {
                result.keys.push(key.unwrap());
                result.cnts.push((result.vals.len() - len) as u32);
            }
        }

        result.keys.shrink_to_fit();
        result.cnts.shrink_to_fit();
        result.vals.shrink_to_fit();
        result.wgts.shrink_to_fit();

        result
    }
}

#[cfg(test)]
mod test {

    use super::{Accumulator};

    #[test]
    fn new() {
        let accum = Accumulator::<(), ()>::new();
        assert!(accum.done(&|_| 0).is_none());
    }

    #[test]
    fn cancel() {
        let mut accum = Accumulator::with_capacity(256);

        accum.push(1, 2,  3, &|&x| x);
        accum.push(1, 2, -3, &|&x| x);

        assert!(accum.done(&|&x| x).is_none());
    }

    #[test]
    fn load() {
        let mut accum = Accumulator::with_capacity(256);

        for key in 0..10 {
            for val in 0..10 {
                accum.push(key, key + val, 1, &|&x| x);
            }
        }

        if let Some(compact) = accum.done(&|&x| x) {
            println!("{:?}", compact.keys);
            println!("{:?}", compact.cnts);
            println!("{:?}", compact.vals);
            println!("{:?}", compact.wgts);
            assert_eq!(compact.keys, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
            assert_eq!(compact.cnts, vec![10; 10]);
        }
        else {
            // should have been Some(_)
            assert!(false);
        }
    }
    #[test]
    fn load_compact() {
        let mut accum = Accumulator::with_capacity(256);

        for key in 0..10 {
            for val in 0..10 {
                accum.push(key + val, key + val, 1, &|&x| x);
            }
        }

        if let Some(compact) = accum.done(&|&x| x) {
            println!("{:?}", compact.keys);
            println!("{:?}", compact.cnts);
            println!("{:?}", compact.vals);
            println!("{:?}", compact.wgts);
            assert_eq!(compact.keys, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]);
            assert_eq!(compact.cnts, vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  1,  1,  1,  1,  1,  1,  1,  1,  1]);
            assert_eq!(compact.vals, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]);
            assert_eq!(compact.wgts, vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1), (10, 1),
                                            (9, 1),  (8, 1),  (7, 1),  (6, 1),  (5, 1),  (4, 1),  (3, 1),  (2, 1), (1, 1)]);
        }
        else {
            // should have been Some(_)
            assert!(false);
        }
    }
}
