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

// use iterators::merge::MergeUsing;
use iterators::coalesce::Coalesce;

use timely::drain::DrainExt;
use std::fmt::Debug;

use radix_sort::Unsigned;

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

impl<K: Ord+Debug, V: Ord> Compact<K, V> {
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
    #[inline(never)]
    pub fn extend<I: Iterator<Item=((K, V), i32)>>(&mut self, mut iterator: I) {

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
    }

    #[inline(never)]
    pub fn from_radix<U: Unsigned+Default, F: Fn(&K)->U>(source: Vec<Vec<((K,V),i32)>>, function: &F) -> Option<Compact<K,V>> {

        let mut size = 0;
        for list in &source {
            size += list.len();
        }

        let mut result = Compact::new(size,size,size);
        let mut buffer = vec![];
        let mut current = Default::default();

        for ((key, val), wgt) in source.into_iter().flat_map(|x| x.into_iter()) {
            let hash = function(&key);
            if buffer.len() > 0 && hash != current {
                // if hash < current { println!("  radix sort error? {} < {}", hash, current); }
                // hsort_by(&mut buffer, &|x: &((K,V),i32)| &x.0);
                buffer.sort_by(|x: &((K,V),i32),y: &((K,V),i32)| x.0.cmp(&y.0));
                result.extend(buffer.drain_temp().coalesce());
            }
            buffer.push(((key,val),wgt));
            current = hash;
        }

        if buffer.len() > 0 {
            // hsort_by(&mut buffer, &|x: &((K,V),i32)| &x.0);
            buffer.sort_by(|x: &((K,V),i32),y: &((K,V),i32)| x.0.cmp(&y.0));
            result.extend(buffer.drain_temp().coalesce());
        }

        if result.vals.len() > 0 {
            result.keys.shrink_to_fit();
            result.cnts.shrink_to_fit();
            result.vals.shrink_to_fit();
            result.wgts.shrink_to_fit();

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
    //
    // /// Merges a set of `Compact` into a new single `Compact`.
    // ///
    // /// This method uses the `Ord` property of `K` and `V` to merge the elements into a single
    // /// `Compact` whose `keys` and `wgts` are ideally further compressed. It is likely that this
    // /// method will need to be expanded to take a function ordering `K`, so that the radix-based
    // /// sorting and ordering used by the ultimate consumers can be implemented.
    // pub fn merge<F: Fn(&K)->u64>(columns: Vec<Compact<K, V>>, func: &F) -> Compact<K, V> {
    //
    //     // storage for our results. hard to estimate size, other than the max of what we have now.
    //
    //     let mut key_cnt = 0;
    //     let mut val_cnt = 0;
    //
    //     for compact in &columns {
    //         key_cnt += compact.keys.len();
    //         val_cnt += compact.vals.len();
    //     }
    //
    //     let mut result = Compact::new(key_cnt,val_cnt,val_cnt);
    //
    //     let mut keyvals = vec![];
    //
    //     for column in columns {
    //         let mut keycnt = column.keys.into_iter().zip(column.cnts.into_iter());
    //         if let Some((key, cnt)) = keycnt.next() {
    //             keyvals.push(((func(&key), key, cnt), keycnt, column.vals.into_iter()
    //                 .zip(column.wgts.into_iter()
    //                     .flat_map(|(w,c)| ::std::iter::repeat(w).take(c as usize)))));
    //         }
    //     }
    //
    //     keyvals.sort_by(|&(ref kx,_,_),&(ref ky,_,_)| kx.cmp(&ky));
    //
    //     // TODO : this is embarassing. I am basically guessing at the size of the underlying
    //     // TODO : iterator, which I think is a pair of "remaining count" and "iterator ptr".
    //     let mut heap = Vec::<((V,i32),(usize, usize))>::new();
    //
    //     while keyvals.len() > 0 {
    //
    //         // determine the prefix of equivalently small keys
    //         let mut min_keys = 1;
    //         while min_keys < keyvals.len() && (keyvals[min_keys].0).0 == (keyvals[0].0).0 {
    //             min_keys += 1;
    //         }
    //
    //         let mut session = result.session();
    //         for (val, wgt) in keyvals[0..min_keys].iter_mut()
    //                                               .map(|&mut ((_,_,c), _, ref mut v)| v.by_ref().take(c as usize))
    //                                               .merge_using(unsafe { ::std::mem::transmute(&mut heap) })
    //                                             // .merge()
    //                                               .coalesce() {
    //
    //                                                 //   assert!(session.compact.vals.capacity() > session.compact.vals.len());
    //                                                 //   assert!(session.compact.wgts.capacity() > session.compact.wgts.len());
    //
    //             // println!("pushing (_, {})", wgt);
    //             session.push(val, wgt);
    //         }
    //
    //         // pop the keys from each participating iterator,
    //         // recover a key to push into result.keys...
    //         // TODO : Try to use a Merge iterator on the (key,cnt) iterator pair?
    //         let mut key = None;
    //         for i in 0..min_keys {
    //             let index = min_keys - i - 1;
    //             key = Some(
    //                 // if we can advance, do so but worry about maintaining order.
    //                 if let Some((key, cnt)) = keyvals[index].1.next() {
    //                     let result = ::std::mem::replace(&mut keyvals[index].0, (func(&key),key,cnt)).1;
    //
    //                     // walk forward from next, swapping out-of-order keys.
    //                     let mut next = index;
    //                     while next + 1 < keyvals.len() && keyvals[next].0 > keyvals[next+1].0 {
    //                         keyvals.swap(next, next+1);
    //                         next += 1;
    //                     }
    //
    //                     result
    //                 }
    //                 else {
    //                     // if we have no more elements, remove the iterators.
    //                     (keyvals.remove(index).0).1
    //                 }
    //             );
    //
    //         }
    //
    //         assert!(session.compact.keys.capacity() > session.compact.keys.len());
    //         assert!(session.compact.cnts.capacity() > session.compact.cnts.len());
    //
    //
    //         session.done(key.unwrap());
    //     }
    //
    //     result
    // }
}

pub struct CompactSession<'a, K: 'a, V: 'a> {
    compact: &'a mut Compact<K, V>,
    len: usize,
    old_wgt: i32,
    wgt_cnt: u32,
}

impl<'a, K: 'a, V: 'a> CompactSession<'a, K, V> {
    pub fn new(compact: &'a mut Compact<K, V>) -> CompactSession<'a, K, V> {
        let len = compact.vals.len();
        CompactSession {
            compact: compact,
            len: len,
            old_wgt: 0,
            wgt_cnt: 0,
        }
    }
    #[inline]
    pub fn push(&mut self, val: V, wgt: i32) {
        self.compact.vals.push(val);
        if wgt != self.old_wgt {
            if self.wgt_cnt > 0 {
                self.compact.wgts.push((self.old_wgt, self.wgt_cnt));
            }
            self.old_wgt = wgt;
            self.wgt_cnt = 0;
        }
        self.wgt_cnt += 1;
    }
    pub fn done(self, key: K) {
        if self.compact.vals.len() > self.len {
            self.compact.keys.push(key);
            self.compact.cnts.push((self.compact.vals.len() - self.len) as u32);
            self.compact.wgts.push((self.old_wgt, self.wgt_cnt));
        }
    }
}
