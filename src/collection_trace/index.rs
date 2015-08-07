use std::cmp::Ordering;
use std::mem;
use std::fmt::Debug;

use timely::drain::DrainExt;

use iterators::merge::Merge;


/// An ordered collection of (K,V) pairs, which from an iterator over Ks can produce &mut V entries
/// even for Ks that do not yet exist (it will add them).

/*

When iterating over the elements, it should be "easy" to merge a few ordered lists of geometric sizes
and return references when found, and mint new items and add to a list when not. At the end, we should
have an ordered list of new additions, and this can be merged with the existing lists free of cursors
into everything.

*/

pub trait Index {
    type Key;
    type Value;

    fn for_each<F: FnMut(&Self::Key, &mut Self::Value)>(&mut self, &mut Vec<Self::Key>, logic: F);

    // fn seek<'a, 'b>(&'a mut self, key: &'b Self::Key) -> &'a mut Self::Value;
}

pub struct OrdIndex<K, V> {
    kvs: Vec<Vec<(K,V)>>,      // geometrically sized ordered key-value lists.
}


impl<K: Ord+Clone+Debug, V: Ord+Default+Debug> Index for OrdIndex<K, V> {
    type Key = K;
    type Value = V;

    /// finds the value of a key, or mints a new pair if absent.
    fn for_each<F>(&mut self, keys: &mut Vec<Self::Key>, mut logic: F)
    where F: FnMut(&Self::Key, &mut Self::Value) {

        for kvs in &mut self.kvs {
            // compare keys.len() and kvs.len()
            if keys.len() < kvs.len() / 4 {
                // gallop across self.kvs
                let mut cursor = 0;

                keys.retain(|value| {
                    if cursor < kvs.len() && &kvs[cursor].0 < value {
                        let mut step = 1;
                        while cursor + step < kvs.len() && &kvs[cursor + step].0 < value {
                            cursor += step;
                            step = step << 1;
                        }

                        step = step >> 1;
                        while step > 0 {
                            if cursor + step < kvs.len() && &kvs[cursor + step].0 < value {
                                cursor += step;
                            }
                            step = step >> 1;
                        }

                        cursor += 1;
                    }
                    if cursor < kvs.len() && &kvs[cursor].0 == value {
                        logic(value, &mut kvs[cursor].1);
                        false
                    }
                    else { true }
                });
            }
            else if kvs.len() < keys.len() / 4 {
                // gallop across keys. not sure how to do.
                // i guess filtering isn't mandatory, since no duplicates.

                let mut cursor = 0;
                for &mut (ref k, ref mut v) in kvs {
                    if cursor < keys.len() && &keys[cursor] < k {
                        let mut step = 1;
                        while cursor + step < keys.len() && &keys[cursor + step] < k {
                            cursor += step;
                            step = step << 1;
                        }

                        step = step >> 1;
                        while step > 0 {
                            if cursor + step < keys.len() && &keys[cursor + step] < k {
                                cursor += step;
                            }
                            step = step >> 1;
                        }

                        cursor += 1;
                    }
                    if cursor < keys.len() && &keys[cursor] == k {
                        logic(k, v);
                    }
                }
            }
            else {
                // move linearly; run logic on hits, retain on misses.
                let mut cursor = 0;
                keys.retain(|value| {
                    while cursor < kvs.len() && &kvs[cursor].0 < value { cursor += 1; }
                    if cursor < kvs.len() && &kvs[cursor].0 == value {
                        logic(value, &mut kvs[cursor].1);
                        false
                    }
                    else { true }
                });
            }
        }

        if keys.len() > 0 {
            let mut size = keys.len();

            let mut new = Vec::<(K,V)>::with_capacity(size);
            for key in keys.drain_temp() {
                let mut value = Default::default();
                logic(&key, &mut value);
                new.push((key, value));
            }

            let mut temp = vec![new];
            let mut len = self.kvs.len();
            while len > 0 && self.kvs[len - 1].len() / 2 < size {
                size += self.kvs[len - 1].len();
                temp.push(self.kvs.pop().unwrap());
                len -= 1;
            }

            let mut result = Vec::with_capacity(size);
            result.extend(temp.into_iter().map(|x| x.into_iter()).merge());
            self.kvs.push(result);
        }
    }
}

impl<K: Ord, V: Ord> OrdIndex<K, V> {
    pub fn new() -> Self {
        OrdIndex {
            kvs: Vec::new(),
        }
    }
}
