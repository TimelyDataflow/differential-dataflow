// use std::cmp::Ordering;
// use std::mem;
use std::fmt::Debug;

// use timely::drain::DrainExt;

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
    fn find_each<F: FnMut(&Self::Key, &mut Self::Value)>(&mut self, &mut Vec<Self::Key>, logic: F);

    // fn seek<'a, 'b>(&'a mut self, key: &'b Self::Key) -> &'a mut Self::Value;
}

pub struct OrdIndex<K, V> {
    // kvs: Vec<Vec<(K,V)>>,      // geometrically sized ordered key-value lists.
    keys: Vec<Vec<K>>,
    vals: Vec<Vec<V>>,
}


impl<K: Ord+Clone+Debug, V: Ord+Default+Debug> Index for OrdIndex<K, V> {
    type Key = K;
    type Value = V;

    #[inline(never)]
    fn find_each<F: FnMut(&K, &mut V)>(&mut self, keys: &mut Vec<K>, mut logic: F) {

        for (ks, vs) in self.keys.iter_mut().zip(self.vals.iter_mut()) {
            // compare keys.len() and kvs.len()
            if keys.len() < ks.len() / 8 {
                OrdIndex::gallop_self(keys, ks, vs, &mut logic);
            }
            else if ks.len() < keys.len() / 4 {
                OrdIndex::gallop_other(keys, ks, vs, &mut logic);
            }
            else {
                OrdIndex::scan(keys, ks, vs, &mut logic);
            }
        }
    }

    /// finds the value of a key, or mints a new pair if absent.
    #[inline(never)]
    fn for_each<F: FnMut(&K, &mut V)>(&mut self, keys: &mut Vec<K>, mut logic: F) {

        for (ks, vs) in self.keys.iter_mut().zip(self.vals.iter_mut()) {
            // compare keys.len() and kvs.len()
            if keys.len() < ks.len() / 4 {
                OrdIndex::gallop_self(keys, ks, vs, &mut logic);
            }
            else if ks.len() < keys.len() / 4 {
                OrdIndex::gallop_other(keys, ks, vs, &mut logic);
            }
            else {
                OrdIndex::scan(keys, ks, vs, &mut logic);
            }
        }

        if keys.len() > 0 {
            let mut size = keys.len();

            let mut new = Vec::with_capacity(size);
            for key in keys.iter() {
                let mut value = Default::default();
                logic(&key, &mut value);
                new.push(value);
            }

            let keys = ::std::mem::replace(keys, Vec::new());

            let mut temp = vec![(keys, new)];
            let mut len = self.keys.len();
            while len > 0 && self.keys[len - 1].len() / 2 < size {
                size += self.keys[len - 1].len();
                temp.push((self.keys.pop().unwrap(), self.vals.pop().unwrap()));
                len -= 1;
            }

            if temp.len() > 1 {
                let mut keys_result = Vec::with_capacity(size);
                let mut vals_result = Vec::with_capacity(size);
                for (key, val) in temp.into_iter().map(|(x,y)| x.into_iter().zip(y.into_iter())).merge() {
                    keys_result.push(key);
                    vals_result.push(val);
                }
                self.keys.push(keys_result);
                self.vals.push(vals_result);
            }
            else {
                let (k, v) = temp.pop().unwrap();
                self.keys.push(k);
                self.vals.push(v);
            }
        }
    }
}

impl<K: Ord, V> OrdIndex<K, V> {
    pub fn new() -> Self {
        OrdIndex {
            keys: Vec::new(),
            vals: Vec::new(),
        }
    }
    #[inline(never)]
    fn _search_self<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F) where F: FnMut(&K, &mut V) {
        keys.retain(|value| {
            let mut hi = ks.len();
            let mut lo = 0;
            while (hi + lo) / 2 > lo {
                if value < &ks[(hi + lo)/2] {
                    hi = (hi + lo) / 2;
                }
                else {
                    lo = (hi + lo) / 2;
                }
            }
            if value == &ks[lo] {
                logic(value, &mut vs[lo]);
                false
                // true
            }
            else { true }
        });
    }
    #[inline(never)]
    fn gallop_self<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F) where F: FnMut(&K, &mut V) {
        // gallop across self.kvs
        let mut cursor = 0;

        let mut step = 1;

        keys.retain(|value| {
            if cursor < ks.len() && &ks[cursor] < value {
                if step < 1 { step = 1; }
                while cursor + step < ks.len() && &ks[cursor + step] < value {
                    cursor += step;
                    step = step << 1;
                }

                step = step >> 1;
                let mut down_step = step;

                while down_step > 0 {
                    if cursor + down_step < ks.len() && &ks[cursor + down_step] < value {
                        cursor += down_step;
                    }
                    down_step = down_step >> 1;
                }

                cursor += 1;
            }
            if cursor < ks.len() && &ks[cursor] == value {
                logic(value, &mut vs[cursor]);
                false
                // true
            }
            else { true }
        });
    }
    #[inline(never)]
    fn gallop_other<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F) where F: FnMut(&K, &mut V) {
        // gallop across keys. not sure how to do.
        // i guess filtering isn't mandatory, since no duplicates.

        let mut cursor = 0;
        for (k, ref mut v) in ks.iter().zip(vs.iter_mut()) {
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
    #[inline(never)]
    fn scan<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F) where F: FnMut(&K, &mut V) {

        // move linearly; run logic on hits, retain on misses.
        let mut cursor = 0;
        keys.retain(|value| {
            while cursor < ks.len() && &ks[cursor] < value { cursor += 1; }
            if cursor < ks.len() && &ks[cursor] == value {
                logic(value, &mut vs[cursor]);
                false
                // true
            }
            else { true }
        });
    }
}
