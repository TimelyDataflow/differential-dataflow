// use std::cmp::Ordering;
// use std::mem;
use std::fmt::Debug;

// use timely::drain::DrainExt;

use iterators::merge::Merge;


/// An ordered collection of (K,V) pairs, which from an iterator over Ks can produce &mut V entries
/// even for Ks that do not yet exist (it will add them).

/*

For a list of k keys, the strategy is:

1.  Gallop across any lists of size >> k, removing matching elements using `retain`.
    It may make sense to fuse gallops to avoid calling `retain` (and moving data) too often.
    On the other hand, we are already doing (more than) linear work in k for each list. Later.

2.  For lists of size ~= k, walk through both, removing matching elements using retain.

3.  For lists of size << k, gallop through the *keys*, moving state out of the lists on matches.
    This avoids calling `retain` on the possibly much larger keys, moving lots of data many times.

*/



pub trait Index {
    type Key;
    type Value;

    /// Applies `logic` to each matching (key,val) pair.
    fn for_each<F>(&mut self, &mut Vec<Self::Key>, logic: F)
        where F: FnMut(&Self::Key, &mut Self::Value);

    /// Applies `logic` to each matching (key,val) pair, and `init` to add values for absent keys.
    fn for_each_or<F, G>(&mut self, &mut Vec<Self::Key>, logic: F, init: G)
        where F: FnMut(&Self::Key, &mut Self::Value),
              G: FnMut(&Self::Key)->Self::Value;


    // fn seek<'a, 'b>(&'a mut self, key: &'b Self::Key) -> &'a mut Self::Value;
}

/// An index from `key: K` to `value: V` represented as ordered lists of keys.
///
/// Data are stored in two separate lists of lists, each of which
pub struct OrdIndex<K, V> {
    keys: Vec<Vec<K>>,
    vals: Vec<Vec<V>>,
}


impl<K: Ord+Clone+Debug, V: Ord+Default+Debug> Index for OrdIndex<K, V> {
    type Key = K;
    type Value = V;

    #[inline(never)]
    fn for_each<F>(&mut self, keys: &mut Vec<K>, mut logic: F)
        where F: FnMut(&K, &mut V)
    {

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
    fn for_each_or<F, G>(&mut self, keys: &mut Vec<K>, mut logic: F, mut init: G)
        where F: FnMut(&K, &mut V),
              G: FnMut(&K)->V,
    {

        // filter keys by each element of self.keys larger than it.
        let mut index = 0;
        while index < self.keys.len() && keys.len() < self.keys[index].len() * 4 {

            if keys.len() < self.keys[index].len() / 4 {
                OrdIndex::gallop_self(keys, &mut self.keys[index], &mut self.vals[index], &mut logic);
            }
            else {
                OrdIndex::scan(keys, &mut self.keys[index], &mut self.vals[index], &mut logic);
            }

            index += 1;
        }

        // we sort-of know that as keys.len() >= 4 * self.keys[index].len(), there must be at least
        // some number of new keys being added. we don't really know how many, though, so it seems
        // a bit tricky to plan the merge out ahead of time.
        //
        // I think the main goal is to provide insurance against bad performance for very small
        // remaining keys, and a large query keys. We are going to do a linear amount of work
        // with the vals initialization, but this could be pretty fast (setting things to zero).

        // all remaining self.keys are [much?] shorter than keys.
        // merge remaining keys and walk through keys either initializing or moving state into vals
        if keys.len() > 0 {

            // we are going to end up with exactly this many vals.
            let mut vals = Vec::with_capacity(keys.len());

            // merge remaining (determined by index) (self.keys, self.vals).
            let mut to_merge = vec![];
            for _ in index .. self.keys.len() {
                to_merge.push((self.keys.pop().unwrap(), self.vals.pop().unwrap()));
            }

            // destinations for merged but not found keys/vals
            let mut merge_size = 0;
            for len in to_merge.iter().map(|x| x.0.len()) {
                merge_size += len;
            }

            let mut merge_keys = Vec::with_capacity(merge_size);
            let mut merge_vals = Vec::with_capacity(merge_size);

            let mut keys_index = 0;
            let merged = to_merge.into_iter().map(|(k,v)| k.into_iter().zip(v.into_iter())).merge();
            for (key, mut val) in merged {
                // advance keys_index
                // TODO : we could gallop in this first case, if key comparison is expensive.
                while keys[keys_index] < key {
                    vals.push(init(&keys[keys_index]));
                    keys_index += 1;
                }
                if keys[keys_index] == key {
                    logic(&keys[keys_index], &mut val);
                    vals.push(val);
                    keys_index += 1;
                }
                else {
                    merge_keys.push(key);
                    merge_vals.push(val);
                }
            }

            while keys_index < keys.len() {
                vals.push(init(&keys[keys_index]));
                keys_index += 1;
            }

            self.keys.push(::std::mem::replace(keys, Vec::new()));
            self.vals.push(vals);

            if merge_keys.len() > 0 {
                // merge_keys and merge_vals may be longer than they need
                // TODO : determine if this actually re-allocates, or not.
                merge_keys.shrink_to_fit();
                merge_vals.shrink_to_fit();

                self.keys.push(merge_keys);
                self.vals.push(merge_vals);
            }
            
            self.merge_lists();
        }
    }
}

impl<K: Ord, V: Ord> OrdIndex<K, V> {
    pub fn new() -> Self {
        OrdIndex {
            keys: Vec::new(),
            vals: Vec::new(),
        }
    }
    pub fn size(&self) -> usize {
        let mut size = 0;
        for (index, len) in self.keys.iter().map(|x| x.len()).enumerate() {
            println!("len[{}]: {}", index, len);
            size += len;
        }
        size
    }
    #[inline(never)]
    fn merge_lists(&mut self) {

        // put the keys and vals in order by length.
        // keys and vals each have the same length, so can be sorted independently (stable sort!)
        self.keys.sort_by(|x,y| y.len().cmp(&x.len()));
        self.vals.sort_by(|x,y| y.len().cmp(&x.len()));

        // we want to maintain the invariant that each element is at least as long as everything
        // that comes after it added together.
        let mut total = 0;
        let mut index = self.keys.len();
        for i in 0..self.keys.len() {
            if self.keys[self.keys.len() - i - 1].len() < total {
                index = self.keys.len() - i - 1;
            }
            total += self.keys[self.keys.len() - i - 1].len();
        }

        // index should now contain the position we need to merge from.
        // index should *never* be self.keys.len() - 1, so we should never do singleton merges.
        assert!(index != self.keys.len() - 1);
        if index < self.keys.len() {
            let mut temp = vec![];
            for _ in index..self.keys.len() {
                temp.push((self.keys.pop().unwrap(), self.vals.pop().unwrap()));
            }

            // allocate exactly the right amount of space and drain the merged results.
            let mut size = 0;
            for len in temp.iter().map(|x| x.0.len()) {
                size += len;
            }
            let mut merged_keys = Vec::with_capacity(size);
            let mut merged_vals = Vec::with_capacity(size);
            for (key, val) in temp.into_iter().map(|(x,y)| x.into_iter().zip(y.into_iter())).merge() {
                merged_keys.push(key);
                merged_vals.push(val);
            }

            self.keys.push(merged_keys);
            self.vals.push(merged_vals);
        }
    }

    #[inline(never)]
    fn _search_self<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F)
        where F: FnMut(&K, &mut V)
    {
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
                // false
                true
            }
            else { true }
        });
    }
    #[inline(never)]
    fn gallop_self<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F)
        where F: FnMut(&K, &mut V)
    {
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
                // false
                true
            }
            else { true }
        });
    }
    #[inline(never)]
    fn gallop_other<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F)
        where F: FnMut(&K, &mut V)
    {
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
    fn scan<F>(keys: &mut Vec<K>, ks: &mut Vec<K>, vs: &mut Vec<V>, logic: &mut F)
        where F: FnMut(&K, &mut V)
    {

        // move linearly; run logic on hits, retain on misses.
        let mut cursor = 0;
        keys.retain(|value| {
            while cursor < ks.len() && &ks[cursor] < value { cursor += 1; }
            if cursor < ks.len() && &ks[cursor] == value {
                logic(value, &mut vs[cursor]);
                // false
                true
            }
            else { true }
        });
    }
}
