//! A naive hash-based collection trace.

use std::collections::HashMap;
use std::hash::Hash;

use ::Data;
use lattice::Lattice;
use collection::{Trace, TraceReference};
use collection::compact::Compact;

/// A naive trace implementation based on HashMap.
pub struct HashTrace<K, T, V> {
    // TODO : the hash val could be a (Vec<(T,u32)>, Vec<(V,i32)>) instead...
    map: HashMap<K, Vec<(T, Vec<(V, i32)>)>>,
}

impl<K,V,T> Trace for HashTrace<K, T, V> 
    where 
        K: Data+Hash, 
        V: Data,
        T: Lattice+Clone+'static {

    type Key = K;
    type Index = T;
    type Value = V;

    #[inline(never)]
    fn set_difference(&mut self, time: T, accumulation: Compact<K, V>) {

        // extract the relevant fields
        let keys = accumulation.keys;
        let cnts = accumulation.cnts;
        let vals = accumulation.vals;

        let mut vals_offset = 0;

        // for each key and count ...
        for (key, cnt) in keys.into_iter().zip(cnts.into_iter()) {

            let cnt = cnt as usize;

            // clone the next `cnt` values for the key.
            let new_vals = vals[vals_offset..][..cnt].to_vec();

            // add them and the time to self.map.
            self.map.entry(key)
                    .or_insert(Vec::new())
                    .push((time.clone(), new_vals));

            // advance vals_offset.
            vals_offset += cnt;
        }
    }
}

impl<'a,K,V,T> TraceReference<'a> for HashTrace<K,T,V> 
where K: Data+Hash+'a, 
      V: Data+'a, 
      T: Lattice+Clone+'static {
    type VIterator = DifferenceIterator<'a, V>;
    type TIterator = TraceIterator<'a,T,V>;
    fn trace(&'a self, key: &K) -> Self::TIterator {
        if let Some(val) = self.map.get(key) {
            TraceIterator {
                slice: val
            }
        }
        else {
            TraceIterator { 
                slice: &[] 
            }
        }
    }   
}


/// Enumerates pairs of time `&T` and `DifferenceIterator<V>` of `(&V, i32)` elements.
pub struct TraceIterator<'a, T: 'a, V: 'a> {
    slice: &'a [(T, Vec<(V, i32)>)],
}

impl<'a, T, V> Iterator for TraceIterator<'a, T, V>
where T: Lattice+'a,
      V: Data {

    type Item = (&'a T, DifferenceIterator<'a, V>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.slice.len() > 0 {
            let result = (&self.slice[0].0, DifferenceIterator::new(&self.slice[0].1));
            self.slice = &self.slice[1..];
            Some(result)
        }
        else {
            None
        }
    }
}

impl<'a, T, V> Clone for TraceIterator<'a, T, V> 
where T: Lattice+'a,
      V: Data {
    fn clone(&self) -> TraceIterator<'a, T, V> {
        TraceIterator {
            slice: self.slice
        }
    }
}

/// Enumerates `(&V,i32)` elements of a difference.
///
/// Morally equivalent to a `&[(V,i32)]` slice iterator, except it returns a `(&V,i32)` rather than a `&(V,i32)`.
/// This is important for consolidate and merge.
pub struct DifferenceIterator<'a, V: 'a> {
    vals: &'a [(V, i32)],
}

impl<'a, V: 'a> DifferenceIterator<'a, V> {
    fn new(vals: &'a [(V, i32)]) -> DifferenceIterator<'a, V> {
        DifferenceIterator {
            vals: vals,
        }
    }
}

impl<'a, V: 'a> Clone for DifferenceIterator<'a, V> {
    fn clone(&self) -> Self {
        DifferenceIterator {
            vals: self.vals,
        }
    }
}

impl<'a, V: 'a> Iterator for DifferenceIterator<'a, V> {
    type Item = (&'a V, i32);

    #[inline]
    fn next(&mut self) -> Option<(&'a V, i32)> {
        if self.vals.len() > 0 {
            let result = (&self.vals[0].0, self.vals[0].1);
            self.vals = &self.vals[1..];
            Some(result)
        }
        else {
            None
        }
    }
}