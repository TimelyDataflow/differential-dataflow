use std::fmt::Debug;

use collection_trace::{CollectionTrace, LeastUpperBound, Lookup, Offset};
use collection_trace::collection_trace::CollectionIterator;

pub type Trace<K, T, S, L> = CollectionTrace<K, T, S, L>;

pub struct OperatorTrace<K: Ord, T, S: Ord, R: Ord, L: Lookup<K, Offset>> {
    dst:            Vec<(R,i32)>,
    pub source:     Trace<K, T, S, L>,
    pub result:     Trace<K, T, R, L>,
}

impl<K: Ord+Clone, T:Clone+LeastUpperBound, S: Ord+Clone+Debug, R: Ord+Clone+Debug, L: Lookup<K, Offset>> OperatorTrace<K, T, S, R, L> {
    pub fn new<F: Fn()->L>(lookup: F) -> OperatorTrace<K, T, S, R, L> {
        OperatorTrace {
            source:  Trace::new(lookup()),
            result:  Trace::new(lookup()),
            dst:     Vec::new(),
        }
    }

    pub fn set_collection_from<F: Fn(&K, &mut CollectionIterator<S>, &mut Vec<(R, i32)>)>(&mut self, key: &K, index: &T, logic: F) {
        let mut iter = self.source.get_collection_iterator(key, index);
        if iter.peek().is_some() { logic(key, &mut iter, &mut self.dst); }
        self.result.set_collection(key.clone(), index.clone(), &mut self.dst);
        self.dst.clear();
    }
}

// // special-cased for set_collection.
// fn _sum<V: Ord+Clone>(mut a: &[(V, i32)], mut b: &[(V, i32)], target: &mut Vec<(V, i32)>) {
//     while a.len() > 0 && b.len() > 0 {
//         match a[0].0.cmp(&b[0].0) {
//             Ordering::Less    => { target.push(a[0].clone()); a = &a[1..]; },
//             Ordering::Greater => { target.push(b[0].clone()); b = &b[1..]; },
//             Ordering::Equal   => { target.push((a[0].0.clone(), a[0].1 + b[0].1));
//                                    a = &a[1..]; b = &b[1..]; },
//         }
//     }
//
//     if a.len() > 0 { target.extend(a.iter().map(|x| x.clone())); }
//     if b.len() > 0 { target.extend(b.iter().map(|x| x.clone())); }
// }
//
