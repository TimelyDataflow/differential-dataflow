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
