use std::marker::PhantomData;

use collection_trace::{CollectionTrace, LeastUpperBound, Lookup};

pub struct OperatorTrace<K, T, S: Ord, R: Ord, L: Lookup<K, usize>> {
    phantom:        PhantomData<K>,
    src:            Vec<(S,i32)>,
    dst:            Vec<(R,i32)>,
    pub source:     CollectionTrace<K, T, S, L>,
    pub result:     CollectionTrace<K, T, R, L>,
}

impl<K: Eq+Clone, T:Clone+LeastUpperBound, S: Ord+Clone, R: Ord+Clone, L: Lookup<K, usize>> OperatorTrace<K, T, S, R, L> {
    pub fn new<F: Fn()->L>(lookup: F) -> OperatorTrace<K, T, S, R, L> {
        OperatorTrace {
            phantom: PhantomData,
            source:  CollectionTrace::new(lookup()),
            result:  CollectionTrace::new(lookup()),
            src:     Vec::new(),
            dst:     Vec::new(),
        }
    }
    pub fn set_collection_with<F: Fn(&K, &[(S, i32)], &mut Vec<(R, i32)>)>(&mut self, key: &K, index: &T, logic: F) {
        self.source.get_collection(key, index, &mut self.src);
        if self.src.len() > 0 { logic(key, &mut self.src, &mut self.dst); }
        self.result.set_collection(key.clone(), index.clone(), &mut self.dst);
        self.src.clear();
        self.dst.clear();
    }
}
