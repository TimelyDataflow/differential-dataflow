use std::marker::PhantomData;
use std::iter::Peekable;

use collection_trace::{CollectionTrace, LeastUpperBound, Lookup, Offset};
use collection_trace::collection_trace::MergeIterator;

pub type Trace<K, T, S, L> = CollectionTrace<K, T, S, L>;

pub struct OperatorTrace<K: Ord, T, S: Ord, R: Ord, L: Lookup<K, Offset>> {
    phantom:        PhantomData<K>,
    src:            Vec<(S,i32)>,
    dst:            Vec<(R,i32)>,
    pub source:     Trace<K, T, S, L>,
    pub result:     Trace<K, T, R, L>,
}

impl<K: Ord+Clone, T:Clone+LeastUpperBound, S: Ord+Clone, R: Ord+Clone, L: Lookup<K, Offset>> OperatorTrace<K, T, S, R, L> {
    pub fn new<F: Fn()->L>(lookup: F) -> OperatorTrace<K, T, S, R, L> {
        OperatorTrace {
            phantom: PhantomData,
            source:  Trace::new(lookup()),
            result:  Trace::new(lookup()),
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

    // The iterator implementation is complicated by the fact that we only want to invoke the logic
    // for keys with non-empty accumulated collections. This means finding the first element, and
    // providing an iterator for the remaining elements. Or using an alternate iterator interface
    // which provides the current element, and an `advance` method to move it forward

    pub fn set_collection_from<F: Fn(&K, Peekable<MergeIterator<S>>, &mut Vec<(R, i32)>)>(&mut self, key: &K, index: &T, logic: F) {
        let mut iter = self.source.get_collection_iterator(key, index).peekable();
        if iter.peek().is_some() { logic(key, iter, &mut self.dst); }
        self.result.set_collection(key.clone(), index.clone(), &mut self.dst);
        self.dst.clear();
    }
}

pub trait AltIterator {
    type Item;
    fn item(&self) -> &Self::Item;              //
    fn take(&mut self) -> Option<Self::Item>;   // consumes self.item() and advances iterator
}

// pub struct BinaryOperatorTrace<K, T, S1: Ord, S2: Ord, R: Ord, L: Lookup<K, Offset>> {
//     phantom:        PhantomData<K>,
//     src1:           Vec<(S1,i32)>,
//     src2:           Vec<(S2,i32)>,
//     dst:            Vec<(R,i32)>,
//     pub source1:    CollectionTrace<K, T, S1, L>,
//     pub source2:    CollectionTrace<K, T, S2, L>,
//     pub result:     CollectionTrace<K, T, R, L>,
// }
//
// impl<K: Eq+Clone, T:Clone+LeastUpperBound, S1: Ord+Clone, S2: Ord+Clone, R: Ord+Clone, L: Lookup<K, Offset>> BinaryOperatorTrace<K, T, S1, S2, R, L> {
//     pub fn new<F: Fn()->L>(lookup: F) -> BinaryOperatorTrace<K, T, S1, S2, R, L> {
//         BinaryOperatorTrace {
//             phantom: PhantomData,
//             source1: CollectionTrace::new(lookup()),
//             source2: CollectionTrace::new(lookup()),
//             result:  CollectionTrace::new(lookup()),
//             src1:    Vec::new(),
//             src2:    Vec::new(),
//             dst:     Vec::new(),
//         }
//     }
//     pub fn set_collection_with<F: Fn(&K, &[(S1, i32)], &[(S2, i32)], &mut Vec<(R, i32)>)>(&mut self, key: &K, index: &T, logic: F) {
//         self.source1.get_collection(key, index, &mut self.src1);
//         self.source2.get_collection(key, index, &mut self.src2);
//         if self.src1.len() > 0 || self.src2.len() > 0 { logic(key, &mut self.src1, &mut self.src2, &mut self.dst); }
//         self.result.set_collection(key.clone(), index.clone(), &mut self.dst);
//         self.src1.clear();
//         self.src2.clear();
//         self.dst.clear();
//     }
// }
