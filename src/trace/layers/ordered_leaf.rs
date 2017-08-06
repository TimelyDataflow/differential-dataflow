//! Implementation using ordered keys and exponential search.

use difference::Diff;

use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder};

/// A layer of unordered values. 
#[derive(Debug, Eq, PartialEq)]
pub struct OrderedLeaf<K, R> {
    /// Unordered values.
    pub vals: Vec<(K, R)>,
}

impl<K: Ord+Clone, R: Diff+Clone> Trie for OrderedLeaf<K, R> {
    type Item = (K, R);
    type Cursor = OrderedLeafCursor;
    type MergeBuilder = OrderedLeafBuilder<K, R>;
    type TupleBuilder = OrderedLeafBuilder<K, R>;
    fn keys(&self) -> usize { self.vals.len() }
    fn tuples(&self) -> usize { <OrderedLeaf<K, R> as Trie>::keys(&self) }
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor { 
        // println!("unordered: {} .. {}", lower, upper);
        OrderedLeafCursor {
            // vals: owned_self.map(|x| &x.vals[..]),
            bounds: (lower, upper),
            pos: lower,
        }
    }
}

/// A builder for unordered values.
pub struct OrderedLeafBuilder<K, R> {
    /// Unordered values.
    pub vals: Vec<(K, R)>,
}

impl<K: Ord+Clone, R: Diff+Clone> Builder for OrderedLeafBuilder<K, R> {
    type Trie = OrderedLeaf<K, R>; 
    fn boundary(&mut self) -> usize { self.vals.len() } 
    fn done(self) -> Self::Trie { OrderedLeaf { vals: self.vals } }
}

impl<K: Ord+Clone, R: Diff+Clone> MergeBuilder for OrderedLeafBuilder<K, R> {
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
        OrderedLeafBuilder {
            vals: Vec::with_capacity(<OrderedLeaf<K, R> as Trie>::keys(other1) + <OrderedLeaf<K, R> as Trie>::keys(other2)),
        }
    }
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        self.vals.extend_from_slice(&other.vals[lower .. upper]);
    }
    fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize {
        // self.copy_range(&other1.0, other1.1, other1.2);
        // self.copy_range(&other2.0, other2.1, other2.2);
        // self.vals.len()

        let (trie1, mut lower1, upper1) = other1;
        let (trie2, mut lower2, upper2) = other2;

        self.vals.reserve((upper1 - lower1) + (upper2 - lower2));

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {

            match trie1.vals[lower1].0.cmp(&trie2.vals[lower2].0) {
                ::std::cmp::Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.vals[(1+lower1)..upper1], |x| x < &trie2.vals[lower2]);
                    <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie1, lower1, lower1 + step);
                    lower1 += step;
                }
                ::std::cmp::Ordering::Equal => {

                    let sum = trie1.vals[lower1].1 + trie2.vals[lower2].1;
                    if !sum.is_zero() {
                        self.vals.push((trie1.vals[lower1].0.clone(), sum));
                    }

                    lower1 += 1;
                    lower2 += 1;
                } 
                ::std::cmp::Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance(&trie2.vals[(1+lower2)..upper2], |x| x < &trie1.vals[lower1]);
                    <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie2, lower2, lower2 + step);
                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 { <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie1, lower1, upper1); }
        if lower2 < upper2 { <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie2, lower2, upper2); }

        self.vals.len()
    }
}

impl<K: Ord+Clone, R: Diff+Clone> TupleBuilder for OrderedLeafBuilder<K, R> {
    type Item = (K, R);
    fn new() -> Self { OrderedLeafBuilder { vals: Vec::new() } }
    fn with_capacity(cap: usize) -> Self { OrderedLeafBuilder { vals: Vec::with_capacity(cap) } }
    #[inline(always)] fn push_tuple(&mut self, tuple: (K, R)) { self.vals.push(tuple) }
}

/// A cursor for walking through an unordered sequence of values.
///
/// This cursor does not support `seek`, though I'm not certain how to expose this.
#[derive(Debug)]
pub struct OrderedLeafCursor {
    // vals: OwningRef<Rc<Erased>, [(K, R)]>,
    pos: usize,
    bounds: (usize, usize),
}

impl<K: Clone, R: Clone> Cursor<OrderedLeaf<K, R>> for OrderedLeafCursor {
    type Key = (K, R);
    fn key<'a>(&self, storage: &'a OrderedLeaf<K, R>) -> &'a Self::Key { &storage.vals[self.pos] }
    fn step(&mut self, storage: &OrderedLeaf<K, R>) {
        self.pos += 1; 
        if !self.valid(storage) {
            self.pos = self.bounds.1;
        }
    }
    fn seek(&mut self, _storage: &OrderedLeaf<K, R>, _key: &Self::Key) {
        panic!("seeking in an OrderedLeafCursor; should be fine, panic is wrong.");
    }
    fn valid(&self, _storage: &OrderedLeaf<K, R>) -> bool { self.pos < self.bounds.1 }
    fn rewind(&mut self, _storage: &OrderedLeaf<K, R>) {
        self.pos = self.bounds.0;
    }
    fn reposition(&mut self, _storage: &OrderedLeaf<K, R>, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
    }
}


/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to 
/// count the number of elements in time logarithmic in the result.
#[inline(never)]
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {

        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step = step << 1;
        }

        // advance in exponentially shrinking steps.
        step = step >> 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step = step >> 1;
        }

        index += 1;
    }   

    index
}
