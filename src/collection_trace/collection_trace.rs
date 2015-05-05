pub trait BatchCollectionTrace<Key: Eq> {
    type Index;
    type Value;
    fn set_difference<I: Iterator<Item=(Self::Value, i32)>>(&mut self, key: Key, time: Self::Index, difference: I);
    fn set_collection(&mut self, Key, Self::Index, &mut Vec<(Self::Value, i32)>); // first determines diffs
    fn get_difference(&self, &Key, &Self::Index) -> &[(Self::Value, i32)];         // directly reads diffs
    fn get_collection(&self, &Key, &Self::Index, &mut Vec<(Self::Value, i32)>);    // assembles collection
    fn interesting_times(&mut self, &Key, &Self::Index, &mut Vec<Self::Index>);
    fn map_over_times<F:FnMut(&Self::Index, &[(Self::Value, i32)])>(&self, &Key, func: F);
    // fn new() -> Self;
}

pub trait OperatorTrace {
    type Key;
    type Index;
    type Input;
    type Output;

    fn set_difference_at(&mut self, &Self::Key, &Self::Index, &mut Vec<(Self::Input, i32)>, &mut Vec<(Self::Index)>);
    fn get_difference_at(&mut self, &Self::Key, &Self::Index, &mut Vec<(Self::Output, i32)>);
}

pub trait LeastUpperBound : PartialOrd {
    fn max() -> Self;
    fn least_upper_bound(&self, &Self) -> Self;
}

// impl<T: Ord+Clone> LeastUpperBound for T {
//     fn least_upper_bound(&self, other: &T) -> T {
//         if self < other { other.clone() }
//         else            { self.clone() }
//     }
// }

use timely::progress::nested::product::Product;

impl<T1: LeastUpperBound, T2: LeastUpperBound> LeastUpperBound for Product<T1, T2> {
    #[inline(always)]
    fn max() -> Self { Product::new(T1::max(), T2::max()) }
    fn least_upper_bound(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.least_upper_bound(&other.outer),
            inner: self.inner.least_upper_bound(&other.inner),
        }
    }
}

use timely::progress::timestamp::RootTimestamp;

impl LeastUpperBound for RootTimestamp {
    fn max() -> RootTimestamp { RootTimestamp }
    fn least_upper_bound(&self, _: &RootTimestamp) -> RootTimestamp { RootTimestamp }
}

impl LeastUpperBound for u64 {
    fn max() -> u64 { u64::max_value() }
    fn least_upper_bound(&self, other: &u64) -> u64 { if self < other { *other } else { *self }}
}

impl LeastUpperBound for u32 {
    fn max() -> u32 { u32::max_value() }
    fn least_upper_bound(&self, other: &u32) -> u32 { if self < other { *other } else { *self }}
}

impl LeastUpperBound for i32 {
    fn max() -> i32 { i32::max_value() }
    fn least_upper_bound(&self, other: &i32) -> i32 { if self < other { *other } else { *self }}
}


pub fn close_under_lub<T: LeastUpperBound>(vector: &mut Vec<T>) {
    // compares each element to those elements after it.
    let mut first = 0;
    while first < vector.len() {
        let mut next = first + 1;
        while next < vector.len() {
            let lub = vector[first].least_upper_bound(&vector[next]);
            if !vector.contains(&lub) {
                vector.push(lub);
            }

            next += 1;
        }

        first += 1;
    }
}
