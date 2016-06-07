//! Partially ordered elements with a least upper bound.

/// Partially ordered elements with a least upper bound.
pub trait LeastUpperBound : PartialOrd {
    /// A maximum value for the type.
    fn max() -> Self;
    /// The least element greater or equal to each argument.
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

/// Extends `vector` to contain all least upper bounds of pairs of elements.
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
