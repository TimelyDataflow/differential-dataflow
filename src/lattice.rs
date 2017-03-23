//! Partially ordered elements with a least upper bound.

/// A bounded partially ordered type supporting joins and meets.
pub trait Lattice : PartialOrd {
    /// The smallest element of the type.
    fn min() -> Self;
    /// The largest element of the type.
    fn max() -> Self;
    /// The smallest element greater than both arguments.
    fn join(&self, &Self) -> Self;
    /// The largest element lesser than both arguments.
    fn meet(&self, &Self) -> Self;

    /// Advances self to the largest time indistinguishable under `frontier`.
    // TODO : add an example here demonstrating the result invariant.
    #[inline(always)]
    fn advance_by(&self, frontier: &[Self]) -> Option<Self> where Self: Sized{
        if frontier.len() > 0 {
            let mut result = self.join(&frontier[0]);
            for f in &frontier[1..] {
                result = result.meet(&self.join(f));
            }
            Some(result)
        }  
        else {
            None
        }
    }
}

use timely::progress::nested::product::Product;

impl<T1: Lattice, T2: Lattice> Lattice for Product<T1, T2> {
    #[inline(always)]
    fn min() -> Self { Product::new(T1::min(), T2::min()) }
    #[inline(always)]
    fn max() -> Self { Product::new(T1::max(), T2::max()) }
    #[inline(always)]
    fn join(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.join(&other.outer),
            inner: self.inner.join(&other.inner),
        }
    }
    #[inline(always)]
    fn meet(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.meet(&other.outer),
            inner: self.inner.meet(&other.inner),
        }
    }
}

use timely::progress::timestamp::RootTimestamp;

impl Lattice for RootTimestamp {
    #[inline(always)]
    fn min() -> RootTimestamp { RootTimestamp }
    #[inline(always)]
    fn max() -> RootTimestamp { RootTimestamp }
    #[inline(always)]
    fn join(&self, _: &RootTimestamp) -> RootTimestamp { RootTimestamp }
    #[inline(always)]
    fn meet(&self, _: &RootTimestamp) -> RootTimestamp { RootTimestamp }
}

impl Lattice for usize {
    #[inline(always)]
    fn min() -> usize { usize::min_value() }
    #[inline(always)]
    fn max() -> usize { usize::max_value() }
    #[inline(always)]
    fn join(&self, other: &usize) -> usize { ::std::cmp::max(*self, *other) }
    #[inline(always)]
    fn meet(&self, other: &usize) -> usize { ::std::cmp::min(*self, *other) }
}

impl Lattice for u64 {
    #[inline(always)]
    fn min() -> u64 { u64::min_value() }
    #[inline(always)]
    fn max() -> u64 { u64::max_value() }
    #[inline(always)]
    fn join(&self, other: &u64) -> u64 { ::std::cmp::max(*self, *other) }
    #[inline(always)]
    fn meet(&self, other: &u64) -> u64 { ::std::cmp::min(*self, *other) }
}

impl Lattice for u32 {
    #[inline(always)]
    fn min() -> u32 { u32::min_value() }
    #[inline(always)]
    fn max() -> u32 { u32::max_value() }
    #[inline(always)]
    fn join(&self, other: &u32) -> u32 { ::std::cmp::max(*self, *other) }
    #[inline(always)]
    fn meet(&self, other: &u32) -> u32 { ::std::cmp::min(*self, *other) }
}

impl Lattice for i32 {
    #[inline(always)]
    fn min() -> i32 { i32::min_value() }
    #[inline(always)]
    fn max() -> i32 { i32::max_value() }
    #[inline(always)]
    fn join(&self, other: &i32) -> i32 { ::std::cmp::max(*self, *other) }
    #[inline(always)]
    fn meet(&self, other: &i32) -> i32 { ::std::cmp::min(*self, *other) }
}

impl Lattice for () {
    #[inline(always)]
    fn min() -> () { () }
    #[inline(always)]
    fn max() -> () { () }
    #[inline(always)]
    fn join(&self, _other: &()) -> () { () }
    #[inline(always)]
    fn meet(&self, _other: &()) -> () { () }
}

/// Extends `vector` to contain all joins of pairs of elements.
pub fn close_under_join<T: Lattice>(vector: &mut Vec<T>) {
    // compares each element to those elements after it.
    let mut first = 0;
    while first < vector.len() {
        let mut next = first + 1;
        while next < vector.len() {
            let lub = vector[first].join(&vector[next]);
            if !vector.contains(&lub) {
                vector.push(lub);
            }

            next += 1;
        }

        first += 1;
    }
}
