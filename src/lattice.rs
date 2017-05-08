//! Partially ordered elements with a least upper bound.
//!
//! Lattices form the basis of differential dataflow's efficient execution in the presence of 
//! iterative sub-computations. All logical times in differential dataflow must implement the 
//! `Lattice` trait, and all reasoning in operators are done it terms of `Lattice` methods.

use timely::order::PartialOrder;

/// A bounded partially ordered type supporting joins and meets.
pub trait Lattice : PartialOrder {
    /// The smallest element of the type.
    fn min() -> Self;
    /// The largest element of the type.
    fn max() -> Self;
    /// The smallest element greater than or equal to both arguments.
    fn join(&self, &Self) -> Self;
    /// The largest element less than or equal to both arguments.
    fn meet(&self, &Self) -> Self;

    /// Advances self to the largest time indistinguishable under `frontier`.
    ///
    /// This method produces the "largest" lattice element with the property that for every
    /// lattice element greater than some element of `frontier`, both the result and `self`
    /// compare identically to the lattice element. The result is the "largest" element in
    /// the sense that any other element with the same property (compares identically to times
    /// greater or equal to `frontier`) must be less or equal to the result.
    ///
    /// When provided an empty frontier, the result is `<Self as Lattice>::max()`. It should
    /// perhaps be distinguished by an `Option<Self>` type, but the `None` case only happens
    /// when `frontier` is empty, which the caller can see for themselves if they want to be
    /// clever.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::progress::nested::product::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let time = Product::new(3, 7);
    /// let frontier = vec![Product::new(4, 8), Product::new(5, 3)];
    /// let advanced = time.advance_by(&frontier[..]);
    ///
    /// // `time` and `advanced` are indistinguishable to elements >= an element of `frontier`
    /// for i in 0 .. 10 {
    ///     for j in 0 .. 10 {
    ///         let test = Product::new(i, j);
    ///         // for `test` in the future of `frontier` ..
    ///         if frontier.iter().any(|t| t.less_equal(&test)) {
    ///             assert_eq!(time.less_equal(&test), advanced.less_equal(&test));
    ///         }
    ///     }
    /// }
    ///
    /// assert_eq!(advanced, Product::new(4, 7));
    /// # }
    /// ```
    #[inline(always)]
    fn advance_by(&self, frontier: &[Self]) -> Self where Self: Sized{
        if frontier.len() > 0 {
            let mut result = self.join(&frontier[0]);
            for f in &frontier[1..] {
                result = result.meet(&self.join(f));
            }
            result
        }  
        else {
            Self::max()
        }
    }
}

/// A carrier trait for totally ordered lattices.
///
/// Types that implement `TotalOrder` are stating that their `Lattice` is in fact a total order.
/// This type is only used to restrict implementations to certain types of lattices.
///
/// This trait is automatically implemented for integer scalars, and for products of these types
/// with "empty" timestamps (e.g. `RootTimestamp` and `()`). Be careful implementing this trait
/// for your own timestamp types, as it may lead to the applicability of incorrect implementations.
///
/// Note that this trait is distinct from `Ord`; many implementors of `Lattice` also implement 
/// `Ord` so that they may be sorted, deduplicated, etc. This implementation neither derives any
/// information from an `Ord` implementation nor informs it in any way.
pub trait TotalOrder : Lattice { }

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

impl<T> TotalOrder for Product<RootTimestamp, T> where T: TotalOrder { } 
impl<T> TotalOrder for Product<(), T> where T: TotalOrder { } 

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

impl TotalOrder for RootTimestamp { }

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

impl TotalOrder for usize { }

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

impl TotalOrder for u64 { }

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

impl TotalOrder for u32 { }

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

impl TotalOrder for i32 { }

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

impl TotalOrder for () { }

// /// Extends `vector` to contain all joins of pairs of elements.
// pub fn close_under_join<T: Lattice>(vector: &mut Vec<T>) {
//     // compares each element to those elements after it.
//     let mut first = 0;
//     while first < vector.len() {
//         let mut next = first + 1;
//         while next < vector.len() {
//             let lub = vector[first].join(&vector[next]);
//             if !vector.contains(&lub) {
//                 vector.push(lub);
//             }

//             next += 1;
//         }

//         first += 1;
//     }
// }
