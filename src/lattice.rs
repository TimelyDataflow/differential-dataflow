//! Partially ordered elements with a least upper bound.
//!
//! Lattices form the basis of differential dataflow's efficient execution in the presence of
//! iterative sub-computations. All logical times in differential dataflow must implement the
//! `Lattice` trait, and all reasoning in operators are done it terms of `Lattice` methods.

use timely::order::PartialOrder;
use timely::progress::{Timestamp, Antichain};

/// A bounded partially ordered type supporting joins and meets.
pub trait Lattice : PartialOrder+Timestamp {

    /// The smallest element greater than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// let join = time1.join(&time2);
    ///
    /// assert_eq!(join, Product::new(4, 7));
    /// # }
    /// ```
    fn join(&self, &Self) -> Self;

    /// Updates `self` to the smallest element greater than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.join_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(4, 7));
    /// # }
    /// ```
    fn join_assign(&mut self, other: &Self) where Self: Sized {
        *self = self.join(other);
    }

    /// The largest element less than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// let meet = time1.meet(&time2);
    ///
    /// assert_eq!(meet, Product::new(3, 6));
    /// # }
    /// ```
    fn meet(&self, &Self) -> Self;

    /// Updates `self` to the largest element less than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.meet_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(3, 6));
    /// # }
    /// ```
    fn meet_assign(&mut self, other: &Self) where Self: Sized  {
        *self = self.meet(other);
    }

    /// Advances self to the largest time indistinguishable under `frontier`.
    ///
    /// This method produces the "largest" lattice element with the property that for every
    /// lattice element greater than some element of `frontier`, both the result and `self`
    /// compare identically to the lattice element. The result is the "largest" element in
    /// the sense that any other element with the same property (compares identically to times
    /// greater or equal to `frontier`) must be less or equal to the result.
    ///
    /// When provided an empty frontier `self` is not modified.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let time = Product::new(3, 7);
    /// let mut advanced = Product::new(3, 7);
    /// let frontier = vec![Product::new(4, 8), Product::new(5, 3)];
    /// advanced.advance_by(&frontier[..]);
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
    #[inline]
    fn advance_by(&mut self, frontier: &[Self]) where Self: Sized {
        if let Some(first) = frontier.get(0) {
            let mut result = self.join(first);
            for f in &frontier[1..] {
                result.meet_assign(&self.join(f));
            }
            *self = result;
        }
    }
}

use timely::order::Product;

impl<T1: Lattice, T2: Lattice> Lattice for Product<T1, T2> {
    #[inline]
    fn join(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.join(&other.outer),
            inner: self.inner.join(&other.inner),
        }
    }
    #[inline]
    fn meet(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.meet(&other.outer),
            inner: self.inner.meet(&other.inner),
        }
    }
}

macro_rules! implement_lattice {
    ($index_type:ty, $minimum:expr) => (
        impl Lattice for $index_type {
            #[inline] fn join(&self, other: &Self) -> Self { ::std::cmp::max(*self, *other) }
            #[inline] fn meet(&self, other: &Self) -> Self { ::std::cmp::min(*self, *other) }
        }
    )
}

use std::time::Duration;

implement_lattice!(Duration, Duration::new(0, 0));
implement_lattice!(usize, 0);
implement_lattice!(u128, 0);
implement_lattice!(u64, 0);
implement_lattice!(u32, 0);
implement_lattice!(u16, 0);
implement_lattice!(u8, 0);
implement_lattice!(isize, 0);
implement_lattice!(i128, 0);
implement_lattice!(i64, 0);
implement_lattice!(i32, 0);
implement_lattice!(i16, 0);
implement_lattice!(i8, 0);
implement_lattice!((), ());

/// Given two slices representing minimal antichains,
/// returns the "smallest" minimal antichain "greater or equal" to them.
///
/// # Examples
///
/// ```
/// # extern crate timely;
/// # extern crate differential_dataflow;
/// # use timely::PartialOrder;
/// # use timely::order::Product;
/// # use differential_dataflow::lattice::Lattice;
/// # use differential_dataflow::lattice::antichain_join;
/// # fn main() {
///
/// let f1 = &[Product::new(3, 7), Product::new(5, 6)];
/// let f2 = &[Product::new(4, 6)];
/// let join = antichain_join(f1, f2);
/// assert_eq!(join.elements(), &[Product::new(4, 7), Product::new(5, 6)]);
/// # }
/// ```
pub fn antichain_join<T: Lattice>(one: &[T], other: &[T]) -> Antichain<T> {
    let mut upper = Antichain::new();
    for time1 in one {
        for time2 in other {
            upper.insert(time1.join(time2));
        }
    }
    upper
}
