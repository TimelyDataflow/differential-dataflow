//! A lexicographically ordered pair of timestamps.
//!
//! Two timestamps (s1, t1) and (s2, t2) are ordered either if
//! s1 and s2 are ordered, or if s1 equals s2 and t1 and t2 are
//! ordered.
//!
//! The join of two timestamps should have as its first coordinate
//! the join of the first coordinates, and for its second coordinate
//! the join of the second coordinates for elements whose first
//! coordinate equals the computed join. That may be the minimum
//! element of the second lattice, if neither first element equals
//! the join.

/// A pair of timestamps, partially ordered by the product order.
#[derive(Debug, Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation)]
pub struct Pair<S, T> {
    pub first: S,
    pub second: T,
}

impl<S, T> Pair<S, T> {
    /// Create a new pair.
    pub fn new(first: S, second: T) -> Self {
        Pair { first, second }
    }
}

// Implement timely dataflow's `PartialOrder` trait.
use timely::order::PartialOrder;
impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
    fn less_equal(&self, other: &Self) -> bool {
        if self.first.eq(&other.first) {
            self.second.less_equal(&other.second)
        }
        else {
            self.first.less_equal(&other.first)
        }
    }
}

// Implement timely dataflow's `PathSummary` trait.
// This is preparation for the `Timestamp` implementation below.
use timely::progress::PathSummary;
impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
    fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> {
        Some(timestamp.clone())
    }
    fn followed_by(&self, other: &Self) -> Option<Self> {
        Some(other.clone())
    }
}

// Implement timely dataflow's `Timestamp` trait.
use timely::progress::Timestamp;
impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
    type Summary = ();
}

// Implement differential dataflow's `Lattice` trait.
// This extends the `PartialOrder` implementation with additional structure.
use differential_dataflow::lattice::Lattice;
impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
    fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
    fn maximum() -> Self { Pair { first: S::maximum(), second: T::maximum() }}
    fn join(&self, other: &Self) -> Self {
        let first = self.first.join(&other.first);
        let mut second = T::minimum();
        if first == self.first {
            second = second.join(&self.second);
        }
        if first == other.first {
            second = second.join(&other.second);
        }

        Pair { first, second }
    }
    fn meet(&self, other: &Self) -> Self {
        let first = self.first.meet(&other.first);
        let mut second = T::maximum();
        if first == self.first {
            second = second.meet(&self.second);
        }
        if first == other.first {
            second = second.meet(&other.second);
        }
        Pair { first, second }
    }
}