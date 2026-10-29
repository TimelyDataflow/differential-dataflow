//! A timestamp type as in Naiad, where a vector of timestamps of different lengths are comparable.
//!
//! This type compares using "standard" tuple logic as if each timestamp were extended indefinitely with minimal elements.
//!
//! The path summary for this type allows *run-time* rather than *type-driven* iterative scopes.
//! Each summary represents some journey within and out of some number of scopes, followed by entry
//! into and iteration within some other number of scopes.
//!
//! As a result, summaries describe some number of trailing coordinates to truncate, and some increments
//! to the resulting vector. Structurally, the increments can only be to one non-truncated coordinate
//! (as iteration within a scope requires leaving contained scopes), and then to any number of appended
//! default coordinates (which is effectively just *setting* the coordinate).

use columnar::Columnar;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// A sequence of timestamps, partially ordered by the product order.
///
/// Sequences of different lengths are compared as if extended indefinitely by `T::minimum()`.
/// `N` coordinates are stored inline (one by default); longer sequences spill to the heap.
/// Sequences are guaranteed to be "minimal", and may not end with `T::minimum()` entries.
#[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Columnar)]
#[columnar(derive(Eq, PartialEq, Ord, PartialOrd))]
pub struct PointStamp<T, const N: usize = 1> {
    /// A sequence of timestamps corresponding to timestamps in a sequence of nested scopes.
    vector: SmallVec<[T; N]>,
}

impl<T: Timestamp, const N: usize> PartialEq<[T]> for PointStamp<T, N> {
    fn eq(&self, other: &[T]) -> bool {
        self.vector.iter()
            .zip(other.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.eq(t2))
    }
}

impl<T: Timestamp, const N: usize> PartialEq<PointStamp<T, N>> for [T] {
    fn eq(&self, other: &PointStamp<T, N>) -> bool {
        self.iter()
            .zip(other.vector.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.eq(t2))
    }
}

impl<T: Timestamp, const N: usize> PartialOrder<[T]> for PointStamp<T, N> {
    fn less_equal(&self, other: &[T]) -> bool {
        self.vector.iter()
            .zip(other.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.less_equal(t2))
    }
}

impl<T: Timestamp, const N: usize> PartialOrder<PointStamp<T, N>> for [T] {
    fn less_equal(&self, other: &PointStamp<T, N>) -> bool {
        self.iter()
            .zip(other.vector.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.less_equal(t2))
    }
}

impl<T: Timestamp, const N: usize> PointStamp<T, N> {
    /// Create a new sequence.
    ///
    /// This method will modify `vector` to ensure it does not end with `T::minimum()`.
    pub fn new(mut vector: SmallVec<[T; N]>) -> Self {
        while vector.last() == Some(&T::minimum()) {
            vector.pop();
        }
        PointStamp { vector }
    }
    /// Returns the wrapped small vector.
    ///
    /// This method is the support way to mutate the contents of `self`, by extracting
    /// the vector and then re-introducing it with `PointStamp::new` to re-establish
    /// the invariant that the vector not end with `T::minimum`.
    pub fn into_inner(self) -> SmallVec<[T; N]> {
        self.vector
    }
}

// Deserialized sequences go through `new`, so they are minimal whatever the input was;
// `less_equal` relies on it.
impl<'de, T: Timestamp, const N: usize> Deserialize<'de> for PointStamp<T, N> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // The serialized form, field for field, before trimming.
        #[derive(Deserialize)]
        struct PointStamp<T, const N: usize> { vector: SmallVec<[T; N]> }
        let PointStamp { vector } = PointStamp::<T, N>::deserialize(deserializer)?;
        Ok(Self::new(vector))
    }
}

impl<T, const N: usize> std::ops::Deref for PointStamp<T, N> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.vector
    }
}

// Implement timely dataflow's `PartialOrder` trait.
use timely::order::PartialOrder;
impl<T: Timestamp, const N: usize> PartialOrder for PointStamp<T, N> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        // Every present coordinate must be less-equal the corresponding coordinate,
        // where absent corresponding coordinates are `T::minimum()`. Coordinates
        // absent from `self.vector` are themselves `T::minimum()` and are less-equal
        // any corresponding coordinate in `other.vector`. A longer `self` is never
        // less-equal: sequences are minimal, so its last coordinate is not `T::minimum()`,
        // and it would have to be less-equal the absent, minimal coordinate of `other`.
        let (this, that) = (&self.vector[..], &other.vector[..]);
        this.len() <= that.len() && this.iter().zip(that.iter()).all(|(t1, t2)| t1.less_equal(t2))
    }
}

use timely::progress::timestamp::Refines;
impl<T: Timestamp, const N: usize> Refines<()> for PointStamp<T, N> {
    fn to_inner(_outer: ()) -> Self {
        Self { vector: Default::default() }
    }
    fn to_outer(self) -> () {
        ()
    }
    fn summarize(_summary: <Self>::Summary) -> () {
        ()
    }
}

// Implement timely dataflow's `PathSummary` trait.
// This is preparation for the `Timestamp` implementation below.
use timely::progress::PathSummary;

/// Describes an action on a `PointStamp`: truncation to `length` followed by `actions`.
#[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct PointStampSummary<TS> {
    /// Number of leading coordinates to retain.
    ///
    /// A `None` value indicates that all coordinates should be retained.
    pub retain: Option<usize>,
    /// Summary actions to apply to all coordinates.
    ///
    /// If `actions.len()` is greater than `retain`, a timestamp should be extended by
    /// `T::minimum()` in order to be subjected to `actions`.
    pub actions: Vec<TS>,
}

impl<T: Timestamp, const N: usize> PathSummary<PointStamp<T, N>> for PointStampSummary<T::Summary> {
    fn results_in(&self, timestamp: &PointStamp<T, N>) -> Option<PointStamp<T, N>> {
        // Get a slice of timestamp coordinates appropriate for consideration.
        let timestamps = if let Some(retain) = self.retain {
            if retain < timestamp.vector.len() {
                &timestamp.vector[..retain]
            } else {
                &timestamp.vector[..]
            }
        } else {
            &timestamp.vector[..]
        };

        let mut vector = Vec::with_capacity(std::cmp::max(timestamps.len(), self.actions.len()));
        // Introduce elements where both timestamp and action exist.
        let min_len = std::cmp::min(timestamps.len(), self.actions.len());
        for (action, timestamp) in self.actions.iter().zip(timestamps.iter()) {
            vector.push(action.results_in(timestamp)?);
        }
        // Any remaining timestamps should be copied in.
        for timestamp in timestamps.iter().skip(min_len) {
            vector.push(timestamp.clone());
        }
        // Any remaining actions should be applied to the empty timestamp.
        for action in self.actions.iter().skip(min_len) {
            vector.push(action.results_in(&T::minimum())?);
        }

        Some(PointStamp::new(vector.into()))
    }
    fn followed_by(&self, other: &Self) -> Option<Self> {
        // The output `retain` will be the minimum of the two inputs.
        let retain = match (self.retain, other.retain) {
            (Some(x), Some(y)) => Some(std::cmp::min(x, y)),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };

        // The output `actions` will depend on the relative sizes of the input `retain`s.
        let self_actions = if let Some(retain) = other.retain {
            if retain < self.actions.len() {
                &self.actions[..retain]
            } else {
                &self.actions[..]
            }
        } else {
            &self.actions[..]
        };

        let mut actions = Vec::with_capacity(std::cmp::max(self_actions.len(), other.actions.len()));
        // Introduce actions where both input actions apply.
        let min_len = std::cmp::min(self_actions.len(), other.actions.len());
        for (action1, action2) in self_actions.iter().zip(other.actions.iter()) {
            actions.push(action1.followed_by(action2)?);
        }
        // Append any remaining self actions.
        actions.extend(self_actions.iter().skip(min_len).cloned());
        // Append any remaining other actions.
        actions.extend(other.actions.iter().skip(min_len).cloned());

        Some(Self { retain, actions })
    }
}

impl<TS: PartialOrder> PartialOrder for PointStampSummary<TS> {
    fn less_equal(&self, other: &Self) -> bool {
        // If the `retain`s are not the same, there is some coordinate which
        // could either be bigger or smaller as the timestamp or the replacement.
        // In principle, a `T::minimum()` extension could break this rule, and
        // we could tighten this logic if needed; I think it is fine not to though.
        self.retain == other.retain
            && self.actions.len() <= other.actions.len()
            && self
                .actions
                .iter()
                .zip(other.actions.iter())
                .all(|(t1, t2)| t1.less_equal(t2))
    }
}

// Implement timely dataflow's `Timestamp` trait.
use timely::progress::Timestamp;
impl<T: Timestamp, const N: usize> Timestamp for PointStamp<T, N> {
    fn minimum() -> Self {
        Self::new(Default::default())
    }
    type Summary = PointStampSummary<T::Summary>;
}

// Implement differential dataflow's `Lattice` trait.
// This extends the `PartialOrder` implementation with additional structure.
use crate::lattice::Lattice;
impl<T: Lattice + Timestamp, const N: usize> Lattice for PointStamp<T, N> {
    #[inline(always)]
    fn join(&self, other: &Self) -> Self {
        // The longer input's coordinates, joined with the shorter input's where it has them.
        // The result is minimal without trimming: its last coordinate is the longer input's
        // last, which is not `T::minimum()`, or that coordinate joined with another.
        let (this, that) = (&self.vector[..], &other.vector[..]);
        // An empty input (the minimum time) leaves the other unchanged.
        if this.is_empty() { return other.clone(); }
        if that.is_empty() { return self.clone(); }
        let (long, short) = if this.len() >= that.len() { (this, that) } else { (that, this) };
        let vector = long
            .iter()
            .enumerate()
            .map(|(index, time)| if index < short.len() { time.join(&short[index]) } else { time.clone() })
            .collect();
        Self { vector }
    }
    #[inline]
    fn join_assign(&mut self, other: &Self) {
        let that = &other.vector[..];
        let my_len = self.vector.len();
        for (this, that) in self.vector.iter_mut().zip(that.iter()) {
            this.join_assign(that);
        }
        if that.len() > my_len {
            self.vector.extend(that[my_len..].iter().cloned());
        }
    }
    #[inline(always)]
    fn meet(&self, other: &Self) -> Self {
        // Coordinates both inputs have, met; the rest are `T::minimum()` in one input, and so
        // in the output, which `new` then trims from the result.
        let vector = self.vector.iter().zip(other.vector.iter()).map(|(this, that)| this.meet(that)).collect();
        Self::new(vector)
    }
    #[inline]
    fn meet_assign(&mut self, other: &Self) {
        let that = &other.vector[..];
        self.vector.truncate(that.len());
        for (this, that) in self.vector.iter_mut().zip(that.iter()) {
            this.meet_assign(that);
        }
        while self.vector.last() == Some(&T::minimum()) { self.vector.pop(); }
    }
}

mod columnation {
    use columnation::{Columnation, Region};
    use smallvec::SmallVec;
    use crate::dynamic::pointstamp::PointStamp;

    impl<T: Columnation+Clone, const N: usize> Columnation for PointStamp<T, N> {
        type InnerRegion = PointStampStack<T::InnerRegion, N>;
    }

    /// Stack for PointStamp. Part of Columnation implementation.
    pub struct PointStampStack<R: Region<Item: Columnation+Clone>, const N: usize = 1>(<SmallVec<[R::Item; N]> as Columnation>::InnerRegion);

    impl<R: Region<Item: Columnation+Clone>, const N: usize> Default for PointStampStack<R, N> {
        #[inline]
        fn default() -> Self {
            Self(Default::default())
        }
    }

    impl<R: Region<Item: Columnation+Clone>, const N: usize> Region for PointStampStack<R, N> {
        type Item = PointStamp<R::Item, N>;

        #[inline]
        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            Self::Item { vector: self.0.copy(&item.vector) }
        }

        fn clear(&mut self) {
            self.0.clear();
        }

        fn reserve_items<'a, I>(&mut self, items: I) where Self: 'a, I: Iterator<Item=&'a Self::Item> + Clone {
            self.0.reserve_items(items.map(|x| &x.vector));
        }

        fn reserve_regions<'a, I>(&mut self, regions: I) where Self: 'a, I: Iterator<Item=&'a Self> + Clone {
            self.0.reserve_regions(regions.map(|r| &r.0));
        }

        fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.0.heap_size(callback);
        }
    }
}
