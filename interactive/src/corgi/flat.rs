//! A fixed-width representation of a DDIR time, for the proxy tactics to reason in.
//!
//! A DDIR time is `Product<u64, PointStamp<u64>>`: an epoch, then one coordinate per enclosing
//! iterative scope. The `PointStamp` is dynamically sized, which the scope structure needs, but
//! it spills to the heap two scopes deep and every clone, join and comparison walks a vector.
//! An operator knows its depth when it is rendered, and every time it sees has at most that many
//! coordinates, so its tactic can reason in `Flat<K>`: the epoch and coordinates in a `[u64; K]`.
//!
//! Absent coordinates are zero in both. `PointStamp` strips trailing zeros and `Flat` pads to `K`,
//! so both are canonical and their orders agree: lexicographic over stripped vectors (a prefix
//! first) is lexicographic over padded ones. The conversions preserve the partial order and the
//! lattice operations, as the proxy tactics require.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use timely::order::{PartialOrder, Product};
use timely::progress::{PathSummary, Timestamp};

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;

use crate::ir::Time;

/// `K` coordinates, partially ordered as a product and totally ordered lexicographically.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Flat<const K: usize>(pub [u64; K]);

/// The epoch, then the point stamp's coordinates. Panics beyond `K` coordinates.
impl<const K: usize> From<Time> for Flat<K> {
    #[inline]
    fn from(time: Time) -> Self {
        let inner = &time.inner[..];
        assert!(inner.len() < K, "a time with {} scope coordinates exceeds Flat<{K}>", inner.len());
        let mut out = [0; K];
        out[0] = time.outer;
        out[1..1 + inner.len()].copy_from_slice(inner);
        Flat(out)
    }
}

impl<const K: usize> From<Flat<K>> for Time {
    #[inline]
    fn from(flat: Flat<K>) -> Self {
        Product::new(flat.0[0], PointStamp::new(flat.0[1..].iter().copied().collect()))
    }
}

impl<const K: usize> PartialOrder for Flat<K> {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.0.iter().zip(other.0.iter()).all(|(a, b)| a <= b)
    }
}

impl<const K: usize> Lattice for Flat<K> {
    #[inline]
    fn join(&self, other: &Self) -> Self { let mut out = *self; out.join_assign(other); out }
    #[inline]
    fn join_assign(&mut self, other: &Self) {
        for (a, b) in self.0.iter_mut().zip(other.0.iter()) { *a = (*a).max(*b); }
    }
    #[inline]
    fn meet(&self, other: &Self) -> Self { let mut out = *self; out.meet_assign(other); out }
    #[inline]
    fn meet_assign(&mut self, other: &Self) {
        for (a, b) in self.0.iter_mut().zip(other.0.iter()) { *a = (*a).min(*b); }
    }
}

/// `Flat` is never a dataflow's timestamp, so its only path summary is the identity.
impl<const K: usize> Timestamp for Flat<K> {
    type Summary = ();
    fn minimum() -> Self { Flat([0; K]) }
}

impl<const K: usize> PathSummary<Flat<K>> for () {
    fn results_in(&self, time: &Flat<K>) -> Option<Flat<K>> { Some(*time) }
    fn followed_by(&self, _other: &()) -> Option<()> { Some(()) }
}

// serde derives arrays only to fixed lengths; the coordinates travel as a sequence.
impl<const K: usize> Serialize for Flat<K> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de, const K: usize> Deserialize<'de> for Flat<K> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let coords = Vec::<u64>::deserialize(deserializer)?;
        coords.try_into().map(Flat).map_err(|v: Vec<u64>| serde::de::Error::invalid_length(v.len(), &"K coordinates"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(outer: u64, coords: &[u64]) -> Time { Product::new(outer, PointStamp::new(coords.iter().copied().collect())) }

    #[test]
    fn conversions_round_trip_and_preserve_order_and_lattice() {
        let coords: [&[u64]; 9] = [&[], &[1], &[0, 1], &[2], &[1, 1], &[2, 1], &[0, 4], &[3, 0], &[1, 5]];
        let times: Vec<Time> = coords.iter().flat_map(|c| [time(0, c), time(5, c)]).collect();
        for a in &times {
            assert_eq!(Time::from(Flat::<3>::from(a.clone())), *a);
            for b in &times {
                let (fa, fb) = (Flat::<3>::from(a.clone()), Flat::<3>::from(b.clone()));
                assert_eq!(a.cmp(b), fa.cmp(&fb), "{a:?} vs {b:?}");
                assert_eq!(a.less_equal(b), fa.less_equal(&fb));
                assert_eq!(Flat::<3>::from(a.join(b)), fa.join(&fb));
                assert_eq!(Flat::<3>::from(a.meet(b)), fa.meet(&fb));
            }
        }
    }

    #[test]
    #[should_panic]
    fn conversion_refuses_too_many_coordinates() {
        let _ = Flat::<2>::from(time(0, &[1, 2]));
    }
}
