//! DDIR's timestamp: a list of nonnegative integer coordinates.
//!
//! Order and lattice operations read absent coordinates as zero: `[e]` and `[e, 0]` are
//! equal, and the empty list is the minimum. The partial order, join and meet are coordinate-wise;
//! `Ord` is lexicographic. Lengths are preserved, with join taking the longer length and meet
//! the shorter. [`TimeSummary`] adds or removes coordinates as collections cross scopes.
//!
//! Up to three coordinates are stored inline, with unused lanes held at zero so comparisons
//! and lattice operations can use the whole array. Longer times use a vector.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use differential_dataflow::lattice::Lattice;
use timely::order::PartialOrder;
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};

const INLINE: usize = 3;

/// A time: its coordinates, in order. See the module documentation.
#[derive(Clone)]
pub struct Time {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    /// `len` coordinates in `lanes[..len]`; the lanes beyond are zero.
    Inline([u64; INLINE], u8),
    /// More than `INLINE` coordinates.
    Heap(Vec<u64>),
}

use Repr::{Heap, Inline};

impl Time {
    /// The time with these coordinates.
    pub fn new(lanes: impl IntoIterator<Item = u64>) -> Self {
        let mut iter = lanes.into_iter();
        let mut inline = [0; INLINE];
        let mut len = 0;
        for x in iter.by_ref() {
            if len == INLINE {
                let mut v = Vec::with_capacity(2 * INLINE + iter.size_hint().0);
                v.extend_from_slice(&inline);
                v.push(x);
                v.extend(iter);
                return Time { repr: Heap(v) };
            }
            inline[len] = x;
            len += 1;
        }
        Time { repr: Inline(inline, len as u8) }
    }

    /// The time with these coordinates, keeping the vector when they do not fit inline.
    fn from_vec(lanes: Vec<u64>) -> Self {
        if lanes.len() > INLINE { Time { repr: Heap(lanes) } } else { Time::new(lanes) }
    }

    /// The coordinates.
    #[inline]
    pub fn as_slice(&self) -> &[u64] {
        match &self.repr {
            Inline(lanes, len) => &lanes[..*len as usize],
            Heap(lanes) => lanes,
        }
    }

    /// Coordinate `j`, zero when the time has fewer.
    #[inline]
    pub fn lane(&self, j: usize) -> u64 {
        match &self.repr {
            Inline(lanes, _) => lanes.get(j).copied().unwrap_or(0),
            Heap(lanes) => lanes.get(j).copied().unwrap_or(0),
        }
    }

    /// Append a coordinate: entering a scope appends a zero.
    pub fn push(&mut self, x: u64) {
        match &mut self.repr {
            Inline(lanes, len) if (*len as usize) < INLINE => {
                lanes[*len as usize] = x;
                *len += 1;
            }
            Inline(lanes, _) => {
                let mut v = Vec::with_capacity(INLINE + 1);
                v.extend_from_slice(lanes);
                v.push(x);
                self.repr = Heap(v);
            }
            Heap(lanes) => lanes.push(x),
        }
    }

    /// Keep the first `n` coordinates: leaving a scope drops one.
    pub fn truncate(&mut self, n: usize) {
        match &mut self.repr {
            Inline(lanes, len) => {
                for x in &mut lanes[n.min(INLINE)..] {
                    *x = 0;
                }
                *len = (*len as usize).min(n) as u8;
            }
            Heap(v) if n <= INLINE => {
                let mut lanes = [0; INLINE];
                lanes[..n].copy_from_slice(&v[..n]);
                self.repr = Inline(lanes, n as u8);
            }
            Heap(v) => v.truncate(n),
        }
    }

    /// The coordinates through the last non-zero one: what identifies the time.
    fn trimmed(&self) -> &[u64] {
        let lanes = self.as_slice();
        let mut n = lanes.len();
        while n > 0 && lanes[n - 1] == 0 {
            n -= 1;
        }
        &lanes[..n]
    }
}

impl Default for Time {
    fn default() -> Self {
        Time { repr: Inline([0; INLINE], 0) }
    }
}

impl std::ops::Deref for Time {
    type Target = [u64];
    #[inline]
    fn deref(&self) -> &[u64] {
        self.as_slice()
    }
}

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

/// Operations on slices with absent coordinates read as zero.
mod padded {
    use std::cmp::Ordering;

    pub(super) fn eq(a: &[u64], b: &[u64]) -> bool {
        let n = a.len().min(b.len());
        a[..n] == b[..n] && a[n..].iter().all(|&x| x == 0) && b[n..].iter().all(|&x| x == 0)
    }

    pub(super) fn cmp(a: &[u64], b: &[u64]) -> Ordering {
        let n = a.len().min(b.len());
        match a[..n].cmp(&b[..n]) {
            // The longer time is the greater exactly when its extra coordinates are not all zero.
            Ordering::Equal if a[n..].iter().any(|&x| x != 0) => Ordering::Greater,
            Ordering::Equal if b[n..].iter().any(|&x| x != 0) => Ordering::Less,
            o => o,
        }
    }

    pub(super) fn less_equal(a: &[u64], b: &[u64]) -> bool {
        let n = a.len().min(b.len());
        a[..n].iter().zip(&b[..n]).all(|(x, y)| x <= y) && a[n..].iter().all(|&x| x == 0)
    }

    /// `f` over paired coordinates, the longer's tail paired with zeros.
    pub(super) fn zip_with(a: &[u64], b: &[u64], f: impl Fn(u64, u64) -> u64) -> Vec<u64> {
        (0..a.len().max(b.len()))
            .map(|j| f(a.get(j).copied().unwrap_or(0), b.get(j).copied().unwrap_or(0)))
            .collect()
    }
}

impl PartialEq for Time {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (&self.repr, &other.repr) {
            (Inline(a, _), Inline(b, _)) => a == b,
            _ => padded::eq(self, other),
        }
    }
}

impl Eq for Time {}

impl Hash for Time {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.trimmed().hash(state)
    }
}

impl Ord for Time {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.repr, &other.repr) {
            (Inline(a, _), Inline(b, _)) => a.cmp(b),
            _ => padded::cmp(self, other),
        }
    }
}

impl PartialOrd for Time {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrder for Time {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        match (&self.repr, &other.repr) {
            (Inline(a, _), Inline(b, _)) => a.iter().zip(b).all(|(x, y)| x <= y),
            _ => padded::less_equal(self, other),
        }
    }
}

impl Lattice for Time {
    #[inline]
    fn join(&self, other: &Self) -> Self {
        match (&self.repr, &other.repr) {
            (Inline(a, na), Inline(b, nb)) => {
                Time { repr: Inline(std::array::from_fn(|j| a[j].max(b[j])), (*na).max(*nb)) }
            }
            _ => Time::from_vec(padded::zip_with(self, other, u64::max)),
        }
    }

    #[inline]
    fn join_assign(&mut self, other: &Self) {
        match (&mut self.repr, &other.repr) {
            (Inline(a, na), Inline(b, nb)) => {
                for (x, y) in a.iter_mut().zip(b) {
                    *x = (*x).max(*y);
                }
                *na = (*na).max(*nb);
            }
            (Heap(a), _) if a.len() >= other.len() => {
                for (x, y) in a.iter_mut().zip(other.as_slice()) {
                    *x = (*x).max(*y);
                }
            }
            _ => *self = self.join(other),
        }
    }

    /// As long as the shorter time: coordinates absent from either input meet zero.
    #[inline]
    fn meet(&self, other: &Self) -> Self {
        match (&self.repr, &other.repr) {
            (Inline(a, na), Inline(b, nb)) => {
                Time { repr: Inline(std::array::from_fn(|j| a[j].min(b[j])), (*na).min(*nb)) }
            }
            _ => {
                let mut lanes = padded::zip_with(self, other, u64::min);
                lanes.truncate(self.len().min(other.len()));
                Time::from_vec(lanes)
            }
        }
    }
}

impl Timestamp for Time {
    type Summary = TimeSummary;
    fn minimum() -> Self {
        Time::default()
    }
}

/// Under a root scope at the epoch: the epoch is coordinate 0.
impl Refines<u64> for Time {
    fn to_inner(epoch: u64) -> Self {
        Time::new([epoch])
    }
    fn to_outer(self) -> u64 {
        self.lane(0)
    }
    fn summarize(path: TimeSummary) -> u64 {
        debug_assert_ne!(path.retain, Some(0), "a summary never drops the epoch");
        path.actions.first().copied().unwrap_or(0)
    }
}

/// A dataflow can also use this time directly, without an outer clock.
impl Refines<()> for Time {
    fn to_inner(_: ()) -> Self {
        Time::minimum()
    }
    fn to_outer(self) {}
    fn summarize(_: TimeSummary) {}
}

impl serde::Serialize for Time {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_slice().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Time {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<u64>::deserialize(deserializer).map(Time::from_vec)
    }
}

/// A path summary: keep the first `retain` coordinates (all when `None`), then add `actions[j]`
/// to coordinate `j`, appending the coordinates the time lacks (they start at zero).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct TimeSummary {
    pub retain: Option<usize>,
    pub actions: Vec<u64>,
}

impl TimeSummary {
    /// Entering a scope of `width` coordinates: a time one shorter gains a zero.
    pub fn enter(width: usize) -> Self {
        TimeSummary { retain: None, actions: vec![0; width] }
    }

    /// Leaving to a scope of `width` coordinates: the rest are dropped.
    pub fn leave(width: usize) -> Self {
        TimeSummary { retain: Some(width), actions: Vec::new() }
    }

    /// Feedback: add `by` to coordinate `lane`.
    pub fn advance(lane: usize, by: u64) -> Self {
        let mut actions = vec![0; lane + 1];
        actions[lane] = by;
        TimeSummary { retain: None, actions }
    }
}

impl PathSummary<Time> for TimeSummary {
    fn results_in(&self, t: &Time) -> Option<Time> {
        let kept = self.retain.map_or(t.len(), |r| r.min(t.len()));
        let mut out = t[..kept].to_vec();
        for (j, &a) in self.actions.iter().enumerate() {
            if j < kept {
                out[j] = out[j].checked_add(a)?;
            } else {
                out.push(a);
            }
        }
        Some(Time::from_vec(out))
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        let retain = match (self.retain, other.retain) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (x, None) => x,
            (None, y) => y,
        };
        // Of this summary's actions, `other` keeps those before its `retain`.
        let mine = match other.retain {
            Some(r) if r < self.actions.len() => &self.actions[..r],
            _ => &self.actions[..],
        };
        let n = mine.len().min(other.actions.len());
        let mut actions = Vec::with_capacity(mine.len().max(other.actions.len()));
        for (a, b) in mine.iter().zip(&other.actions) {
            actions.push(a.checked_add(*b)?);
        }
        actions.extend_from_slice(&mine[n..]);
        actions.extend_from_slice(&other.actions[n..]);
        Some(TimeSummary { retain, actions })
    }
}

impl PartialOrder for TimeSummary {
    fn less_equal(&self, other: &Self) -> bool {
        // Summaries with different `retain`s are incomparable: some coordinate may come out
        // either larger or smaller, depending on the time.
        self.retain == other.retain
            && self.actions.len() <= other.actions.len()
            && self.actions.iter().zip(&other.actions).all(|(a, b)| a <= b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(lanes: &[u64]) -> Time {
        Time::new(lanes.iter().copied())
    }

    /// Entering and leaving nested scopes to depth 3, by summary and by the owned operations,
    /// with the feedback summary at each depth: times inside a scope have its width. Depth 3
    /// is one coordinate past the inline storage, so this crosses to the heap and back.
    #[test]
    fn enter_feedback_leave_to_depth_three() {
        let mut t = s(&[7]);
        for depth in 1..=3 {
            let width = depth + 1;
            let entered = TimeSummary::enter(width).results_in(&t).unwrap();
            t.push(0);
            assert_eq!(entered, t);
            assert_eq!(entered.len(), width);
            let stepped = TimeSummary::advance(depth, 1).results_in(&t).unwrap();
            assert_eq!(stepped.lane(depth), 1);
            assert!(t.less_equal(&stepped) && !stepped.less_equal(&t));
            t = stepped;
        }
        assert_eq!(t, s(&[7, 1, 1, 1]));
        assert_eq!(t.join(&s(&[8, 0, 2])), s(&[8, 1, 2, 1]));
        assert_eq!(t.meet(&s(&[8, 0, 2])), s(&[7, 0, 1, 0]));
        for depth in (0..3).rev() {
            let width = depth + 1;
            let left = TimeSummary::leave(width).results_in(&t).unwrap();
            t.truncate(width);
            assert_eq!(left, t);
            assert_eq!(left.len(), width);
        }
        assert_eq!(t, s(&[7]));
        assert_eq!(t.join(&s(&[6, 4])), s(&[7, 4]));
        // A loop's round trip composes to the loop's own step at its lane, and enter then leave
        // to a summary that acts as the identity.
        let round = TimeSummary::enter(2).followed_by(&TimeSummary::advance(1, 1)).unwrap().followed_by(&TimeSummary::leave(1)).unwrap();
        assert_eq!(round.results_in(&s(&[3])), Some(s(&[3])));
        let twice = TimeSummary::advance(1, 1).followed_by(&TimeSummary::advance(1, 1)).unwrap();
        assert_eq!(twice, TimeSummary::advance(1, 2));
        assert_eq!(TimeSummary::advance(1, u64::MAX).results_in(&s(&[0, 1])), None);
    }

    /// Absent coordinates are zero: `minimum()` is below everything, a shorter time equals its
    /// zero-padded form in every trait, and join, meet and order agree across lengths, inline
    /// and on the heap.
    #[test]
    fn absent_coordinates_are_zero() {
        use std::collections::hash_map::DefaultHasher;
        let h = |t: &Time| { let mut h = DefaultHasher::new(); t.hash(&mut h); h.finish() };
        let pairs = [(s(&[]), s(&[0, 0])), (s(&[4]), s(&[4, 0])), (s(&[4, 2]), s(&[4, 2, 0, 0])), (s(&[4, 2, 0, 0, 0]), s(&[4, 2, 0, 0]))];
        for (a, b) in &pairs {
            assert_eq!(a, b);
            assert_eq!(a.cmp(b), Ordering::Equal);
            assert_eq!(h(a), h(b));
            assert!(a.less_equal(b) && b.less_equal(a));
        }
        assert!(Time::minimum().less_equal(&s(&[0, 0, 5])));
        assert!(s(&[4]).less_equal(&s(&[4, 1])) && !s(&[4, 1]).less_equal(&s(&[4])));
        assert!(s(&[4]).less_equal(&s(&[4, 0, 0, 1])) && !s(&[4, 0, 0, 1]).less_equal(&s(&[4])));
        assert_eq!(s(&[4]).cmp(&s(&[4, 1])), Ordering::Less);
        assert_eq!(s(&[5]).cmp(&s(&[4, 1])), Ordering::Greater);
        assert_eq!(s(&[4, 0, 0, 1]).cmp(&s(&[4])), Ordering::Greater);
        assert_eq!(s(&[4, 1]).join(&s(&[5])), s(&[5, 1]));
        assert_eq!(s(&[4, 1]).meet(&s(&[5])), s(&[4, 0]));
        assert_eq!(s(&[4, 1]).meet(&s(&[5])).len(), 1, "the meet is as short as the shorter time");
        assert_eq!(s(&[4, 1, 2, 3]).meet(&s(&[5, 0])).len(), 2);
        assert_eq!(s(&[4, 1, 2]).join(&s(&[3, 5, 1])), s(&[4, 5, 2]));
        assert_eq!(s(&[4, 1, 2]).meet(&s(&[3, 5, 1])), s(&[3, 1, 1]));
        let mut t = s(&[4]);
        t.join_assign(&s(&[3, 2]));
        assert_eq!(t, s(&[4, 2]));
        t.join_assign(&s(&[3, 2, 0, 9]));
        assert_eq!(t, s(&[4, 2, 0, 9]));
        assert_eq!(<Time as Refines<u64>>::to_inner(9), s(&[9]));
        assert_eq!(<Time as Refines<u64>>::to_outer(s(&[9, 3])), 9u64);
    }
}
