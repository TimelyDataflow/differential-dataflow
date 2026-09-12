//! Per-tuple times for `CorgiChunk`, stored as lanes.
//!
//! Every DDIR time is a product of integers: an epoch and one iteration coordinate per enclosing
//! scope, `Product<u64, PointStamp<u64>>`. A column of them is therefore `width` lanes of `u64`
//! per row, `width` the most coordinates any row carries, shorter rows padded with the coordinate
//! minimum, zero. On lanes every operation the chunk performs is lane-wise, and no row ever
//! materializes a timestamp except at the boundaries where DD wants one (`get`, `to_vec`):
//!
//!   * the total order used for merging is the lexicographic order of the lanes — the
//!     timestamps' own `Ord`, since a `PointStamp` strips trailing zeros and a shorter prefix
//!     sorts first, exactly as the padded zeros do;
//!   * the partial order `less_equal` is a lane-wise comparison folded with and;
//!   * the lattice join is a lane-wise max and the meet a lane-wise min;
//!   * advancing by a frontier is one join with the frontier's meet: `advance_by` is the meet
//!     over the frontier of the joins with each element, and a product of chains is a
//!     distributive lattice, so that is the join with the meet of the elements — one constant
//!     per call, one max per lane per row.
//!
//! Rows are contiguous (`row(i)` is a slice), so comparisons, copies and gathers touch one span;
//! a lane-wise pass strides by `width`.

use std::cmp::Ordering;
use std::marker::PhantomData;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;
use timely::order::Product;
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

/// A timestamp that is a product of integers: its coordinates, in order, as `u64` lanes.
///
/// Requires: zero is each coordinate's minimum, so lanes beyond the ones written denote the same
/// time when zero, and `from_lanes` accepts such padding.
pub trait Lanes: Sized {
    /// Append the coordinates.
    fn write_lanes(&self, out: &mut Vec<u64>);
    /// The time with these coordinates, trailing zeros allowed beyond its own.
    fn from_lanes(lanes: &[u64]) -> Self;
}

impl Lanes for u64 {
    fn write_lanes(&self, out: &mut Vec<u64>) {
        out.push(*self);
    }
    fn from_lanes(lanes: &[u64]) -> Self {
        lanes.first().copied().unwrap_or(0)
    }
}

impl Lanes for PointStamp<u64> {
    fn write_lanes(&self, out: &mut Vec<u64>) {
        out.extend_from_slice(self);
    }
    fn from_lanes(lanes: &[u64]) -> Self {
        // `new` strips trailing zeros: the canonical form.
        PointStamp::new(lanes.iter().copied().collect())
    }
}

/// A product whose outer coordinate is one lane; the inner takes the rest.
impl<B: Lanes> Lanes for Product<u64, B> {
    fn write_lanes(&self, out: &mut Vec<u64>) {
        out.push(self.outer);
        self.inner.write_lanes(out);
    }
    fn from_lanes(lanes: &[u64]) -> Self {
        let outer = lanes.first().copied().unwrap_or(0);
        let inner = B::from_lanes(lanes.get(1..).unwrap_or(&[]));
        Product::new(outer, inner)
    }
}

/// A timestamp usable as a lane column: `Timestamp + Lattice` (DD's algebra) plus [`Lanes`].
pub trait ColTime: Timestamp + Lattice + Lanes {}
impl<T: Timestamp + Lattice + Lanes> ColTime for T {}

/// A column of times as lanes: row `i` is `lanes[i * width .. (i + 1) * width]`.
pub struct ColTimes<T> {
    lanes: Vec<u64>,
    width: usize,
    len: usize,
    scratch: Vec<u64>,
    _t: PhantomData<T>,
}

impl<T> Default for ColTimes<T> {
    fn default() -> Self {
        ColTimes { lanes: Vec::new(), width: 0, len: 0, scratch: Vec::new(), _t: PhantomData }
    }
}

impl<T: Lanes> ColTimes<T> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Empty the column, retaining allocation and width.
    #[inline]
    pub fn clear(&mut self) {
        self.lanes.clear();
        self.len = 0;
    }

    /// Row `i`'s lanes.
    #[inline]
    pub fn row(&self, i: usize) -> &[u64] {
        &self.lanes[i * self.width..(i + 1) * self.width]
    }

    /// Re-pad every row to `width` lanes (only ever wider).
    fn widen(&mut self, width: usize) {
        if width <= self.width {
            return;
        }
        let mut lanes = Vec::with_capacity(self.len * width);
        for i in 0..self.len {
            lanes.extend_from_slice(self.row(i));
            lanes.resize((i + 1) * width, 0);
        }
        self.lanes = lanes;
        self.width = width;
    }

    /// Append a row given as lanes, padded or widening as needed.
    fn push_row(&mut self, row: &[u64]) {
        if row.len() > self.width {
            self.widen(row.len());
        }
        self.lanes.extend_from_slice(row);
        self.lanes.resize((self.len + 1) * self.width, 0);
        self.len += 1;
    }

    /// Append an owned time.
    #[inline]
    pub fn push(&mut self, t: &T) {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        t.write_lanes(&mut scratch);
        self.push_row(&scratch);
        self.scratch = scratch;
    }

    /// Append `other`'s row `i` — a row copy, no `T`.
    #[inline]
    pub fn push_ref(&mut self, other: &ColTimes<T>, i: usize) {
        self.push_row(other.row(i));
    }

    /// Append rows `[s, e)` of `other`: one slice copy when the widths agree.
    pub fn push_range(&mut self, other: &ColTimes<T>, s: usize, e: usize) {
        if e <= s {
            return;
        }
        if other.width > self.width {
            self.widen(other.width);
        }
        if other.width == self.width {
            self.lanes.extend_from_slice(&other.lanes[s * other.width..e * other.width]);
            self.len += e - s;
        } else {
            for i in s..e {
                self.push_row(other.row(i));
            }
        }
    }

    /// The owned time at row `i` — materializes a `T`. Reserve for the emit boundary.
    #[inline]
    pub fn get(&self, i: usize) -> T {
        T::from_lanes(self.row(i))
    }

    /// Materialize the whole column to `Vec<T>` — the egress boundary.
    pub fn to_vec(&self) -> Vec<T> {
        (0..self.len).map(|i| self.get(i)).collect()
    }

    /// Order rows `i` and `j` within this column: lexicographic over the lanes.
    #[inline]
    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        self.row(i).cmp(self.row(j))
    }

    /// Order this column's row `i` against `other`'s row `j`, widths padded with zeros.
    #[inline]
    pub fn cmp_cross(&self, i: usize, other: &ColTimes<T>, j: usize) -> Ordering {
        cmp_padded(self.row(i), other.row(j))
    }

    /// The frontier's elements as rows of this column's width (an element wider than the column
    /// widens it first).
    fn frontier_rows(&mut self, frontier: AntichainRef<T>) -> Vec<u64> {
        let mut rows = Vec::new();
        for f in frontier.iter() {
            let at = rows.len();
            f.write_lanes(&mut rows);
            if rows.len() - at > self.width {
                self.widen(rows.len() - at);
            }
            rows.resize(at + self.width, 0);
        }
        rows
    }

    /// Advance rows `0..end` by `frontier`, in place: each row becomes the meet over the
    /// frontier of its joins with each element, which is its join with the frontier's meet —
    /// a lane-wise max with one constant row. An empty frontier leaves every row as it is.
    pub fn advance_by(&mut self, frontier: AntichainRef<T>, end: usize) {
        if frontier.is_empty() || end == 0 {
            return;
        }
        let rows = self.frontier_rows(frontier);
        let w = self.width;
        let mut meet = rows[..w].to_vec();
        for f in rows[w..].chunks(w) {
            for (m, &x) in meet.iter_mut().zip(f) {
                *m = (*m).min(x);
            }
        }
        for row in self.lanes[..end * w].chunks_mut(w) {
            for (x, &m) in row.iter_mut().zip(&meet) {
                *x = (*x).max(m);
            }
        }
    }

    /// For each row, whether some frontier element is at or below it (`frontier.less_equal`):
    /// the partial order lane by lane. An empty frontier is below nothing.
    pub fn beyond(&mut self, frontier: AntichainRef<T>) -> Vec<bool> {
        let rows = self.frontier_rows(frontier);
        let w = self.width;
        (0..self.len)
            .map(|i| {
                let t = self.row(i);
                rows.chunks(w).any(|f| f.iter().zip(t).all(|(a, b)| a <= b))
            })
            .collect()
    }
}

/// Lexicographic order of two lane rows, the shorter padded with zeros.
fn cmp_padded(a: &[u64], b: &[u64]) -> Ordering {
    let n = a.len().max(b.len());
    for k in 0..n {
        let (x, y) = (a.get(k).copied().unwrap_or(0), b.get(k).copied().unwrap_or(0));
        match x.cmp(&y) {
            Ordering::Equal => continue,
            o => return o,
        }
    }
    Ordering::Equal
}

impl<T: Lanes> FromIterator<T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut out = ColTimes::new();
        for t in iter {
            out.push(&t);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::progress::Antichain;

    type T = Product<u64, PointStamp<u64>>;

    fn t(outer: u64, coords: &[u64]) -> T {
        Product::new(outer, PointStamp::new(coords.iter().copied().collect()))
    }

    #[test]
    fn range_copy_rebases_variable_length_timestamps() {
        let values = vec![t(0, &[]), t(1, &[2]), t(2, &[3, 4]), t(3, &[]), t(4, &[5, 6, 7])];
        let source: ColTimes<T> = values.iter().cloned().collect();
        let mut copy: ColTimes<T> = std::iter::once(t(9, &[8, 7])).collect();
        copy.push_range(&source, 1, 4);
        copy.push_range(&source, 2, 2);
        copy.push_range(&source, 0, 5);
        let expected: Vec<_> = std::iter::once(t(9, &[8, 7]))
            .chain(values[1..4].iter().cloned()).chain(values.iter().cloned()).collect();
        assert_eq!(copy.to_vec(), expected);
        copy.clear();
        copy.push_range(&source, 4, 5);
        assert_eq!(copy.to_vec(), values[4..5]);
        // a narrower column copied into a wider one, and the reverse, both by range.
        let narrow: ColTimes<T> = vec![t(1, &[1]), t(2, &[])].into_iter().collect();
        let mut wide: ColTimes<T> = std::iter::once(t(0, &[1, 2, 3])).collect();
        wide.push_range(&narrow, 0, 2);
        assert_eq!(wide.to_vec(), vec![t(0, &[1, 2, 3]), t(1, &[1]), t(2, &[])]);
        let mut narrow2: ColTimes<T> = std::iter::once(t(5, &[])).collect();
        narrow2.push_range(&wide, 0, 3);
        assert_eq!(narrow2.to_vec(), vec![t(5, &[]), t(0, &[1, 2, 3]), t(1, &[1]), t(2, &[])]);
    }

    /// The lane order must agree with the timestamp's OWN `Ord` for every pair — the chunk layer
    /// sorts and merges by the former and every other layer reasons with the latter.
    #[test]
    fn col_times_order_matches_owned_order() {
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[0]), t(0, &[1]), t(0, &[2]), t(0, &[3]),
            t(0, &[1, 1]), t(0, &[1, 2]), t(0, &[2, 1]), t(0, &[3, 1]), t(0, &[3, 2]),
            t(0, &[1, 1, 1]), t(0, &[3, 1, 2]), t(0, &[3, 2, 1]),
            t(1, &[]), t(1, &[3]), t(1, &[3, 1]),
        ];
        let mut store = ColTimes::<T>::new();
        for x in &times { store.push(x); }
        let narrow: ColTimes<T> = times[..5].iter().cloned().collect();
        for i in 0..times.len() {
            for j in 0..times.len() {
                assert_eq!(times[i].cmp(&times[j]), store.cmp(i, j), "{:?} vs {:?}", times[i], times[j]);
                if j < 5 {
                    assert_eq!(times[i].cmp(&times[j]), store.cmp_cross(i, &narrow, j), "cross {:?} vs {:?}", times[i], times[j]);
                    assert_eq!(times[j].cmp(&times[i]), narrow.cmp_cross(j, &store, i), "cross {:?} vs {:?}", times[j], times[i]);
                }
            }
        }
        assert_eq!(store.get(7), times[7]);
    }

    /// Advancing by a frontier lane-wise must agree with `Lattice::advance_by` on every row, for
    /// frontiers of one, two and three elements of assorted lengths, and `beyond` with the
    /// frontier's `less_equal`.
    #[test]
    fn advance_and_beyond_match_the_lattice() {
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[2]), t(1, &[1, 1]), t(2, &[0, 3]), t(3, &[4]), t(1, &[5, 0, 2]), t(5, &[]),
        ];
        let frontiers = [
            Antichain::new(),
            Antichain::from_elem(t(2, &[1])),
            Antichain::from(vec![t(1, &[3]), t(2, &[1, 2])]),
            Antichain::from(vec![t(0, &[6]), t(1, &[2, 4]), t(3, &[0, 0, 1])]),
        ];
        for frontier in &frontiers {
            let mut store: ColTimes<T> = times.iter().cloned().collect();
            assert_eq!(
                store.beyond(frontier.borrow()),
                times.iter().map(|x| frontier.less_equal(x)).collect::<Vec<_>>(),
                "beyond {frontier:?}"
            );
            store.advance_by(frontier.borrow(), 5);
            let mut expected = times.clone();
            for x in expected[..5].iter_mut() { x.advance_by(frontier.borrow()); }
            assert_eq!(store.to_vec(), expected, "advance by {frontier:?}");
        }
    }
}
