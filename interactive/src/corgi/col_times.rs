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

use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::int_proxy::TimeColumn;
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

/// A path summary applied to a row of lanes: what timely's `results_in` does to a time, on
/// its coordinates. Returns `false` where the result does not exist (an overflow), in which
/// case the row is dropped, as the owned form's `None` drops it.
pub trait LaneSummary {
    fn apply(&self, lanes: &mut Vec<u64>) -> bool;
}

impl LaneSummary for u64 {
    fn apply(&self, lanes: &mut Vec<u64>) -> bool {
        if lanes.is_empty() {
            lanes.push(0);
        }
        match lanes[0].checked_add(*self) {
            Some(x) => { lanes[0] = x; true }
            None => false,
        }
    }
}

/// Truncation to `retain` coordinates, then each action added to its coordinate, actions past
/// the end applied to the minimum: `PointStampSummary::results_in` on lanes.
impl LaneSummary for PointStampSummary<u64> {
    fn apply(&self, lanes: &mut Vec<u64>) -> bool {
        if let Some(retain) = self.retain {
            lanes.truncate(retain);
        }
        if lanes.len() < self.actions.len() {
            lanes.resize(self.actions.len(), 0);
        }
        for (lane, action) in lanes.iter_mut().zip(&self.actions) {
            match lane.checked_add(*action) {
                Some(x) => *lane = x,
                None => return false,
            }
        }
        true
    }
}

/// The outer summary on lane 0, the inner on the rest.
impl<B: LaneSummary> LaneSummary for Product<u64, B> {
    fn apply(&self, lanes: &mut Vec<u64>) -> bool {
        if lanes.is_empty() {
            lanes.push(0);
        }
        match lanes[0].checked_add(self.outer) {
            Some(x) => lanes[0] = x,
            None => return false,
        }
        let mut rest = lanes.split_off(1);
        let ok = self.inner.apply(&mut rest);
        lanes.append(&mut rest);
        ok
    }
}

/// A column of times as lanes: row `i` is `lanes[i * width .. (i + 1) * width]`.
#[derive(Clone)]
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

impl<T> ColTimes<T> {
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

    /// Room for `rows` more rows, so that filling a column of a known size grows it once
    /// rather than by doubling (which copies what is already there each time).
    #[inline]
    pub fn reserve(&mut self, rows: usize) {
        self.lanes.reserve(rows * self.width.max(1));
    }

    /// How many rows the allocation holds.
    #[inline]
    pub fn capacity_rows(&self) -> usize {
        if self.width == 0 { self.len } else { self.lanes.capacity() / self.width }
    }

    /// This column, its allocation cut to its rows when it is holding much more than it needs —
    /// what a caller that reserved for the worst case does before the result is kept. Cheap
    /// when the reservation was about right: the column is returned as it stands.
    pub fn shrunk(self) -> Self {
        if self.len == 0 {
            return ColTimes { lanes: Vec::new(), width: self.width, len: 0, scratch: self.scratch, _t: PhantomData };
        }
        if self.capacity_rows() <= 2 * self.len {
            return self;
        }
        let mut out = ColTimes { lanes: Vec::with_capacity(self.len * self.width), width: self.width, len: self.len, scratch: Vec::new(), _t: PhantomData };
        out.lanes.extend_from_slice(&self.lanes[..self.len * self.width]);
        out
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

    /// Append a row given as lanes, padded or widening as needed. A row is a few lanes, so this
    /// is a short loop rather than a memcpy call.
    pub(crate) fn push_row(&mut self, row: &[u64]) {
        if row.len() > self.width {
            self.widen(row.len());
        }
        let w = self.width;
        self.lanes.reserve(w);
        for k in 0..w {
            self.lanes.push(row.get(k).copied().unwrap_or(0));
        }
        self.len += 1;
    }

    /// Append an owned time; its row.
    #[inline]
    pub fn push(&mut self, t: &T) -> usize
    where
        T: Lanes,
    {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        t.write_lanes(&mut scratch);
        self.push_row(&scratch);
        self.scratch = scratch;
        self.len - 1
    }

    /// Append a copy of this column's own row `i`; its row.
    #[inline]
    pub fn push_copy(&mut self, i: usize) -> usize {
        let w = self.width;
        self.lanes.reserve(w);
        for k in 0..w {
            let x = self.lanes[i * w + k];
            self.lanes.push(x);
        }
        self.len += 1;
        self.len - 1
    }

    /// Insert row `i` of `other` into this column read as an antichain: dropped if some row is at
    /// or below it, else added and the rows at or above it dropped. The antichain of a set of
    /// times, built one row at a time, without a time built.
    pub fn insert_antichain(&mut self, other: &ColTimes<T>, i: usize) {
        if (0..self.len).any(|r| self.less_equal_cross(r, other, i)) {
            return;
        }
        let w = self.width.max(other.width);
        let mut kept = ColTimes { lanes: Vec::with_capacity((self.len + 1) * w), width: w, len: 0, scratch: Vec::new(), _t: PhantomData };
        for r in 0..self.len {
            if !other.less_equal_cross(i, self, r) {
                kept.push_row(self.row(r));
            }
        }
        kept.push_row(other.row(i));
        *self = kept;
    }

    /// Whether this column's row `i` is at or below `other`'s row `j`, widths padded with zeros.
    pub fn less_equal_cross(&self, i: usize, other: &ColTimes<T>, j: usize) -> bool {
        let (a, b) = (self.row(i), other.row(j));
        let n = a.len().max(b.len());
        (0..n).all(|k| a.get(k).copied().unwrap_or(0) <= b.get(k).copied().unwrap_or(0))
    }

    /// Whether row `i` is at or below row `j` in the partial order: lane by lane.
    #[inline]
    pub fn less_equal(&self, i: usize, j: usize) -> bool {
        self.row(i).iter().zip(self.row(j)).all(|(a, b)| a <= b)
    }

    /// Row `i` becomes its join with row `j`: a lane-wise max.
    #[inline]
    pub fn join_assign(&mut self, i: usize, j: usize) {
        let w = self.width;
        for k in 0..w {
            let other = self.lanes[j * w + k];
            let x = &mut self.lanes[i * w + k];
            *x = (*x).max(other);
        }
    }

    /// Row `i` becomes its meet with row `j`: a lane-wise min.
    #[inline]
    pub fn meet_assign(&mut self, i: usize, j: usize) {
        let w = self.width;
        for k in 0..w {
            let other = self.lanes[j * w + k];
            let x = &mut self.lanes[i * w + k];
            *x = (*x).min(other);
        }
    }

    /// Append `other`'s row `i` — a row copy, no `T`; its row.
    #[inline]
    pub fn push_ref(&mut self, other: &ColTimes<T>, i: usize) -> usize {
        self.push_row(other.row(i));
        self.len - 1
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
    pub fn get(&self, i: usize) -> T
    where
        T: Lanes,
    {
        T::from_lanes(self.row(i))
    }

    /// Materialize the whole column to `Vec<T>` — the egress boundary.
    pub fn to_vec(&self) -> Vec<T>
    where
        T: Lanes,
    {
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
    fn frontier_rows(&mut self, frontier: AntichainRef<T>) -> Vec<u64>
    where
        T: Lanes,
    {
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
    pub fn advance_by(&mut self, frontier: AntichainRef<T>, end: usize)
    where
        T: Lanes,
    {
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

    /// The rows `idx[..]`, in that order, as a column.
    pub fn gather(&self, idx: &[usize]) -> ColTimes<T> {
        let w = self.width;
        let mut out = ColTimes { lanes: Vec::with_capacity(idx.len() * w), width: w, len: 0, scratch: Vec::new(), _t: PhantomData };
        for &i in idx {
            for k in 0..w {
                out.lanes.push(self.lanes[i * w + k]);
            }
        }
        out.len = idx.len();
        out
    }

    /// Lane `k` of every row, zero where the column is narrower.
    pub fn lane(&self, k: usize) -> Vec<u64> {
        if k >= self.width {
            return vec![0; self.len];
        }
        (0..self.len).map(|i| self.lanes[i * self.width + k]).collect()
    }

    /// Raise lane `k` of each row to at least `values[i]` (a join with a time that is `values[i]`
    /// at lane `k` and the minimum elsewhere), widening the column if lane `k` is beyond it.
    pub fn lane_max(&mut self, k: usize, values: &[u64]) {
        debug_assert_eq!(values.len(), self.len);
        if k >= self.width {
            self.widen(k + 1);
        }
        let w = self.width;
        for (i, &v) in values.iter().enumerate() {
            let x = &mut self.lanes[i * w + k];
            *x = (*x).max(v);
        }
    }

    /// Keep only the first `width` lanes of every row (dropping trailing coordinates, which is
    /// what leaving a scope does); a narrower column is left as it is.
    pub fn truncate_lanes(&mut self, width: usize) {
        if width >= self.width {
            return;
        }
        let old = self.width;
        for i in 0..self.len {
            self.lanes.copy_within(i * old..i * old + width, i * width);
        }
        self.lanes.truncate(self.len * width);
        self.width = width;
    }

    /// Apply a path summary to every row: the rows the summary carries over (as lanes) and the
    /// indices of the rows it dropped as overflowed, if any.
    pub fn results_in<S: LaneSummary>(&self, step: &S) -> (ColTimes<T>, Option<Vec<usize>>) {
        let mut out = ColTimes::new();
        let mut kept: Vec<usize> = Vec::with_capacity(self.len);
        let mut scratch = Vec::new();
        for i in 0..self.len {
            scratch.clear();
            scratch.extend_from_slice(self.row(i));
            if step.apply(&mut scratch) {
                out.push_row(&scratch);
                kept.push(i);
            }
        }
        let dropped = if kept.len() == self.len { None } else { Some(kept) };
        (out, dropped)
    }

    /// The raw lanes, for a wire format: `(width, rows, lanes)`.
    pub fn raw(&self) -> (usize, usize, &[u64]) {
        (self.width, self.len, &self.lanes)
    }

    /// A column from raw lanes (`lanes.len() == width * rows`).
    pub fn from_raw(width: usize, rows: usize, lanes: Vec<u64>) -> Self {
        assert_eq!(lanes.len(), width * rows, "lane column: {} lanes for {rows} rows of {width}", lanes.len());
        ColTimes { lanes, width, len: rows, scratch: Vec::new(), _t: PhantomData }
    }

    /// For each row, whether some frontier element is at or below it (`frontier.less_equal`):
    /// the partial order lane by lane. An empty frontier is below nothing.
    pub fn beyond(&mut self, frontier: AntichainRef<T>) -> Vec<bool>
    where
        T: Lanes,
    {
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

/// The proxy tactics' column: every verb lane-wise, rows padded with zeros where widths differ.
impl<T: Lanes> TimeColumn for ColTimes<T> {
    type Time = T;
    fn len(&self) -> usize { self.len }
    fn clear(&mut self) { ColTimes::clear(self) }
    fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.lanes.truncate(len * self.width);
            self.len = len;
        }
    }
    fn push(&mut self, time: &T) -> usize { ColTimes::push(self, time) }
    fn push_from(&mut self, other: &Self, i: usize) -> usize {
        self.push_row(other.row(i));
        self.len - 1
    }
    fn push_copy(&mut self, i: usize) -> usize { ColTimes::push_copy(self, i) }
    fn push_range(&mut self, other: &Self, s: usize, e: usize) { ColTimes::push_range(self, other, s, e) }
    fn get(&self, i: usize) -> T { ColTimes::get(self, i) }
    fn cmp(&self, i: usize, j: usize) -> Ordering { ColTimes::cmp(self, i, j) }
    fn cmp_cross(&self, i: usize, other: &Self, j: usize) -> Ordering { ColTimes::cmp_cross(self, i, other, j) }
    fn less_equal(&self, i: usize, j: usize) -> bool { ColTimes::less_equal(self, i, j) }
    fn less_equal_cross(&self, i: usize, other: &Self, j: usize) -> bool { ColTimes::less_equal_cross(self, i, other, j) }
    fn join_assign(&mut self, i: usize, j: usize) { ColTimes::join_assign(self, i, j) }
    fn meet_assign(&mut self, i: usize, j: usize) { ColTimes::meet_assign(self, i, j) }
    fn push_join_cross(&mut self, a: &Self, i: usize, b: &Self, j: usize) -> usize {
        let (x, y) = (a.row(i), b.row(j));
        let n = x.len().max(y.len());
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        scratch.extend((0..n).map(|k| x.get(k).copied().unwrap_or(0).max(y.get(k).copied().unwrap_or(0))));
        self.push_row(&scratch);
        self.scratch = scratch;
        self.len - 1
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

impl<'a, T: Lanes> FromIterator<&'a T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = &'a T>>(iter: I) -> Self {
        let mut out = ColTimes::new();
        for t in iter {
            out.push(t);
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

    /// The lane verbs the containers use agree with the owned operations: a path summary on
    /// every row (`results_in`), a lane raised by a per-row delay (`ENTER_AT`), a lane read out
    /// (`LIFT_ITER`), truncation (leaving a scope), and a gather.
    #[test]
    fn lane_verbs_match_owned_operations() {
        use timely::progress::PathSummary;
        let times: Vec<T> = vec![t(0, &[]), t(0, &[2]), t(1, &[1, 1]), t(2, &[0, 3]), t(3, &[4]), t(1, &[5, 0, 2])];
        let col: ColTimes<T> = times.iter().cloned().collect();
        let steps = [
            Product::new(0u64, PointStampSummary { retain: None, actions: vec![0, 1] }),
            Product::new(1u64, PointStampSummary { retain: Some(1), actions: vec![] }),
            Product::new(0u64, PointStampSummary { retain: Some(0), actions: vec![7] }),
            Product::new(0u64, PointStampSummary { retain: None, actions: vec![0, 0, 0, 1] }),
        ];
        for step in &steps {
            let (out, dropped) = col.results_in(step);
            let want: Vec<T> = times.iter().map(|x| step.results_in(x).unwrap()).collect();
            assert!(dropped.is_none());
            assert_eq!(out.to_vec(), want, "{step:?}");
        }
        let delays = vec![3u64, 0, 1, 5, 0, 0];
        let mut raised = col.clone();
        raised.lane_max(2, &delays);
        let want: Vec<T> = times.iter().zip(&delays).map(|(x, &d)| x.join(&t(0, &[0, d]))).collect();
        assert_eq!(raised.to_vec(), want);
        assert_eq!(col.lane(1), vec![0, 2, 1, 0, 4, 5]);
        assert_eq!(col.lane(3), vec![0, 0, 0, 0, 0, 2]);
        assert_eq!(col.lane(9), vec![0; 6]);
        let mut cut = col.clone();
        cut.truncate_lanes(2);
        assert_eq!(cut.to_vec(), vec![t(0, &[]), t(0, &[2]), t(1, &[1]), t(2, &[0]), t(3, &[4]), t(1, &[5])]);
        assert_eq!(col.gather(&[5, 0, 2]).to_vec(), vec![times[5].clone(), times[0].clone(), times[2].clone()]);
        let (w, n, lanes) = col.raw();
        assert_eq!(ColTimes::<T>::from_raw(w, n, lanes.to_vec()).to_vec(), times);
    }

    /// The proxy tactics' verbs agree with the timestamp's own order and lattice: partial order,
    /// join and meet in place, a copied row, a cross-column join, a truncation.
    #[test]
    fn column_verbs_match_the_lattice() {
        use timely::PartialOrder;
        let times: Vec<T> = vec![t(0, &[]), t(0, &[2]), t(1, &[1, 1]), t(2, &[0, 3]), t(3, &[4]), t(1, &[5, 0, 2])];
        let col: ColTimes<T> = times.iter().cloned().collect();
        let narrow: ColTimes<T> = vec![t(1, &[1]), t(0, &[3])].into_iter().collect();
        for i in 0..times.len() {
            for j in 0..times.len() {
                assert_eq!(TimeColumn::less_equal(&col, i, j), times[i].less_equal(&times[j]), "{:?} <= {:?}", times[i], times[j]);
                let mut c = col.clone();
                TimeColumn::join_assign(&mut c, i, j);
                assert_eq!(c.get(i), times[i].join(&times[j]));
                let mut c = col.clone();
                TimeColumn::meet_assign(&mut c, i, j);
                assert_eq!(c.get(i), times[i].meet(&times[j]));
                let mut c = col.clone();
                let r = TimeColumn::push_join(&mut c, i, j);
                assert_eq!(c.get(r), times[i].join(&times[j]));
            }
            for j in 0..2 {
                assert_eq!(TimeColumn::less_equal_cross(&col, i, &narrow, j), times[i].less_equal(&narrow.get(j)));
                assert_eq!(TimeColumn::less_equal_cross(&narrow, j, &col, i), narrow.get(j).less_equal(&times[i]));
                let mut out = ColTimes::<T>::new();
                let r = TimeColumn::push_join_cross(&mut out, &col, i, &narrow, j);
                assert_eq!(out.get(r), times[i].join(&narrow.get(j)));
            }
        }
        let mut c = col.clone();
        let r = TimeColumn::push_copy(&mut c, 5);
        assert_eq!(c.get(r), times[5]);
        TimeColumn::truncate(&mut c, 2);
        assert_eq!(c.to_vec(), times[..2]);
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
