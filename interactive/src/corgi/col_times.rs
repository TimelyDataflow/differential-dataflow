//! Per-tuple times for `CorgiChunk`, stored as lanes of integers.
//!
//! Every DDIR time is a product of integers: an epoch and one coordinate per enclosing scope.
//! A column of them is `width` lanes of `u64`, one lane per coordinate, `width` the most
//! coordinates any row carries; shorter rows are padded with zero, each coordinate's minimum.
//! The chunk's time operations are then lane-wise passes over integers, and a timestamp is only
//! built where DD wants one (`get`, `to_vec`, and the residual frontier).
//!
//! The total order is the lexicographic order of the lanes, which is the timestamps' own `Ord`:
//! a `PointStamp` strips trailing zeros, and a shorter prefix sorts first just as zero padding does.
//! The partial order and the lattice operations are lane-wise, as a product of chains is distributive.

use std::cmp::Ordering;
use std::marker::PhantomData;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;
use timely::order::Product;
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

/// A timestamp that is a product of integers, each with minimum zero.
pub trait Lanes: Sized {
    /// Append the coordinates, in order.
    fn write_lanes(&self, out: &mut Vec<u64>);
    /// The time with these coordinates; trailing zeros beyond its own coordinates are allowed.
    fn from_lanes(lanes: &[u64]) -> Self;
}

impl Lanes for u64 {
    fn write_lanes(&self, out: &mut Vec<u64>) { out.push(*self); }
    fn from_lanes(lanes: &[u64]) -> Self { lanes.first().copied().unwrap_or(0) }
}

impl Lanes for PointStamp<u64> {
    fn write_lanes(&self, out: &mut Vec<u64>) { out.extend_from_slice(self); }
    // `new` strips trailing zeros, which is the canonical form.
    fn from_lanes(lanes: &[u64]) -> Self { PointStamp::new(lanes.iter().copied().collect()) }
}

impl<B: Lanes> Lanes for Product<u64, B> {
    fn write_lanes(&self, out: &mut Vec<u64>) {
        out.push(self.outer);
        self.inner.write_lanes(out);
    }
    fn from_lanes(lanes: &[u64]) -> Self {
        Product::new(lanes.first().copied().unwrap_or(0), B::from_lanes(lanes.get(1..).unwrap_or(&[])))
    }
}

/// A timestamp usable as a lane column: DD's time algebra plus [`Lanes`].
pub trait ColTime: Timestamp + Lattice + Lanes {}
impl<T: Timestamp + Lattice + Lanes> ColTime for T {}

/// A column of times as lanes: `lanes[j][i]` is coordinate `j` of row `i`.
pub struct ColTimes<T> {
    lanes: Vec<Vec<u64>>,
    len: usize,
    _t: PhantomData<T>,
}

impl<T> Default for ColTimes<T> {
    fn default() -> Self { ColTimes { lanes: Vec::new(), len: 0, _t: PhantomData } }
}

impl<T: Lanes> ColTimes<T> {
    #[inline]
    pub fn new() -> Self { Self::default() }

    #[inline]
    pub fn len(&self) -> usize { self.len }

    #[inline]
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Empty the column, retaining allocation.
    pub fn clear(&mut self) {
        for lane in &mut self.lanes { lane.clear(); }
        self.len = 0;
    }

    /// Coordinate `j` of row `i`, zero beyond the column's width.
    #[inline]
    fn at(&self, j: usize, i: usize) -> u64 {
        self.lanes.get(j).map_or(0, |lane| lane[i])
    }

    /// Add zero lanes until the column has `width` lanes.
    fn widen(&mut self, width: usize) {
        while self.lanes.len() < width {
            self.lanes.push(vec![0; self.len]);
        }
    }

    /// Append an owned time.
    pub fn push(&mut self, t: &T) {
        self.push_with(t, &mut Vec::new());
    }

    fn push_with(&mut self, t: &T, scratch: &mut Vec<u64>) {
        scratch.clear();
        t.write_lanes(scratch);
        self.widen(scratch.len());
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            lane.push(scratch.get(j).copied().unwrap_or(0));
        }
        self.len += 1;
    }

    /// Append `other`'s row `i`.
    #[inline]
    pub fn push_ref(&mut self, other: &ColTimes<T>, i: usize) {
        self.push_range(other, i, i + 1);
    }

    /// Append rows `[s, e)` of `other`, one slice copy per lane.
    pub fn push_range(&mut self, other: &ColTimes<T>, s: usize, e: usize) {
        if e <= s { return; }
        self.widen(other.lanes.len());
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            match other.lanes.get(j) {
                Some(src) => lane.extend_from_slice(&src[s..e]),
                None => lane.resize(lane.len() + (e - s), 0),
            }
        }
        self.len += e - s;
    }

    /// A new column of the selected rows, in selection order.
    pub fn gather(&self, rows: &[usize]) -> Self {
        let lanes = self.lanes.iter().map(|lane| rows.iter().map(|&i| lane[i]).collect()).collect();
        ColTimes { lanes, len: rows.len(), _t: PhantomData }
    }

    /// The owned time at row `i`. Reserve for boundaries where DD wants a timestamp.
    pub fn get(&self, i: usize) -> T {
        let row: Vec<u64> = self.lanes.iter().map(|lane| lane[i]).collect();
        T::from_lanes(&row)
    }

    /// Materialize the whole column to `Vec<T>`.
    pub fn to_vec(&self) -> Vec<T> {
        (0..self.len).map(|i| self.get(i)).collect()
    }

    /// Order rows `i` and `j` within this column.
    #[inline]
    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        for lane in &self.lanes {
            match lane[i].cmp(&lane[j]) {
                Ordering::Equal => continue,
                o => return o,
            }
        }
        Ordering::Equal
    }

    /// Order this column's row `i` against `other`'s row `j`.
    #[inline]
    pub fn cmp_cross(&self, i: usize, other: &ColTimes<T>, j: usize) -> Ordering {
        for k in 0..self.lanes.len().max(other.lanes.len()) {
            match self.at(k, i).cmp(&other.at(k, j)) {
                Ordering::Equal => continue,
                o => return o,
            }
        }
        Ordering::Equal
    }

    /// Whether row `i` is less than or equal to row `j` in the partial order.
    fn less_equal(&self, i: usize, j: usize) -> bool {
        self.lanes.iter().all(|lane| lane[i] <= lane[j])
    }

    /// Advance rows `0..end` by `frontier`, in place. Advancing by a frontier is the meet over its
    /// elements of the joins with each; in a product of chains that is the join with the elements'
    /// meet, so each lane takes a max with one constant. An empty frontier leaves rows unchanged.
    pub fn advance_by(&mut self, frontier: AntichainRef<T>, end: usize) {
        if frontier.is_empty() || end == 0 { return; }
        let mut meet: Option<Vec<u64>> = None;
        let mut row = Vec::new();
        for f in frontier.iter() {
            row.clear();
            f.write_lanes(&mut row);
            match &mut meet {
                // Coordinates absent from either side are zero, so the meet is as short as the shortest.
                Some(m) => {
                    m.truncate(row.len());
                    for (m, &x) in m.iter_mut().zip(&row) { *m = (*m).min(x); }
                }
                None => meet = Some(row.clone()),
            }
        }
        let meet = meet.unwrap();
        self.widen(meet.len());
        for (lane, &m) in self.lanes.iter_mut().zip(&meet) {
            if m == 0 { continue; }
            for x in &mut lane[..end] { *x = (*x).max(m); }
        }
    }

    /// For each row, whether some element of `frontier` is less than or equal to it.
    pub fn beyond(&self, frontier: AntichainRef<T>) -> Vec<bool> {
        let mut beyond = vec![false; self.len];
        let mut dominated = vec![true; self.len];
        let mut row = Vec::new();
        for f in frontier.iter() {
            row.clear();
            f.write_lanes(&mut row);
            dominated.fill(true);
            for (j, &bound) in row.iter().enumerate() {
                if bound == 0 { continue; }
                match self.lanes.get(j) {
                    Some(lane) => for (d, &x) in dominated.iter_mut().zip(lane) { *d &= bound <= x; },
                    None => dominated.fill(false),
                }
            }
            for (b, &d) in beyond.iter_mut().zip(&dominated) { *b |= d; }
        }
        beyond
    }

    /// The minimal elements among the selected rows, as row indices, one per distinct minimal time.
    /// Quadratic in the number of minimal elements; callers first discard rows a known frontier covers.
    pub fn minimal(&self, rows: &[usize]) -> Vec<usize> {
        let mut minimal: Vec<usize> = Vec::new();
        for &i in rows {
            if minimal.iter().any(|&m| self.less_equal(m, i)) { continue; }
            minimal.retain(|&m| !self.less_equal(i, m));
            minimal.push(i);
        }
        minimal
    }
}

impl<T: Lanes> FromIterator<T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut out = ColTimes::new();
        let mut scratch = Vec::new();
        for t in iter { out.push_with(&t, &mut scratch); }
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
    fn range_copy_and_gather_across_widths() {
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
        let narrow: ColTimes<T> = values[..2].iter().cloned().collect();
        let mut wide: ColTimes<T> = std::iter::once(t(0, &[1, 2, 3])).collect();
        wide.push_range(&narrow, 0, 2);
        assert_eq!(wide.to_vec(), vec![t(0, &[1, 2, 3]), values[0].clone(), values[1].clone()]);
        assert_eq!(source.gather(&[4, 0, 4]).to_vec(), vec![values[4].clone(), values[0].clone(), values[4].clone()]);
    }

    /// The lane order must agree with the timestamp's own `Ord` for every pair: the chunk sorts and
    /// merges by the former and every other layer reasons with the latter.
    #[test]
    fn col_times_order_matches_owned_order() {
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[0]), t(0, &[1]), t(0, &[2]), t(0, &[3]),
            t(0, &[1, 1]), t(0, &[1, 2]), t(0, &[2, 1]), t(0, &[3, 1]), t(0, &[3, 2]),
            t(0, &[1, 1, 1]), t(0, &[3, 1, 2]), t(0, &[3, 2, 1]),
            t(1, &[]), t(1, &[3]), t(1, &[3, 1]),
        ];
        let store: ColTimes<T> = times.iter().cloned().collect();
        let narrow: ColTimes<T> = times[..5].iter().cloned().collect();
        for i in 0..times.len() {
            for j in 0..times.len() {
                assert_eq!(times[i].cmp(&times[j]), store.cmp(i, j), "{:?} vs {:?}", times[i], times[j]);
                if j < 5 {
                    assert_eq!(times[i].cmp(&times[j]), store.cmp_cross(i, &narrow, j));
                    assert_eq!(times[j].cmp(&times[i]), narrow.cmp_cross(j, &store, i));
                }
            }
        }
    }

    /// `advance_by`, `beyond` and `minimal` must agree with the lattice on owned timestamps.
    #[test]
    fn lattice_operations_match_owned_times() {
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
            let expected: Vec<_> = times.iter().map(|x| frontier.less_equal(x)).collect();
            assert_eq!(store.beyond(frontier.borrow()), expected, "beyond {frontier:?}");
            store.advance_by(frontier.borrow(), 5);
            let mut expected = times.clone();
            for x in expected[..5].iter_mut() { x.advance_by(frontier.borrow()); }
            assert_eq!(store.to_vec(), expected, "advance by {frontier:?}");
        }
        let store: ColTimes<T> = times.iter().cloned().chain(times.iter().cloned()).collect();
        let rows: Vec<usize> = (0..store.len()).rev().collect();
        let actual: Antichain<T> = store.minimal(&rows).into_iter().map(|i| store.get(i)).collect();
        assert_eq!(store.minimal(&rows).len(), actual.len(), "one row per distinct minimal time");
        assert_eq!(actual, times.iter().cloned().collect::<Antichain<T>>());
    }
}
