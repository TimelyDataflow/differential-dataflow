//! The time column the proxy tactics work over.
//!
//! The tactics never hold a timestamp per record. Every time they read, compare, join or meet
//! is a row of a [`TimeColumn`], addressed by index, and the column implements the algebra:
//! a backend whose times are products of integers implements every verb lane-wise over a flat
//! store, while the reference [`VecTimes`] calls the timestamp's own methods. A timestamp is
//! built (`get`) or consumed (`push`) only at the boundaries where the harness wants one: the
//! frontiers of a retire, the times carried between retires.
//!
//! A [`Bridge`] is the integer-only exchange medium: `(key_hash, value_id)` ids, a time column
//! and diffs, aligned by row, sorted and consolidated by `((key_hash, value_id), time)`.

use std::cmp::Ordering;

use crate::difference::Semigroup;
use crate::lattice::Lattice;

/// A column of times addressed by row.
///
/// Rows are appended and never removed except by `clear` and `truncate`; a row's contents change
/// only through `join_assign` and `meet_assign`. `cmp` is the timestamp's total order (`Ord`),
/// `less_equal` its partial order.
pub trait TimeColumn: Default {
    /// The timestamp the rows denote.
    type Time;

    /// The number of rows.
    fn len(&self) -> usize;
    /// Whether there are no rows.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Remove every row, keeping the allocation.
    fn clear(&mut self);
    /// Keep the first `len` rows.
    fn truncate(&mut self, len: usize);

    /// Append an owned time; its row.
    fn push(&mut self, time: &Self::Time) -> usize;
    /// Append a copy of row `i` of `other`; its row.
    fn push_from(&mut self, other: &Self, i: usize) -> usize;
    /// Append a copy of this column's own row `i`; its row.
    fn push_copy(&mut self, i: usize) -> usize;
    /// Append rows `s..e` of `other`.
    fn push_range(&mut self, other: &Self, s: usize, e: usize) {
        for i in s..e { self.push_from(other, i); }
    }
    /// The owned time at row `i`.
    fn get(&self, i: usize) -> Self::Time;

    /// The total order of rows `i` and `j`.
    fn cmp(&self, i: usize, j: usize) -> Ordering;
    /// The total order of this column's row `i` and `other`'s row `j`.
    fn cmp_cross(&self, i: usize, other: &Self, j: usize) -> Ordering;
    /// The partial order: row `i` at or below row `j`.
    fn less_equal(&self, i: usize, j: usize) -> bool;
    /// The partial order across columns: this column's row `i` at or below `other`'s row `j`.
    fn less_equal_cross(&self, i: usize, other: &Self, j: usize) -> bool;

    /// Insert `other`'s row `i` into this column read as an ANTICHAIN: dropped when some row is
    /// already at or below it, else added and the rows at or above it dropped. The antichain of a
    /// set of times, built one row at a time, with no timestamp materialized.
    fn insert_antichain(&mut self, other: &Self, i: usize) {
        if (0..self.len()).any(|r| self.less_equal_cross(r, other, i)) {
            return;
        }
        let mut kept = Self::default();
        for r in 0..self.len() {
            if !other.less_equal_cross(i, self, r) { kept.push_from(self, r); }
        }
        kept.push_from(other, i);
        *self = kept;
    }

    /// Row `i` becomes its join with row `j`.
    fn join_assign(&mut self, i: usize, j: usize);
    /// Row `i` becomes its meet with row `j`.
    fn meet_assign(&mut self, i: usize, j: usize);
    /// Append the join of rows `i` and `j`; its row.
    fn push_join(&mut self, i: usize, j: usize) -> usize {
        let r = self.push_copy(i);
        self.join_assign(r, j);
        r
    }
    /// Append the join of `a`'s row `i` and `b`'s row `j`; its row.
    fn push_join_cross(&mut self, a: &Self, i: usize, b: &Self, j: usize) -> usize;
}

/// The reference column: a `Vec` of owned timestamps.
pub struct VecTimes<T>(pub Vec<T>);

impl<T> Default for VecTimes<T> {
    fn default() -> Self { VecTimes(Vec::new()) }
}

impl<T: Lattice + Ord + Clone> TimeColumn for VecTimes<T> {
    type Time = T;
    fn len(&self) -> usize { self.0.len() }
    fn clear(&mut self) { self.0.clear() }
    fn truncate(&mut self, len: usize) { self.0.truncate(len) }
    fn push(&mut self, time: &T) -> usize { self.0.push(time.clone()); self.0.len() - 1 }
    fn push_from(&mut self, other: &Self, i: usize) -> usize { self.push(&other.0[i]) }
    fn push_copy(&mut self, i: usize) -> usize { let t = self.0[i].clone(); self.push(&t) }
    fn get(&self, i: usize) -> T { self.0[i].clone() }
    fn cmp(&self, i: usize, j: usize) -> Ordering { self.0[i].cmp(&self.0[j]) }
    fn cmp_cross(&self, i: usize, other: &Self, j: usize) -> Ordering { self.0[i].cmp(&other.0[j]) }
    fn less_equal(&self, i: usize, j: usize) -> bool { self.0[i].less_equal(&self.0[j]) }
    fn less_equal_cross(&self, i: usize, other: &Self, j: usize) -> bool { self.0[i].less_equal(&other.0[j]) }
    fn join_assign(&mut self, i: usize, j: usize) { let t = self.0[j].clone(); self.0[i].join_assign(&t) }
    fn meet_assign(&mut self, i: usize, j: usize) { let t = self.0[j].clone(); self.0[i].meet_assign(&t) }
    fn push_join_cross(&mut self, a: &Self, i: usize, b: &Self, j: usize) -> usize {
        let t = a.0[i].join(&b.0[j]);
        self.push(&t)
    }
}

/// Integer-only exchange medium: `(key_hash, value_id)` ids, times and diffs aligned by row,
/// sorted and consolidated by `((key_hash, value_id), time)` when presented.
pub struct Bridge<C, R> {
    /// `(key_hash, value_id)` per record.
    pub ids: Vec<(u64, u64)>,
    /// The record's time, at the same row.
    pub times: C,
    /// The record's diff.
    pub diffs: Vec<R>,
}

impl<C: Default, R> Default for Bridge<C, R> {
    fn default() -> Self { Bridge { ids: Vec::new(), times: C::default(), diffs: Vec::new() } }
}

impl<C: TimeColumn, R> Bridge<C, R> {
    /// The number of records.
    pub fn len(&self) -> usize { self.ids.len() }
    /// Whether there are no records.
    pub fn is_empty(&self) -> bool { self.ids.is_empty() }
    /// Remove every record, keeping the allocations.
    pub fn clear(&mut self) {
        self.ids.clear();
        self.times.clear();
        self.diffs.clear();
    }
    /// Append a record whose time is row `i` of `times`.
    pub fn push_from(&mut self, id: (u64, u64), times: &C, i: usize, diff: R) {
        self.ids.push(id);
        self.times.push_from(times, i);
        self.diffs.push(diff);
    }
    /// Append a record with an owned time.
    pub fn push(&mut self, id: (u64, u64), time: &C::Time, diff: R) {
        self.ids.push(id);
        self.times.push(time);
        self.diffs.push(diff);
    }
    /// The order of records `i` and `j`: by id, then time.
    #[inline]
    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        self.ids[i].cmp(&self.ids[j]).then_with(|| self.times.cmp(i, j))
    }
}

impl<C: TimeColumn, R: Semigroup + Clone> Bridge<C, R> {
    /// Sort by `(id, time)` and merge records with equal ids and times, dropping zero diffs.
    pub fn consolidate(&mut self) {
        let n = self.ids.len();
        if n == 0 { return; }
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| self.cmp(a, b));
        let mut out: Bridge<C, R> = Bridge::default();
        let mut k = 0;
        while k < n {
            let rep = order[k];
            let mut diff = self.diffs[rep].clone();
            k += 1;
            while k < n && self.cmp(order[k], rep) == Ordering::Equal {
                diff.plus_equals(&self.diffs[order[k]]);
                k += 1;
            }
            if !diff.is_zero() {
                out.push_from(self.ids[rep], &self.times, rep, diff);
            }
        }
        *self = out;
    }

    /// Debug check that a presented bridge is sorted and consolidated by `((key_hash, value_id), time)`.
    pub fn debug_assert_sorted(&self, who: &str) {
        debug_assert!(
            (1..self.ids.len()).all(|i| self.cmp(i - 1, i) == Ordering::Less),
            "{}: a presented bridge must be sorted & consolidated by ((key_hash, value_id), time)",
            who,
        );
    }
}

/// Times held for later, each under a key hash: one column and `(key, row)` entries, in place
/// of a timestamp per time. The reduce tactic carries interesting times beyond a retire's upper
/// frontier here, and hands a key's due ones to its sweep as a range of rows.
pub struct Carried<C> {
    /// `(key hash, row)`, sorted by key and then by time once [`Carried::order`] has run.
    pub entries: Vec<(u64, usize)>,
    /// The times the entries' rows index.
    pub times: C,
}

impl<C: Default> Default for Carried<C> {
    fn default() -> Self { Carried { entries: Vec::new(), times: C::default() } }
}

impl<C: TimeColumn> Carried<C> {
    /// How many times are held.
    pub fn len(&self) -> usize { self.entries.len() }
    /// Whether no time is held.
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
    /// Forget every time, keeping the allocations.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.times.clear();
    }
    /// Hold `other`'s row `i` under `key`.
    pub fn push_from(&mut self, key: u64, other: &C, i: usize) {
        let row = self.times.push_from(other, i);
        self.entries.push((key, row));
    }
    /// Sort the entries by key and then time, and drop the duplicates — a key holds a SET of
    /// times. The rows the entries name are left as they are; a compaction rebuilds the column.
    pub fn order(&mut self) {
        let times = &self.times;
        self.entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| times.cmp(a.1, b.1)));
        let times = &self.times;
        self.entries.dedup_by(|a, b| a.0 == b.0 && times.cmp(a.1, b.1) == Ordering::Equal);
    }
}

/// `(key_hash, time)` pairs: the raw time support the reduce tactic seeds interesting times from.
pub struct Seeds<C> {
    /// The key hash of each seed.
    pub keys: Vec<u64>,
    /// The seed's time, at the same row.
    pub times: C,
}

impl<C: Default> Default for Seeds<C> {
    fn default() -> Self { Seeds { keys: Vec::new(), times: C::default() } }
}

impl<C: TimeColumn> Seeds<C> {
    /// The number of seeds.
    pub fn len(&self) -> usize { self.keys.len() }
    /// Whether there are no seeds.
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }
    /// Remove every seed, keeping the allocations.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.times.clear();
    }
    /// Append a seed whose time is row `i` of `times`.
    pub fn push_from(&mut self, key: u64, times: &C, i: usize) {
        self.keys.push(key);
        self.times.push_from(times, i);
    }
    /// Append a seed with an owned time.
    pub fn push(&mut self, key: u64, time: &C::Time) {
        self.keys.push(key);
        self.times.push(time);
    }
    /// The order of seeds `i` and `j`: by key, then time.
    #[inline]
    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        self.keys[i].cmp(&self.keys[j]).then_with(|| self.times.cmp(i, j))
    }
    /// Sort by `(key, time)` and drop duplicates.
    pub fn sort_dedup(&mut self) {
        let n = self.keys.len();
        if n == 0 { return; }
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| self.cmp(a, b));
        let mut out: Seeds<C> = Seeds::default();
        let mut last: Option<usize> = None;
        for &i in &order {
            if last.is_none_or(|l| self.cmp(l, i) != Ordering::Equal) {
                out.push_from(self.keys[i], &self.times, i);
                last = Some(i);
            }
        }
        *self = out;
    }
    /// Debug check that the seeds are sorted by `(key_hash, time)` and deduplicated.
    pub fn debug_assert_sorted(&self, who: &str) {
        debug_assert!(
            (1..self.keys.len()).all(|i| self.cmp(i - 1, i) == Ordering::Less),
            "{}: seeds must be sorted by (key_hash, time) and deduplicated",
            who,
        );
    }
}
