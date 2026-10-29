//! The corgi half join: a [`Batcher`](differential_dataflow::batcher::Batcher) that releases a
//! lifted stream's updates once the arrangement can no longer gain rows that precede them, and a
//! [`ProxyHalfJoinBackend`](differential_dogs3::operators::half_join::proxy::ProxyHalfJoinBackend)
//! that presents released updates and the arrangement's chunks to dogs3's proxy half-join tactic,
//! column at a time, and projects the pairs the tactic matches. Both plug into dogs3's
//! `half_join_with_tactic`, which owns progress, capabilities, and yielding; the tactic owns which
//! pairs meet and at what times.
//!
//! An update is `(key, (val, s))` at time `t`: `s` is a time as data, a `Prod` of `Int` lanes as
//! `lift` writes it, and `t` the update's own time. It meets each arranged row `(key, v1)` at time
//! `t2` with `t2 < t` in the total order on times (`≤` unless strict), giving
//! `(key', (val', s ∨ t2))` at time `t` with the product of the diffs, where `(key', val')` is the
//! projection over `(key, val, v1)`. The total order is the lexicographic order of the time lanes,
//! which is DD's `Ord` on these times.
//!
//! The batcher keeps arrivals in time order and releases them as runs in identifier order; the
//! backend reads a unit's runs by identifier a block at a time. A [`Run`] is what passes between
//! them.

use std::borrow::Cow;
use std::rc::Rc;

use differential_dataflow::trace::chunk::ChunkBatch;

use corgi::arrange::gather;
use corgi::Value as CValue;

use crate::runtime::chunk::{ident, present_key, CorgiChunk};
use crate::runtime::col_times::ColTimes;
use crate::runtime::container::CorgiContainer;
use crate::ir::{Diff, Time};

mod backend;
mod batcher;

pub(crate) use backend::CorgiHalfJoinBackend;
pub(crate) use batcher::HalfJoinBatcher;

type CC = CorgiContainer<Time, Diff>;
type NativeBatch = Rc<ChunkBatch<CorgiChunk<Time, Diff>>>;

/// A run of released updates, as the batcher hands them to the tactic: in identifier order, as the
/// arrangement's chunks are, with keys as [`present_key`] presents them. One extraction releases
/// several runs, each sorted within a window of what was released, and the tactic merges them as
/// it works. Runs are not consolidated: what one extraction releases from different chains may
/// repeat a row.
#[derive(Clone)]
pub(crate) struct Run {
    keys: CValue,
    vals: CValue,
    times: ColTimes<Time>,
    diffs: Vec<Diff>,
}

impl Run {
    /// Updates put in identifier order, as one run.
    #[cfg(test)]
    pub(crate) fn new(keys: CValue, vals: CValue, times: ColTimes<Time>, diffs: Vec<Diff>) -> Self {
        let n = diffs.len();
        let width = times.width();
        let part = Part { keys, vals: &vals, times: PartTimes::Col(&times), diffs: &diffs, rows: 0..n };
        let mut runs = Vec::new();
        part.runs(n.max(1), width, &mut runs);
        runs.pop().unwrap_or_else(|| Run { keys: present_key(part.keys.clone()), vals: vals.clone(), times: times.clone(), diffs: Vec::new() })
    }
    pub(crate) fn len(&self) -> usize { self.diffs.len() }
    pub(crate) fn keys(&self) -> &CValue { &self.keys }
    pub(crate) fn vals(&self) -> &CValue { &self.vals }
    pub(crate) fn times(&self) -> &ColTimes<Time> { &self.times }
    pub(crate) fn diffs(&self) -> &[Diff] { &self.diffs }
}

/// Rows `rows` of released columns, from a time chunk or from arrivals released on arrival.
struct Part<'a> {
    keys: CValue,
    vals: &'a CValue,
    times: PartTimes<'a>,
    diffs: &'a [Diff],
    rows: std::ops::Range<usize>,
}

impl Part<'_> {
    /// The part's rows as runs of at most `size` rows, their times written as `width` lanes. Each
    /// run is sorted and gathered from one window of the part, so its reads stay within it.
    fn runs(&self, size: usize, width: usize, out: &mut Vec<Run>) {
        let keys = present_key(self.keys.clone());
        let ids = ident(&keys);
        for lo in self.rows.clone().step_by(size) {
            let hi = (lo + size).min(self.rows.end);
            let mut order: Vec<(i64, usize)> = (lo..hi).map(|i| (ids[i], i)).collect();
            order.sort_unstable_by_key(|&(id, _)| id);
            let rows: Vec<usize> = order.into_iter().map(|(_, i)| i).collect();
            let lanes = (0..width).map(|k| rows.iter().map(|&i| self.times.at(k, i)).collect()).collect();
            out.push(Run {
                keys: gather(&keys, &rows),
                vals: gather(self.vals, &rows),
                times: ColTimes::from_raw_lanes(lanes, rows.len()),
                diffs: rows.iter().map(|&i| self.diffs[i]).collect(),
            });
        }
    }
}

/// A part's times: a time chunk's lanes, or an arrival's column.
enum PartTimes<'a> {
    Lanes(Vec<Cow<'a, [i64]>>),
    Col(&'a ColTimes<Time>),
}

impl PartTimes<'_> {
    /// Coordinate `k` of row `i`, zero beyond the times' width.
    fn at(&self, k: usize, i: usize) -> u64 {
        match self {
            PartTimes::Lanes(lanes) => lanes.get(k).map_or(0, |lane| lane[i] as u64),
            PartTimes::Col(times) => times.raw_lanes().get(k).map_or(0, |lane| lane[i]),
        }
    }
}
/// The rows of `c` at `rows`, in that order.
fn gather_rows(c: &CC, rows: &[usize]) -> CC {
    CorgiContainer {
        keys: gather(&c.keys, rows),
        vals: gather(&c.vals, rows),
        times: c.times.gather(rows),
        diffs: rows.iter().map(|&i| c.diffs[i]).collect(),
    }
}

#[cfg(test)]
mod test_util {
    use super::*;

    /// The time `[outer, inner]`, at scope depth one.
    pub(super) fn t(outer: u64, inner: u64) -> Time { Time::new([outer, inner]) }
    pub(super) fn ints(xs: &[i64]) -> CValue { CValue::i64(xs.to_vec()) }
    pub(super) fn lifted(s: &[(i64, i64)]) -> CValue { CValue::Prod(vec![ints(&s.iter().map(|p| p.0).collect::<Vec<_>>()), ints(&s.iter().map(|p| p.1).collect::<Vec<_>>())]) }
    pub(super) fn container(keys: &[i64], vals: &[i64], s: &[(i64, i64)], times: &[Time], diffs: &[Diff]) -> CC {
        CorgiContainer { keys: ints(keys), vals: CValue::Prod(vec![ints(vals), lifted(s)]), times: times.iter().cloned().collect(), diffs: diffs.to_vec() }
    }
    /// A row's key or value as a feed writes it: an integer, or a tuple of them.
    pub(super) fn int(n: i64) -> String { n.to_string() }
    pub(super) fn tup(xs: Vec<String>) -> String { format!("tuple({})", xs.join(",")) }

    /// A deterministic source of small numbers.
    pub(super) struct Rng(pub(super) u64);
    impl Rng {
        pub(super) fn below(&mut self, n: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }
        pub(super) fn diff(&mut self) -> Diff { if self.below(3) == 0 { -1 } else { 1 } }
    }

    pub(super) fn consolidated(mut rows: Vec<((String, String), Time, Diff)>) -> Vec<((String, String), Time, Diff)> {
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        rows
    }
}
