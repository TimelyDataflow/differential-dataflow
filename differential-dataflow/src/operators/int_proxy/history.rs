//! Time-ordered replay of proxy update histories, with meet-advancement.

/// A value history suitable for integer proxy values.
pub(in crate::operators) type IdHistory<T, R> = crate::operators::history::ValueHistory<u64, T, R>;

use crate::lattice::Lattice;
use super::diffs::{Consolidation, DiffContainer, Records};

/// Reduce history with opaque differences and a separate time-ordered replay index.
/// Stepping gathers all edits at a time; advancement consolidates the buffered metadata.
pub(super) struct DiffHistory<T, C> {
    edits: Records<(u64, T), C>,
    history: Vec<(T, T, usize)>, // (time, suffix meet, row)
    pub buffer: Records<(u64, T), C>,
    scratch: Consolidation<(u64, T), C>,
}

impl<T: Ord + Clone + Lattice, C: DiffContainer> DiffHistory<T, C> {
    pub fn new(diffs: &C) -> Self {
        Self {
            edits: Records::new(diffs.empty()), history: Vec::new(),
            buffer: Records::new(diffs.empty()), scratch: Consolidation::new(diffs),
        }
    }
    pub fn load(&mut self, source: &Records<((u64, u64), T), C>, rows: std::ops::Range<usize>, meet: Option<&T>) {
        self.edits.clear();
        self.edits.extend(source, rows, |((_, id), time)| {
            (*id, meet.map_or_else(|| time.clone(), |meet| time.join(meet)))
        });
        self.scratch.consolidate(&mut self.edits.data, &mut self.edits.diffs);
        self.buffer.clear();
        self.history.clear();
        self.history.extend(self.edits.data.iter().enumerate().map(|(row, (_, time))| (time.clone(), time.clone(), row)));
        self.history.sort_unstable_by(|a, b| b.cmp(a));
        self.history.iter_mut().reduce(|prev, cur| { cur.1.meet_assign(&prev.1); cur });
    }
    pub fn time(&self) -> Option<&T> { self.history.last().map(|r| &r.0) }
    pub fn meet(&self) -> Option<&T> { self.history.last().map(|r| &r.1) }
    pub fn step_while_time_is(&mut self, time: &T) {
        let mut start = self.history.len();
        while start > 0 && &self.history[start - 1].0 == time { start -= 1; }
        self.buffer.extend(&self.edits, self.history[start..].iter().rev().map(|r| r.2), Clone::clone);
        self.history.truncate(start);
    }
    pub fn advance_buffer_by(&mut self, meet: &T) {
        for (_, time) in &mut self.buffer.data { time.join_assign(meet); }
        self.scratch.consolidate(&mut self.buffer.data, &mut self.buffer.diffs);
    }
}
