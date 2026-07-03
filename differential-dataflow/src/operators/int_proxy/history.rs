//! Time-ordered replay of proxy update histories, with meet-advancement.
//!
//! The id-space port of the cursor tactics' [`ValueHistory`] machinery: a run of
//! `(value_id, time, diff)` edits is replayed in ascending time order, stepping edits
//! into a working buffer that is repeatedly *advanced* — times joined with the meet of
//! the times still to be considered — and consolidated. Advancement is what keeps the
//! buffer collapsed (for totally ordered times, a whole prefix folds to a single time
//! per value id), which is what keeps a key with many distinct times linear instead of
//! quadratic: accumulations read the small buffer, never the raw history.
//!
//! [`ValueHistory`]: crate::operators::ValueHistory (private; see `operators/mod.rs`)

use crate::difference::Semigroup;
use crate::lattice::Lattice;

/// A replayable history of `(value_id, time, diff)` edits.
pub(crate) struct IdHistory<T, R> {
    /// Un-replayed edits as `(time, meet, value_id, diff)`, sorted descending by
    /// `(time, value_id)` so popping replays ascending; `meet` is the meet of this
    /// edit's time with all times later in the replay (i.e. of the remaining suffix).
    history: Vec<(T, T, u64, R)>,
    /// Stepped-in edits, advanced and consolidated: `((value_id, time), diff)`, sorted.
    buffer: Vec<((u64, T), R)>,
}

impl<T: Lattice + Clone + Ord, R: Semigroup + Clone> IdHistory<T, R> {
    pub fn new() -> Self {
        IdHistory { history: Vec::new(), buffer: Vec::new() }
    }

    /// Load `edits`, advancing each time by `advance_by` if supplied, and organize the
    /// replay (sort + suffix meets).
    pub fn load(&mut self, edits: impl Iterator<Item = (u64, T, R)>, advance_by: Option<&T>) {
        self.history.clear();
        self.buffer.clear();
        for (vid, mut time, diff) in edits {
            if let Some(m) = advance_by {
                time = time.join(m);
            }
            self.history.push((time.clone(), time, vid, diff));
        }
        self.history.sort_by(|x, y| (&y.0, y.2).cmp(&(&x.0, x.2)));
        self.history.iter_mut().reduce(|prev, cur| {
            cur.1.meet_assign(&prev.1);
            cur
        });
    }

    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<&T> {
        self.history.last().map(|x| &x.0)
    }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<&T> {
        self.history.last().map(|x| &x.1)
    }
    /// The next un-replayed edit, as `(value_id, time, diff)`.
    pub fn edit(&self) -> Option<(u64, &T, &R)> {
        self.history.last().map(|(t, _, v, d)| (*v, t, d))
    }

    /// Move the next edit into the buffer.
    pub fn step(&mut self) {
        let (time, _, vid, diff) = self.history.pop().unwrap();
        self.buffer.push(((vid, time), diff));
    }
    /// Step edits while the next time equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.time() == Some(time) {
            found = true;
            self.step();
        }
        found
    }
    /// Step edits while the next time is at most `time` in the *total* order (a
    /// superset of the partially-ordered downset; readers filter the buffer by
    /// `less_equal` themselves).
    pub fn step_through(&mut self, time: &T) {
        while self.time().is_some_and(|t| t <= time) {
            self.step();
        }
    }

    /// Advance buffered times by `meet` and consolidate — the collapse that keeps
    /// replay linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        for ((_, time), _) in self.buffer.iter_mut() {
            *time = time.join(meet);
        }
        crate::consolidation::consolidate(&mut self.buffer);
    }

    /// The buffered (stepped-in, advanced, consolidated) edits.
    pub fn buffer(&self) -> &[((u64, T), R)] {
        &self.buffer
    }

}

/// A replayable history of interesting *times* — the seed source, ported from the same
/// [`ValueHistory`] machinery but carrying no values or diffs.
///
/// The reduce tactic seeds from the batch's `(key, time)` support and never reads the
/// batch's values or diffs (only its times drive interestingness and the synthetic-join
/// closure). Seeds may over-approximate `b.support` — a spurious interesting time yields
/// a zero delta and is discarded — so the times are taken raw, unconsolidated. Meet-
/// advancement still collapses the buffer, keeping a key with many distinct batch times
/// linear (the `*_scaling` shapes) rather than quadratic.
///
/// [`ValueHistory`]: crate::operators::ValueHistory (private; see `operators/mod.rs`)
pub(crate) struct TimeHistory<T> {
    /// Un-replayed `(time, meet)`, sorted descending by time so popping replays ascending;
    /// `meet` is the meet of this time with all times later in the replay.
    history: Vec<(T, T)>,
    /// Stepped-in times, advanced and deduplicated, sorted ascending.
    buffer: Vec<T>,
}

impl<T: Lattice + Clone + Ord> TimeHistory<T> {
    pub fn new() -> Self {
        TimeHistory { history: Vec::new(), buffer: Vec::new() }
    }

    /// Load `times`, advancing each by `advance_by` if supplied, and organize the replay
    /// (sort + suffix meets).
    pub fn load(&mut self, times: impl Iterator<Item = T>, advance_by: Option<&T>) {
        self.history.clear();
        self.buffer.clear();
        for mut time in times {
            if let Some(m) = advance_by {
                time = time.join(m);
            }
            self.history.push((time.clone(), time));
        }
        self.history.sort_by(|x, y| y.0.cmp(&x.0));
        self.history.iter_mut().reduce(|prev, cur| {
            cur.1.meet_assign(&prev.1);
            cur
        });
    }

    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<&T> {
        self.history.last().map(|x| &x.0)
    }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<&T> {
        self.history.last().map(|x| &x.1)
    }

    /// Step times while the next equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.time() == Some(time) {
            found = true;
            let (t, _) = self.history.pop().unwrap();
            self.buffer.push(t);
        }
        found
    }

    /// Advance buffered times by `meet` and deduplicate — the collapse that keeps replay
    /// linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        for time in self.buffer.iter_mut() {
            *time = time.join(meet);
        }
        self.buffer.sort();
        self.buffer.dedup();
    }

    /// The buffered (stepped-in, advanced) times.
    pub fn buffer(&self) -> &[T] {
        &self.buffer
    }
}
