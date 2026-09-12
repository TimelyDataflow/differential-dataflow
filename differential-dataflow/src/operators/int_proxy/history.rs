//! Time-ordered replay of proxy update histories over a time column, with meet-advancement.
//!
//! The same organization as [`crate::operators::history::ValueHistory`] — edits grouped by
//! value, a history sorted by time with suffix meets, a buffer of stepped-in edits collapsed
//! under the running meet — with every time a row of a [`TimeColumn`] the caller owns (the
//! sweep's pool). Rows are copied into the pool when loaded and joined in place thereafter;
//! nothing here holds a timestamp.

use std::cmp::Ordering;

use super::times::TimeColumn;
use crate::difference::Semigroup;

/// One key's `(value, time, diff)` edits, organized for replay in time order.
pub(super) struct ColumnHistory<V, D> {
    /// `(value, end)`: the edits of a value are `edits[prev_end..end]`.
    values: Vec<(V, usize)>,
    /// `(time row, diff)`.
    edits: Vec<(usize, D)>,
    /// `(time row, meet row, value index, edit index)`, descending by time so `last()` is next.
    history: Vec<(usize, usize, usize, usize)>,
    /// The stepped-in edits, `((value, time row), diff)`, consolidated under the running meet.
    buffer: Vec<((V, usize), D)>,
}

impl<V: Copy + Ord, D: Semigroup + Clone> ColumnHistory<V, D> {
    pub fn new() -> Self {
        ColumnHistory { values: Vec::new(), edits: Vec::new(), history: Vec::new(), buffer: Vec::new() }
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.edits.clear();
        self.history.clear();
        self.buffer.clear();
    }

    /// Load edits `(value, row in `src`, diff)`, grouped by consecutive value, copying each time
    /// into `pool` and joining it with `advance_by` if given; then organize for replay.
    pub fn load<C: TimeColumn>(
        &mut self,
        pool: &mut C,
        src: &C,
        edits: impl Iterator<Item = (V, usize, D)>,
        advance_by: Option<usize>,
    ) {
        self.clear();
        let mut cur: Option<V> = None;
        for (v, row, diff) in edits {
            if cur != Some(v) {
                if let Some(pv) = cur { self.seal(pool, pv); }
                cur = Some(v);
            }
            let r = pool.push_from(src, row);
            if let Some(m) = advance_by { pool.join_assign(r, m); }
            self.edits.push((r, diff));
        }
        if let Some(pv) = cur { self.seal(pool, pv); }
        self.build(pool);
    }

    /// Associate the edits pushed since the last seal with `value`, consolidated by time.
    fn seal<C: TimeColumn>(&mut self, pool: &C, value: V) {
        let prev = self.values.last().map(|x| x.1).unwrap_or(0);
        consolidate_edits(pool, &mut self.edits, prev);
        if self.edits.len() > prev {
            self.values.push((value, self.edits.len()));
        }
    }

    /// Organize: one history entry per edit, sorted descending by time, with suffix meets.
    fn build<C: TimeColumn>(&mut self, pool: &mut C) {
        self.buffer.clear();
        self.history.clear();
        for value_index in 0..self.values.len() {
            let lower = if value_index > 0 { self.values[value_index - 1].1 } else { 0 };
            let upper = self.values[value_index].1;
            for edit_index in lower..upper {
                let time = self.edits[edit_index].0;
                let meet = pool.push_copy(time);
                self.history.push((time, meet, value_index, edit_index));
            }
        }
        self.history.sort_by(|x, y| pool.cmp(y.0, x.0));
        for k in 1..self.history.len() {
            let prev = self.history[k - 1].1;
            let cur = self.history[k].1;
            pool.meet_assign(cur, prev);
        }
    }

    /// The next (least) un-replayed time, as a row.
    pub fn time(&self) -> Option<usize> { self.history.last().map(|x| x.0) }
    /// The meet of all un-replayed times, as a row.
    pub fn meet(&self) -> Option<usize> { self.history.last().map(|x| x.1) }
    /// The next un-replayed edit: `(value, time row, diff)`.
    pub fn edit(&self) -> Option<(V, usize, &D)> {
        self.history.last().map(|&(t, _, v, e)| (self.values[v].0, t, &self.edits[e].1))
    }
    /// The buffered (stepped-in, advanced, consolidated) edits.
    pub fn buffer(&self) -> &[((V, usize), D)] { &self.buffer[..] }
    /// Move the next edit into the buffer.
    pub fn step(&mut self) {
        let (time, _, value_index, edit_offset) = self.history.pop().unwrap();
        self.buffer.push(((self.values[value_index].0, time), self.edits[edit_offset].1.clone()));
    }
    /// Step edits while the next time equals row `at`; true iff any did.
    pub fn step_while_time_is<C: TimeColumn>(&mut self, pool: &C, at: usize) -> bool {
        let mut found = false;
        while self.time().is_some_and(|t| pool.cmp(t, at) == Ordering::Equal) {
            found = true;
            self.step();
        }
        found
    }
    /// Join every buffered time with row `meet` and consolidate — the collapse that keeps replay
    /// linear.
    pub fn advance_buffer_by<C: TimeColumn>(&mut self, pool: &mut C, meet: usize) {
        for element in self.buffer.iter_mut() { pool.join_assign((element.0).1, meet); }
        consolidate_buffer(pool, &mut self.buffer);
    }

    /// Every pool row this history refers to, for a pool compaction.
    pub fn rows(&self, out: &mut Vec<usize>) {
        out.extend(self.edits.iter().map(|e| e.0));
        out.extend(self.history.iter().flat_map(|h| [h.0, h.1]));
        out.extend(self.buffer.iter().map(|b| (b.0).1));
    }
    /// Renumber every pool row through `map` (old row to new row).
    pub fn remap(&mut self, map: &[usize]) {
        for e in self.edits.iter_mut() { e.0 = map[e.0]; }
        for h in self.history.iter_mut() { h.0 = map[h.0]; h.1 = map[h.1]; }
        for b in self.buffer.iter_mut() { (b.0).1 = map[(b.0).1]; }
    }
}

/// Sort `edits[from..]` by time and merge equal times, dropping zero diffs.
fn consolidate_edits<C: TimeColumn, D: Semigroup + Clone>(pool: &C, edits: &mut Vec<(usize, D)>, from: usize) {
    let tail = &mut edits[from..];
    tail.sort_by(|a, b| pool.cmp(a.0, b.0));
    let mut w = from;
    let mut r = from;
    while r < edits.len() {
        let (row, mut diff) = (edits[r].0, edits[r].1.clone());
        r += 1;
        while r < edits.len() && pool.cmp(edits[r].0, row) == Ordering::Equal {
            diff.plus_equals(&edits[r].1);
            r += 1;
        }
        if !diff.is_zero() {
            edits[w] = (row, diff);
            w += 1;
        }
    }
    edits.truncate(w);
}

/// Sort a buffer by `(value, time)` and merge equal entries, dropping zero diffs.
pub(super) fn consolidate_buffer<C: TimeColumn, V: Copy + Ord, D: Semigroup + Clone>(pool: &C, buffer: &mut Vec<((V, usize), D)>) {
    buffer.sort_by(|a, b| (a.0).0.cmp(&(b.0).0).then_with(|| pool.cmp((a.0).1, (b.0).1)));
    let mut w = 0;
    let mut r = 0;
    while r < buffer.len() {
        let (v, row, mut diff) = ((buffer[r].0).0, (buffer[r].0).1, buffer[r].1.clone());
        r += 1;
        while r < buffer.len() && (buffer[r].0).0 == v && pool.cmp((buffer[r].0).1, row) == Ordering::Equal {
            diff.plus_equals(&buffer[r].1);
            r += 1;
        }
        if !diff.is_zero() {
            buffer[w] = ((v, row), diff);
            w += 1;
        }
    }
    buffer.truncate(w);
}
