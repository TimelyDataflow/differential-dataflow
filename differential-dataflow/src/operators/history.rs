//! Shared organization and replay of value histories.

use crate::lattice::Lattice;

/// An accumulation of (value, time, diff) updates.
pub struct EditList<V, T, D> {
    pending: Vec<(T, D)>,
    edits: Vec<((T, V), D)>,
}

impl<V: Copy, T: Ord + Lattice, D: crate::difference::Semigroup> EditList<V, T, D> {
    /// Creates an empty list of edits.
    #[inline]
    fn new() -> Self {
        EditList {
            pending: Vec::new(),
            edits: Vec::new(),
        }
    }
    /// Clears the list of edits.
    #[inline]
    pub fn clear(&mut self) {
        self.pending.clear();
        self.edits.clear();
    }
    fn len(&self) -> usize { self.edits.len() }
    /// Inserts a new edit for an as-yet undetermined value.
    #[inline]
    pub fn push(&mut self, time: T, diff: D) {
        // TODO: Could attempt "insertion-sort" like behavior here, where we collapse if possible.
        self.pending.push((time, diff));
    }
    /// Associates all edits pushed since the previous `seal_value` call with `value`.
    #[inline]
    pub fn seal(&mut self, value: V) {
        crate::consolidation::consolidate(&mut self.pending);
        self.edits.extend(self.pending.drain(..).map(|(time, diff)| ((time, value), diff)));
    }
    fn map<F: FnMut(V, &T, &D)>(&self, mut logic: F) {
        for ((time, value), diff) in &self.edits { logic(*value, time, diff); }
    }
}

/// A loaded, time-ordered replay of one key's `(value, time, diff)` edits, with meet-advanced
/// buffer collapse — the shared machinery under replay-based tactics.
/// Its local contract: after
/// `advance_buffer_by(meet)` with the meet of the un-replayed times, the buffer is consolidated
/// and replay cost stays linear in edits rather than quadratic.
pub struct ValueHistory<V, T, D> {
    edits: EditList<V, T, D>,
    history: Vec<T>,                       // prefix meets of the descending edits
    buffer: Vec<((V, T), D)>,               // where we accumulate / collapse updates.
}

impl<V: Copy + Ord, T: Ord + Clone + Lattice, D: crate::difference::Semigroup> ValueHistory<V, T, D> {
    /// Creates an empty history.
    pub fn new() -> Self {
        ValueHistory {
            edits: EditList::new(),
            history: Vec::new(),
            buffer: Vec::new(),
        }
    }
    /// Discards all loaded state (capacity retained).
    pub fn clear(&mut self) {
        self.edits.clear();
        self.history.clear();
        self.buffer.clear();
    }

    pub(in crate::operators) fn edits_mut(&mut self) -> &mut EditList<V, T, D> {
        &mut self.edits
    }

    pub(in crate::operators) fn edit_len(&self) -> usize {
        self.edits.len()
    }

    pub(in crate::operators) fn map_edits<F: FnMut(V, &T, &D)>(&self, logic: F) {
        self.edits.map(logic)
    }

    /// Organizes history based on current contents of edits (sort + suffix meets).
    fn build(&mut self) {
        self.buffer.clear();
        self.history.clear();
        self.edits.edits.sort_unstable_by(|x, y| y.0.cmp(&x.0));
        self.history.extend(self.edits.edits.iter().map(|((time, _), _)| time.clone()));
        for i in 1..self.history.len() {
            let (prefix, suffix) = self.history.split_at_mut(i);
            suffix[0].meet_assign(&prefix[i - 1]);
        }
    }

    /// Organizes history based on current contents of edits, returning a consuming replay.
    pub(in crate::operators) fn replay<'history>(&'history mut self) -> HistoryReplay<'history, V, T, D> {
        self.build();
        HistoryReplay { replay: self }
    }

    /// Loads `edits` from a plain iterator (grouped by consecutive value — the presentation
    /// order), advancing each time by `advance_by` if supplied, then organizes. This is the
    /// iterator ingestion path: the `int_proxy` tactics present `(value_id, time, diff)` runs
    /// directly and share this machinery instead of re-implementing it.
    /// Ungrouped input is still correct, only less compact.
    pub fn load_iter(&mut self, edits: impl Iterator<Item = (V, T, D)>, advance_by: Option<&T>) {
        self.edits.clear();
        let mut cur: Option<V> = None;
        for (v, mut time, diff) in edits {
            if cur != Some(v) {
                if let Some(pv) = cur { self.edits.seal(pv); }
                cur = Some(v);
            }
            if let Some(m) = advance_by { time.join_assign(m); }
            self.edits.push(time, diff);
        }
        if let Some(pv) = cur { self.edits.seal(pv); }
        self.build();
    }
}

impl<V: Copy + Ord, T: Ord + Clone + Lattice, D: Clone + crate::difference::Semigroup> ValueHistory<V, T, D> {
    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<&T> { self.edits.edits.last().map(|x| &x.0.0) }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<&T> { self.history.last() }
    /// The next un-replayed edit, as `(value, time, diff)`.
    pub fn edit(&self) -> Option<(V, &T, &D)> {
        self.edits.edits.last().map(|((t, v), d)| (*v, t, d))
    }
    /// The buffered (stepped-in, advanced, consolidated) edits.
    pub fn buffer(&self) -> &[((V, T), D)] { &self.buffer[..] }
    /// Move the next edit into the buffer.
    pub fn step(&mut self) {
        self.history.pop().unwrap();
        let ((time, value), diff) = self.edits.edits.pop().unwrap();
        self.buffer.push(((value, time), diff));
    }
    /// Step edits while the next time equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.time() == Some(time) { found = true; self.step(); }
        found
    }
    /// Step edits while the next time is `<= time` in the TOTAL order (a superset of the
    /// partially-ordered downset; readers filter the buffer by `less_equal` themselves).
    pub fn step_through(&mut self, time: &T) {
        while self.time().is_some_and(|t| t <= time) { self.step(); }
    }
    /// Advance buffered times by `meet` and consolidate — the collapse that keeps replay linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        for element in self.buffer.iter_mut() { (element.0).1.join_assign(meet); }
        crate::consolidation::consolidate(&mut self.buffer);
    }
    /// True when every edit has been replayed.
    pub fn is_done(&self) -> bool { self.history.is_empty() }
}

pub(in crate::operators) struct HistoryReplay<'history, V, T, D> {
    replay: &'history mut ValueHistory<V, T, D>,
}

// A `HistoryReplay` is a thin handle over a `ValueHistory`.
// The replay machinery lives on `ValueHistory` itself, and these methods forward to it.
impl<'history, V: Copy + Ord, T: Ord + Clone + Lattice, D: Clone + crate::difference::Semigroup> HistoryReplay<'history, V, T, D> {
    pub(in crate::operators) fn time(&self) -> Option<&T> { self.replay.time() }
    pub(in crate::operators) fn meet(&self) -> Option<&T> { self.replay.meet() }
    pub(in crate::operators) fn edit(&self) -> Option<(V, &T, &D)> { self.replay.edit() }
    pub(in crate::operators) fn buffer(&self) -> &[((V, T), D)] { self.replay.buffer() }
    pub(in crate::operators) fn step(&mut self) { self.replay.step() }
    pub(in crate::operators) fn step_while_time_is(&mut self, time: &T) -> bool { self.replay.step_while_time_is(time) }
    pub(in crate::operators) fn advance_buffer_by(&mut self, meet: &T) { self.replay.advance_buffer_by(meet) }
    pub(in crate::operators) fn is_done(&self) -> bool { self.replay.is_done() }
}
