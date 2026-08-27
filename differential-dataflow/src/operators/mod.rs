//! Specialize differential dataflow operators.
//!
//! Differential dataflow introduces a small number of specialized operators on collections. These
//! operators have specialized implementations to make them work efficiently, and are in addition
//! to several operations defined directly on the `Collection` type (e.g. `map` and `filter`).

pub use self::iterate::Iterate;
pub use self::count::CountTotal;
pub use self::threshold::ThresholdTotal;

pub mod arrange;
pub mod cursor;
pub mod int_proxy;
pub mod reduce;
pub mod iterate;
pub mod join;
pub mod count;
pub mod threshold;

use crate::lattice::Lattice;
use crate::trace::Cursor;

/// An accumulation of (value, time, diff) updates.
pub struct EditList<V, T, D> {
    values: Vec<(V, usize)>,
    edits: Vec<(T, D)>,
}

impl<V: Copy, T: Ord + Lattice, D: crate::difference::Semigroup> EditList<V, T, D> {
    /// Creates an empty list of edits.
    #[inline]
    fn new() -> Self {
        EditList {
            values: Vec::new(),
            edits: Vec::new(),
        }
    }
    /// Walks the cursor's vals at the current key into `self`, advancing times by `meet` if supplied.
    ///
    /// The cursor is assumed to be positioned at a key already; callers that need
    /// to seek should use [`Cursor::populate_key`] (or [`ValueHistory::replay_key`])
    /// instead. This split avoids a redundant seek in the merge-join inner loop,
    /// where the cursor is positioned by the upstream merge step.
    fn load<'a, C>(&mut self, cursor: &mut C, storage: &'a C::Storage, meet: Option<&T>)
    where
        C: Cursor<Val<'a> = V, Time = T, Diff = D>,
    {
        self.clear();
        while let Some(val) = cursor.get_val(storage) {
            cursor.map_times(storage, |time, diff| {
                let mut t = C::owned_time(time);
                if let Some(m) = meet { t.join_assign(m); }
                self.push(t, C::owned_diff(diff));
            });
            self.seal(val);
            cursor.step_val(storage);
        }
    }
    /// Clears the list of edits.
    #[inline]
    pub fn clear(&mut self) {
        self.values.clear();
        self.edits.clear();
    }
    fn len(&self) -> usize { self.edits.len() }
    /// Inserts a new edit for an as-yet undetermined value.
    #[inline]
    pub fn push(&mut self, time: T, diff: D) {
        // TODO: Could attempt "insertion-sort" like behavior here, where we collapse if possible.
        self.edits.push((time, diff));
    }
    /// Associates all edits pushed since the previous `seal_value` call with `value`.
    #[inline]
    pub fn seal(&mut self, value: V) {
        let prev = self.values.last().map(|x| x.1).unwrap_or(0);
        crate::consolidation::consolidate_from(&mut self.edits, prev);
        if self.edits.len() > prev {
            self.values.push((value, self.edits.len()));
        }
    }
    fn map<F: FnMut(V, &T, &D)>(&self, mut logic: F) {
        for index in 0 .. self.values.len() {
            let lower = if index == 0 { 0 } else { self.values[index-1].1 };
            let upper = self.values[index].1;
            for edit in lower .. upper {
                logic(self.values[index].0, &self.edits[edit].0, &self.edits[edit].1);
            }
        }
    }
}

/// A loaded, time-ordered replay of one key's `(value, time, diff)` edits, with meet-advanced
/// buffer collapse — the shared machinery under the cursor reduce and the `int_proxy` tactics.
/// Its local contract: after
/// `advance_buffer_by(meet)` with the meet of the un-replayed times, the buffer is consolidated
/// and replay cost stays linear in edits rather than quadratic.
pub struct ValueHistory<V, T, D> {
    edits: EditList<V, T, D>,
    history: Vec<(T, T, usize, usize)>,     // (time, meet, value_index, edit_offset)
    buffer: Vec<((V, T), D)>,               // where we accumulate / collapse updates.
}

impl<V: Copy + Ord, T: Ord + Clone + Lattice, D: crate::difference::Semigroup> ValueHistory<V, T, D> {
    /// An empty history, to be `load`ed.
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

    /// Loads and replays a specified key.
    ///
    /// If the key is absent, the replayed history will be empty.
    fn replay_key<'a, 'history, C>(
        &'history mut self,
        cursor: &mut C,
        storage: &'a C::Storage,
        key: C::Key<'a>,
        meet: Option<&T>,
    ) -> HistoryReplay<'history, V, T, D>
    where
        C: Cursor<Val<'a> = V, Time = T, Diff = D>,
    {
        self.clear();
        cursor.populate_key(storage, key, meet, &mut self.edits);
        self.replay()
    }

    /// Organizes history based on current contents of edits (sort + suffix meets).
    fn build(&mut self) {
        self.buffer.clear();
        self.history.clear();
        for value_index in 0 .. self.edits.values.len() {
            let lower = if value_index > 0 { self.edits.values[value_index-1].1 } else { 0 };
            let upper = self.edits.values[value_index].1;
            for edit_index in lower .. upper {
                let time = self.edits.edits[edit_index].0.clone();
                self.history.push((time.clone(), time, value_index, edit_index));
            }
        }

        self.history.sort_by(|x,y| y.cmp(x));
        self.history.iter_mut().reduce(|prev, cur| { cur.1.meet_assign(&prev.1); cur });
    }

    /// Organizes history based on current contents of edits, returning a fresh replay.
    fn replay<'history>(&'history mut self) -> HistoryReplay<'history, V, T, D> {
        self.build();
        HistoryReplay { replay: self }
    }

    /// Loads `edits` from a plain iterator (grouped by consecutive value — the presentation
    /// order), advancing each time by `advance_by` if supplied, then organizes. This is the
    /// cursor-free ingestion path: the `int_proxy` tactics present `(value_id, time, diff)`
    /// runs directly rather than through a `Cursor`, and share this machinery instead of
    /// re-implementing it. Ungrouped input is still correct, only less compact.
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
    pub fn time(&self) -> Option<&T> { self.history.last().map(|x| &x.0) }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<&T> { self.history.last().map(|x| &x.1) }
    /// The next un-replayed edit, as `(value, time, diff)`.
    pub fn edit(&self) -> Option<(V, &T, &D)> {
        self.history.last().map(|&(ref t, _, v, e)| (self.edits.values[v].0, t, &self.edits.edits[e].1))
    }
    /// The buffered (stepped-in, advanced, consolidated) edits.
    pub fn buffer(&self) -> &[((V, T), D)] { &self.buffer[..] }
    /// Move the next edit into the buffer.
    pub fn step(&mut self) {
        let (time, _, value_index, edit_offset) = self.history.pop().unwrap();
        self.buffer.push(((self.edits.values[value_index].0, time), self.edits.edits[edit_offset].1.clone()));
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

struct HistoryReplay<'history, V, T, D> {
    replay: &'history mut ValueHistory<V, T, D>,
}

// A `HistoryReplay` is a thin cursor-facing handle over a `ValueHistory`; the replay
// machinery lives on `ValueHistory` itself (shared with the `int_proxy` tactics), and
// these forward to it.
impl<'history, V: Copy + Ord, T: Ord + Clone + Lattice, D: Clone + crate::difference::Semigroup> HistoryReplay<'history, V, T, D> {
    fn time(&self) -> Option<&T> { self.replay.time() }
    fn meet(&self) -> Option<&T> { self.replay.meet() }
    fn edit(&self) -> Option<(V, &T, &D)> { self.replay.edit() }
    fn buffer(&self) -> &[((V, T), D)] { self.replay.buffer() }
    fn step(&mut self) { self.replay.step() }
    fn step_while_time_is(&mut self, time: &T) -> bool { self.replay.step_while_time_is(time) }
    fn advance_buffer_by(&mut self, meet: &T) { self.replay.advance_buffer_by(meet) }
    fn is_done(&self) -> bool { self.replay.is_done() }
}
