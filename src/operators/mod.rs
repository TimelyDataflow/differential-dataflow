//! Specialize differential dataflow operators.
//!
//! Differential dataflow introduces a small number of specialized operators on collections. These
//! operators have specialized implementations to make them work efficiently, and are in addition
//! to several operations defined directly on the `Collection` type (e.g. `map` and `filter`).

pub use self::reduce::{Reduce, Threshold, Count};
pub use self::iterate::Iterate;
pub use self::join::{Join, JoinCore};
pub use self::count::CountTotal;
pub use self::threshold::ThresholdTotal;

pub mod arrange;
pub mod reduce;
pub mod consolidate;
pub mod iterate;
pub mod join;
pub mod count;
pub mod threshold;

use crate::lattice::Lattice;
use crate::trace::Cursor;

/// An accumulation of (value, time, diff) updates.
struct EditList<'a, C: Cursor> {
    values: Vec<(C::Val<'a>, usize)>,
    edits: Vec<(C::Time, C::Diff)>,
}

impl<'a, C: Cursor> EditList<'a, C> {
    /// Creates an empty list of edits.
    #[inline]
    fn new() -> Self {
        EditList {
            values: Vec::new(),
            edits: Vec::new(),
        }
    }
    /// Loads the contents of a cursor.
    fn load<L>(&mut self, cursor: &mut C, storage: &'a C::Storage, logic: L)
    where
        L: Fn(&C::Time)->C::Time,
    {
        self.clear();
        while cursor.val_valid(storage) {
            cursor.map_times(storage, |time1, diff1| self.push(logic(time1), diff1.clone()));
            self.seal(cursor.val(storage));
            cursor.step_val(storage);
        }
    }
    /// Clears the list of edits.
    #[inline]
    fn clear(&mut self) {
        self.values.clear();
        self.edits.clear();
    }
    fn len(&self) -> usize { self.edits.len() }
    /// Inserts a new edit for an as-yet undetermined value.
    #[inline]
    fn push(&mut self, time: C::Time, diff: C::Diff) {
        // TODO: Could attempt "insertion-sort" like behavior here, where we collapse if possible.
        self.edits.push((time, diff));
    }
    /// Associates all edits pushed since the previous `seal_value` call with `value`.
    #[inline]
    fn seal(&mut self, value: C::Val<'a>) {
        let prev = self.values.last().map(|x| x.1).unwrap_or(0);
        crate::consolidation::consolidate_from(&mut self.edits, prev);
        if self.edits.len() > prev {
            self.values.push((value, self.edits.len()));
        }
    }
    fn map<F: FnMut(C::Val<'a>, &C::Time, &C::Diff)>(&self, mut logic: F) {
        for index in 0 .. self.values.len() {
            let lower = if index == 0 { 0 } else { self.values[index-1].1 };
            let upper = self.values[index].1;
            for edit in lower .. upper {
                logic(self.values[index].0, &self.edits[edit].0, &self.edits[edit].1);
            }
        }
    }
}

struct ValueHistory<'storage, C: Cursor> {
    edits: EditList<'storage, C>,
    history: Vec<(C::Time, C::Time, usize, usize)>,     // (time, meet, value_index, edit_offset)
    buffer: Vec<((C::Val<'storage>, C::Time), C::Diff)>,   // where we accumulate / collapse updates.
}

impl<'storage, C: Cursor> ValueHistory<'storage, C> {
    fn new() -> Self {
        ValueHistory {
            edits: EditList::new(),
            history: Vec::new(),
            buffer: Vec::new(),
        }
    }
    fn clear(&mut self) {
        self.edits.clear();
        self.history.clear();
        self.buffer.clear();
    }
    fn load<L>(&mut self, cursor: &mut C, storage: &'storage C::Storage, logic: L)
    where
        L: Fn(&C::Time)->C::Time,
    {
        self.edits.load(cursor, storage, logic);
    }

    /// Loads and replays a specified key.
    ///
    /// If the key is absent, the replayed history will be empty.
    fn replay_key<'history, L>(
        &'history mut self,
        cursor: &mut C,
        storage: &'storage C::Storage,
        key: C::Key<'storage>,
        logic: L
    ) -> HistoryReplay<'storage, 'history, C>
    where
        L: Fn(&C::Time)->C::Time,
    {
        self.clear();
        cursor.seek_key(storage, key);
        if cursor.get_key(storage) == Some(key) {
            self.load(cursor, storage, logic);
        }
        self.replay()
    }

    /// Organizes history based on current contents of edits.
    fn replay<'history>(&'history mut self) -> HistoryReplay<'storage, 'history, C> {

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
        for index in 1 .. self.history.len() {
            self.history[index].1 = self.history[index].1.meet(&self.history[index-1].1);
        }

        HistoryReplay {
            replay: self
        }
    }
}

struct HistoryReplay<'storage, 'history, C: Cursor> {
    replay: &'history mut ValueHistory<'storage, C>
}

impl<'storage, 'history, C: Cursor> HistoryReplay<'storage, 'history, C> {
    fn time(&self) -> Option<&C::Time> { self.replay.history.last().map(|x| &x.0) }
    fn meet(&self) -> Option<&C::Time> { self.replay.history.last().map(|x| &x.1) }
    fn edit(&self) -> Option<(C::Val<'storage>, &C::Time, &C::Diff)> {
        self.replay.history.last().map(|&(ref t, _, v, e)| (self.replay.edits.values[v].0, t, &self.replay.edits.edits[e].1))
    }

    fn buffer(&self) -> &[((C::Val<'storage>, C::Time), C::Diff)] {
        &self.replay.buffer[..]
    }

    fn step(&mut self) {
        let (time, _, value_index, edit_offset) = self.replay.history.pop().unwrap();
        self.replay.buffer.push(((self.replay.edits.values[value_index].0, time), self.replay.edits.edits[edit_offset].1.clone()));
    }
    fn step_while_time_is(&mut self, time: &C::Time) -> bool {
        let mut found = false;
        while self.time() == Some(time) {
            found = true;
            self.step();
        }
        found
    }
    fn advance_buffer_by(&mut self, meet: &C::Time) {
        for element in self.replay.buffer.iter_mut() {
            (element.0).1 = (element.0).1.join(meet);
        }
        crate::consolidation::consolidate(&mut self.replay.buffer);
    }
    fn is_done(&self) -> bool { self.replay.history.is_empty() }
}
