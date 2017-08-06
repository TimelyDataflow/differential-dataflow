//! Specialize differential dataflow operators.
//!
//! Differential dataflow introduces a small number of specialized operators on collections. These 
//! operators have specialized implementations to make them work efficiently, and are in addition 
//! to several operations defined directly on the `Collection` type (e.g. `map` and `filter`).

pub use self::group::{Group, Distinct, Count, consolidate_from};
pub use self::consolidate::Consolidate;
pub use self::iterate::Iterate;
pub use self::join::{Join, JoinUnsigned, JoinCore};
pub use self::count::CountTotal;

pub mod arrange;
pub mod group;
pub mod consolidate;
pub mod iterate;
pub mod join;
pub mod count;
// pub mod min;

use ::Diff;
use lattice::Lattice;
use trace::{Cursor, consolidate};

/// Some types used to sit in front of a Cursor<K, V, T, R> titrated through a frontier, allowing compaction of times.
///
/// The `EditList` type is where we accept updates from the cursor, and which in a future world might hold a copy of 
/// the cursor so that it can lazily populate its updates. The edit list only collects updates and accumulates them, 
/// which can involve some sorting of times to collapse them down. The list can be used immediately if there are not
/// all that many updates, or it can be wrapped in a `ValueHistory`
///
/// The `ValueHistory` type wraps an edit list, and presents a time-ordered view of the edits which supports accepting
/// updates from the edit list into a running accumulation, and compacting the accumulation with frontier information.
/// The value history also exposes the running frontier information for remaining times in the history, but do note
/// that this is not the frontier used for compacting the trace; this is supplied by the user, as the distinction is 
/// important in something like `join`.

/// An accumulation of (value, time, diff) updates.
struct EditList<'a, V: 'a, T, R> {
    values: Vec<(&'a V, usize)>,
    edits: Vec<(T, R)>,
}

impl<'a, V:'a, T, R> EditList<'a, V, T, R> where T: Ord+Clone, R: Diff {
    /// Creates an empty list of edits.
    #[inline(always)]
    fn new() -> Self {
        EditList {
            values: Vec::new(),
            edits: Vec::new(),
        }
    }
    /// Loads the contents of a cursor.
    fn load<K, C, L>(&mut self, cursor: &mut C, storage: &'a C::Storage, logic: L)
    where K: Eq, V: Clone, C: Cursor<K, V, T, R>, L: Fn(&T)->T { 
        self.clear();
        while cursor.val_valid(storage) {
            cursor.map_times(storage, |time1, diff1| self.push(logic(time1), diff1));
            self.seal(cursor.val(storage));
            cursor.step_val(storage);
        }
    }
    /// Clears the list of edits.
    #[inline(always)]
    fn clear(&mut self) { 
        self.values.clear();
        self.edits.clear();
    }
    fn len(&self) -> usize { self.edits.len() }
    /// Inserts a new edit for an as-yet undetermined value.
    #[inline(always)]
    fn push(&mut self, time: T, diff: R) {
        // TODO: Could attempt "insertion-sort" like behavior here, where we collapse if possible.
        self.edits.push((time, diff));
    }
    /// Associates all edits pushed since the previous `seal_value` call with `value`.
    #[inline(always)]
    fn seal(&mut self, value: &'a V) {
        let prev = self.values.last().map(|x| x.1).unwrap_or(0);
        consolidate_from(&mut self.edits, prev);
        if self.edits.len() > prev {
            self.values.push((value, self.edits.len()));
        }
    }
    fn map<F: FnMut(&V, &T, R)>(&self, mut logic: F) {
        for index in 0 .. self.values.len() {
            let lower = if index == 0 { 0 } else { self.values[index-1].1 };
            let upper = self.values[index].1;
            for edit in lower .. upper {
                logic(&self.values[index].0, &self.edits[edit].0, self.edits[edit].1);
            }
        }
    }
}

struct ValueHistory2<'a, V: 'a, T, R> {

    edits: EditList<'a, V, T, R>,
    history: Vec<(T, T, usize, usize)>,        // (time, meet, value_index, edit_offset)
    // frontier: FrontierHistory<T>,           // tracks frontiers of remaining times.
    buffer: Vec<((&'a V, T), R)>,               // where we accumulate / collapse updates.
}

impl<'a, V: Ord+Clone+'a, T: Lattice+Ord+Clone, R: Diff> ValueHistory2<'a, V, T, R> {
    fn new() -> Self {
        ValueHistory2 {
            edits: EditList::new(), // empty; will swap out for real list later
            history: Vec::new(),
            buffer: Vec::new(),
        }
    }
    fn clear(&mut self) {
        self.edits.clear();
        self.history.clear();
        self.buffer.clear();
    }
    fn load<K, C, L>(&mut self, cursor: &mut C, storage: &'a C::Storage, logic: L)
    where K: Eq, C: Cursor<K, V, T, R>, L: Fn(&T)->T { 
        self.edits.load(cursor, storage, logic);
        self.order();
    }
    /// Organizes history based on current contents of edits.
    fn order(&mut self) {

        self.buffer.clear();
        self.history.clear();
        for value_index in 0 .. self.edits.values.len() {
            let lower = if value_index > 0 { self.edits.values[value_index-1].1 } else { 0 };
            let upper = self.edits.values[value_index].1;
            for edit_index in lower .. upper {
                let time = self.edits.edits[edit_index].0.clone();
                // assert!(value_index < self.values.len());
                // assert!(edit_index < self.edits.len());
                self.history.push((time.clone(), time.clone(), value_index, edit_index));
            }
        }

        self.history.sort_by(|x,y| y.cmp(x));
        for index in 1 .. self.history.len() {
            self.history[index].1 = self.history[index].1.meet(&self.history[index-1].1);
        }
    }
    fn time(&self) -> Option<&T> { self.history.last().map(|x| &x.0) }
    fn meet(&self) -> Option<&T> { self.history.last().map(|x| &x.1) }
    fn edit(&self) -> Option<(&V, &T, R)> { 
        self.history.last().map(|&(ref t, _, v, e)| (self.edits.values[v].0, t, self.edits.edits[e].1))
    }

    fn step(&mut self) {
        let (time, _, value_index, edit_offset) = self.history.pop().unwrap();
        self.buffer.push(((self.edits.values[value_index].0, time), self.edits.edits[edit_offset].1));
    }
    fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.time() == Some(time) {
            found = true;
            self.step();
        }
        found
    }
    fn advance_buffer_by(&mut self, meet: &T) {
        for element in self.buffer.iter_mut() {
            (element.0).1 = (element.0).1.join(meet);
        }
        consolidate(&mut self.buffer, 0);
    }
    fn is_done(&self) -> bool { self.history.len() == 0 }

    fn _print(&self) where V: ::std::fmt::Debug, T: ::std::fmt::Debug, R: ::std::fmt::Debug {
        for value_index in 0 .. self.edits.values.len() {
            let lower = if value_index > 0 { self.edits.values[value_index-1].1 } else { 0 };
            let upper = self.edits.values[value_index].1;
            for edit_index in lower .. upper {
                println!("{:?}, {:?}, {:?}", self.edits.values[value_index].0, self.edits.edits[edit_index].0, self.edits.edits[edit_index].1);
            }
        }
    }
}