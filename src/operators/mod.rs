//! Timely dataflow operators specific to differential dataflow.
//!
//! Differential dataflow introduces a small number of specialized operators, designed to apply to
//! streams of *typed updates*, records of the form `(T, Delta)` indicating that the frequency of the
//! associated record of type `T` has changed.
//!
//! These operators have specialized implementations to make them work efficiently, but are in all
//! other ways compatible with timely dataflow. In fact, many operators are currently absent because
//! their timely dataflow analogues are sufficient (e.g. `map`, `filter`, `concat`).

pub use self::group::{Group, Distinct, Count, consolidate_from};
pub use self::consolidate::Consolidate;
pub use self::iterate::Iterate;
pub use self::join::Join;

pub mod arrange;
pub mod group;
pub mod consolidate;
pub mod iterate;
pub mod join;

// use std::cmp::Ordering;

use ::Ring;
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
pub struct EditList<V, T, R> {
    values: Vec<(V, usize)>,
    edits: Vec<(T, R)>,
}

impl<V, T, R> EditList<V, T, R> where T: Ord+Clone, R: Ring {
    /// Creates an empty list of edits.
    #[inline(always)]
    fn new() -> Self {
        EditList {
            values: Vec::new(),
            edits: Vec::new(),
        }
    }
    /// Loads the contents of a cursor.
    fn load<K, C, L>(&mut self, cursor: &mut C, logic: L)
    where K: Eq, V: Clone, C: Cursor<K, V, T, R>, L: Fn(&T)->T { 
        self.clear();
        while cursor.val_valid() {
            cursor.map_times(|time1, diff1| self.push(logic(time1), diff1));
            self.seal(cursor.val());
            cursor.step_val();
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
    fn seal(&mut self, value: &V) where V: Clone {
        let prev = self.values.last().map(|x| x.1).unwrap_or(0);
        consolidate_from(&mut self.edits, prev);
        if self.edits.len() > prev {
            self.values.push((value.clone(), self.edits.len()));
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

struct ValueHistory2<V, T, R> {

    edits: EditList<V, T, R>,
    history: Vec<(T, T, usize, usize)>,        // (time, meet, value_index, edit_offset)
    // frontier: FrontierHistory<T>,           // tracks frontiers of remaining times.
    buffer: Vec<((V, T), R)>,               // where we accumulate / collapse updates.
}

impl<V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> ValueHistory2<V, T, R> {
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
    fn load<K, C, L>(&mut self, cursor: &mut C, logic: L)
    where K: Eq, C: Cursor<K, V, T, R>, L: Fn(&T)->T { 
        self.edits.load(cursor, logic);
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
        self.history.last().map(|&(ref t, _, v, e)| (&self.edits.values[v].0, t, self.edits.edits[e].1))
    }

    fn step(&mut self) {
        let (time, _, value_index, edit_offset) = self.history.pop().unwrap();
        self.buffer.push(((self.edits.values[value_index].0.clone(), time), self.edits.edits[edit_offset].1));
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



// struct ValueHistory<V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> {
//     edits: Vec<((T, V), R)>,
//     buffer: Vec<((T, V), R)>,
//     frontier: Vec<T>,
//     entrance: Vec<(T, usize)>,
//     cursor: usize,
// }

// impl<V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> ValueHistory<V, T, R> {
//     fn new() -> Self {
//         ValueHistory {
//             edits: Vec::new(),
//             buffer: Vec::new(),
//             frontier: Vec::new(),
//             entrance: Vec::new(),
//             cursor: 0,
//         }
//     }

//     /// Loads the contents of the cursor. 
//     // TODO: Could be optimized to not `seek_key` if we know that we are already there (e.g. with batch)
//     // TODO: Could re-wire whole thing to bind to a cursor, and load the values lazily. Later. Much later.
//     #[inline(never)]
//     pub fn reload<K, C>(&mut self, key: &K, cursor: &mut C, frontier: &[T]) 
//     where K: Eq+Clone, C: Cursor<K, V, T, R> { 

//         self.clear();

//         cursor.seek_key(&key);
//         if cursor.key_valid() && cursor.key() == key {
//             while cursor.val_valid() {
//                 let val: V = cursor.val().clone();
//                 cursor.map_times(|time, diff| {
//                     self.edits.push(((time.advance_by(frontier), val.clone()), diff));
//                 });
//                 cursor.step_val();
//             }
//             // cursor.step_key();
//         }

//         self.order();
//     }

//     fn clear(&mut self) {
//         self.edits.clear();
//         self.buffer.clear();
//         self.frontier.clear();
//         self.entrance.clear();
//     }
//     fn push(&mut self, val: V, time: T, ring: R) {
//         self.edits.push(((time, val), ring));
//     }
//     fn order(&mut self) {
        
//         consolidate(&mut self.edits, 0);

//         self.buffer.clear();
//         self.cursor = 0;
//         self.frontier.clear();
//         self.entrance.clear();

//         self.entrance.reserve(self.edits.len());
//         let mut position = self.edits.len();
//         while position > 0 {
//             position -= 1;
//             // "add" edits[position] and seeing who drops == who is exposed when edits[position] removed.
//             let mut index = 0;
//             while index < self.frontier.len() {
//                 if (self.edits[position].0).0.le(&self.frontier[index]) {
//                     self.entrance.push((self.frontier.swap_remove(index), position));
//                 }
//                 else {
//                     index += 1;
//                 }
//             }
//             self.frontier.push((self.edits[position].0).0.clone());
//         }
//     }

//     fn advance_buffer_by(&mut self, frontier: &[T]) {
//         if frontier.len() > 0 {
//             for &mut ((ref mut time, _), _) in &mut self.buffer {
//                 *time = time.advance_by(frontier);
//             }
//             consolidate(&mut self.buffer, 0);
//         }
//         else {
//             self.buffer.clear();
//         }
//     }

//     fn time(&self) -> Option<&T> { 
//         if self.cursor < self.edits.len() { 
//             Some(&(self.edits[self.cursor].0).0) 
//         } 
//         else { 
//             None 
//         } 
//     }
//     fn edit(&self) -> &((T, V), R) { &self.edits[self.cursor] }
//     fn frontier(&self) -> &[T] { &self.frontier[..] }
//     fn step(&mut self) { 

//         // a. remove time from frontier; it's not there any more.
//         let new_time = &(self.edits[self.cursor].0).0;
//         self.frontier.retain(|x| !x.eq(new_time));
//         // b. add any indicated elements
//         while self.entrance.last().map(|x| x.1) == Some(self.cursor) {
//             self.frontier.push(self.entrance.pop().unwrap().0);
//         }

//         self.buffer.push(self.edits[self.cursor].clone());
//         self.cursor += 1; 
//     }
//     /// Advance history through `time` and indicate if an update occurs at `time`.
//     pub fn step_while_time_is(&mut self, time: &T, frontier: &[T]) -> bool {
//         let mut found = false;
//         while self.time().map(|x| x.cmp(time)) == Some(Ordering::Equal) {
//             found = true;
//             self.step();
//         }
//         // if found {
//         //     self.advance_buffer_by(frontier);
//         // }
//         found
//     }
//     fn not_done(&self) -> bool { self.cursor < self.edits.len() }
//     fn is_done(&self) -> bool { self.cursor >= self.edits.len() }
// }


// We would like the ability to take in a large pile of updates, perhaps organized by value, and put them in 
// a format that makes it easy to walk forward through the times, maintaining a compact representation of the 
// accumulated updates. There are several constraints to avoid being problematic:
//
//   1. There could be arbitrarily many values; we should not assume that their number is relatively small.
//   2. There can be arbitrarily many times
//   3. While the accumulation at any point can be arbitrarily large, no magic there. 



// /// Trace the frontier of all suffixes of a sequence of times.
// ///
// /// Capable of playing forward through 
// /// the times with the exact frontier for each remaining suffix. Could be simplified to just track
// /// the meet, simplifying the logic at a loss in accuracy (for non-distributive lattices at least).
// pub struct FrontierHistory<T> {
//     frontier: Vec<T>,
//     entrance: Vec<(T, usize)>,
// }

// impl<T: Lattice+Clone> FrontierHistory<T> {
//     fn new() -> Self {
//         FrontierHistory {
//             frontier: Vec::new(),
//             entrance: Vec::new(),
//         }
//     }
//     fn clear(&mut self) {
//         self.frontier.clear();
//         self.entrance.clear();
//     }
//     /// Loads the frontier history of sequence of pairs of decreasing positions and non-increasing times.
//     ///
//     /// The `length` argument should be the length of the iterator, 
//     fn load<I: Iterator<Item=(usize,T)>>(&mut self, iterator: I) {
//         self.frontier.clear();
//         self.entrance.clear();
//         for (position, time) in iterator {
//             let mut index = 0;
//             while index < self.frontier.len() {
//                 if time.le(&self.frontier[index]) {
//                     self.entrance.push((self.frontier.swap_remove(index), position));
//                 }
//                 else {
//                     index += 1;
//                 }
//             }
//             self.frontier.push(time);
//         }
//     }
//     /// Move the frontier forward to a given position as indicated when we loaded the frontier history.
//     fn advance_to(&mut self, position: usize) {
//         while self.entrance.last().map(|x| x.1 == position) == Some(true) {
//             let time = self.entrance.pop().unwrap().0;
//             self.frontier.retain(|x| !x.le(&time));
//             self.frontier.push(time);
//         }
//     }
// }