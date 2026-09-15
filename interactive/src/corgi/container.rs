//! Phase 2 — the corgi-native container: corgi columns for the (key,val) payload, times as a
//! lane column, diffs a plain Rust Vec (corgi never touches the lattice). This is what flows on
//! dataflow edges in the corgi backend; operators transform block→block via `eval_graph` with NO
//! per-op transcode. Conversion to/from DDIR rows happens only at I/O boundaries
//! (`from_updates`/`into_updates`).
//!
//! Trait surface: `Accountable + Default + Clone` (= `timely::Container`) for dataflow edges,
//! `Negate`/`Enter`/`Leave`/`ResultsIn` for iterative scopes, and `ContainerBytes` (in
//! [`bytes`](crate::corgi::bytes)) for crossing a process boundary. Splitting one of these across
//! workers is [`exchange`](crate::corgi::exchange)'s job.

use timely::Accountable;
use timely::progress::Timestamp;

use differential_dataflow::collection::containers::{Enter, Leave, Negate, ResultsIn};
use differential_dataflow::difference::Abelian;

use crate::corgi::col_times::{ColTimes, LaneSummary, Lanes};
use crate::corgi::logic::{transcode, untranscode};
use crate::ir::Value as DValue;

type Row = DValue;

/// A batch of `(key, val, time, diff)` updates: payload columnar (corgi), times as lanes, diffs native Rust.
///
/// Same payload as the chunk contents in [`chunk`](crate::corgi::chunk), and the two should
/// eventually be ONE type — see the note on the chunk's `Inner`. They now share their time
/// storage, and differ in invariants (sorted+consolidated+shared there, raw+owned here).
pub struct CorgiContainer<T, R> {
    /// Key column (corgi columnar `Value`).
    pub keys: corgi::Value,
    /// Val column (corgi columnar `Value`).
    pub vals: corgi::Value,
    /// Per-update times as lanes (corgi never reads these; the Rust side keeps the lattice algebra).
    pub times: ColTimes<T>,
    /// Per-update diffs.
    pub diffs: Vec<R>,
}

impl<T, R> Default for CorgiContainer<T, R> {
    fn default() -> Self {
        // Empty sentinel columns (shape-agnostic length-0 unit columns).
        CorgiContainer { keys: corgi::Value::Unit(0), vals: corgi::Value::Unit(0), times: ColTimes::default(), diffs: Vec::new() }
    }
}

impl<T, R: Clone> Clone for CorgiContainer<T, R> {
    fn clone(&self) -> Self {
        // corgi `Value` clone is an Arc bump on the leaf buffers — columns are shared, not copied.
        CorgiContainer { keys: self.keys.clone(), vals: self.vals.clone(), times: self.times.clone(), diffs: self.diffs.clone() }
    }
}

impl<T: 'static, R: 'static> Accountable for CorgiContainer<T, R> {
    #[inline]
    fn record_count(&self) -> i64 {
        self.times.len() as i64
    }
}

impl<T: Lanes + 'static, R: Clone + 'static> CorgiContainer<T, R> {
    /// Build a container from DDIR row updates at the collection's PINNED shapes — the **ingest
    /// boundary** transcode (once per batch). The only rows→columns conversion in the corgi
    /// backend: inside the dataflow every operator is columnar.
    pub fn from_updates(updates: Vec<((Row, Row), T, R)>, kshape: &corgi::Shape, vshape: &corgi::Shape) -> Self {
        let len = updates.len();
        let (mut keys_rows, mut vals_rows) = (Vec::with_capacity(len), Vec::with_capacity(len));
        let (mut times, mut diffs) = (ColTimes::new(), Vec::with_capacity(len));
        for ((key, val), time, diff) in updates {
            keys_rows.push(key);
            vals_rows.push(val);
            times.push(&time);
            diffs.push(diff);
        }
        CorgiContainer { keys: transcode(&keys_rows, kshape), vals: transcode(&vals_rows, vshape), times, diffs }
    }

    /// Test convenience: build a container from row updates, pinning the shapes from the first
    /// row (what the ingest operator does with the first batch it sees).
    #[cfg(test)]
    pub(crate) fn from_updates_pinned(updates: Vec<((Row, Row), T, R)>) -> Self {
        use crate::corgi::logic::shape_of_row;
        let Some(((k, v), _, _)) = updates.first() else { return Self::default() };
        let (ks, vs) = (shape_of_row(k).unwrap(), shape_of_row(v).unwrap());
        Self::from_updates(updates, &ks, &vs)
    }

    /// Read the container back to DDIR row updates — the **egress boundary** transcode (once).
    /// corgi `Value` is self-describing, so shapes come from `shape_of_value`.
    pub fn into_updates(self) -> Vec<((Row, Row), T, R)> {
        if self.times.is_empty() {
            return Vec::new();
        }
        let kshape = corgi::shape_of_value(&self.keys);
        let vshape = corgi::shape_of_value(&self.vals);
        let keys_rows = untranscode(self.keys, &kshape);
        let vals_rows = untranscode(self.vals, &vshape);
        keys_rows
            .into_iter()
            .zip(vals_rows)
            .zip(self.times.to_vec())
            .zip(self.diffs)
            .map(|(((k, v), t), d)| ((k, v), t, d))
            .collect()
    }
}

// --- Container traits required by the `Backend` bound + iterative scopes ---
// Diffs live in Rust and times in lanes, so these are plain passes; the corgi key/val columns only
// move (`gather`) when `ResultsIn` drops rows. `Enter`/`Leave` are identity for DDIR's same-Time
// dynamic timestamp model (region entry doesn't change the time type; `leave_dynamic` pops the coord).

impl<T: Timestamp, R: Abelian + 'static> Negate for CorgiContainer<T, R> {
    fn negate(mut self) -> Self {
        for d in self.diffs.iter_mut() {
            d.negate();
        }
        self
    }
}

impl<T: Timestamp, R: 'static> Enter<T, T> for CorgiContainer<T, R> {
    type InnerContainer = Self;
    fn enter(self) -> Self {
        self
    }
}

impl<T: Timestamp, R: 'static> Leave<T, T> for CorgiContainer<T, R> {
    type OuterContainer = Self;
    fn leave(self) -> Self {
        self
    }
}

impl<T: Timestamp, R: Clone + 'static> ResultsIn<T::Summary> for CorgiContainer<T, R>
where
    T::Summary: LaneSummary,
{
    fn results_in(mut self, step: &T::Summary) -> Self {
        // The summary applies lane by lane; a row whose result overflows is dropped, as the
        // owned summary's `None` drops it.
        let Some(keep) = self.times.results_in(step) else { return self };
        let keys = corgi::arrange::gather(&self.keys, &keep);
        let vals = corgi::arrange::gather(&self.vals, &keep);
        let diffs = keep.iter().map(|&i| self.diffs[i].clone()).collect();
        CorgiContainer { keys, vals, times: self.times, diffs }
    }
}
