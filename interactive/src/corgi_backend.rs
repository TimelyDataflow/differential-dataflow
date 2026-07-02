//! Phase 2 — the corgi-native container: corgi columns for the (key,val) payload, plain Rust
//! Vecs for time/diff (corgi never touches the lattice). This is what flows on dataflow edges in
//! the corgi backend; operators transform block→block via `eval_graph` with NO per-op transcode.
//! Conversion to/from DDIR rows happens only at I/O boundaries (`from_updates`/`into_updates`).
//!
//! Minimal trait surface for a pipelined (single-worker) dataflow: `Accountable + Default + Clone`
//! (= `timely::Container`). `Negate`/`Enter`/`Leave`/`ResultsIn` (iterative scopes) come with M3.

use timely::Accountable;
use timely::progress::{PathSummary, Timestamp};

use differential_dataflow::collection::containers::{Enter, Leave, Negate, ResultsIn};
use differential_dataflow::difference::Abelian;

use crate::corgi_logic::{infer_shape_cols, transcode, untranscode};
use crate::ir::Value as DValue;

type Row = DValue;

/// A batch of `(key, val, time, diff)` updates: payload columnar (corgi), time/diff native Rust.
pub struct CorgiContainer<T, R> {
    /// Key column (corgi columnar `Value`).
    pub keys: corgi::Value,
    /// Val column (corgi columnar `Value`).
    pub vals: corgi::Value,
    /// Per-update times (corgi never reads these; the Rust side keeps the lattice algebra).
    pub times: Vec<T>,
    /// Per-update diffs.
    pub diffs: Vec<R>,
}

impl<T, R> Default for CorgiContainer<T, R> {
    fn default() -> Self {
        // Empty sentinel columns (shape-agnostic length-0 unit columns).
        CorgiContainer { keys: corgi::Value::Unit(0), vals: corgi::Value::Unit(0), times: Vec::new(), diffs: Vec::new() }
    }
}

impl<T: Clone, R: Clone> Clone for CorgiContainer<T, R> {
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

/// Draining a `CorgiContainer` yields its DDIR row updates (untranscode) — lets the standard
/// `ContainerChunker<Vec<((key,val),T,R)>>` chunk a corgi-container stream into row chains for the
/// reused `MergeBatcher`, which the `CorgiBatchBuilder` then transcodes back to corgi columns at
/// arrangement build (the one ingest-boundary round-trip; reduce/join read corgi columns).
impl<T: Clone + 'static, R: Clone + 'static> timely::container::DrainContainer for CorgiContainer<T, R> {
    type Item<'a> = ((Row, Row), T, R) where Self: 'a;
    type DrainIter<'a> = std::vec::IntoIter<((Row, Row), T, R)> where Self: 'a;
    fn drain(&mut self) -> Self::DrainIter<'_> {
        std::mem::take(self).into_updates().into_iter()
    }
}

impl<T: Clone + 'static, R: Clone + 'static> CorgiContainer<T, R> {
    /// Build a container from DDIR row updates — the **ingest boundary** transcode (once per batch).
    /// Shapes are inferred by scanning the whole column ([`infer_shape_cols`]) — required so a
    /// `Variant` column discovers all its arms (a single sample shows only one tag).
    pub fn from_updates(updates: Vec<((Row, Row), T, R)>) -> Self {
        if updates.is_empty() {
            return Self::default();
        }
        let keys_rows: Vec<DValue> = updates.iter().map(|u| u.0 .0.clone()).collect();
        let vals_rows: Vec<DValue> = updates.iter().map(|u| u.0 .1.clone()).collect();
        let kshape = infer_shape_cols(&keys_rows);
        let vshape = infer_shape_cols(&vals_rows);
        let times = updates.iter().map(|u| u.1.clone()).collect();
        let diffs = updates.iter().map(|u| u.2.clone()).collect();
        CorgiContainer { keys: transcode(&keys_rows, &kshape), vals: transcode(&vals_rows, &vshape), times, diffs }
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
            .zip(self.times)
            .zip(self.diffs)
            .map(|(((k, v), t), d)| ((k, v), t, d))
            .collect()
    }
}

// --- Container traits required by the `Backend` bound + iterative scopes ---
// time/diff live in Rust, so these are plain Rust passes; the corgi key/val columns only move
// (`gather`) when `ResultsIn` drops rows. `Enter`/`Leave` are identity for DDIR's same-Time dynamic
// timestamp model (region entry doesn't change the time type; `leave_dynamic` pops the coord).

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

impl<T: Timestamp, R: Clone + 'static> ResultsIn<T::Summary> for CorgiContainer<T, R> {
    fn results_in(self, step: &T::Summary) -> Self {
        let n = self.times.len();
        let mut keep = Vec::with_capacity(n);
        let mut new_times = Vec::with_capacity(n);
        for (i, t) in self.times.iter().enumerate() {
            if let Some(nt) = step.results_in(t) {
                keep.push(i);
                new_times.push(nt);
            }
        }
        if keep.len() == n {
            return CorgiContainer { keys: self.keys, vals: self.vals, times: new_times, diffs: self.diffs };
        }
        let keys = corgi::arrange::gather(&self.keys, &keep);
        let vals = corgi::arrange::gather(&self.vals, &keep);
        let diffs = keep.iter().map(|&i| self.diffs[i].clone()).collect();
        CorgiContainer { keys, vals, times: new_times, diffs }
    }
}
