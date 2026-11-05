//! The corgi-native container: corgi columns for the (key,val) payload, times as a
//! lane column, diffs a plain Rust Vec (corgi never touches the lattice). This is what flows on
//! dataflow edges in the corgi backend; operators transform block→block via `eval_graph` with NO
//! per-op transcode.
//!
//! Trait surface: `Accountable + Default + Clone` (= `timely::Container`) for dataflow edges,
//! `Negate`/`Enter`/`Leave`/`ResultsIn` for iterative scopes, and `ContainerBytes` (in
//! `bytes`) for crossing a process boundary. Splitting one of these across
//! workers is [`exchange`](crate::runtime::exchange)'s job.

use timely::Accountable;
use timely::progress::Timestamp;

use differential_dataflow::collection::containers::{Enter, Leave, Negate, ResultsIn};
use differential_dataflow::difference::Abelian;

use crate::runtime::col_times::{ColTimes, LaneSummary, Lanes};
use crate::runtime::feed::print;

/// A batch of `(key, val, time, diff)` updates: payload columnar (corgi), times as lanes, diffs native Rust.
///
/// Same payload as the chunk contents in [`chunk`](crate::runtime::chunk), and the two should
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
    /// Test convenience: a container from updates whose keys and values are written as a feed
    /// writes them, at the collection's shapes.
    #[cfg(test)]
    pub(crate) fn from_updates(updates: Vec<((String, String), T, R)>, kshape: &corgi::Shape, vshape: &corgi::Shape) -> Self {
        let mut decoder = crate::runtime::feed::FeedDecoder::new(kshape, vshape);
        let (mut times, mut diffs) = (ColTimes::new(), Vec::with_capacity(updates.len()));
        for ((key, val), time, diff) in updates {
            decoder.push(&key, Some(&val), 1).unwrap();
            times.push(&time);
            diffs.push(diff);
        }
        let Updates { keys, vals, .. } = decoder.finish();
        CorgiContainer { keys, vals, times, diffs }
    }

    /// Test convenience: the container's updates, their keys and values printed as a feed writes
    /// them.
    #[cfg(test)]
    pub(crate) fn into_updates(self) -> Vec<((String, String), T, R)> {
        let rows = (0..self.diffs.len()).map(|i| (print(&self.keys, i), print(&self.vals, i)));
        rows.zip(self.times.to_vec()).zip(self.diffs).map(|((row, time), diff)| (row, time, diff)).collect()
    }
}

/// A collection's updates as columns, without times: update `i` is row `i` of `keys` and `vals`,
/// with multiplicity `diffs[i]`. What a feed brings in, and what a snapshot gives back.
#[derive(Clone, Debug, PartialEq)]
pub struct Updates {
    pub keys: corgi::Value,
    pub vals: corgi::Value,
    pub diffs: Vec<crate::ir::Diff>,
}

impl Updates {
    /// No updates, at these shapes.
    pub(crate) fn empty((k, v): &(corgi::Shape, corgi::Shape)) -> Self {
        Updates { keys: corgi::Value::empty(k), vals: corgi::Value::empty(v), diffs: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.diffs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }

    /// The updates at `rows`, in that order.
    pub fn select(&self, rows: &[usize]) -> Self {
        Updates {
            keys: corgi::arrange::gather(&self.keys, rows),
            vals: corgi::arrange::gather(&self.vals, rows),
            diffs: rows.iter().map(|&r| self.diffs[r]).collect(),
        }
    }

    /// `parts`, all of the shapes `shape`, as one: the updates of equal key and value summed, those
    /// summing to zero dropped, and the rest in key-then-value order.
    pub fn consolidate(parts: Vec<Updates>, shape: &(corgi::Shape, corgi::Shape)) -> Updates {
        let parts: Vec<Updates> = parts.into_iter().filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return Updates::empty(shape);
        }
        // The parts end to end, as one key-and-value column, then sorted.
        let (tags, offs): (Vec<usize>, Vec<usize>) = parts.iter().enumerate().flat_map(|(p, part)| (0..part.len()).map(move |i| (p, i))).unzip();
        let rows: Vec<corgi::Value> = parts.iter().map(|p| corgi::Value::Prod(vec![p.keys.clone(), p.vals.clone()])).collect();
        let rows = corgi::arrange::gather_lanes(&rows.iter().map(Some).collect::<Vec<_>>(), &tags, &offs);
        let diffs: Vec<crate::ir::Diff> = parts.iter().flat_map(|p| p.diffs.iter().copied()).collect();
        // The sort labels runs of equal rows as it orders them; one sum per run.
        let (order, runs) = corgi::arrange::sort_blocks(&[], &rows);
        let (mut keep, mut sums) = (Vec::new(), Vec::new());
        let mut start = 0;
        for run in runs.chunk_by(|a, b| a == b) {
            let members = &order[start..start + run.len()];
            start += run.len();
            let sum: crate::ir::Diff = members.iter().map(|&row| diffs[row]).sum();
            if sum != 0 { keep.push(members[0]); sums.push(sum); }
        }
        let mut fields = corgi::arrange::gather(&rows, &keep).into_prod("consolidated rows").unwrap();
        let vals = fields.pop().unwrap();
        let keys = fields.pop().unwrap();
        Updates { keys, vals, diffs: sums }
    }

    /// The updates as text: each key and value as a feed writes it ([`print`]), and its diff.
    pub fn rows(&self) -> Vec<(String, String, crate::ir::Diff)> {
        (0..self.len()).map(|i| (print(&self.keys, i), print(&self.vals, i), self.diffs[i])).collect()
    }
}

// Commands cross Timely as serde records; the columns travel in corgi's byte encoding.
impl serde::Serialize for Updates {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let encode = |v: &corgi::Value| {
            let mut bytes = Vec::with_capacity(corgi::bytes::length_in_bytes(v));
            corgi::bytes::write_to(v, &mut bytes).map(|()| bytes).map_err(serde::ser::Error::custom)
        };
        serde::Serialize::serialize(&(encode(&self.keys)?, encode(&self.vals)?, &self.diffs), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Updates {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (keys, vals, diffs): (Vec<u8>, Vec<u8>, Vec<crate::ir::Diff>) = serde::Deserialize::deserialize(deserializer)?;
        let decode = |bytes: &[u8]| corgi::bytes::read_from(bytes).map(|(v, _)| v).map_err(serde::de::Error::custom);
        Ok(Updates { keys: decode(&keys)?, vals: decode(&vals)?, diffs })
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
