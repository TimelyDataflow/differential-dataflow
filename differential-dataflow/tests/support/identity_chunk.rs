//! [`IdentityChunk`]: sorted, consolidated runs of `((key_hash, value_id), time, diff)` — the
//! integers-only boundary between DD's operator logic and a columnar value backend.
//!
//! A backend arrangement keeps its real keys and values in whatever layout it likes; what
//! crosses to DD is a *projection* of each record onto two integers:
//!
//! * `key_hash: u64` — a content hash of the key, **stable across the whole system**: the
//!   same key hashes identically in every operator, including across the output→input
//!   boundary (a reduce output re-ingested as a downstream input), with no registry.
//!   It drives grouping, ordering, and merging. Collisions are an accepted risk (see the
//!   note in [`operators::int_proxy`](differential_dataflow::operators::int_proxy)).
//! * `value_id: u64` — an intra-key identifier for a value, **ephemeral**: consistent only
//!   within a single operator computation (one presentation, one retire's waves). Equal
//!   values ⇔ equal ids there — a bijection per computation, which is all DD needs to
//!   consolidate diffs per value and decide presence; a hash is one scheme, not a
//!   requirement. It is *not* order-preserving with respect to the value type's
//!   semantic order — see the design note in [`operators::int_proxy`](differential_dataflow::operators::int_proxy).
//!
//! Chunks sort by `(key_hash, value_id, time)` — an arbitrary but consistent total order
//! over which DD's sort/merge/consolidate machinery is completely data-blind. Time and
//! diff stay DD-native: the backend never needs to understand timestamps.
//!
//! # Relation to the `Chunk` / `NavigableChunk` split
//!
//! `IdentityChunk` implements [`Chunk`], so it slots directly into the #778 machinery: batches
//! (`ChunkBatch<IdentityChunk>`), fueled merging (`ChunkBatchMerger`), grading (`settle`) all
//! come for free, entirely in integer space. Like a columnar backend's chunk (and unlike
//! [`VecChunk`](super::vec::VecChunk)) it is deliberately **not** [`NavigableChunk`](super::NavigableChunk):
//! its consumers are whole-chunk tactics that read the columns in bulk, not cursors.
//! (`u64` keys and values are `Ord`, so the capability could be added if a cursor-driven
//! consumer ever wants it.)
//!
//! Only the ids live here; the real data stays in the backend. Read-side chunks are
//! produced by a backend's `present` (see [`operators::int_proxy`](differential_dataflow::operators::int_proxy)),
//! which retains the alignment from record index to real record. Write-side chunks are
//! assembled by the reduce tactic from `((key_hash, value_id), time, diff)` deltas and
//! handed back to the backend's `materialize` to become a real output batch.

use std::collections::VecDeque;
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;

use differential_dataflow::trace::chunk::Chunk;

/// The grading target. Records are four machine words plus time; small chunks would
/// under-amortize per-chunk overhead, and the fueled merger wants to yield.
const TARGET: usize = 8192;

/// Shared, immutable chunk contents; `Clone` of an [`IdentityChunk`] is an `Rc` bump.
struct Inner<T, R> {
    /// Key hash column, aligned with the others, sorted by `(key, val, time)`.
    keys: Vec<u64>,
    /// Value id column.
    vals: Vec<u64>,
    /// Per-record times (the lattice algebra lives DD-side; ids never meet time).
    times: Vec<T>,
    /// Per-record diffs.
    diffs: Vec<R>,
}

/// A sorted, consolidated run of `((key_hash, value_id), time, diff)`, shared via `Rc`.
pub struct IdentityChunk<T, R>(Rc<Inner<T, R>>);

impl<T, R> Clone for IdentityChunk<T, R> {
    fn clone(&self) -> Self { IdentityChunk(Rc::clone(&self.0)) }
}

impl<T, R> Default for IdentityChunk<T, R> {
    fn default() -> Self {
        IdentityChunk(Rc::new(Inner { keys: Vec::new(), vals: Vec::new(), times: Vec::new(), diffs: Vec::new() }))
    }
}

impl<T, R> IdentityChunk<T, R> {
    /// The number of records.
    pub fn len(&self) -> usize { self.0.times.len() }
    /// True iff there are no records.
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    /// The key hash column.
    pub fn key_hashes(&self) -> &[u64] { &self.0.keys }
    /// The value id column.
    pub fn value_ids(&self) -> &[u64] { &self.0.vals }
    /// The time column.
    pub fn times(&self) -> &[T] { &self.0.times }
    /// The diff column.
    pub fn diffs(&self) -> &[R] { &self.0.diffs }
}

impl<T: Clone + Ord, R: Clone> IdentityChunk<T, R> {
    /// Assemble a chunk from columns already sorted by `(key, val, time)` with no two
    /// records equal in all three (i.e. consolidated).
    pub fn from_sorted(keys: Vec<u64>, vals: Vec<u64>, times: Vec<T>, diffs: Vec<R>) -> Self {
        debug_assert!(keys.len() == vals.len() && vals.len() == times.len() && times.len() == diffs.len());
        debug_assert!((1..keys.len()).all(|i| {
            (keys[i - 1], vals[i - 1], &times[i - 1]) < (keys[i], vals[i], &times[i])
        }));
        IdentityChunk(Rc::new(Inner { keys, vals, times, diffs }))
    }

}

impl<T, R> IdentityChunk<T, R>
where
    T: Clone + Ord,
    R: Semigroup + Clone,
{
    /// Sort columns by `(key, val, time)` and consolidate equal triples (summing diffs,
    /// dropping zeros). Returns the chunk and, per retained record, the original index of
    /// a *representative* input record — the link a backend needs to keep its real columns
    /// aligned with the id run it presents (equal ids denote equal values, so any member
    /// of a consolidated group serves).
    pub fn from_unsorted(keys: Vec<u64>, vals: Vec<u64>, times: Vec<T>, diffs: Vec<R>) -> (Self, Vec<usize>) {
        let n = times.len();
        debug_assert!(keys.len() == n && vals.len() == n && diffs.len() == n);
        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_by(|&a, &b| (keys[a], vals[a], &times[a]).cmp(&(keys[b], vals[b], &times[b])));

        let (mut ok, mut ov) = (Vec::new(), Vec::new());
        let (mut ot, mut od) = (Vec::new(), Vec::new());
        let mut reps = Vec::new();
        let mut i = 0;
        while i < n {
            let r = perm[i];
            let (k, v, t) = (keys[r], vals[r], &times[r]);
            let mut d = diffs[r].clone();
            let mut j = i + 1;
            while j < n && {
                let s = perm[j];
                keys[s] == k && vals[s] == v && times[s] == *t
            } {
                d.plus_equals(&diffs[perm[j]]);
                j += 1;
            }
            if !d.is_zero() {
                ok.push(k);
                ov.push(v);
                ot.push(t.clone());
                od.push(d);
                reps.push(r);
            }
            i = j;
        }
        (IdentityChunk(Rc::new(Inner { keys: ok, vals: ov, times: ot, diffs: od })), reps)
    }
}

impl<T, R> Chunk for IdentityChunk<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    type Time = T;
    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.times.len() }

    // IdentityChunk is a build-and-read test mock: the reduce fuzz hand-builds batches
    // (`from_sorted`/`from_unsorted`) and reads them directly (accessors), driving `retire` without
    // ever *arranging* them — so the Batcher/Builder/Merger compaction path is unused (SCC exercises
    // arrangements via `VecChunk`, not this). These `Chunk` methods exist only to satisfy
    // `ChunkBatch<_>: BatchReader`; they panic if the mock is ever fed into a spine.
    fn merge(_: &mut VecDeque<Self>, _: &mut VecDeque<Self>, _: &mut VecDeque<Self>) {
        unimplemented!("IdentityChunk: build-and-read mock; the arrangement/merge path is unused")
    }
    fn extract(_: &mut VecDeque<Self>, _: AntichainRef<T>, _: &mut Antichain<T>, _: &mut VecDeque<Self>, _: &mut VecDeque<Self>) {
        unimplemented!("IdentityChunk: build-and-read mock; the arrangement/merge path is unused")
    }
    fn advance(_: &mut VecDeque<Self>, _: AntichainRef<T>, _: bool, _: &mut VecDeque<Self>) {
        unimplemented!("IdentityChunk: build-and-read mock; the arrangement/merge path is unused")
    }
    fn settle(_: &mut VecDeque<Self>, _: bool, _: &mut VecDeque<Self>) {
        unimplemented!("IdentityChunk: build-and-read mock; the arrangement/merge path is unused")
    }
}

