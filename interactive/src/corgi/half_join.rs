//! The corgi HALF-JOIN tactic: probing columnar chunks for a delta join.
//!
//! [`CorgiHalfJoinTactic`] implements [`HalfJoinTactic`], the third of corgi's tactic bindings
//! beside [`join`](crate::corgi::join) and [`reduce`](crate::corgi::reduce). It exists for the
//! same reason those do: `half_join`'s stock implementation walks a [`Cursor`], and `CorgiChunk`
//! is deliberately not `Navigable` — it has no cursor to walk.
//!
//! # What is columnar and what is not
//!
//! Storage and the probe are columnar. Chunks are sorted by their leading identifier lane
//! ([`key_lane`]), so a whole block of probe keys is located with one `find_ranges` per chunk —
//! a binary search per key over a `u64` leaf, never a scan — and the matched rows' values are
//! pulled out in one `gather_lanes`.
//!
//! The arithmetic is not. A delta join's intermediate value is a *partial binding*, which is not
//! a DDIR row and has no columnar form until the join completes, so the prefix stream is rows in
//! every backend. Deciding a match therefore reads one time and one diff per matched row. That is
//! the boundary a fully columnar tactic would move, not a fallback to correctness: capability
//! never depends on the lowering's coverage here either.
//!
//! # Collisions
//!
//! An identifier is the key itself for primitive integer keys and a content hash otherwise, so
//! for a hashed key a matched identifier range may hold *different* keys. Those rows are adjacent
//! and sub-sorted, and this tactic tells them apart by recovering the real keys of the candidate
//! rows ([`recover_key`], one gather per chunk per block) and comparing them as DDIR values — the
//! shape-independent check, which cannot go wrong when the probe column's shape and the
//! arrangement's disagree.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::rc::Rc;

use timely::progress::Antichain;

use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};
use differential_dogs3::operators::half_join::HalfJoinTactic;

use corgi::arrange::{find_ranges, gather_lanes};
use corgi::{shape_of_value, Value as CValue};

use crate::corgi::chunk::{key_ids, key_is_hashed, key_lane, present_key, recover_key, CorgiChunk};
use crate::corgi::col_times::ColTime;
use crate::corgi::logic::{infer_shape_cols, transcode, untranscode};
use crate::ir::{Diff, Value as DValue};

/// The batch a corgi arrangement presents.
type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Probe keys located per `find_ranges` sweep. Bounds the candidate rows and the gathered value
/// column a single `next` holds, so a work unit's peak is one block's matches rather than all of
/// them — the driver's fuel then paces the rest.
const BLOCK: usize = 1 << 14;

/// A streamed update: the probe key, the payload the tactic carries untouched, and the payload
/// time, at the update's own time and with the update's own diff.
type Update<V, T> = ((DValue, V, T), T, Diff);

/// The corgi [`HalfJoinTactic`]: holds the per-match output logic and the strictness of the
/// time comparison. `logic` is shared across outstanding units (an `Rc<RefCell<_>>`), as in the
/// stock cursor tactic: each unit is a self-contained `'static` iterator and cannot borrow the
/// tactic, but the single-mutable-state semantics of one closure threaded through every match
/// is preserved.
pub struct CorgiHalfJoinTactic<T, V, DOut, L> {
    logic: Rc<RefCell<L>>,
    /// Whether an arrangement time equal to an update's own time is excluded.
    strict: bool,
    _marker: PhantomData<(T, V, DOut)>,
}

impl<T, V, DOut, L> CorgiHalfJoinTactic<T, V, DOut, L> {
    /// Construct a tactic applying `logic` to each `(probe key, payload, matched value)`.
    pub fn new(logic: L, strict: bool) -> Self {
        CorgiHalfJoinTactic { logic: Rc::new(RefCell::new(logic)), strict, _marker: PhantomData }
    }
}

impl<T, V, DOut, L> HalfJoinTactic<T, CBatch<T>, Vec<Update<V, T>>, Vec<((DOut, T), T, Diff)>>
    for CorgiHalfJoinTactic<T, V, DOut, L>
where
    T: ColTime,
    V: 'static,
    DOut: 'static,
    L: FnMut(&DValue, &V, &DValue) -> DOut + 'static,
{
    fn prep(
        &mut self,
        released: Vec<Vec<Update<V, T>>>,
        batches: Vec<CBatch<T>>,
        lower: Antichain<T>,
    ) -> Box<dyn Iterator<Item = (Vec<((DOut, T), T, Diff)>, T)>> {
        let updates: Vec<Update<V, T>> = released.into_iter().flatten().collect();
        if updates.is_empty() {
            return Box::new(std::iter::empty());
        }

        // The identifiers the arrangement is sorted by, computed once for the whole unit. The
        // probe column goes through `present_key` exactly as the arrangement's keys did at
        // ingest, so a hashed key hashes the same way here as it did there.
        let key_rows: Vec<DValue> = updates.iter().map(|u| u.0 .0.clone()).collect();
        let presented = present_key(transcode(&key_rows, &infer_shape_cols(&key_rows)));
        let hashed = key_is_hashed(&presented);
        let ids = key_ids(&presented);

        // Chunk clones are `Rc` bumps, so flattening the batches costs a refcount each; the unit
        // owns them for its whole life and what it joins against cannot change underneath it.
        let chunks: Vec<CorgiChunk<T, Diff>> = batches
            .iter()
            .flat_map(|b| b.chunks.iter())
            .filter(|c| c.len() > 0)
            .cloned()
            .collect();

        Box::new(Probing {
            updates,
            ids,
            hashed,
            chunks,
            next: 0,
            lower: lower.elements().to_vec(),
            ready: VecDeque::new(),
            logic: Rc::clone(&self.logic),
            strict: self.strict,
        })
    }
}

/// Deferred half-join work, as an iterator of output containers and the times they ship at.
///
/// Each `next` sweeps the next block of probe keys across every chunk, or serves a container an
/// earlier sweep left behind. The driver stops pulling once it has done enough work and resumes
/// the same iterator on the next activation.
struct Probing<T: ColTime, V, DOut, L> {
    updates: Vec<Update<V, T>>,
    /// The arrangement identifier of each update's probe key, aligned with `updates`.
    ids: Vec<u64>,
    /// Whether identifiers are hashes, and so may cover more than one real key.
    hashed: bool,
    chunks: Vec<CorgiChunk<T, Diff>>,
    /// How far through `updates` the sweeps have reached.
    next: usize,
    /// The times output ships at, one per capability. An update is assigned the first that is
    /// less or equal to its own time, mirroring the capability the driver will find.
    lower: Vec<T>,
    ready: VecDeque<(Vec<((DOut, T), T, Diff)>, T)>,
    logic: Rc<RefCell<L>>,
    strict: bool,
}

impl<T, V, DOut, L> Iterator for Probing<T, V, DOut, L>
where
    T: ColTime,
    L: FnMut(&DValue, &V, &DValue) -> DOut,
{
    type Item = (Vec<((DOut, T), T, Diff)>, T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Some(item);
            }
            if self.next >= self.updates.len() {
                return None;
            }
            self.sweep();
        }
    }
}

impl<T, V, DOut, L> Probing<T, V, DOut, L>
where
    T: ColTime,
    L: FnMut(&DValue, &V, &DValue) -> DOut,
{
    /// Locate the next block of probe keys in every chunk and emit what matches.
    fn sweep(&mut self) {
        let start = self.next;
        let end = (start + BLOCK).min(self.updates.len());
        self.next = end;

        let needles = CValue::u64(self.ids[start..end].to_vec());
        // One bucket per capability, so output shipped at one is never mixed with output that
        // requires another.
        let mut buckets: Vec<Vec<((DOut, T), T, Diff)>> = (0..self.lower.len()).map(|_| Vec::new()).collect();
        let mut logic = self.logic.borrow_mut();

        for chunk in self.chunks.iter() {
            debug_assert_eq!(
                self.hashed,
                key_is_hashed(chunk.keys()),
                "probe keys and arrangement keys disagree about hashing; their identifiers cannot match",
            );
            let (lo, hi) = find_ranges(&needles, key_lane(chunk.keys()));

            // The candidate rows of this chunk, and which update each answers.
            let mut rows: Vec<usize> = Vec::new();
            let mut owners: Vec<usize> = Vec::new();
            for (j, (&l, &h)) in lo.iter().zip(hi.iter()).enumerate() {
                rows.extend(l..h);
                owners.resize(rows.len(), start + j);
            }
            if rows.is_empty() {
                continue;
            }

            // Values (and, when identifiers are hashes, real keys) for every candidate, pulled
            // out of chunk storage in one gather each rather than a row at a time.
            let tags = vec![0usize; rows.len()];
            let vals_col = gather_lanes(&[Some(chunk.vals())], &tags, &rows);
            let vals = untranscode(vals_col.clone(), &shape_of_value(&vals_col));
            let keys = self.hashed.then(|| {
                let real = recover_key(chunk.keys());
                let col = gather_lanes(&[Some(&real)], &tags, &rows);
                untranscode(col.clone(), &shape_of_value(&col))
            });

            for (c, (&row, &j)) in rows.iter().zip(owners.iter()).enumerate() {
                let ((probe, payload_val, payload), initial, diff) = &self.updates[j];
                // A hash collision puts an unequal key in the identifier's range.
                if let Some(keys) = &keys {
                    if keys[c] != *probe {
                        continue;
                    }
                }
                // The delta discipline: the arrangement time must precede the update's own time
                // in the TOTAL order, strictly or not according to which relation is later.
                let when = chunk.times().get(row);
                if !(if self.strict { when < *initial } else { when <= *initial }) {
                    continue;
                }
                // ... and the payload advances by the lattice join, which is the moment the
                // match actually takes effect.
                let mut when = when;
                when.join_assign(payload);
                let bucket = self.lower.iter().position(|l| l.less_equal(initial))
                    .expect("no capability covers a released update");
                let out = logic(probe, payload_val, &vals[c]);
                buckets[bucket].push(((out, when), initial.clone(), diff * chunk.diffs()[row]));
            }
        }

        drop(logic);
        for (b, updates) in buckets.into_iter().enumerate() {
            if !updates.is_empty() {
                self.ready.push_back((updates, self.lower[b].clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Time;
    use corgi::Shape;
    use timely::progress::Timestamp;

    /// Two distinct keys forced under one identifier — what a hash collision looks like from
    /// inside the probe. No realistic input produces one, the identifier being a 64-bit content
    /// hash, so the guard is driven here rather than waited for.
    fn colliding_chunk() -> CorgiChunk<Time, Diff> {
        let shape = Shape::Prod(vec![Shape::Prim(64)]);
        let key = |n: i64| DValue::Tuple(vec![DValue::Int(n)]);
        // `Prod([identifier lane, real key])` is exactly `present_key`'s hashed form, with both
        // rows given identifier 7.
        let keys = CValue::Prod(vec![CValue::u64(vec![7, 7]), transcode(&[key(1), key(2)], &shape)]);
        let vals = transcode(&[key(10), key(20)], &shape);
        CorgiChunk::from_columns(keys, vals, vec![Time::minimum(); 2], vec![1, 1])
    }

    #[test]
    fn a_hash_collision_does_not_match_the_wrong_key() {
        let key = |n: i64| DValue::Tuple(vec![DValue::Int(n)]);
        let chunk = colliding_chunk();

        // The fixture really is a collision: the identifier alone selects both rows, so the only
        // thing that can separate them is the real-key comparison.
        let (lo, hi) = find_ranges(&CValue::u64(vec![7]), key_lane(chunk.keys()));
        assert_eq!((lo[0], hi[0]), (0, 2), "both rows sit under identifier 7");

        let mut probing = Probing {
            updates: vec![((key(2), (), Time::minimum()), Time::minimum(), 1)],
            ids: vec![7],
            hashed: true,
            chunks: vec![chunk],
            next: 0,
            lower: vec![Time::minimum()],
            ready: VecDeque::new(),
            logic: Rc::new(RefCell::new(|_k: &DValue, _v: &(), val: &DValue| val.clone())),
            strict: false,
        };
        let mut out = Vec::new();
        while let Some((batch, _time)) = probing.next() {
            out.extend(batch.into_iter().map(|((val, _payload), _initial, _diff)| val));
        }
        out.sort();
        assert_eq!(out, vec![key(20)], "only the row whose real key equals the probe");
    }
}
