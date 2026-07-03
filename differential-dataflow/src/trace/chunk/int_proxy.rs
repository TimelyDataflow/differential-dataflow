//! [`ProxyChunk`]: sorted, consolidated runs of `((key_hash, value_id), time, diff)` — the
//! integers-only boundary between DD's operator logic and a columnar value backend.
//!
//! A backend arrangement keeps its real keys and values in whatever layout it likes; what
//! crosses to DD is a *projection* of each record onto two integers:
//!
//! * `key_hash: u64` — a content hash of the key, **stable across the whole system**: the
//!   same key hashes identically in every operator, including across the output→input
//!   boundary (a reduce output re-ingested as a downstream input), with no registry.
//!   It drives grouping, ordering, and merging. Collisions are an accepted risk (see the
//!   note in [`operators::int_proxy`](crate::operators::int_proxy)).
//! * `value_id: u64` — an intra-key identifier for a value, **ephemeral**: consistent only
//!   within a single operator computation (one presentation, one retire's waves). Equal
//!   values ⇔ equal ids there — a bijection per computation, which is all DD needs to
//!   consolidate diffs per value and decide presence; a hash is one scheme, not a
//!   requirement. It is *not* order-preserving with respect to the value type's
//!   semantic order — see the design note in [`operators::int_proxy`](crate::operators::int_proxy).
//!
//! Chunks sort by `(key_hash, value_id, time)` — an arbitrary but consistent total order
//! over which DD's sort/merge/consolidate machinery is completely data-blind. Time and
//! diff stay DD-native: the backend never needs to understand timestamps.
//!
//! # Relation to the `Chunk` / `NavigableChunk` split
//!
//! `ProxyChunk` implements [`Chunk`], so it slots directly into the #778 machinery: batches
//! (`ChunkBatch<ProxyChunk>`), fueled merging (`ChunkBatchMerger`), grading (`settle`) all
//! come for free, entirely in integer space. Like a columnar backend's chunk (and unlike
//! [`VecChunk`](super::vec::VecChunk)) it is deliberately **not** [`NavigableChunk`](super::NavigableChunk):
//! its consumers are whole-chunk tactics that read the columns in bulk, not cursors.
//! (`u64` keys and values are `Ord`, so the capability could be added if a cursor-driven
//! consumer ever wants it.)
//!
//! Only the ids live here; the real data stays in the backend. Read-side chunks are
//! produced by a backend's `present` (see [`operators::int_proxy`](crate::operators::int_proxy)),
//! which retains the alignment from record index to real record. Write-side chunks are
//! assembled by the reduce tactic from `((key_hash, value_id), time, diff)` deltas and
//! handed back to the backend's `materialize` to become a real output batch.

use std::collections::VecDeque;
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::lattice::Lattice;

use super::{pack, Chunk};

/// The grading target. Records are four machine words plus time; small chunks would
/// under-amortize per-chunk overhead, and the fueled merger wants to yield.
const TARGET: usize = 8192;

/// Shared, immutable chunk contents; `Clone` of an [`ProxyChunk`] is an `Rc` bump.
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
pub struct ProxyChunk<T, R>(Rc<Inner<T, R>>);

impl<T, R> Clone for ProxyChunk<T, R> {
    fn clone(&self) -> Self { ProxyChunk(Rc::clone(&self.0)) }
}

impl<T, R> Default for ProxyChunk<T, R> {
    fn default() -> Self {
        ProxyChunk(Rc::new(Inner { keys: Vec::new(), vals: Vec::new(), times: Vec::new(), diffs: Vec::new() }))
    }
}

impl<T, R> ProxyChunk<T, R> {
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

impl<T: Clone + Ord, R: Clone> ProxyChunk<T, R> {
    /// Assemble a chunk from columns already sorted by `(key, val, time)` with no two
    /// records equal in all three (i.e. consolidated).
    pub fn from_sorted(keys: Vec<u64>, vals: Vec<u64>, times: Vec<T>, diffs: Vec<R>) -> Self {
        debug_assert!(keys.len() == vals.len() && vals.len() == times.len() && times.len() == diffs.len());
        debug_assert!((1..keys.len()).all(|i| {
            (keys[i - 1], vals[i - 1], &times[i - 1]) < (keys[i], vals[i], &times[i])
        }));
        ProxyChunk(Rc::new(Inner { keys, vals, times, diffs }))
    }

    /// Extract the columns, copying only if the `Rc` is shared.
    fn into_parts(self) -> (Vec<u64>, Vec<u64>, Vec<T>, Vec<R>) {
        match Rc::try_unwrap(self.0) {
            Ok(inner) => (inner.keys, inner.vals, inner.times, inner.diffs),
            Err(rc) => (rc.keys.clone(), rc.vals.clone(), rc.times.clone(), rc.diffs.clone()),
        }
    }

    /// The records at `idx`, as a new chunk (the integer `gather`).
    fn gather(&self, idx: impl Iterator<Item = usize> + Clone) -> Self {
        let inner = &*self.0;
        ProxyChunk(Rc::new(Inner {
            keys: idx.clone().map(|i| inner.keys[i]).collect(),
            vals: idx.clone().map(|i| inner.vals[i]).collect(),
            times: idx.clone().map(|i| inner.times[i].clone()).collect(),
            diffs: idx.map(|i| inner.diffs[i].clone()).collect(),
        }))
    }
}

impl<T, R> ProxyChunk<T, R>
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
        let perm = sort_perm(&keys, &vals, &times);

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
        (ProxyChunk(Rc::new(Inner { keys: ok, vals: ov, times: ot, diffs: od })), reps)
    }
}

/// The permutation sorting records by `(key_hash, value_id, time)`.
///
/// `key_hash` is a full 64-bit content hash (uniformly distributed), so a single MSD counting pass on
/// its high byte splits the records into 256 near-equal buckets; each bucket is then finished by a
/// comparison sort on the full `(key_hash, value_id, time)` key. That replaces most of the global
/// `n log n` with one linear pass plus small local sorts — and degrades gracefully to the plain
/// comparison sort when the hashes cluster (one bucket) or `n` is too small to amortize the pass.
/// Stability is irrelevant: `from_unsorted` consolidates by full-key equality, order-blind within a
/// group.
fn sort_perm<T: Ord>(keys: &[u64], vals: &[u64], times: &[T]) -> Vec<usize> {
    let n = keys.len();
    let cmp = |&a: &usize, &b: &usize| (keys[a], vals[a], &times[a]).cmp(&(keys[b], vals[b], &times[b]));
    if n < 512 {
        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_unstable_by(cmp);
        return perm;
    }
    // MSD counting sort on the top byte of key_hash → contiguous, ascending buckets.
    let bucket = |i: usize| (keys[i] >> 56) as usize;
    let mut counts = [0usize; 256];
    for i in 0..n { counts[bucket(i)] += 1; }
    let mut starts = [0usize; 257];
    for b in 0..256 { starts[b + 1] = starts[b] + counts[b]; }
    let mut perm = vec![0usize; n];
    let mut cursor = starts;
    for i in 0..n {
        let b = bucket(i);
        perm[cursor[b]] = i;
        cursor[b] += 1;
    }
    // Finish each bucket by the full key (all share the high byte, so ties on it are common).
    for b in 0..256 {
        perm[starts[b]..starts[b + 1]].sort_unstable_by(cmp);
    }
    perm
}

/// Column builders for one output chunk, emitted at `TARGET`.
struct OutBuf<T, R> {
    keys: Vec<u64>,
    vals: Vec<u64>,
    times: Vec<T>,
    diffs: Vec<R>,
}

impl<T: Clone + Ord, R: Clone> OutBuf<T, R> {
    fn new() -> Self {
        OutBuf { keys: Vec::new(), vals: Vec::new(), times: Vec::new(), diffs: Vec::new() }
    }
    fn push(&mut self, k: u64, v: u64, t: T, d: R) {
        self.keys.push(k);
        self.vals.push(v);
        self.times.push(t);
        self.diffs.push(d);
    }
    fn len(&self) -> usize { self.times.len() }
    /// Emit the buffered records if at `TARGET` (or `force` and non-empty).
    fn flush(&mut self, force: bool, out: &mut VecDeque<ProxyChunk<T, R>>) {
        if self.len() >= TARGET || (force && self.len() > 0) {
            let full = std::mem::replace(self, OutBuf::new());
            out.push_back(ProxyChunk(Rc::new(Inner {
                keys: full.keys,
                vals: full.vals,
                times: full.times,
                diffs: full.diffs,
            })));
        }
    }
}

impl<T, R> Chunk for ProxyChunk<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    type Time = T;
    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.times.len() }

    /// Two-pointer merge of the two front chunks through their shared horizon,
    /// consolidating equal `((key, val), time)` triples. Processes only the two front
    /// chunks per call (the `Chunk` contract permits it — "consume at least one input; the
    /// harness may re-invoke"); the survivor's suffix is pushed back once.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let c1 = in1.pop_front().unwrap();
        let c2 = in2.pop_front().unwrap();
        let (a, b) = (&*c1.0, &*c2.0);
        let (n1, n2) = (a.times.len(), b.times.len());

        let mut buf = OutBuf::new();
        let (mut p1, mut p2) = (0usize, 0usize);
        while p1 < n1 && p2 < n2 {
            match (a.keys[p1], a.vals[p1], &a.times[p1]).cmp(&(b.keys[p2], b.vals[p2], &b.times[p2])) {
                std::cmp::Ordering::Less => {
                    buf.push(a.keys[p1], a.vals[p1], a.times[p1].clone(), a.diffs[p1].clone());
                    p1 += 1;
                }
                std::cmp::Ordering::Greater => {
                    buf.push(b.keys[p2], b.vals[p2], b.times[p2].clone(), b.diffs[p2].clone());
                    p2 += 1;
                }
                std::cmp::Ordering::Equal => {
                    let mut d = a.diffs[p1].clone();
                    d.plus_equals(&b.diffs[p2]);
                    if !d.is_zero() {
                        buf.push(a.keys[p1], a.vals[p1], a.times[p1].clone(), d);
                    }
                    p1 += 1;
                    p2 += 1;
                }
            }
            buf.flush(false, out);
        }
        buf.flush(true, out);

        // Push back the survivor's unconsumed suffix (all `>` the horizon).
        if p1 < n1 { in1.push_front(c1.gather(p1..n1)); }
        if p2 < n2 { in2.push_front(c2.gather(p2..n2)); }
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        // One input chunk per call: partition into keep (`>= frontier`) and ship pieces.
        let Some(chunk) = input.pop_front() else { return };
        let times = chunk.times();
        let (mut ki, mut si) = (Vec::new(), Vec::new());
        for (i, t) in times.iter().enumerate() {
            if frontier.less_equal(t) {
                residual.insert_ref(t);
                ki.push(i);
            } else {
                si.push(i);
            }
        }
        if !ki.is_empty() { keep.push_back(chunk.gather(ki.iter().copied())); }
        if !si.is_empty() { ship.push_back(chunk.gather(si.iter().copied())); }
    }

    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        // Concatenate the pushed-back carry with the newly-arrived chunks, then advance
        // and consolidate each *complete* `(key, val)` group; withhold the trailing group
        // as the carry unless `done`.
        if input.is_empty() { return; }
        let (mut keys, mut vals) = (Vec::new(), Vec::new());
        let (mut times, mut diffs) = (Vec::new(), Vec::new());
        for chunk in input.drain(..) {
            let (mut k, mut v, mut t, mut d) = chunk.into_parts();
            keys.append(&mut k);
            vals.append(&mut v);
            times.append(&mut t);
            diffs.append(&mut d);
        }
        let n = times.len();
        if n == 0 { return; }

        // Giant-key case: the whole buffer is one `(key, val)` — no group provably complete.
        if !done && (keys[0], vals[0]) == (keys[n - 1], vals[n - 1]) {
            input.push_front(ProxyChunk(Rc::new(Inner { keys, vals, times, diffs })));
            return;
        }

        // Withhold the trailing group as the carry unless `done`.
        let end = if done {
            n
        } else {
            let last = (keys[n - 1], vals[n - 1]);
            let mut start = n;
            while start > 0 && (keys[start - 1], vals[start - 1]) == last { start -= 1; }
            start
        };
        if end < n {
            input.push_front(ProxyChunk(Rc::new(Inner {
                keys: keys[end..].to_vec(),
                vals: vals[end..].to_vec(),
                times: times[end..].to_vec(),
                diffs: diffs[end..].to_vec(),
            })));
        }

        // Advance + consolidate each complete group; emit `TARGET`-sized chunks.
        let mut buf = OutBuf::new();
        let mut i = 0;
        while i < end {
            let mut j = i;
            while j < end && (keys[j], vals[j]) == (keys[i], vals[i]) { j += 1; }
            let mut pairs: Vec<(T, R)> = (i..j)
                .map(|k| {
                    let mut t = times[k].clone();
                    t.advance_by(frontier);
                    (t, diffs[k].clone())
                })
                .collect();
            // Advancing is lattice-monotone but not total-order-monotone; re-sort by time.
            pairs.sort_by(|x, y| x.0.cmp(&y.0));
            let mut k = 0;
            while k < pairs.len() {
                let t = pairs[k].0.clone();
                let mut d = pairs[k].1.clone();
                k += 1;
                while k < pairs.len() && pairs[k].0 == t {
                    d.plus_equals(&pairs[k].1);
                    k += 1;
                }
                if !d.is_zero() {
                    buf.push(keys[i], vals[i], t, d);
                    buf.flush(false, out);
                }
            }
            i = j;
        }
        buf.flush(true, out);
    }

    /// Maximal packing via the harness [`pack`]: coalesce by extending the columns in
    /// place, split by copying the ranges, seal as a no-op.
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        pack(
            input,
            done,
            out,
            |acc, next| {
                let (mut k, mut v, mut t, mut d) = std::mem::take(acc).into_parts();
                let (mut k2, mut v2, mut t2, mut d2) = next.into_parts();
                k.append(&mut k2);
                v.append(&mut v2);
                t.append(&mut t2);
                d.append(&mut d2);
                *acc = ProxyChunk(Rc::new(Inner { keys: k, vals: v, times: t, diffs: d }));
            },
            |chunk, m| {
                let n = chunk.len();
                let l = chunk.gather(0..m);
                let r = chunk.gather(m..n);
                (l, r)
            },
            |chunk| chunk,
        );
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, VecDeque};
    use super::{Chunk, ProxyChunk};
    use crate::trace::chunk::{is_graded, ChunkBatch, ChunkBatchMerger};
    use crate::trace::{Description, Merger};
    use timely::progress::Antichain;

    fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    /// Build a single sorted+consolidated chunk from `((key, val), time, diff)` rows.
    fn chunk(rows: &[((u64, u64), u64, i64)]) -> ProxyChunk<u64, i64> {
        let mut m: BTreeMap<((u64, u64), u64), i64> = BTreeMap::new();
        for &(kv, t, d) in rows { *m.entry((kv, t)).or_insert(0) += d; }
        m.retain(|_, d| *d != 0);
        ProxyChunk::from_sorted(
            m.keys().map(|((k, _), _)| *k).collect(),
            m.keys().map(|((_, v), _)| *v).collect(),
            m.keys().map(|(_, t)| *t).collect(),
            m.values().copied().collect(),
        )
    }

    fn read_batch(b: &ChunkBatch<ProxyChunk<u64, i64>>) -> BTreeMap<((u64, u64), u64), i64> {
        let mut m = BTreeMap::new();
        for ch in &b.chunks {
            for i in 0..ProxyChunk::len(ch) {
                *m.entry(((ch.key_hashes()[i], ch.value_ids()[i]), ch.times()[i])).or_insert(0) += ch.diffs()[i];
            }
        }
        m.retain(|_, d| *d != 0);
        m
    }

    fn reference(u1: &[((u64, u64), u64, i64)], u2: &[((u64, u64), u64, i64)], f: u64) -> BTreeMap<((u64, u64), u64), i64> {
        let mut m = BTreeMap::new();
        // `advance_by` on totally-ordered `u64` is `max`.
        for u in u1.iter().chain(u2) { *m.entry((u.0, u.1.max(f))).or_insert(0) += u.2; }
        m.retain(|_, d| *d != 0);
        m
    }

    /// Cut a consolidated set into a batch of small chunks (globally sorted; groups straddle).
    fn batch(rows: &[((u64, u64), u64, i64)], sz: usize) -> ChunkBatch<ProxyChunk<u64, i64>> {
        let mut m: BTreeMap<((u64, u64), u64), i64> = BTreeMap::new();
        for &(kv, t, d) in rows { *m.entry((kv, t)).or_insert(0) += d; }
        m.retain(|_, d| *d != 0);
        let all: Vec<((u64, u64), u64, i64)> = m.into_iter().map(|((kv, t), d)| (kv, t, d)).collect();
        let chunks: Vec<_> = all.chunks(sz.max(1)).map(chunk).collect();
        let desc = Description::new(Antichain::from_elem(0u64), Antichain::from_elem(10u64), Antichain::from_elem(0u64));
        ChunkBatch::new(chunks, desc)
    }

    // The resumable merge→advance→settle pipeline, driven with tiny fuel so it suspends
    // and settles on nearly every tick, must match a one-shot reference and stay graded.
    #[test]
    fn batch_merger_resumable_matches_reference() {
        let mut seed = 0x9E3779B97F4A7C15u64;
        for _ in 0..200 {
            let gen = |seed: &mut u64| -> Vec<((u64, u64), u64, i64)> {
                let n = (xorshift(seed) % 40) as usize + 1;
                (0..n).map(|_| {
                    let k = xorshift(seed) % 10;
                    let v = xorshift(seed) % 3;
                    let t = xorshift(seed) % 6;
                    let d = if xorshift(seed) % 4 == 0 { -1 } else { 1 };
                    ((k, v), t, d)
                }).collect()
            };
            let u1 = gen(&mut seed);
            let u2 = gen(&mut seed);
            let sz = (xorshift(&mut seed) % 4) as usize + 1;
            let f = xorshift(&mut seed) % 6;
            let (s1, s2) = (batch(&u1, sz), batch(&u2, sz));
            let frontier = Antichain::from_elem(f);

            let mut merger = ChunkBatchMerger::new(&s1, &s2, frontier.borrow());
            loop {
                let mut fuel = 1isize; // tiny → many yields, each settling
                merger.work(&s1, &s2, &mut fuel);
                if fuel > 0 { break; }
            }
            let result = merger.done();
            assert!(is_graded(&result.chunks), "ungraded: {:?}", result.chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            assert_eq!(read_batch(&result), reference(&u1, &u2, f), "u1={u1:?}\nu2={u2:?}\nf={f}");
        }
    }

    // `from_unsorted` must sort by `(key, val, time)`, sum diffs of equal triples, drop
    // zeros, and report a representative original index per retained record.
    #[test]
    fn from_unsorted_consolidates_with_provenance() {
        let keys = vec![2, 1, 2, 1, 1];
        let vals = vec![7, 5, 7, 5, 6];
        let times = vec![0u64, 3, 0, 3, 1];
        let diffs = vec![1i64, 1, 1, -1, 1];
        let (chunk, reps) = ProxyChunk::from_unsorted(keys, vals, times, diffs);
        // (1,5)@3 nets to zero and is dropped; (2,7)@0 consolidates to 2.
        assert_eq!(chunk.key_hashes(), &[1, 2]);
        assert_eq!(chunk.value_ids(), &[6, 7]);
        assert_eq!(chunk.times(), &[1, 0]);
        assert_eq!(chunk.diffs(), &[1, 2]);
        // Representatives point at original records carrying those (key, val) pairs.
        assert_eq!(reps.len(), 2);
        assert_eq!(reps[0], 4); // the only (1,6) record
        assert!(reps[1] == 0 || reps[1] == 2); // either (2,7) record serves
    }

    // Settling a run of undersized chunks yields a maximal packing with contents preserved.
    #[test]
    fn settle_grades_and_preserves() {
        let n = 3 * super::TARGET as u64;
        let mut input: VecDeque<_> = (0..n).map(|i| chunk(&[((i, 0), 0, 1)])).collect();
        let mut out = VecDeque::new();
        ProxyChunk::settle(&mut input, true, &mut out);
        let chunks: Vec<_> = out.into();
        assert!(is_graded(&chunks));
        let total: usize = chunks.iter().map(Chunk::len).sum();
        assert_eq!(total, n as usize);
        let keys: Vec<u64> = chunks.iter().flat_map(|c| c.key_hashes().iter().copied()).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }
}
