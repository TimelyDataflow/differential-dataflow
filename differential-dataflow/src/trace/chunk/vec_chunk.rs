//! A worked [`Chunk`] implementation: `Vec<((K, V), T, R)>` behind an `Rc`.
//!
//! This is the reference example — a next implementor (e.g. columnar) follows
//! its *shape*, not its layout. It shows the two integration points any chunk
//! type satisfies, and how leaning on the parent module's generic harnesses
//! keeps the code terse:
//!
//! * **Batcher side.** The merge batcher's `ContainerChunker` builds chunks, so
//!   the type implements timely's container traits (`Accountable`,
//!   `SizableContainer`, `Consolidate`, `PushInto`). Here they delegate to the
//!   inner `Vec` via `Rc::make_mut` — free while a chunk is being built
//!   (refcount 1), and it never copies a *shared* chunk because batches are
//!   immutable once built.
//! * **Trace side.** [`Chunk`] (merge / extract / advance / regrade / bounds)
//!   plus a cursor. Key lookups are logarithmic by galloping search (`seek_*`),
//!   independent of chunk size; stepping stays linear (short hops).
//!
//! `Clone` is a refcount bump, so the trace merger shares source chunks instead
//! of copying them.
//!
//! **What a columnar impl can and can't reuse.** The protocol (the `VecDeque`
//! in/out, withhold-by-`push_front`, grade-at-seal) is layout-agnostic and carries
//! over unchanged. The *merge body* does not: this one merges a single contiguous
//! `&[((K,V),T,R)]` and bulk-copies disjoint runs with `extend_from_slice` +
//! `chunks(TARGET)`. A columnar chunk (ranging over `ord_neu`'s deduped layout) has
//! no such slice — it must range-copy the key / val / time / diff columns with
//! offset bookkeeping, emitting one key + its val/time run rather than repeated rows.
//! That is the operation that beats the flat layout on repetitive keys (see the
//! module-level note on the row-major vs. columnar crossover), and it is also where
//! the earlier `col_chunk` got into trouble (decompress-and-recompress instead of a
//! true range-copy). So a columnar `Chunk` is the open bet: nothing here exercises a
//! columnar merge, and that body — not the protocol — is the phase-2 risk.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::rc::Rc;

use timely::Accountable;
use timely::container::{PushInto, SizableContainer};
use timely::progress::{Antichain, Timestamp};

use crate::consolidation::Consolidate;
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::cursor::Cursor;
use crate::trace::implementations::{Vector, WithLayout};

use super::Chunk;

/// The chunk size: both the maximum updates per chunk and the coalescing
/// threshold (see [`Chunk::TARGET`]). Chosen for the reference impl; exposed as
/// the associated const below, and used internally for buffer sizing.
const TARGET: usize = 1024;

/// A sorted, consolidated run of `((key, val), time, diff)`, shared via `Rc`.
pub struct VecChunk<K, V, T, R>(Rc<Vec<((K, V), T, R)>>);

impl<K, V, T, R> Clone for VecChunk<K, V, T, R> {
    fn clone(&self) -> Self { VecChunk(Rc::clone(&self.0)) }
}
impl<K, V, T, R> Default for VecChunk<K, V, T, R> {
    fn default() -> Self { VecChunk(Rc::new(Vec::new())) }
}

/// The trace type for `arrange`: a spine of `Rc`-shared chunk batches.
pub type ChunkSpine<K, V, T, R> = super::ChunkSpine<VecChunk<K, V, T, R>>;
/// Merge batcher over `VecChunk`s. Unordered `Vec<((K, V), T, R)>` input is
/// consolidated into sorted `VecChunk`s by a `ContainerChunker<VecChunk>` supplied
/// at the `arrange_core` callsite (it drives the container-trait impls below); the
/// batcher itself only merges the resulting chunks.
pub type ChunkBatcher<K, V, T, R> = super::ChunkBatcher<VecChunk<K, V, T, R>>;
/// Reference-counted batch builder.
pub type ChunkRcBuilder<K, V, T, R> = super::ChunkRcBuilder<VecChunk<K, V, T, R>>;

// --- batcher side: timely container traits, delegating to the inner `Vec` ---

impl<K: 'static, V: 'static, T: 'static, R: 'static> Accountable for VecChunk<K, V, T, R> {
    fn record_count(&self) -> i64 { self.0.len() as i64 }
}

impl<K, V, T, R> SizableContainer for VecChunk<K, V, T, R>
where K: Clone+'static, V: Clone+'static, T: Clone+'static, R: Clone+'static {
    // The absorb point is the grading target: the chunker fills a scratch chunk
    // to `TARGET` updates before emitting, so chunks arrive pre-graded rather than
    // at timely's byte-derived buffer size (which downstream regrading re-melds).
    fn at_capacity(&self) -> bool { self.0.len() >= TARGET }
    fn ensure_capacity(&mut self, _stash: &mut Option<Self>) {
        let inner = Rc::make_mut(&mut self.0);
        inner.reserve(TARGET.saturating_sub(inner.len()));
    }
}

impl<K, V, T, R> Consolidate for VecChunk<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Ord+Clone+'static, R: Semigroup+'static {
    fn len(&self) -> usize { self.0.len() }
    fn clear(&mut self) { Rc::make_mut(&mut self.0).clear() }
    fn consolidate_into(&mut self, target: &mut Self) {
        Rc::make_mut(&mut self.0).consolidate_into(Rc::make_mut(&mut target.0));
    }
}

impl<K, V, T, R> PushInto<((K, V), T, R)> for VecChunk<K, V, T, R>
where K: Clone+'static, V: Clone+'static, T: Clone+'static, R: Clone+'static {
    fn push_into(&mut self, item: ((K, V), T, R)) { Rc::make_mut(&mut self.0).push(item); }
}

// --- trace side: a logarithmic cursor and the `Chunk` operations ---

/// First index `>= start` at which `pred` turns false, by galloping (exponential)
/// search. `pred` must hold for a prefix then not — i.e. `|u| u < target`.
/// O(log distance), so O(1) for short hops and logarithmic for long ones.
fn gallop<U>(s: &[U], start: usize, pred: impl Fn(&U) -> bool) -> usize {
    let mut pos = start;
    if pos < s.len() && pred(&s[pos]) {
        let mut step = 1;
        while pos + step < s.len() && pred(&s[pos + step]) { pos += step; step <<= 1; }
        step >>= 1;
        while step > 0 {
            if pos + step < s.len() && pred(&s[pos + step]) { pos += step; }
            step >>= 1;
        }
        pos += 1;
    }
    pos
}

/// A cursor over a [`VecChunk`], tracking the current key and `(key, val)`
/// group starts as indices into the flat vector.
pub struct VecChunkCursor<K, V, T, R> {
    key_pos: usize,
    val_pos: usize,
    phantom: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> WithLayout for VecChunk<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    type Layout = Vector<((K, V), T, R)>;
}

impl<K, V, T, R> WithLayout for VecChunkCursor<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    type Layout = Vector<((K, V), T, R)>;
}

impl<K, V, T, R> Cursor for VecChunkCursor<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    type Storage = VecChunk<K, V, T, R>;

    fn key_valid(&self, s: &Self::Storage) -> bool { self.key_pos < s.0.len() }
    fn val_valid(&self, s: &Self::Storage) -> bool {
        self.key_pos < s.0.len() && self.val_pos < s.0.len() && s.0[self.val_pos].0.0 == s.0[self.key_pos].0.0
    }
    fn key<'a>(&self, s: &'a Self::Storage) -> &'a K { &s.0[self.key_pos].0.0 }
    fn val<'a>(&self, s: &'a Self::Storage) -> &'a V { &s.0[self.val_pos].0.1 }
    fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<&'a K> {
        if self.key_valid(s) { Some(self.key(s)) } else { None }
    }
    fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<&'a V> {
        if self.val_valid(s) { Some(self.val(s)) } else { None }
    }
    fn map_times<L: FnMut(&T, &R)>(&mut self, s: &Self::Storage, mut logic: L) {
        if !self.val_valid(s) { return; }
        let kv = &s.0[self.val_pos].0;
        let mut i = self.val_pos;
        while i < s.0.len() && &s.0[i].0 == kv {
            logic(&s.0[i].1, &s.0[i].2);
            i += 1;
        }
    }
    fn step_key(&mut self, s: &Self::Storage) {
        // Linear: stepping is a short hop to the next group; an inlined scan
        // beats a gallop call for the common small-group case.
        if self.key_pos >= s.0.len() { return; }
        let key = s.0[self.key_pos].0.0.clone();
        let mut i = self.key_pos;
        while i < s.0.len() && s.0[i].0.0 == key { i += 1; }
        self.key_pos = i;
        self.val_pos = i;
    }
    fn seek_key(&mut self, s: &Self::Storage, key: &K) {
        // Logarithmic: O(log distance), independent of chunk size.
        self.key_pos = gallop(&s.0, self.key_pos, |u| &u.0.0 < key);
        self.val_pos = self.key_pos;
    }
    fn step_val(&mut self, s: &Self::Storage) {
        if !self.val_valid(s) { return; }
        let kv = s.0[self.val_pos].0.clone();
        let mut i = self.val_pos;
        while i < s.0.len() && s.0[i].0 == kv { i += 1; }
        self.val_pos = i;
    }
    fn seek_val(&mut self, s: &Self::Storage, val: &V) {
        if !self.key_valid(s) { return; }
        let key = s.0[self.key_pos].0.0.clone();
        self.val_pos = gallop(&s.0, self.val_pos, |u| (&u.0.0, &u.0.1) < (&key, val));
    }
    fn rewind_keys(&mut self, _s: &Self::Storage) { self.key_pos = 0; self.val_pos = 0; }
    fn rewind_vals(&mut self, _s: &Self::Storage) { self.val_pos = self.key_pos; }
}

/// Take the `Vec` out of a chunk, copying only if the `Rc` is shared.
fn take<K: Clone, V: Clone, T: Clone, R: Clone>(chunk: VecChunk<K, V, T, R>) -> Vec<((K, V), T, R)> {
    Rc::try_unwrap(chunk.0).unwrap_or_else(|rc| (*rc).clone())
}

impl<K, V, T, R> Chunk for VecChunk<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    type Cursor = VecChunkCursor<K, V, T, R>;

    const TARGET: usize = TARGET;

    fn cursor(&self) -> Self::Cursor {
        VecChunkCursor { key_pos: 0, val_pos: 0, phantom: PhantomData }
    }

    fn bounds(&self) -> ((&K, &V, &T), (&K, &V, &T)) {
        let s = &self.0[..];
        let (f, l) = (&s[0], &s[s.len() - 1]);
        ((&f.0.0, &f.0.1, &f.1), (&l.0.0, &l.0.1, &l.1))
    }

    fn len(&self) -> usize { self.0.len() }

    /// A two-pointer binary merge that drains the two deques' *loaded* content
    /// through their shared horizon — the lesser of the two deques' last loaded
    /// `(key, val, time)`s — rather than one front-pair at a time. Consolidates
    /// equal triples and bulk-copies disjoint runs as slices, walking across chunk
    /// boundaries with local indices (`p1`/`p2`) that reset as each working chunk
    /// is retired. The side owning the horizon drains fully; the other's partial
    /// working chunk is pruned (its prefix dropped) and `push_front`ed back exactly
    /// once at the yield boundary — so the per-call prune cost amortizes over the
    /// whole burst the harness loaded, not over each chunk.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        fn kv<K, V, T, R>(u: &((K, V), T, R)) -> (&K, &V) { (&u.0.0, &u.0.1) }

        let mut result: Vec<((K, V), T, R)> = Vec::with_capacity(TARGET);
        let mut flush = |result: &mut Vec<((K, V), T, R)>, force: bool| {
            if result.len() >= TARGET || (force && !result.is_empty()) {
                out.push_back(VecChunk(Rc::new(std::mem::replace(result, Vec::with_capacity(TARGET)))));
            }
        };

        // Working chunks (the shared `Rc`, read by index — never `take`n, so a
        // source clone is not deep-copied) and their positions; both deques are
        // non-empty on entry. The guard keeps both cursors valid for indexing; a
        // working chunk consumed mid-merge is refilled at the foot of the loop, and
        // when a deque runs dry we stop — that side has presented all its loaded
        // data, so its last triple is the horizon and the rest is left for next time.
        let mut c1 = in1.pop_front().unwrap();
        let mut c2 = in2.pop_front().unwrap();
        let (mut p1, mut p2) = (0usize, 0usize);
        while p1 < c1.0.len() && p2 < c2.0.len() {
            let a = &c1.0[p1];
            let b = &c2.0[p2];
            match (kv(a), &a.1).cmp(&(kv(b), &b.1)) {
                // Copy the run of one side strictly below the other's head (within
                // the current working chunk): collisions are impossible within it,
                // so it moves as slices cut at the grading target.
                std::cmp::Ordering::Less => {
                    let run = gallop(&c1.0[..], p1 + 1, |u| (kv(u), &u.1) < (kv(b), &b.1));
                    for piece in c1.0[p1..run].chunks(TARGET) {
                        result.extend_from_slice(piece);
                        flush(&mut result, false);
                    }
                    p1 = run;
                }
                std::cmp::Ordering::Greater => {
                    let run = gallop(&c2.0[..], p2 + 1, |u| (kv(u), &u.1) < (kv(a), &a.1));
                    for piece in c2.0[p2..run].chunks(TARGET) {
                        result.extend_from_slice(piece);
                        flush(&mut result, false);
                    }
                    p2 = run;
                }
                std::cmp::Ordering::Equal => {
                    let mut diff = a.2.clone();
                    diff.plus_equals(&b.2);
                    if !diff.is_zero() {
                        result.push((a.0.clone(), a.1.clone(), diff));
                    }
                    p1 += 1;
                    p2 += 1;
                    flush(&mut result, false);
                }
            }
            // Refill either working chunk consumed by the step above; stop the drain
            // once a deque is exhausted (the `&&` guard then never re-enters).
            if p1 == c1.0.len() {
                match in1.pop_front() { Some(c) => { c1 = c; p1 = 0; } None => break }
            }
            if p2 == c2.0.len() {
                match in2.pop_front() { Some(c) => { c2 = c; p2 = 0; } None => break }
            }
        }
        flush(&mut result, true);
        // One side's deque emptied with its working chunk exhausted; the other's
        // working chunk is partial — push back just its unconsumed suffix (one copy
        // per call), ahead of whatever loaded chunks remain in that deque.
        if p1 < c1.0.len() { in1.push_front(VecChunk(Rc::new(c1.0[p1..].to_vec()))); }
        if p2 < c2.0.len() { in2.push_front(VecChunk(Rc::new(c2.0[p2..].to_vec()))); }
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: &Antichain<T>,
        residual: &mut Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        // Fill `TARGET`-sized buffers directly, so the chunks pushed are already
        // graded and `regrade` passes them through as `Rc` moves rather than
        // re-splitting (and re-copying) a monolithic chunk. Emptied input `Vec`s
        // are recycled as the next buffers, so allocations balance input against
        // output instead of one fresh buffer per emitted chunk.
        let mut stash: Vec<Vec<((K, V), T, R)>> = Vec::new();
        let take_buf = |stash: &mut Vec<_>| stash.pop().unwrap_or_default();
        let (mut k, mut s) = (take_buf(&mut stash), take_buf(&mut stash));
        for chunk in input.drain(..) {
            let mut v = take(chunk);
            for u in v.drain(..) {
                if frontier.borrow().less_equal(&u.1) {
                    residual.insert_ref(&u.1);
                    k.push(u);
                    if k.len() >= TARGET { keep.push_back(VecChunk(Rc::new(std::mem::replace(&mut k, take_buf(&mut stash))))); }
                } else {
                    s.push(u);
                    if s.len() >= TARGET { ship.push_back(VecChunk(Rc::new(std::mem::replace(&mut s, take_buf(&mut stash))))); }
                }
            }
            stash.push(v);
        }
        if !k.is_empty() { keep.push_back(VecChunk(Rc::new(k))); }
        if !s.is_empty() { ship.push_back(VecChunk(Rc::new(s))); }
    }

    fn advance(
        input: &mut VecDeque<Self>,
        frontier: &Antichain<T>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        // Advance and consolidate every *complete* `(key, val)` group eagerly,
        // so its updates can be released as soon as the input proves no later
        // time for the pair can arrive. A group is contiguous in the sorted
        // chain, so the only one that might continue in a future call is the last;
        // unless `done`, we process up to its start and `push_front` the rest as
        // the withheld carry for the next call.
        let mut stash: Vec<Vec<((K, V), T, R)>> = Vec::new();
        // Build the working buffer by *reusing the front chunk's storage* (the
        // carry from last time) and appending the rest (recycling each emptied
        // `Vec`). Reusing the front is what keeps a withheld group from being
        // recopied across calls: it just accumulates in place, so a `(key, val)`
        // larger than the working set costs O(total) over the run, not O(total²).
        let mut buf = match input.pop_front() { Some(chunk) => take(chunk), None => return };
        while let Some(chunk) = input.pop_front() {
            let mut v = take(chunk);
            buf.append(&mut v);
            stash.push(v);
        }
        if buf.is_empty() { return; }

        // If every available update shares one `(key, val)`, no group is provably
        // complete — a later call may extend it — so make no progress unless
        // `done`: push the accumulated buffer back as the carry and return. This is
        // the giant-key case; comparing only the first and last pair detects it
        // without scanning, and reusing the front above makes the retention free.
        if !done && buf[0].0 == buf[buf.len() - 1].0 {
            input.push_front(VecChunk(Rc::new(buf)));
            return;
        }

        // Otherwise at least the first group is complete. Withhold the last group
        // (a single `(key, val)`) as the next carry unless the input is complete.
        let end = if done { buf.len() } else {
            let last_kv = buf[buf.len() - 1].0.clone();
            let mut start = buf.len();
            while start > 0 && buf[start - 1].0 == last_kv { start -= 1; }
            start
        };
        if end < buf.len() {
            input.push_front(VecChunk(Rc::new(buf.split_off(end))));
        }
        // Advance + consolidate each group into `TARGET`-sized output chunks,
        // filling buffers reclaimed from the recycled `Vec`s.
        let mut result = stash.pop().unwrap_or_default();
        let mut i = 0;
        while i < buf.len() {
            let mut j = i;
            while j < buf.len() && buf[j].0 == buf[i].0 { j += 1; }
            for u in &mut buf[i..j] { u.1.advance_by(frontier.borrow()); }
            // Advancing is monotone w.r.t. the lattice but not the
            // representation's total order, so re-sort the group by time.
            buf[i..j].sort_by(|a, b| a.1.cmp(&b.1));
            let mut k = i;
            while k < j {
                let kv = buf[k].0.clone();
                let t = buf[k].1.clone();
                let mut diff = buf[k].2.clone();
                k += 1;
                while k < j && buf[k].1 == t { diff.plus_equals(&buf[k].2); k += 1; }
                if !diff.is_zero() {
                    result.push((kv, t, diff));
                    if result.len() >= TARGET { out.push_back(VecChunk(Rc::new(std::mem::replace(&mut result, stash.pop().unwrap_or_default())))); }
                }
            }
            i = j;
        }
        if !result.is_empty() { out.push_back(VecChunk(Rc::new(result))); }
    }

    fn regrade(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        // Maximal packing: emit chunks as large as possible up to `TARGET`,
        // never splitting a pair that could combine into one legal (`<= TARGET`)
        // chunk. A chunk of exactly `TARGET` is maximal — it cannot grow — so it
        // passes straight through as an `Rc` move; only sub-`TARGET` chunks are
        // copied, and only to coalesce with a neighbour. Producers fill to
        // `TARGET`, so in steady state every chunk passes through and only the
        // occasional trailing partial is coalesced.
        //
        // `carry` is the (sub-`TARGET`) chunk under construction. It is flushed
        // once it reaches `TARGET`, `push_front`ed back onto `input` between calls,
        // or emitted on `done`. Whenever `carry` is non-empty its left neighbour in
        // `out` is a `TARGET` chunk (or `carry` is `out`'s first chunk), so
        // emitting `carry` against a neighbour it cannot merge with — their sum
        // exceeds `TARGET` — keeps the packing maximal on both sides.
        let mut carry: Vec<((K, V), T, R)> = Vec::new();
        while let Some(chunk) = input.pop_front() {
            if carry.is_empty() {
                absorb(chunk, &mut carry, out);
            } else if carry.len() + chunk.0.len() <= TARGET {
                // Combines into one legal chunk; coalesce in place.
                carry.extend(take(chunk));
                if carry.len() == TARGET {
                    out.push_back(VecChunk(Rc::new(std::mem::take(&mut carry))));
                }
            } else {
                // Cannot combine without exceeding `TARGET`; `carry` is maximal
                // against this neighbour, so emit it and absorb the chunk afresh.
                out.push_back(VecChunk(Rc::new(std::mem::take(&mut carry))));
                absorb(chunk, &mut carry, out);
            }
        }
        if !carry.is_empty() {
            let chunk = VecChunk(Rc::new(carry));
            if done { out.push_back(chunk); } else { input.push_front(chunk); }
        }
    }
}

/// Emit maximal `TARGET`-sized chunks off the front of `carry`, leaving the
/// sub-`TARGET` tail behind.
fn peel<K: Clone, V: Clone, T: Clone, R: Clone>(
    carry: &mut Vec<((K, V), T, R)>,
    out: &mut VecDeque<VecChunk<K, V, T, R>>,
) {
    let mut start = 0;
    while carry.len() - start >= TARGET {
        out.push_back(VecChunk(Rc::new(carry[start..start + TARGET].to_vec())));
        start += TARGET;
    }
    carry.drain(..start);
}

/// Absorb a chunk when nothing is carried: pass a `TARGET` chunk through as an
/// `Rc` move, hold a smaller one in `carry`, or split a larger one (peeling off
/// `TARGET` pieces and carrying the remainder). `carry` must be empty on entry.
fn absorb<K: Clone, V: Clone, T: Clone, R: Clone>(
    chunk: VecChunk<K, V, T, R>,
    carry: &mut Vec<((K, V), T, R)>,
    out: &mut VecDeque<VecChunk<K, V, T, R>>,
) {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match chunk.0.len().cmp(&TARGET) {
        Equal => out.push_back(chunk),
        Less => *carry = take(chunk),
        Greater => { *carry = take(chunk); peel(carry, out); }
    }
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;
    use super::{Chunk, VecChunk};
    use crate::trace::chunk::merge_chains;
    use std::rc::Rc;

    fn chunk(updates: Vec<((u64, u64), u64, i64)>) -> VecChunk<u64, u64, u64, i64> {
        VecChunk(Rc::new(updates))
    }

    // Flatten a chunk sequence back to its update stream.
    fn flat<I: IntoIterator<Item = VecChunk<u64, u64, u64, i64>>>(chunks: I) -> Vec<((u64, u64), u64, i64)> {
        chunks.into_iter().flat_map(|c| (*c.0).clone()).collect()
    }

    // `extract` partitions by frontier and folds the kept frontier into `residual`;
    // a terminal `regrade` then grades each side (the seams of near-graded output).
    #[test]
    fn extract_partitions_and_grades() {
        use super::TARGET;
        use crate::trace::chunk::{is_graded, regrade_all};
        use timely::progress::Antichain;

        // 4·TARGET updates spread over many input chunks; even times ship
        // (< frontier), odd times keep (>= frontier), so both sides straddle.
        let n = 4 * TARGET as u64;
        let mut input: VecDeque<_> = (0..n).map(|i| chunk(vec![((i, 0), i % 2, 1)])).collect();
        let frontier = Antichain::from_elem(1u64);
        let mut residual = Antichain::new();
        let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
        VecChunk::extract(&mut input, &frontier, &mut residual, &mut keep, &mut ship);
        let (keep, ship) = (regrade_all(keep), regrade_all(ship));

        // Kept times are exactly {1}; that is the residual frontier.
        assert_eq!(residual, Antichain::from_elem(1u64));
        // Both sides are graded after the regrade.
        assert!(is_graded(&keep), "ungraded keep: {:?}", keep.iter().map(Chunk::len).collect::<Vec<_>>());
        assert!(is_graded(&ship), "ungraded ship: {:?}", ship.iter().map(Chunk::len).collect::<Vec<_>>());
        // Nothing lost: half the updates each way.
        assert_eq!(keep.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
        assert_eq!(ship.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
    }

    // `advance` advances and consolidates complete `(key, val)` groups eagerly,
    // pushing the (possibly-growing) last group back as the carry when not `done`.
    #[test]
    fn advance_emits_complete_groups_eagerly() {
        use timely::progress::Antichain;

        let frontier = Antichain::from_elem(5u64);
        // Group (0,0) is complete within this chunk; group (1,0) might still grow.
        let c0 = chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)]);
        let mut input: VecDeque<_> = VecDeque::from([c0]);
        let mut out = VecDeque::new();
        VecChunk::advance(&mut input, &frontier, false, &mut out);

        // The trailing group (1,0) is withheld as the carry at the front of `input`.
        assert_eq!(input.len(), 1);
        assert_eq!(Chunk::len(&input[0]), 1);
        // Group (0,0)'s times {0,1} advanced to 5 and consolidated, emitted now.
        assert_eq!(flat(out), vec![((0, 0), 5, 2)]);
    }

    // Streaming the input one chunk at a time must yield exactly what a single
    // all-at-once flush does — the resumable path is just the one-shot path cut
    // at group boundaries.
    #[test]
    fn advance_resumable_matches_oneshot() {
        use timely::progress::Antichain;

        let frontier = Antichain::from_elem(3u64);
        // Groups span chunk boundaries and carry several times each.
        let input = || vec![
            chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)]),
            chunk(vec![((1, 0), 5, 1), ((1, 1), 0, 1), ((2, 0), 0, 1)]),
            chunk(vec![((2, 0), 2, 1), ((2, 0), 9, 1)]),
        ];

        let oneshot = {
            let mut q: VecDeque<_> = input().into();
            let mut out = VecDeque::new();
            VecChunk::advance(&mut q, &frontier, false, &mut out);
            VecChunk::advance(&mut q, &frontier, true, &mut out);
            flat(out)
        };
        let incremental = {
            let mut q = VecDeque::new();
            let mut out = VecDeque::new();
            for c in input() { q.push_back(c); VecChunk::advance(&mut q, &frontier, false, &mut out); }
            VecChunk::advance(&mut q, &frontier, true, &mut out);
            flat(out)
        };
        assert_eq!(oneshot, incremental);
        // Times are advanced: nothing below the frontier survives.
        for u in &oneshot { assert!(u.1 >= 3); }
    }

    // A single `(key, val)` whose updates span every pushed chunk: `advance`
    // can make no progress until `done`, accumulating in the carry in place.
    // It must still produce the right advanced+consolidated result at the end.
    #[test]
    fn advance_single_key_spanning_pushes() {
        use timely::progress::Antichain;

        let frontier = Antichain::from_elem(100u64);
        let n = 50u64;
        let make = || (0..n).map(|t| chunk(vec![((7u64, 0u64), t, 1i64)])).collect::<Vec<_>>();

        let mut q = VecDeque::new();
        let mut out = VecDeque::new();
        for c in make() { q.push_back(c); VecChunk::advance(&mut q, &frontier, false, &mut out); }
        VecChunk::advance(&mut q, &frontier, true, &mut out);
        // All times advance to 100 and consolidate to one update of diff `n`.
        assert_eq!(flat(out), vec![((7u64, 0u64), 100u64, n as i64)]);
    }

    #[test]
    fn merge_chains_consolidates() {
        let a = chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1)]);
        let b = chunk(vec![((0, 0), 0, 1), ((2, 0), 0, 1)]);
        let mut out = VecDeque::new();
        merge_chains(vec![a], vec![b], &mut out);
        assert_eq!(flat(out), vec![((0, 0), 0, 2), ((1, 0), 0, 1), ((2, 0), 0, 1)]);
    }

    // Merging runs larger than `TARGET`, then regrading, yields a *graded* sequence
    // (each chunk `<= TARGET`, adjacent pairs summing past `TARGET`) reproducing the
    // consolidated sorted contents.
    #[test]
    fn merge_emits_graded_chunks() {
        use super::TARGET;
        use crate::trace::chunk::{is_graded, merge_chains, regrade_all};

        // Two interleaving single-chunk chains: evens and odds over `0..4·TARGET`.
        let n = 4 * TARGET as u64;
        let evens = chunk((0..n).step_by(2).map(|k| ((k, 0), 0, 1)).collect());
        let odds = chunk((0..n).step_by(2).map(|k| ((k + 1, 0), 0, 1)).collect());

        let mut out = VecDeque::new();
        merge_chains(vec![evens], vec![odds], &mut out);
        let chunks = regrade_all(out);

        assert!(is_graded(&chunks), "merge output not graded: {:?}",
            chunks.iter().map(Chunk::len).collect::<Vec<_>>());
        // Contents are exactly the sorted keys `0..4·TARGET`, each once.
        let want: Vec<_> = (0..n).map(|k| ((k, 0u64), 0u64, 1i64)).collect();
        assert_eq!(flat(chunks), want);
    }

    // Property test: merging two *multi-chunk* chains (driven through `merge` by
    // `merge_chains`) reproduces the union of all updates, consolidated. Tiny
    // chunks force `(key, val)` groups — which can span several times — to
    // straddle chunk boundaries on both sides, exercising the refill path the
    // single-chunk merge tests never reach. The independent oracle is
    // `consolidate_updates` over the concatenation.
    #[test]
    fn merge_matches_reference() {
        use crate::trace::chunk::merge_chains;
        use crate::consolidation::consolidate_updates;

        // Deterministic xorshift PRNG — no dev-dependency on `rand`.
        let mut seed = 0x2545F4914F6CDD1Du64;
        let mut rng = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

        // A sorted, consolidated update set over a small (key, val, time) space,
        // so the two chains collide and a `(key, val)` carries several times.
        fn gen(rng: &mut impl FnMut() -> u64, n: usize) -> Vec<((u64, u64), u64, i64)> {
            let mut v: Vec<((u64, u64), u64, i64)> = (0..n).map(|_| {
                let k = rng() % 20; let val = rng() % 3; let t = rng() % 8;
                let d = if rng() % 4 == 0 { -1 } else { 1 };
                ((k, val), t, d)
            }).collect();
            consolidate_updates(&mut v);
            v
        }
        // Split a consolidated set into a chain of small chunks (each sorted and
        // consolidated; together globally sorted), so groups straddle boundaries.
        fn chain(updates: &[((u64, u64), u64, i64)], sz: usize) -> Vec<VecChunk<u64, u64, u64, i64>> {
            updates.chunks(sz).map(|c| VecChunk(Rc::new(c.to_vec()))).collect()
        }

        for _ in 0..300 {
            let n1 = (rng() as usize % 60) + 1;
            let u1 = gen(&mut rng, n1);
            let n2 = (rng() as usize % 60) + 1;
            let u2 = gen(&mut rng, n2);
            if u1.is_empty() || u2.is_empty() { continue; }
            let sz = (rng() as usize % 5) + 1; // tiny chunks → heavy straddling

            let mut out = VecDeque::new();
            merge_chains(chain(&u1, sz), chain(&u2, sz), &mut out);
            let merged = flat(out);

            let mut reference: Vec<_> = u1.iter().chain(u2.iter()).cloned().collect();
            consolidate_updates(&mut reference);

            assert_eq!(merged, reference, "chunk size {sz}\n  u1={u1:?}\n  u2={u2:?}");
        }
    }

    // `regrade` must produce a *maximal packing*: adjacent sub-`TARGET` chunks
    // that could combine into one legal chunk are coalesced, full chunks pass
    // through as `Rc` moves, and contents are preserved exactly.
    #[test]
    fn regrade_maximal_packing() {
        use super::TARGET;
        use crate::trace::chunk::is_graded;

        // A mix of small and full chunks with distinct, increasing keys (so the
        // concatenation is sorted and nothing consolidates away).
        let t = TARGET;
        let sizes = [t / 3, t / 3, t / 3, t, t / 2, t / 2, t, 1, t - 1];
        let total: usize = sizes.iter().sum();
        let mut key = 0u64;
        let mut input = VecDeque::new();
        let mut output = VecDeque::new();
        for &s in &sizes {
            let updates: Vec<_> = (0..s).map(|_| { let k = key; key += 1; ((k, 0u64), 0u64, 1i64) }).collect();
            input.push_back(chunk(updates));
            VecChunk::regrade(&mut input, false, &mut output);
        }
        VecChunk::regrade(&mut input, true, &mut output);
        let chunks: Vec<_> = output.into();

        assert!(is_graded(&chunks), "not graded: {:?}",
            chunks.iter().map(Chunk::len).collect::<Vec<_>>());
        // Nothing lost, and the keys stay strictly sorted across the new breaks.
        let got: Vec<_> = chunks.into_iter().flat_map(|c| (*c.0).clone()).collect();
        assert_eq!(got.len(), total);
        assert!(got.windows(2).all(|w| w[0].0.0 < w[1].0.0));
    }

    // The indexed cursor must reconstruct the same grouped updates as a flat
    // reference, even when a key — and a `(key, val)`'s times — straddle a
    // chunk boundary.
    #[test]
    fn cursor_handles_straddle() {
        use crate::trace::cursor::Cursor;
        use crate::trace::{BatchReader, Description};
        use crate::trace::chunk::ChunkBatch;
        use timely::progress::Antichain;

        let chunks = vec![
            chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1), ((1, 1), 0, 1)]),
            chunk(vec![((1, 1), 1, 1), ((1, 2), 0, 1)]),
            chunk(vec![((2, 0), 0, 1)]),
        ];
        let desc = Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(2u64),
            Antichain::from_elem(0u64),
        );
        let batch = ChunkBatch::new(chunks, desc);

        let mut cursor = batch.cursor();
        let got = cursor.to_vec(&batch, |k| *k, |v| *v);
        let want = vec![
            ((0u64, 0u64), vec![(0u64, 1i64)]),
            ((1, 0), vec![(0, 1)]),
            ((1, 1), vec![(0, 1), (1, 1)]),
            ((1, 2), vec![(0, 1)]),
            ((2, 0), vec![(0, 1)]),
        ];
        assert_eq!(got, want);
    }

    // Isolated: gallop vs linear forward-seek over one big chunk, for sparse to
    // dense probe sets. Run: cargo test seek_microbench -- --ignored --nocapture
    #[test]
    #[ignore]
    fn seek_microbench() {
        use std::time::Instant;
        use std::hint::black_box;
        use super::gallop;
        let n = 1_000_000u64;
        let data: Vec<((u64, ()), u64, isize)> = (0..n).map(|k| ((3 * k, ()), 0u64, 1isize)).collect();
        for probes in [100u64, 10_000, 1_000_000] {
            let targets: Vec<u64> = (0..probes).map(|i| 3 * (i * n / probes)).collect();
            let best = |f: &dyn Fn() -> u64| {
                let mut b = std::time::Duration::MAX;
                for _ in 0..5 { let t = Instant::now(); black_box(f()); b = b.min(t.elapsed()); }
                b
            };
            let data = black_box(&data[..]);
            let g = best(&|| {
                let (mut pos, mut acc) = (0usize, 0u64);
                for &tgt in &targets { pos = gallop(data, pos, |u| u.0.0 < tgt); acc += pos as u64; }
                acc
            });
            let l = best(&|| {
                let (mut pos, mut acc) = (0usize, 0u64);
                for &tgt in &targets { while pos < data.len() && data[pos].0.0 < tgt { pos += 1; } acc += pos as u64; }
                acc
            });
            eprintln!("probes={probes:>7}: gallop={g:>12?}  linear={l:>12?}");
        }
    }
}
