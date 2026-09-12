//! The proxy join framework.
//!
//! A conventional differential join against `(u64, u64)` values, which are provided by
//! and then interpreted by a backend, who is relieved of lattice-time reasoning. Times are rows
//! of the backend's [`TimeColumn`]: a match's time is the join of two rows, appended to the
//! matches' column, never built as a timestamp.

use std::cell::RefCell;
use std::rc::Rc;

use timely::progress::Timestamp;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use super::times::{Bridge, TimeColumn};
use super::history::ColumnHistory;
use crate::operators::join::{Fresh, JoinTactic};

/// A type that can interpret and retire pairs of lists of batches, joined by key hashes.
///
/// The harness repeatedly invokes [`advance`](Self::advance) to draw a block of the proxy collection,
/// then [`cross`](Self::cross) to turn that block's matches into output containers, until `advance` reports the key space exhausted.
pub trait ProxyJoinBackend<T, B0, B1> {
    /// Diff type of the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type of the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the harness.
    type ROut: Semigroup;
    /// The output container built from matched value ids.
    type Output;
    /// The time column the bridges and matches carry.
    type Times: TimeColumn<Time = T>;

    /// Populates the two bridges with all updates for all keys that match in a returned range.
    ///
    /// The `from` indicates an inclusive lower bound on key hash, and should be updated by the implementor to an exclusive
    /// upper bound for the range of keys it intends to return in this call. The `None` value indicates the keys are exhausted.
    /// The returned bridges must contain all updates from both `instance` inputs for keys that are present in both inputs, and
    /// which are greater or equal to the initial `from`, and not greater or equal to its value when returned.
    fn advance(
        &mut self,
        instance: &JoinInstance<T, B0, B1>,
        from: &mut Option<u64>,
        bridge0: &mut Bridge<Self::Times, Self::R0>,
        bridge1: &mut Bridge<Self::Times, Self::R1>,
    );

    /// Interpret matches derived from the immediately preceding [`Self::advance`] call and place
    /// them in `output`. The iterator calls `cross` before another `advance`, so a backend may keep
    /// block-local interpretation state between the two calls. `cross` may be skipped when the
    /// block produced no matches, in which case the next `advance` may overwrite that state.
    fn cross(
        &mut self,
        instance: &JoinInstance<T, B0, B1>,
        matches: &mut JoinMatches<Self::Times, Self::ROut>,
        output: &mut Vec<Self::Output>,
    );
}

/// A unit of proxied join work, for presentation to the backend.
pub struct JoinInstance<T, B0, B1> {
    /// The first input's batches.
    pub batches0: Vec<B0>,
    /// The second input's batches.
    pub batches1: Vec<B1>,
    /// A lower bound on the meet of pairs of update times.
    ///
    /// This can be applied when loading updates to consolidate on load.
    pub lower: T,
}

/// Presentation of discovered join matches.
///
/// The arrays have common lengths, and are in key order but may not be consolidated.
pub struct JoinMatches<C, R> {
    /// Triples of `(key, (val0, val1))` of matches.
    pub ids: Vec<(u64, (u64, u64))>,
    /// Times of the updates, one row per match.
    pub times: C,
    /// Diffs of the updates.
    pub diffs: Vec<R>,
}

impl<C: Default, R> Default for JoinMatches<C, R> {
    fn default() -> Self { Self { ids: vec![], times: C::default(), diffs: vec![] } }
}

impl<C: TimeColumn, R> JoinMatches<C, R> {
    /// The number of matches.
    pub fn len(&self) -> usize { self.ids.len() }
    /// Whether there are no matches.
    pub fn is_empty(&self) -> bool { self.ids.is_empty() }
    /// Remove every match, keeping the allocations.
    pub fn clear(&mut self) {
        self.ids.clear();
        self.times.clear();
        self.diffs.clear();
    }
}

/// A proxy-space [`JoinTactic`]: matches records of the two drawn runs by `key_hash`.
pub struct ProxyJoinTactic<B0, B1, Bk> {
    backend: Rc<RefCell<Bk>>,
    _marker: std::marker::PhantomData<(B0, B1)>,
}

impl<B0, B1, Bk> ProxyJoinTactic<B0, B1, Bk> {
    /// A join tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyJoinTactic { backend: Rc::new(RefCell::new(backend)), _marker: std::marker::PhantomData }
    }
}

impl<T, B0, B1, Bk> JoinTactic<T, B0, B1, Bk::Output> for ProxyJoinTactic<B0, B1, Bk>
where
    T: Timestamp + Lattice + 'static,
    B0: 'static,
    B1: 'static,
    Bk: ProxyJoinBackend<T, B0, B1> + 'static,
    Bk::Output: 'static,
    Bk::Times: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, _fresh: Fresh, meet: T) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(ProxyJoinIter {
            backend: Rc::clone(&self.backend),
            instance: JoinInstance { batches0: input0, batches1: input1, lower: meet },
            from: Some(0),
            p0: Bridge::default(),
            p1: Bridge::default(),
            pool: Bk::Times::default(),
            h0: ColumnHistory::new(),
            h1: ColumnHistory::new(),
            matches: JoinMatches::default(),
            ready: Vec::new(),
        })
    }
}

/// Deferred proxy join computation, as an iterator of output containers.
///
/// The iterator draws the proxy collection from the back-end a block at a time (`Bk::advance`).
/// Each block is then translated to output updates with joined times and multiplied differences,
/// which are provided to the back-end to translate into output containers, which are then returned.
struct ProxyJoinIter<T, B0, B1, Bk>
where
    Bk: ProxyJoinBackend<T, B0, B1>,
{
    /// The backend, shared across all outstanding iterators.
    backend: Rc<RefCell<Bk>>,
    /// The iterator's inputs, and the time at which they can consolidate as they load.
    instance: JoinInstance<T, B0, B1>,
    /// Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
    /// once the backend reports the iteration is complete.
    from: Option<u64>,
    /// The current block: the two runs `advance` last drew, which one `next` consumes entirely.
    p0: Bridge<Bk::Times, Bk::R0>,
    p1: Bridge<Bk::Times, Bk::R1>,
    /// The time column the per-key replay histories work over, cleared per key.
    pool: Bk::Times,
    /// Per-key replay histories, held across the iterator and reloaded per key when needed.
    h0: ColumnHistory<u64, Bk::R0>,
    h1: ColumnHistory<u64, Bk::R1>,
    /// The block's matched records, held across blocks to keep their allocations.
    matches: JoinMatches<Bk::Times, Bk::ROut>,
    /// The last block's containers, in reverse, served from the back one `next` at a time.
    ready: Vec<Bk::Output>,
}

impl<T, B0, B1, Bk> Iterator for ProxyJoinIter<T, B0, B1, Bk>
where
    T: Timestamp + Lattice,
    Bk: ProxyJoinBackend<T, B0, B1>,
{
    type Item = Bk::Output;

    /// Serve a ready container, else draw and cross blocks until one yields any.
    fn next(&mut self) -> Option<Bk::Output> {
        while self.ready.is_empty() && self.from.is_some() {
            self.refill();
            self.work();
            if !self.matches.ids.is_empty() { self.cross(); }
        }
        self.ready.pop()
    }
}

impl<T, B0, B1, Bk> ProxyJoinIter<T, B0, B1, Bk>
where
    T: Timestamp + Lattice,
    Bk: ProxyJoinBackend<T, B0, B1>,
{
    /// Draw the next block from the backend.
    fn refill(&mut self) {
        self.p0.clear();
        self.p1.clear();
        let before = self.from;
        self.backend.borrow_mut().advance(&self.instance, &mut self.from, &mut self.p0, &mut self.p1);
        // Without progress the iterator would never retire, so this guards liveness as well as contract.
        debug_assert!(
            self.from.is_none() || self.from > before,
            "advance must either strictly increase `from` or report the iteration complete",
        );
        self.p0.debug_assert_sorted("advance (bridge0)");
        self.p1.debug_assert_sorted("advance (bridge1)");
        // A key hash outside `[before, from)` is either one an earlier block already retired, or one
        // a later block may yet report: both split a key across blocks, which silently drops the
        // matches that would have crossed the split.
        debug_assert!(
            {
                let mut keys = self.p0.ids.iter().map(|r| r.0).chain(self.p1.ids.iter().map(|r| r.0));
                keys.all(|k| before.is_none_or(|b| b <= k) && self.from.is_none_or(|f| k < f))
            },
            "advance must report a key hash entirely within the block that first mentions it",
        );
    }

    /// Match the whole of the current block into the match buffers.
    fn work(&mut self) {
        // Disjoint field borrows, as `join_key` holds the bridges and the buffers at once.
        let (p0, p1) = (&self.p0, &self.p1);
        let (h0, h1) = (&mut self.h0, &mut self.h1);
        let pool = &mut self.pool;

        let (mut i, mut j) = (0usize, 0usize);
        while i < p0.len() && j < p1.len() {
            let ki = p0.ids[i].0;
            debug_assert_eq!(ki, p1.ids[j].0, "advance must report common keys");
            let mut e0 = i;
            while e0 < p0.len() && p0.ids[e0].0 == ki { e0 += 1; }
            let mut e1 = j;
            while e1 < p1.len() && p1.ids[e1].0 == ki { e1 += 1; }
            join_key(ki, p0, i..e0, p1, j..e1, pool, h0, h1, &mut self.matches);
            i = e0;
            j = e1;
        }
        debug_assert!(i == p0.len() && j == p1.len(), "both bridges must drain together");
    }

    /// Turn the block's matches into containers, ready to be served one at a time.
    fn cross(&mut self) {
        self.backend.borrow_mut().cross(
            &self.instance,
            &mut self.matches,
            &mut self.ready,
        );
        // `next` serves from the back, so reverse to ship in the order the backend produced.
        self.ready.reverse();
        self.matches.clear();
    }
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, this performs a direct cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
#[allow(clippy::too_many_arguments)]
fn join_key<C, R0, R1, RO>(
    kh: u64,
    p0: &Bridge<C, R0>,
    r0: std::ops::Range<usize>,
    p1: &Bridge<C, R1>,
    r1: std::ops::Range<usize>,
    pool: &mut C,
    h0: &mut ColumnHistory<u64, R0>,
    h1: &mut ColumnHistory<u64, R1>,
    matches: &mut JoinMatches<C, RO>,
) where
    C: TimeColumn,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                matches.ids.push((kh, (p0.ids[a].1, p1.ids[b].1)));
                matches.times.push_join_cross(&p0.times, a, &p1.times, b);
                matches.diffs.push(p0.diffs[a].clone().multiply(&p1.diffs[b]));
            }
        }
    }
    else {
        pool.clear();
        h0.load(pool, &p0.times, r0.map(|i| (p0.ids[i].1, i, p0.diffs[i].clone())), None);
        h1.load(pool, &p1.times, r1.map(|i| (p1.ids[i].1, i, p1.diffs[i].clone())), None);
        bilinear_wave(pool, h0, h1, |pool, v0, v1, t0, t1, d| {
            matches.ids.push((kh, (v0, v1)));
            matches.times.push_join_cross(pool, t0, pool, t1);
            matches.diffs.push(d);
        });
    }
}

/// Produces the join of two histories: every pair of edits, diffs multiplied and times
/// joined, visited in time order. Repeatedly steps the history with the earlier un-replayed
/// edit and multiplies it against the other's buffer, which is consolidated under the meet of
/// its remaining times as the wave advances — so work is bounded by the netted accumulation
/// sizes rather than the raw history lengths.
///
/// `emit` receives every produced `(id0, id1, time row 0, time row 1, multiplied diff)`, the two
/// rows of `pool` whose join is the match's time. Both histories must be pre-loaded over `pool`
/// and are fully drained. For small histories a plain cross product is cheaper; callers should
/// gate on size.
fn bilinear_wave<C, V, R0, R1, RO>(
    pool: &mut C,
    h0: &mut ColumnHistory<V, R0>,
    h1: &mut ColumnHistory<V, R1>,
    mut emit: impl FnMut(&C, V, V, usize, usize, RO),
) where
    C: TimeColumn,
    V: Copy + Ord,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    while h0.time().is_some() && h1.time().is_some() {
        if pool.cmp(h0.time().unwrap(), h1.time().unwrap()) == std::cmp::Ordering::Less {
            h1.advance_buffer_by(pool, h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                emit(pool, v0, *v1, t0, *t1, d0.clone().multiply(d1));
            }
            h0.step();
        } else {
            h0.advance_buffer_by(pool, h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                emit(pool, *v0, v1, *t0, t1, d0.clone().multiply(d1));
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(pool, h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            emit(pool, v0, *v1, t0, *t1, d0.clone().multiply(d1));
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(pool, h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            emit(pool, *v0, v1, *t0, t1, d0.clone().multiply(d1));
        }
        h1.step();
    }
}
