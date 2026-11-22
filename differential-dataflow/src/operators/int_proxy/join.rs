//! The proxy join framework.
//!
//! A conventional differential join against ordered, copyable proxies.
//! The backend interprets proxies and is relieved of lattice-time reasoning.
//!
//! A backend need not present every key.
//! A key's output is every pair of its updates, at the join of their times, with the product of their diffs.
//! A backend may produce that itself for a key, *completing* it, and leave it out of the bridges.
//! What the tactic adds is the replay that bounds the work on a key with long histories on both sides.

use std::cell::RefCell;
use std::rc::Rc;

use timely::progress::Timestamp;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use super::{KeyPosition, ProxyBridge};
use crate::operators::join::{Fresh, JoinTactic};
use crate::operators::history::ValueHistory;

use super::history::IdHistory;

/// A type that can interpret and retire pairs of lists of batches, joined by keys.
///
/// The harness repeatedly invokes [`advance`](Self::advance) to draw a block of the proxy collection,
/// then [`cross`](Self::cross) to turn that block's matches into output containers, until `advance` reports the key space exhausted.
/// A backend that presents every key it draws is correct; one that completes some keys itself emits their output from `cross`.
pub trait ProxyJoinBackend<T, B0, B1> {
    /// Independent groups, common to both inputs.
    type Key: Copy + Ord;
    /// First input value proxies, valid throughout a block.
    type V0: Copy + Ord;
    /// Second input value proxies, valid throughout a block.
    type V1: Copy + Ord;
    /// Diff type of the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type of the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the harness.
    type ROut: Semigroup;
    /// The output container built from matched value ids.
    type Output;

    /// Populates the two bridges with all updates for all keys that match in a returned range.
    ///
    /// The `from` position is `Start` or an inclusive `At(key)` lower bound.
    /// The backend writes the window's exclusive upper bound as `At(key)`, or `End` when exhausted.
    /// Each call must strictly advance `from`.
    /// The returned bridges must contain all updates from both `instance` inputs for keys that are present in both inputs, and
    /// which are greater or equal to the initial `from`, and not greater or equal to its value when returned.
    ///
    /// The exception is a key the backend *completes*: it leaves the key out of both bridges, and places the key's whole output
    /// in the following [`Self::cross`] call's `output`: every pair of its updates from the two inputs, at the join of the two times,
    /// with the product of the two diffs. Which keys to complete is the backend's choice, unless [`JoinInstance::present_all`] is set.
    fn advance(
        &mut self,
        instance: &JoinInstance<'_, T, B0, B1>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut ProxyBridge<T, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut ProxyBridge<T, Self::R1, Self::Key, Self::V1>,
    );

    /// Interpret matches derived from the immediately preceding [`Self::advance`] call and place
    /// them in `output`, along with the output of the keys that call completed. The iterator calls
    /// `cross` once after every `advance`, whether or not the block produced matches, so a backend
    /// may keep block-local interpretation state between the two calls.
    fn cross(
        &mut self,
        instance: &JoinInstance<'_, T, B0, B1>,
        matches: &mut JoinMatches<T, Self::ROut, Self::Key, Self::V0, Self::V1>,
        output: &mut Vec<Self::Output>,
    );
}

/// A unit of proxied join work, for presentation to the backend.
pub struct JoinInstance<'a, T, B0, B1> {
    /// The first input's batches.
    pub batches0: &'a [B0],
    /// The second input's batches.
    pub batches1: &'a [B1],
    /// A lower bound on the meet of pairs of update times.
    ///
    /// This can be applied when loading updates to consolidate on load.
    pub lower: &'a T,
    /// The backend must present every key, and complete none itself.
    ///
    /// Set by a caller that reads the bridges other than as the backend stores them,
    /// as [`BackendFrontier`](super::BackendFrontier) does for a unit it reads as of a frontier.
    pub present_all: bool,
}

/// Presentation of discovered join matches.
///
/// The arrays have common lengths, and are in key order but may not be consolidated.
pub struct JoinMatches<T, R, K = u64, V0 = u64, V1 = u64> {
    /// Triples of `(key, (val0, val1))` of matches.
    pub ids: Vec<(K, (V0, V1))>,
    /// Times of the updates.
    pub times: Vec<T>,
    /// Diffs of the updates.
    pub diffs: Vec<R>,
}

impl<T, R, K, V0, V1> Default for JoinMatches<T, R, K, V0, V1> {
    fn default() -> Self { Self { ids: vec![], times: vec![], diffs: vec![] } }
}

/// A proxy-space [`JoinTactic`]: matches records of the two drawn runs by `key`.
/// Its deferred work owns the presentations, so proxy types must be `'static`.
/// Borrowing proxies from a backend's temporary storage requires a different presentation interface.
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
    Bk::Key: 'static,
    Bk::V0: 'static,
    Bk::V1: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, _fresh: Fresh, meet: T) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(ProxyJoinIter {
            backend: Rc::clone(&self.backend),
            batches0: input0,
            batches1: input1,
            lower: meet,
            from: KeyPosition::Start,
            p0: Vec::new(),
            p1: Vec::new(),
            h0: IdHistory::new(),
            h1: IdHistory::new(),
            matches: JoinMatches::default(),
            ready: Vec::new(),
        })
    }
}

/// Deferred proxy join computation, as an iterator of output containers.
///
/// The iterator draws the proxy collection from the back-end a block at a time (`Bk::advance`).
/// Each block is then translated to output updates with joined times and multiplied differences,
/// which are provided to the back-end to translate into output containers, which are then returned
/// together with the containers of the keys the back-end completed itself.
struct ProxyJoinIter<T, B0, B1, Bk>
where
    Bk: ProxyJoinBackend<T, B0, B1>,
{
    /// The backend, shared across all outstanding iterators.
    backend: Rc<RefCell<Bk>>,
    /// The iterator's inputs, and the time at which they can consolidate as they load.
    batches0: Vec<B0>,
    batches1: Vec<B1>,
    lower: T,
    /// Progress through the key space, from `Start` through inclusive key bounds to `End`.
    from: KeyPosition<Bk::Key>,
    /// The current block: the two runs `advance` last drew, which one `next` consumes entirely.
    p0: ProxyBridge<T, Bk::R0, Bk::Key, Bk::V0>,
    p1: ProxyBridge<T, Bk::R1, Bk::Key, Bk::V1>,
    /// Per-key replay histories, held across the iterator and reloaded per key when needed.
    h0: IdHistory<T, Bk::R0, Bk::V0>,
    h1: IdHistory<T, Bk::R1, Bk::V1>,
    /// The block's matched records, held across blocks to keep their allocations.
    matches: JoinMatches<T, Bk::ROut, Bk::Key, Bk::V0, Bk::V1>,
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
        while self.ready.is_empty() && self.from != KeyPosition::End {
            self.refill();
            self.work();
            self.cross();
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
        let instance = JoinInstance { batches0: &self.batches0, batches1: &self.batches1, lower: &self.lower, present_all: false };
        self.backend.borrow_mut().advance(&instance, &mut self.from, &mut self.p0, &mut self.p1);
        // Without progress the iterator would never retire, so this guards liveness as well as contract.
        debug_assert!(
            self.from > before,
            "advance must either strictly increase `from` or report the iteration complete",
        );
        super::debug_assert_sorted_bridge(&self.p0, "advance (bridge0)");
        super::debug_assert_sorted_bridge(&self.p1, "advance (bridge1)");
        // A key outside `[before, from)` belongs to an earlier or later block.
        // Both cases split a key across blocks, silently dropping matches that would cross the split.
        debug_assert!(
            {
                let mut keys = self.p0.iter().map(|r| r.0.0).chain(self.p1.iter().map(|r| r.0.0));
                keys.all(|k| before <= KeyPosition::At(k) && KeyPosition::At(k) < self.from)
            },
            "advance must report a key entirely within the block that first mentions it",
        );
    }

    /// Match the whole of the current block into the match buffers.
    fn work(&mut self) {
        // Disjoint field borrows, as `join_key` holds the bridges and the buffers at once.
        let (p0, p1) = (&self.p0, &self.p1);
        let (h0, h1) = (&mut self.h0, &mut self.h1);

        let (mut i, mut j) = (0usize, 0usize);
        while i < p0.len() && j < p1.len() {
            let ki = p0[i].0.0;
            debug_assert!(ki == p1[j].0.0, "advance must report common keys");
            let mut e0 = i;
            while e0 < p0.len() && p0[e0].0.0 == ki { e0 += 1; }
            let mut e1 = j;
            while e1 < p1.len() && p1[e1].0.0 == ki { e1 += 1; }
            join_key(ki, p0, i..e0, p1, j..e1, h0, h1, &mut self.matches);
            i = e0;
            j = e1;
        }
        debug_assert!(i == p0.len() && j == p1.len(), "both bridges must drain together");
    }

    /// Turn the block's matches, and the keys the backend completed, into containers, ready to be served one at a time.
    fn cross(&mut self) {
        let instance = JoinInstance { batches0: &self.batches0, batches1: &self.batches1, lower: &self.lower, present_all: false };
        self.backend.borrow_mut().cross(
            &instance,
            &mut self.matches,
            &mut self.ready,
        );
        // `next` serves from the back, so reverse to ship in the order the backend produced.
        self.ready.reverse();
        self.matches.ids.clear();
        self.matches.times.clear();
        self.matches.diffs.clear();
    }
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, this performs a direct cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
fn join_key<T, R0, R1, RO, K: Copy + Ord, V0: Copy + Ord, V1: Copy + Ord>(
    kh: K,
    p0: &ProxyBridge<T, R0, K, V0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<T, R1, K, V1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<T, R0, V0>,
    h1: &mut IdHistory<T, R1, V1>,
    matches: &mut JoinMatches<T, RO, K, V0, V1>,
) where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                matches.ids.push((kh, (p0[a].0.1, p1[b].0.1)));
                let mut time = p0[a].1.clone();
                time.join_assign(&p1[b].1);
                matches.times.push(time);
                matches.diffs.push(p0[a].2.clone().multiply(&p1[b].2));
            }
        }
    }
    else {
        h0.load_iter(r0.map(|i| (p0[i].0.1, p0[i].1.clone(), p0[i].2.clone())), None);
        h1.load_iter(r1.map(|i| (p1[i].0.1, p1[i].1.clone(), p1[i].2.clone())), None);
        bilinear_wave(h0, h1, |v0, v1, t, d| {
            matches.ids.push((kh, (v0, v1)));
            matches.times.push(t);
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
/// `emit` receives every produced `(id0, id1, joined time, multiplied diff)`. Both histories
/// must be pre-loaded (`load`/`load_iter`) and are fully drained. For small histories a plain
/// cross product is cheaper; callers should gate on size.
fn bilinear_wave<V0, V1, T, R0, R1, RO>(
    h0: &mut ValueHistory<V0, T, R0>,
    h1: &mut ValueHistory<V1, T, R1>,
    mut emit: impl FnMut(V0, V1, T, RO),
) where
    V0: Copy + Ord,
    V1: Copy + Ord,
    T: Ord + Clone + Lattice,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                let mut time = t0.clone();
                time.join_assign(t1);
                emit(v0, *v1, time, d0.clone().multiply(d1));
            }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                let mut time = t0.clone();
                time.join_assign(t1);
                emit(*v0, v1, time, d0.clone().multiply(d1));
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            let mut time = t0.clone();
            time.join_assign(t1);
            emit(v0, *v1, time, d0.clone().multiply(d1));
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            let mut time = t0.clone();
            time.join_assign(t1);
            emit(*v0, v1, time, d0.clone().multiply(d1));
        }
        h1.step();
    }
}
