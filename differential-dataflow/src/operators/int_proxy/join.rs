//! The proxy join framework.
//!
//! A conventional differential join against `(u64, u64)` values, which are provided by
//! and then interpreted by a backend, who is relieved of lattice-time reasoning.

use std::cell::RefCell;
use std::rc::Rc;

use timely::progress::Timestamp;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::BatchReader;
use super::ProxyBridge;
use crate::operators::join::{Fresh, JoinTactic};

use super::history::IdHistory;

/// A type that can interpret and retire pairs of lists of batches, joined by key hashes.
///
/// The harness repeatedly invokes [`advance`](Self::advance) to draw a block of the proxy collection,
/// then [`cross`](Self::cross) to turn that block's matches into output containers, until `advance` reports the key space exhausted.
pub trait ProxyJoinBackend<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
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
    /// The `from` indicates an inclusive lower bound on key hash, and should be updated by the implementor to an exclusive
    /// upper bound for the range of keys it intends to return in this call. The `None` value indicates the keys are exhausted.
    /// The returned bridges must contain all updates from both `instance` inputs for keys that are present in both inputs, and
    /// which are greater or equal to the initial `from`, and not greater or equal to its value when returned.
    fn advance(
        &mut self,
        instance: &JoinInstance<B0, B1>,
        from: &mut Option<u64>,
        bridge0: &mut ProxyBridge<B0::Time, Self::R0>,
        bridge1: &mut ProxyBridge<B0::Time, Self::R1>,
    );

    /// Interpret a list of matching identifiers, translate them to outputs, and place them in `output`.
    fn cross(
        &mut self,
        instance: &JoinInstance<B0, B1>,
        matches: &mut JoinMatches<B0::Time, Self::ROut>,
        output: &mut Vec<Self::Output>,
    );
}

/// A unit of proxied join work, for presentation to the backend.
pub struct JoinInstance<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// The first input's batches.
    pub batches0: Vec<B0>,
    /// The second input's batches.
    pub batches1: Vec<B1>,
    /// A lower bound on the meet of pairs of update times.
    ///
    /// This can be applied when loading updates to consolidate on load.
    pub lower: B0::Time,
}

/// Presentation of discovered join matches.
///
/// The arrays have common lengths, and are in key order but may not be consolidated.
pub struct JoinMatches<T, R> {
    /// Triples of `(key, (val0, val1))` of matches.
    pub ids: Vec<(u64, (u64, u64))>,
    /// Times of the updates.
    pub times: Vec<T>,
    /// Diffs of the updates.
    pub diffs: Vec<R>,
}

impl<T, R> Default for JoinMatches<T, R> {
    fn default() -> Self { Self { ids: vec![], times: vec![], diffs: vec![] } }
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

impl<B0, B1, Bk> JoinTactic<B0, B1, Bk::Output> for ProxyJoinTactic<B0, B1, Bk>
where
    B0: BatchReader + 'static,
    B1: BatchReader<Time = B0::Time> + 'static,
    Bk: ProxyJoinBackend<B0, B1> + 'static,
    Bk::Output: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, _fresh: Fresh, meet: B0::Time) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(ProxyJoinIter {
            backend: Rc::clone(&self.backend),
            instance: JoinInstance { batches0: input0, batches1: input1, lower: meet },
            from: Some(0),
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
/// which are provided to the back-end to translate into output containers, which are then returned.
struct ProxyJoinIter<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
{
    /// The backend, shared across all outstanding iterators.
    backend: Rc<RefCell<Bk>>,
    /// The iterator's inputs, and the time at which they can consolidate as they load.
    instance: JoinInstance<B0, B1>,
    /// Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
    /// once the backend reports the iteration is complete.
    from: Option<u64>,
    /// The current block: the two runs `advance` last drew, which one `next` consumes entirely.
    p0: ProxyBridge<B0::Time, Bk::R0>,
    p1: ProxyBridge<B0::Time, Bk::R1>,
    /// Per-key replay histories, held across the iterator and reloaded per key when needed.
    h0: IdHistory<B0::Time, Bk::R0>,
    h1: IdHistory<B0::Time, Bk::R1>,
    /// The block's matched records, held across blocks to keep their allocations.
    matches: JoinMatches<B0::Time, Bk::ROut>,
    /// The last block's containers, in reverse, served from the back one `next` at a time.
    ready: Vec<Bk::Output>,
}

impl<B0, B1, Bk> Iterator for ProxyJoinIter<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
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

impl<B0, B1, Bk> ProxyJoinIter<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
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
        super::debug_assert_sorted_bridge(&self.p0, "advance (bridge0)");
        super::debug_assert_sorted_bridge(&self.p1, "advance (bridge1)");
        // A key hash outside `[before, from)` is either one an earlier block already retired, or one
        // a later block may yet report: both split a key across blocks, which silently drops the
        // matches that would have crossed the split.
        debug_assert!(
            {
                let mut keys = self.p0.iter().map(|r| r.0.0).chain(self.p1.iter().map(|r| r.0.0));
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

        let (mut i, mut j) = (0usize, 0usize);
        while i < p0.len() && j < p1.len() {
            let ki = p0[i].0.0;
            debug_assert_eq!(ki, p1[j].0.0, "advance must report common keys");
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

    /// Turn the block's matches into containers, ready to be served one at a time.
    fn cross(&mut self) {
        self.backend.borrow_mut().cross(
            &self.instance,
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
fn join_key<T, R0, R1, RO>(
    kh: u64,
    p0: &ProxyBridge<T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<T, R1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<T, R0>,
    h1: &mut IdHistory<T, R1>,
    matches: &mut JoinMatches<T, RO>,
) where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                matches.ids.push((kh, (p0[a].0.1, p1[b].0.1)));
                matches.times.push(p0[a].1.join(&p1[b].1));
                matches.diffs.push(p0[a].2.clone().multiply(&p1[b].2));
            }
        }
    }
    else {
        h0.load_iter(r0.map(|i| (p0[i].0.1, p0[i].1.clone(), p0[i].2.clone())), None);
        h1.load_iter(r1.map(|i| (p1[i].0.1, p1[i].1.clone(), p1[i].2.clone())), None);
        crate::operators::common::bilinear_wave(h0, h1, |v0, v1, t, d| {
            matches.ids.push((kh, (v0, v1)));
            matches.times.push(t);
            matches.diffs.push(d);
        });
    }
}
