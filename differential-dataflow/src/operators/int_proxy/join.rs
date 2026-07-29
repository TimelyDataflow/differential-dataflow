//! The proxy join framework.
//!
//! A conventional differential join against `(u64, u64)` values, which are provided by
//! and then interpreted by a backend, who is relieved of lattice-time reasoning.

use std::cell::RefCell;
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::BatchReader;
use super::ProxyBridge;
use crate::operators::join::{Fresh, JoinTactic};

use super::history::IdHistory;

/// A unit of proxied join work, presented to the backend.
pub struct JoinInstance<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// The first input's batches.
    pub batches0: Vec<B0>,
    /// The second input's batches.
    pub batches1: Vec<B1>,
    /// The compaction frontier for loading (the unit's capability time).
    pub lower: Antichain<B0::Time>,
}

/// A type that can interpret and retire pairs of batches, joined by key hashes.
///
/// The protocol repeatedly invokes [`advance`](Self::advance) to draw a block of the proxy
/// collection, then [`cross`](Self::cross) to turn that block's matches into output containers.
/// The unit is retired when `advance` reports the key space exhausted.
///
/// The two granularities are the backend's, and independent: `advance` sizes the input it draws,
/// `cross` sizes the containers it produces from the matches that block yields.
///
/// The backend holds no state across the calls of one unit: its progress lives in the `from`
/// argument, which the harness owns. This is load-bearing, as the harness may have many units
/// in flight against the same backend, and interleaves their calls freely.
pub trait ProxyJoinBackend<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// Diff type presented for the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type presented for the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the tactic.
    type ROut: Semigroup;
    /// The output container built from matched value ids.
    type Output;

    /// Populates two bridges with intersecting keys from `from` onward, updating `from` to the new limit.
    ///
    /// The `from` argument is an inclusive lower bound on key identifier, initially `Some(0)`, with `None`
    /// used to indicate that the keyspace has been exhausted.
    ///
    /// A key hash must be reported entirely by the call that first mentions it: all of its updates,
    /// from both inputs, and none in any other block. Equivalently, each reported key hash lies in
    /// `[from, from')` for the argument's values before and after the call.
    fn advance(
        &mut self,
        instance: &JoinInstance<B0, B1>,
        from: &mut Option<u64>,
        bridge0: &mut ProxyBridge<B0::Time, Self::R0>,
        bridge1: &mut ProxyBridge<B0::Time, Self::R1>,
    );

    /// From a list of left and right identifiers, and corresponding times and diffs, the output.
    ///
    /// The four are parallel arrays with one entry per matched pair: `left[i]` and `right[i]` are the
    /// `(key_hash, value_id)` of the pair's two records, which share a key hash, and `times[i]` and
    /// `diffs[i]` are the join of their times and the product of their diffs. They are not
    /// consolidated, and a pair can repeat at distinct times. Drain `times` and `diffs` to take their
    /// contents; the harness keeps their allocations.
    ///
    /// The containers are pushed to `output`, which arrives empty, and are shipped one at a time:
    /// their sizing is the backend's, independent of the block `advance` drew the matches from.
    fn cross(&mut self, instance: &JoinInstance<B0, B1>, left: &[(u64, u64)], right: &[(u64, u64)], times: &mut Vec<B0::Time>, diffs: &mut Vec<Self::ROut>, output: &mut Vec<Self::Output>);
}

/// A proxy-space [`JoinTactic`]: matches records of the two drawn runs by `key_hash`.
///
/// The backend is shared across all outstanding units (an `Rc<RefCell<_>>`), as each prepared unit
/// is a self-contained `'static` iterator and cannot borrow the tactic. Units are safe to interleave
/// because a unit's progress through the key space rides in its own `from`, not in the backend.
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
            instance: JoinInstance { batches0: input0, batches1: input1, lower: Antichain::from_elem(meet) },
            from: Some(0),
            p0: Vec::new(),
            p1: Vec::new(),
            h0: IdHistory::new(),
            h1: IdHistory::new(),
            li: Vec::new(),
            ri: Vec::new(),
            ot: Vec::new(),
            od: Vec::new(),
            ready: Vec::new(),
        })
    }
}

/// Deferred proxy join computation, as an iterator of output containers.
///
/// The unit draws the proxy collection a block at a time (`Bk::advance`), merge-matches the two
/// drawn runs by `key_hash`, and hands the matches back to be cut into containers (`Bk::cross`).
/// Both granularities are the backend's: `advance` sizes the block, `cross` the containers. One
/// key's matches are held whole however the block is sized, as the replay wave cannot be
/// interrupted. The driver stops pulling once its budget is spent and resumes the same iterator on
/// the next activation.
///
/// `meet` is a lower bound on the fresh side's times, and `lower` presents it to the backend
/// (see [`JoinInstance`]). Loaded times can be advanced by it on either input: the fresh side's
/// times dominate `meet`, so the joined time does too.
struct ProxyJoinIter<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
{
    /// The backend, shared across all outstanding units.
    backend: Rc<RefCell<Bk>>,
    /// The unit's inputs, and the compaction frontier at which they load.
    instance: JoinInstance<B0, B1>,
    /// Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
    /// once the backend reports the unit drawn.
    from: Option<u64>,
    /// The current block: the two runs `advance` last drew, which one `next` consumes entirely.
    p0: ProxyBridge<B0::Time, Bk::R0>,
    p1: ProxyBridge<B0::Time, Bk::R1>,
    /// Per-key replay histories, held across the unit and reloaded per key (only the >=16/>=16
    /// replay-wave path touches them), so a high-fanout join pays no per-key `IdHistory` alloc.
    h0: IdHistory<B0::Time, Bk::R0>,
    h1: IdHistory<B0::Time, Bk::R1>,
    /// The block's matched records, as the four parallel arrays `cross` reads. Held across blocks
    /// only to keep their allocations.
    li: Vec<(u64, u64)>,
    ri: Vec<(u64, u64)>,
    ot: Vec<B0::Time>,
    od: Vec<Bk::ROut>,
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
    ///
    /// A block that yields no matches is not an empty container; it is a region of the key space
    /// with no intersection, and the unit moves on to the next block.
    fn next(&mut self) -> Option<Bk::Output> {
        while self.ready.is_empty() && self.from.is_some() {
            self.refill();
            self.work();
            if !self.li.is_empty() { self.cross(); }
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
        // Without progress the unit would never retire, so this guards liveness as well as contract.
        debug_assert!(
            self.from.is_none() || self.from > before,
            "advance must either strictly increase `from` or report the unit drawn",
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

    /// Merge-match the whole of the current block into the match buffers.
    ///
    /// Both runs are sorted by `key_hash`, so matching keys are found by a merge.
    fn work(&mut self) {
        // Disjoint field borrows, as `join_key` holds the bridges and the buffers at once.
        let (p0, p1) = (&self.p0, &self.p1);
        let (li, ri, ot, od) = (&mut self.li, &mut self.ri, &mut self.ot, &mut self.od);
        let (h0, h1) = (&mut self.h0, &mut self.h1);

        let (mut i, mut j) = (0usize, 0usize);
        while i < p0.len() && j < p1.len() {
            let (ki, kj) = (p0[i].0.0, p1[j].0.0);
            if ki < kj {
                i += 1;
            } else if kj < ki {
                j += 1;
            } else {
                let mut e0 = i;
                while e0 < p0.len() && p0[e0].0.0 == ki { e0 += 1; }
                let mut e1 = j;
                while e1 < p1.len() && p1[e1].0.0 == ki { e1 += 1; }
                join_key(ki, p0, i..e0, p1, j..e1, h0, h1, li, ri, ot, od);
                i = e0;
                j = e1;
            }
        }
    }

    /// Turn the block's matches into containers, ready to be served one at a time.
    fn cross(&mut self) {
        self.backend.borrow_mut().cross(
            &self.instance,
            self.li.as_slice(),
            self.ri.as_slice(),
            &mut self.ot,
            &mut self.od,
            &mut self.ready,
        );
        // `next` serves from the back, so reverse to ship in the order the backend produced.
        self.ready.reverse();
        self.li.clear();
        self.ri.clear();
        self.ot.clear();
        self.od.clear();
    }
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, this performs a simple cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
#[allow(clippy::too_many_arguments)]
fn join_key<T, R0, R1, RO>(
    kh: u64,
    p0: &ProxyBridge<T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<T, R1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<T, R0>,
    h1: &mut IdHistory<T, R1>,
    li: &mut Vec<(u64, u64)>,
    ri: &mut Vec<(u64, u64)>,
    ot: &mut Vec<T>,
    od: &mut Vec<RO>,
) where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                li.push((kh, p0[a].0.1));
                ri.push((kh, p1[b].0.1));
                ot.push(p0[a].1.join(&p1[b].1));
                od.push(p0[a].2.clone().multiply(&p1[b].2));
            }
        }
        return;
    }

    // Reusable replay scratch, reloaded per key (`load_iter` clears + rebuilds, keeping capacity);
    // the caller holds `h0`/`h1` across the unit so a high-fanout join allocates no per-key history.
    h0.load_iter(r0.map(|i| (p0[i].0.1, p0[i].1.clone(), p0[i].2.clone())), None);
    h1.load_iter(r1.map(|i| (p1[i].0.1, p1[i].1.clone(), p1[i].2.clone())), None);

    crate::operators::common::bilinear_wave(h0, h1, |v0, v1, t, d| {
        li.push((kh, v0));
        ri.push((kh, v1));
        ot.push(t);
        od.push(d);
    });
}
