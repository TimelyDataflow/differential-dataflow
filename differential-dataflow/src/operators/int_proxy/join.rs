//! The proxy join framework.
//!
//! A conventional differential join against ordered, copyable proxies.
//! The backend interprets proxies and is relieved of lattice-time reasoning.

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;

use timely::progress::Timestamp;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use super::{KeyPosition, TimeColumn};
use crate::operators::join::{Fresh, JoinTactic};
use crate::operators::history::ValueHistory;

use super::history::IdHistory;

/// A type that can interpret and retire pairs of lists of batches, joined by keys.
///
/// The harness repeatedly invokes [`advance`](Self::advance) to draw a block of the proxy collection,
/// then [`cross`](Self::cross) to turn that block's matches into output containers, until `advance` reports the key space exhausted.
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
    /// Diff type of matched records (`R0 * R1`).
    type ROut: Semigroup;
    /// The column both bridges hold their times in.
    type Times: TimeColumn<T>;
    /// The output container built from matched value ids.
    type Output;

    /// Populates the two bridges with all updates for all keys that match in a returned range.
    ///
    /// The `from` position is `Start` or an inclusive `At(key)` lower bound.
    /// The backend writes the window's exclusive upper bound as `At(key)`, or `End` when exhausted.
    /// Each call must strictly advance `from`.
    /// The returned bridges must contain all updates from both `instance` inputs for keys that are present in both inputs, and
    /// which are greater or equal to the initial `from`, and not greater or equal to its value when returned.
    fn advance(
        &mut self,
        instance: &JoinInstance<T, B0, B1>,
        from: &mut KeyPosition<Self::Key>,
        bridge0: &mut JoinBridge<Self::Times, Self::R0, Self::Key, Self::V0>,
        bridge1: &mut JoinBridge<Self::Times, Self::R1, Self::Key, Self::V1>,
    );

    /// Interpret the blocks of matches between the two bridges of the immediately preceding
    /// [`Self::advance`] call, and place them in `output`. Each [`JoinBlock`] stands for its rows by
    /// the rule documented there; the backend forms their times with
    /// [`TimeColumn::extend_cross_join`] and their diffs with [`super::extend_cross_product`], after
    /// whatever filtering it does. The bridges may hold rows beyond those `advance` presented: the
    /// tactic appends the rows its long-history replay produces. The iterator calls `cross` before
    /// another `advance`, so a backend may keep block-local interpretation state between the two
    /// calls. `cross` may be skipped when the block produced no matches, in which case the next
    /// `advance` may overwrite that state.
    fn cross(
        &mut self,
        instance: &JoinInstance<T, B0, B1>,
        bridge0: &JoinBridge<Self::Times, Self::R0, Self::Key, Self::V0>,
        bridge1: &JoinBridge<Self::Times, Self::R1, Self::Key, Self::V1>,
        blocks: &[JoinBlock],
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

/// A consolidated collection of `((key, val), time, diff)` proxies, as columns.
///
/// Rows are sorted and consolidated by `((key, val), time)`, as a [`super::ProxyBridge`]'s are.
pub struct JoinBridge<C, R, K = u64, V = u64> {
    /// The key of each row.
    pub keys: Vec<K>,
    /// The value proxy of each row.
    pub vals: Vec<V>,
    /// The time of each row.
    pub times: C,
    /// The diff of each row.
    pub diffs: Vec<R>,
}

impl<C: Default, R, K, V> Default for JoinBridge<C, R, K, V> {
    fn default() -> Self { JoinBridge { keys: Vec::new(), vals: Vec::new(), times: C::default(), diffs: Vec::new() } }
}

impl<C, R, K, V> JoinBridge<C, R, K, V> {
    /// The number of rows.
    pub fn len(&self) -> usize { self.keys.len() }
    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }
    /// Remove every row, keeping the allocations.
    pub fn clear<T>(&mut self) where C: TimeColumn<T> {
        self.keys.clear();
        self.vals.clear();
        self.times.clear();
        self.diffs.clear();
    }
}

/// Debug check that a presented [`JoinBridge`] is consolidated.
fn debug_assert_sorted_join_bridge<T: Ord, C: TimeColumn<T>, R, K: Copy + Ord, V: Copy + Ord>(bridge: &JoinBridge<C, R, K, V>, who: &str) {
    debug_assert!(
        (1..bridge.len()).all(|i| {
            ((bridge.keys[i-1], bridge.vals[i-1]), bridge.times.owned_get(i-1)) < ((bridge.keys[i], bridge.vals[i]), bridge.times.owned_get(i))
        }),
        "{}: a presented bridge must be sorted & consolidated by ((key, val), time)",
        who,
    );
}

/// Matches between two [`JoinBridge`]s: every row of `r0` in the first with every row of `r1` in
/// the second.
///
/// Block `(r0, r1)` stands for the rows `(i, j)`, `i` in `r0` (outer) and `j` in `r1`, with key
/// `bridge0.keys[i]`, values `(bridge0.vals[i], bridge1.vals[j])`, time
/// `bridge0.times[i] ∨ bridge1.times[j]`, and diff `bridge0.diffs[i] · bridge1.diffs[j]`.
pub type JoinBlock = (Range<usize>, Range<usize>);

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
            instance: JoinInstance { batches0: input0, batches1: input1, lower: meet },
            from: KeyPosition::Start,
            p0: JoinBridge::default(),
            p1: JoinBridge::default(),
            h0: IdHistory::new(),
            h1: IdHistory::new(),
            blocks: Vec::new(),
            ready: Vec::new(),
        })
    }
}

/// Deferred proxy join computation, as an iterator of output containers.
///
/// The iterator draws the proxy collection from the back-end a block at a time (`Bk::advance`).
/// Each block's matches are found as [`JoinBlock`]s, which the back-end translates into output
/// containers with joined times and multiplied differences, which are then returned.
struct ProxyJoinIter<T, B0, B1, Bk>
where
    Bk: ProxyJoinBackend<T, B0, B1>,
{
    /// The backend, shared across all outstanding iterators.
    backend: Rc<RefCell<Bk>>,
    /// The iterator's inputs, and the time at which they can consolidate as they load.
    instance: JoinInstance<T, B0, B1>,
    /// Progress through the key space, from `Start` through inclusive key bounds to `End`.
    from: KeyPosition<Bk::Key>,
    /// The current block: the two runs `advance` last drew, which one `next` consumes entirely.
    p0: JoinBridge<Bk::Times, Bk::R0, Bk::Key, Bk::V0>,
    p1: JoinBridge<Bk::Times, Bk::R1, Bk::Key, Bk::V1>,
    /// Per-key replay histories, held across the iterator and reloaded per key when needed.
    h0: IdHistory<T, Bk::R0, Bk::V0>,
    h1: IdHistory<T, Bk::R1, Bk::V1>,
    /// The block's matches, held across blocks to keep their allocation.
    blocks: Vec<JoinBlock>,
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
            if !self.blocks.is_empty() { self.cross(); }
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
        self.p0.clear::<T>();
        self.p1.clear::<T>();
        let before = self.from;
        self.backend.borrow_mut().advance(&self.instance, &mut self.from, &mut self.p0, &mut self.p1);
        // Without progress the iterator would never retire, so this guards liveness as well as contract.
        debug_assert!(
            self.from > before,
            "advance must either strictly increase `from` or report the iteration complete",
        );
        debug_assert_sorted_join_bridge(&self.p0, "advance (bridge0)");
        debug_assert_sorted_join_bridge(&self.p1, "advance (bridge1)");
        // A key outside `[before, from)` belongs to an earlier or later block.
        // Both cases split a key across blocks, silently dropping matches that would cross the split.
        debug_assert!(
            {
                let mut keys = self.p0.keys.iter().chain(&self.p1.keys);
                keys.all(|&k| before <= KeyPosition::At(k) && KeyPosition::At(k) < self.from)
            },
            "advance must report a key entirely within the block that first mentions it",
        );
    }

    /// Match the whole of the current block into blocks of matches.
    fn work(&mut self) {
        let (n0, n1) = (self.p0.len(), self.p1.len());
        let (mut i, mut j) = (0usize, 0usize);
        while i < n0 && j < n1 {
            let ki = self.p0.keys[i];
            debug_assert!(ki == self.p1.keys[j], "advance must report common keys");
            let e0 = i + self.p0.keys[i..n0].partition_point(|k| *k == ki);
            let e1 = j + self.p1.keys[j..n1].partition_point(|k| *k == ki);
            join_key(ki, (&mut self.p0, i..e0), (&mut self.p1, j..e1), (&mut self.h0, &mut self.h1), &mut self.blocks);
            i = e0;
            j = e1;
        }
        debug_assert!(i == n0 && j == n1, "both bridges must drain together");
    }

    /// Turn the block's matches into containers, ready to be served one at a time.
    fn cross(&mut self) {
        self.backend.borrow_mut().cross(
            &self.instance,
            &self.p0,
            &self.p1,
            &self.blocks,
            &mut self.ready,
        );
        // `next` serves from the back, so reverse to ship in the order the backend produced.
        self.ready.reverse();
        self.blocks.clear();
    }
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, the whole cross product is one block.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up, appending the rows each step pairs to the
/// bridges and emitting a block per step.
fn join_key<T, C, R0, R1, K: Copy + Ord, V0: Copy + Ord, V1: Copy + Ord>(
    kh: K,
    (p0, r0): (&mut JoinBridge<C, R0, K, V0>, Range<usize>),
    (p1, r1): (&mut JoinBridge<C, R1, K, V1>, Range<usize>),
    (h0, h1): (&mut IdHistory<T, R0, V0>, &mut IdHistory<T, R1, V1>),
    blocks: &mut Vec<JoinBlock>,
) where
    T: Lattice + Timestamp,
    C: TimeColumn<T>,
    R0: Semigroup + Clone,
    R1: Semigroup + Clone,
{
    if r0.len() < 16 || r1.len() < 16 {
        blocks.push((r0, r1));
    }
    else {
        // Scaffolding: the replay runs on owned times, read out and written back per row.
        h0.load_iter(r0.map(|i| (p0.vals[i], p0.times.owned_get(i), p0.diffs[i].clone())), None);
        h1.load_iter(r1.map(|i| (p1.vals[i], p1.times.owned_get(i), p1.diffs[i].clone())), None);
        bilinear_wave(h0, h1, |step| {
            let (s0, s1) = (p0.len(), p1.len());
            match step {
                Step::Left((v0, t0, d0), buffer) => {
                    p0.keys.push(kh); p0.vals.push(v0); p0.times.owned_push(t0); p0.diffs.push(d0.clone());
                    for ((v1, t1), d1) in buffer {
                        p1.keys.push(kh); p1.vals.push(*v1); p1.times.owned_push(t1); p1.diffs.push(d1.clone());
                    }
                }
                Step::Right((v1, t1, d1), buffer) => {
                    for ((v0, t0), d0) in buffer {
                        p0.keys.push(kh); p0.vals.push(*v0); p0.times.owned_push(t0); p0.diffs.push(d0.clone());
                    }
                    p1.keys.push(kh); p1.vals.push(v1); p1.times.owned_push(t1); p1.diffs.push(d1.clone());
                }
            }
            blocks.push((s0..p0.len(), s1..p1.len()));
        });
    }
}

/// One step of [`bilinear_wave`]: the stepped edit, and the other history's buffer, which it
/// meets entirely.
enum Step<'a, V0, V1, T, R0, R1> {
    /// The first history's edit, against the second's buffer.
    Left((V0, &'a T, &'a R0), &'a [((V1, T), R1)]),
    /// The second history's edit, against the first's buffer.
    Right((V1, &'a T, &'a R1), &'a [((V0, T), R0)]),
}

/// Produces the join of two histories: every pair of edits, diffs multiplied and times
/// joined, visited in time order. Repeatedly steps the history with the earlier un-replayed
/// edit and multiplies it against the other's buffer, which is consolidated under the meet of
/// its remaining times as the wave advances — so work is bounded by the netted accumulation
/// sizes rather than the raw history lengths.
///
/// `emit` receives each step with a non-empty buffer: every buffered edit pairs with the stepped
/// edit, at the join of their times and the product of their diffs. Both histories must be
/// pre-loaded (`load`/`load_iter`) and are fully drained. For small histories a plain cross
/// product is cheaper; callers should gate on size.
fn bilinear_wave<V0, V1, T, R0, R1>(
    h0: &mut ValueHistory<V0, T, R0>,
    h1: &mut ValueHistory<V1, T, R1>,
    mut emit: impl FnMut(Step<'_, V0, V1, T, R0, R1>),
) where
    V0: Copy + Ord,
    V1: Copy + Ord,
    T: Ord + Clone + Lattice,
    R0: Semigroup + Clone,
    R1: Semigroup + Clone,
{
    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            if !h1.buffer().is_empty() { emit(Step::Left(h0.edit().unwrap(), h1.buffer())); }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            if !h0.buffer().is_empty() { emit(Step::Right(h1.edit().unwrap(), h0.buffer())); }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        if !h1.buffer().is_empty() { emit(Step::Left(h0.edit().unwrap(), h1.buffer())); }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        if !h0.buffer().is_empty() { emit(Step::Right(h1.edit().unwrap(), h0.buffer())); }
        h1.step();
    }
}
