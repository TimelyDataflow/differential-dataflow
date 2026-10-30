//! The proxy half-join tactic: differential's `int_proxy` recipe, for the half join.
//!
//! This lives in dogs3 rather than in differential's `int_proxy` module on purpose: it is built
//! from that module's public surface ([`ProxyBridge`], [`KeyPosition`], the bridge check), as any
//! other implementor of a tactic over proxies would build theirs, and the needs it has of that
//! surface are the measure of whether others are allowed to use the parts.
//!
//! A half join meets a stream of *lifted* updates with a maintained arrangement, and responds to
//! the stream alone. A lifted update `((key, val), time, diff)` carries a second time as data, at
//! or beyond its own; it meets each arranged `((key, val2), time2, diff2)` whose `time2` precedes
//! its own `time` in a total order on times, and produces `((key, (val, val2)), time, diff * diff2)`
//! carrying `data ∨ time2`. Networks of these operators implement delta queries, where the total
//! order is what makes each tuple of updates meet exactly once (see `dogs3`'s `half_join`).
//!
//! The tactic owns the time logic: which arranged times precede which stream times, in which
//! total order, whether equal times count, what the output's times are, and how work is cut into
//! blocks so that the driver can yield. The backend owns the data: which stream and arranged rows
//! share a key, their value proxies, and the output containers built from matched rows.
//!
//! A match is reported as a pair of positions in the block's two sides, with its diff. Its output
//! time is the update's own time, `stream[a].1.time`, and its data time is the update's data time
//! joined with the arranged time *as presented*, [`data_time`]`(&stream[a].1, &arranged[b].1)`.
//! The tactic states that rule and the backend evaluates it in its own layout, column-wise if it
//! likes: materializing a time per match in the tactic was measured at a third of the operator's
//! time, and the rule reads the presented arranged time either way, which is what a wrapper that
//! reads the arrangement as of a frontier moves.
//!
//! # Times
//!
//! The comparison defaults to `Ord` on `T`, which [`Timestamp`] promises is a linear
//! extension of the partial order. A backend can supply a different linear extension through
//! [`ProxyHalfJoinBackend::compare_times`], coordinated with its batcher's release policy. Any linear extension serves, so long as every half join of one
//! delta query uses the same one; `TotalOrder` (the partial order being total) is not required,
//! and product times are welcome. Output data times use the lattice join; nothing here needs the
//! lattice to be distributive.
//!
//! Arranged times must be presented *as stored*. Advancing them by any frontier changes their
//! position in the total order relative to stream times that are incomparable with the frontier,
//! and so changes which pairs meet. (The trace's own logical compaction is the driver's business,
//! chosen so that the comparison survives it.) The driver's lower bound on the released updates'
//! times, which the join and reduce backends may consolidate on load by, is therefore kept from
//! the backend altogether: the tactic holds it only to check what it is given.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};

use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::int_proxy::{debug_assert_sorted_bridge, KeyPosition, ProxyBridge};

use super::HalfJoinTactic;

/// A lifted update's two times: its own, and the one it carries as data, at or beyond its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lifted<T> {
    /// The update's own time, at which its outputs are produced.
    pub time: T,
    /// The time it carries as data, which its outputs carry joined with the arranged time.
    pub data: T,
}

/// A block of released updates: `((key, val), lifted times, diff)`, in key order.
///
/// Rows need not be consolidated; the half join is linear in both inputs.
pub type HalfJoinStream<T, R, K = u64, V = u64> = Vec<((K, V), Lifted<T>, R)>;

/// A type that can interpret and retire released updates against a list of batches, joined by keys.
///
/// The tactic calls [`stage`](Self::stage) once per unit, then repeatedly [`advance`](Self::advance)
/// to draw a block of both sides, and [`cross`](Self::cross) to turn that block's matches into
/// output containers, until `advance` reports the key space exhausted.
pub trait ProxyHalfJoinBackend<T, B, C0> {
    /// Independent groups, common to both inputs.
    type Key: Copy + Ord;
    /// Stream value proxies, valid throughout a block.
    type V0: Copy + Ord;
    /// Arranged value proxies, valid throughout a block.
    type V1: Copy + Ord;
    /// Diff type of the stream.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type of the arrangement.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the tactic.
    type ROut: Semigroup;
    /// The output container built from matched value proxies.
    type Output;
    /// The released updates of one unit, in the form the backend reads them a block at a time.
    type Stream;

    /// Total order used to match times. It must agree with the batcher's release policy,
    /// extend the partial order, and remain consistent for all outstanding work.
    fn compare_times(&self, left: &T, right: &T) -> Ordering where T: Ord { left.cmp(right) }

    /// Put one unit's released containers in the form `advance` reads: sorted by key, say.
    fn stage(&mut self, chunks: Vec<C0>) -> Self::Stream;

    /// Populate the two sides with the updates of all keys in a returned range.
    ///
    /// The `from` position is `Start` or an inclusive `At(key)` lower bound. The backend writes
    /// the block's exclusive upper bound as `At(key)`, or `End` when exhausted. Each call must
    /// strictly advance `from`. Both sides are in key order, and each must hold all the rows of
    /// every key it mentions that the other side holds too: the tactic skips keys found on one
    /// side alone. The arranged side is a [`ProxyBridge`] like the join's and the reduce's:
    /// sorted and consolidated by `((key, val), time)`, so equal values share one proxy. The
    /// stream side is in key order only. Arranged times are presented as stored (see the module
    /// documentation).
    fn advance(
        &mut self,
        instance: &HalfJoinInstance<B, Self::Stream>,
        from: &mut KeyPosition<Self::Key>,
        stream: &mut HalfJoinStream<T, Self::R0, Self::Key, Self::V0>,
        arranged: &mut ProxyBridge<T, Self::R1, Self::Key, Self::V1>,
    );

    /// Interpret the matches of the immediately preceding [`Self::advance`] call, between the
    /// sides it presented, and place them in `output`. Each output row's time is its update's own
    /// time, and its data time is [`data_time`] of the two presented rows. The tactic calls
    /// `cross` before another `advance`, so a backend may keep block-local interpretation state
    /// between the two calls. `cross` is skipped when the block produced no matches, in which
    /// case the next `advance` may overwrite that state.
    fn cross(
        &mut self,
        instance: &HalfJoinInstance<B, Self::Stream>,
        stream: &HalfJoinStream<T, Self::R0, Self::Key, Self::V0>,
        arranged: &ProxyBridge<T, Self::R1, Self::Key, Self::V1>,
        matches: &mut HalfJoinMatches<Self::ROut>,
        output: &mut Vec<Self::Output>,
    );
}

/// A unit of proxied half-join work, for presentation to the backend.
///
/// There is no lower bound here on purpose: the driver's bound on the released updates' times
/// is the tactic's to check, and nothing a backend may advance by (see the module documentation).
pub struct HalfJoinInstance<B, S> {
    /// The released updates, as the backend staged them.
    pub stream: S,
    /// The arrangement's batches.
    pub batches: Vec<B>,
}

/// Presentation of discovered half-join matches: positions in the block's two sides.
///
/// The arrays have common lengths, and are in key order but not consolidated.
pub struct HalfJoinMatches<R> {
    /// `(a, b)`: the block's stream row `a` meets its arranged row `b`.
    pub pairs: Vec<(usize, usize)>,
    /// The product of the two rows' diffs.
    pub diffs: Vec<R>,
}

impl<R> Default for HalfJoinMatches<R> {
    fn default() -> Self { Self { pairs: vec![], diffs: vec![] } }
}

impl<R> HalfJoinMatches<R> {
    fn clear(&mut self) {
        self.pairs.clear();
        self.diffs.clear();
    }
}

/// The data time of a match: the update's time as data joined with the arranged row's time, as
/// presented. The one definition; a backend evaluating it column-wise must agree with it.
#[inline]
pub fn data_time<T: Lattice>(lifted: &Lifted<T>, arranged: &T) -> T {
    lifted.data.join(arranged)
}

/// A proxy-space [`HalfJoinTactic`]: meets released updates with the arranged rows of their key
/// that precede them in the total order on times.
///
/// Its deferred work owns the presentations, so proxy types must be `'static`.
pub struct ProxyHalfJoinTactic<B, C0, Bk> {
    backend: Rc<RefCell<Bk>>,
    /// Whether an arranged time equal to an update's own is excluded.
    strict: bool,
    _marker: std::marker::PhantomData<(B, C0)>,
}

impl<B, C0, Bk> ProxyHalfJoinTactic<B, C0, Bk> {
    /// A half-join tactic deferring all value semantics to `backend`.
    ///
    /// `strict` requires an arranged time to be strictly before an update's own; otherwise an
    /// equal time also meets it. A delta query pairs a strict operator with a non-strict one.
    pub fn new(backend: Bk, strict: bool) -> Self {
        ProxyHalfJoinTactic { backend: Rc::new(RefCell::new(backend)), strict, _marker: std::marker::PhantomData }
    }

    /// Whether an arranged time equal to an update's own is excluded.
    pub fn strict(&self) -> bool { self.strict }
}

impl<T, B, C0, Bk> HalfJoinTactic<T, B, C0, Bk::Output> for ProxyHalfJoinTactic<B, C0, Bk>
where
    T: Timestamp + Lattice + 'static,
    B: 'static,
    C0: 'static,
    Bk: ProxyHalfJoinBackend<T, B, C0> + 'static,
    Bk::Output: 'static,
    Bk::Key: 'static,
    Bk::V0: 'static,
    Bk::V1: 'static,
    Bk::Stream: 'static,
{
    /// One unit of work: the released `chunks` met with `batches`, as an iterator of output
    /// containers that the driver pulls as its yield logic permits. `lower` bounds the released
    /// updates' times; every output is at or beyond some element of it.
    fn prep(&mut self, chunks: Vec<C0>, batches: Vec<B>, lower: Antichain<T>) -> Box<dyn Iterator<Item = Bk::Output>> {
        let stream = self.backend.borrow_mut().stage(chunks);
        Box::new(ProxyHalfJoinIter {
            backend: Rc::clone(&self.backend),
            instance: HalfJoinInstance { stream, batches },
            lower,
            strict: self.strict,
            from: KeyPosition::Start,
            stream: Vec::new(),
            arranged: Vec::new(),
            matches: HalfJoinMatches::default(),
            ready: Vec::new(),
            order: TimeOrder::default(),
            _marker: std::marker::PhantomData,
        })
    }
}

/// Deferred proxy half-join computation, as an iterator of output containers.
///
/// The iterator draws both sides from the backend a block at a time (`Bk::advance`). Each block
/// is matched into pairs whose arranged time precedes the update's own, with data times joined
/// and differences multiplied, which the backend turns into output containers (`Bk::cross`).
struct ProxyHalfJoinIter<T, B, C0, Bk>
where
    Bk: ProxyHalfJoinBackend<T, B, C0>,
{
    /// The backend, shared across all outstanding iterators.
    backend: Rc<RefCell<Bk>>,
    instance: HalfJoinInstance<B, Bk::Stream>,
    /// The driver's lower bound on the released updates' times, which every output is covered by.
    lower: Antichain<T>,
    strict: bool,
    /// Progress through the key space, from `Start` through inclusive key bounds to `End`.
    from: KeyPosition<Bk::Key>,
    /// The current block's two sides, which one `next` consumes entirely.
    stream: HalfJoinStream<T, Bk::R0, Bk::Key, Bk::V0>,
    arranged: ProxyBridge<T, Bk::R1, Bk::Key, Bk::V1>,
    /// The block's matched pairs, held across blocks to keep their allocations.
    matches: HalfJoinMatches<Bk::ROut>,
    /// The last block's containers, in reverse, served from the back one `next` at a time.
    ready: Vec<Bk::Output>,
    /// Scratch for one key's rows in time order.
    order: TimeOrder,
    _marker: std::marker::PhantomData<C0>,
}

impl<T, B, C0, Bk> Iterator for ProxyHalfJoinIter<T, B, C0, Bk>
where
    T: Timestamp + Lattice,
    Bk: ProxyHalfJoinBackend<T, B, C0>,
{
    type Item = Bk::Output;

    /// Serve a ready container, else draw and cross blocks until one yields any.
    fn next(&mut self) -> Option<Bk::Output> {
        while self.ready.is_empty() && self.from != KeyPosition::End {
            self.refill();
            self.work();
            if !self.matches.pairs.is_empty() { self.cross(); }
        }
        self.ready.pop()
    }
}

impl<T, B, C0, Bk> ProxyHalfJoinIter<T, B, C0, Bk>
where
    T: Timestamp + Lattice,
    Bk: ProxyHalfJoinBackend<T, B, C0>,
{
    /// Draw the next block from the backend.
    fn refill(&mut self) {
        self.stream.clear();
        self.arranged.clear();
        let before = self.from;
        self.backend.borrow_mut().advance(&self.instance, &mut self.from, &mut self.stream, &mut self.arranged);
        // Without progress the iterator would never retire, so this guards liveness as well as contract.
        debug_assert!(
            self.from > before,
            "advance must either strictly increase `from` or report the iteration complete",
        );
        debug_assert!(
            self.stream.windows(2).all(|w| w[0].0.0 <= w[1].0.0),
            "advance must present the stream in key order",
        );
        debug_assert_sorted_bridge(&self.arranged, "advance (arranged)");
        // A key outside `[before, from)` belongs to an earlier or later block, and a key split
        // across blocks would meet only part of the other side.
        debug_assert!(
            {
                let mut keys = self.stream.iter().map(|r| r.0.0).chain(self.arranged.iter().map(|r| r.0.0));
                keys.all(|k| before <= KeyPosition::At(k) && KeyPosition::At(k) < self.from)
            },
            "advance must report a key entirely within the block that first mentions it",
        );
        // The lifted invariant and the driver's bound, checked on what the backend presents.
        debug_assert!(
            self.stream.iter().all(|(_, lifted, _)| lifted.time.less_equal(&lifted.data)),
            "a lifted update's time as data must be at or beyond its own time",
        );
        debug_assert!(
            self.stream.iter().all(|(_, lifted, _)| self.lower.less_equal(&lifted.time)),
            "every released update must be at or beyond the unit's lower bound",
        );
    }

    /// Match the whole of the current block into the match buffers.
    fn work(&mut self) {
        let (stream, arranged) = (&self.stream, &self.arranged);
        let backend = self.backend.borrow();
        let (mut i, mut j) = (0usize, 0usize);
        while i < stream.len() && j < arranged.len() {
            let (ki, kj) = (stream[i].0.0, arranged[j].0.0);
            // A key on one side alone meets nothing.
            match ki.cmp(&kj) {
                Ordering::Less => { while i < stream.len() && stream[i].0.0 == ki { i += 1; } }
                Ordering::Greater => { while j < arranged.len() && arranged[j].0.0 == kj { j += 1; } }
                Ordering::Equal => {
                    let mut e0 = i;
                    while e0 < stream.len() && stream[e0].0.0 == ki { e0 += 1; }
                    let mut e1 = j;
                    while e1 < arranged.len() && arranged[e1].0.0 == ki { e1 += 1; }
                    half_join_key(stream, i..e0, arranged, j..e1, self.strict, &mut self.order, &mut self.matches, |a, b| backend.compare_times(a, b));
                    i = e0;
                    j = e1;
                }
            }
        }
    }

    /// Turn the block's matches into containers, ready to be served one at a time.
    fn cross(&mut self) {
        self.backend.borrow_mut().cross(&self.instance, &self.stream, &self.arranged, &mut self.matches, &mut self.ready);
        // `next` serves from the back, so reverse to ship in the order the backend produced.
        self.ready.reverse();
        self.matches.clear();
    }
}

/// Whether an arranged time precedes an update's own, in the total order: strictly, or also when
/// equal unless `strict`.
#[inline]
fn precedes(order: Ordering, strict: bool) -> bool {
    match order {
        Ordering::Less => true,
        Ordering::Equal => !strict,
        Ordering::Greater => false,
    }
}

/// One key's rows on each side, as positions in time order: scratch held across keys.
#[derive(Default)]
struct TimeOrder {
    stream: Vec<usize>,
    arranged: Vec<usize>,
}

/// Match one key's records across the two presented sides.
///
/// If either side is small, every pair is compared. Otherwise both sides are put in time order,
/// and the arranged rows preceding each update form a prefix that only grows along the stream:
/// the comparisons are then linear in the rows, and the work is the matches themselves.
fn half_join_key<T, R0, R1, RO, K: Copy + Ord, V0: Copy + Ord, V1: Copy + Ord>(
    stream: &HalfJoinStream<T, R0, K, V0>,
    r0: std::ops::Range<usize>,
    arranged: &ProxyBridge<T, R1, K, V1>,
    r1: std::ops::Range<usize>,
    strict: bool,
    order: &mut TimeOrder,
    matches: &mut HalfJoinMatches<RO>,
    compare: impl Fn(&T, &T) -> Ordering,
) where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    let mut emit = |a: usize, b: usize| {
        matches.pairs.push((a, b));
        matches.diffs.push(stream[a].2.clone().multiply(&arranged[b].2));
    };
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                if precedes(compare(&arranged[b].1, &stream[a].1.time), strict) { emit(a, b); }
            }
        }
    }
    else {
        let TimeOrder { stream: order0, arranged: order1 } = order;
        order0.clear();
        order0.extend(r0);
        order0.sort_unstable_by(|&a, &b| compare(&stream[a].1.time, &stream[b].1.time));
        order1.clear();
        order1.extend(r1);
        order1.sort_unstable_by(|&a, &b| compare(&arranged[a].1, &arranged[b].1));
        let mut prefix = 0;
        for &a in order0.iter() {
            while prefix < order1.len() && precedes(compare(&arranged[order1[prefix]].1, &stream[a].1.time), strict) { prefix += 1; }
            for &b in &order1[..prefix] { emit(a, b); }
        }
    }
}
