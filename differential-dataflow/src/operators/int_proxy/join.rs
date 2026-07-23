//! The proxy join framework.
//!
//! A conventional differential join against `(group, token)` values, which are provided by
//! and then interpreted by a backend, who is relieved of lattice-time reasoning.

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::BatchReader;
use super::ProxyBridge;
use crate::operators::join::{Fresh, JoinTactic};

use super::history::IdHistory;

/// A unit of proxied join work, presented to the backend.
pub struct JoinInstance<'a, B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// The first input's batches.
    pub batches0: &'a [B0],
    /// The second input's batches.
    pub batches1: &'a [B1],
    /// The compaction frontier for loading (the unit's capability time).
    pub lower: AntichainRef<'a, B0::Time>,
}

/// One window of a join unit: both inputs presented for a contiguous, ascending group range.
pub struct JoinWindow<G, I0, I1, T, R0, R1> {
    /// Window presentation of the first input, sorted & consolidated by `((group, token), time)`.
    pub input0: ProxyBridge<G, I0, T, R0>,
    /// Window presentation of the second input, same ordering, over the same group range.
    pub input1: ProxyBridge<G, I1, T, R1>,
}

/// A type that can interpret and retire pairs of batches, joined by group tokens.
///
/// The protocol repeatedly invokes `next_window` to produce bounded presentations of the two
/// inputs, matched by group; each match is handed to `absorb`, and `flush` yields the final
/// partial container. Windows are produced lazily as the join driver's fuel allows, so at most
/// one window's presentations are live at a time.
pub trait ProxyJoinBackend<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// The group token: names the granule of independence, shared by both inputs.
    ///
    /// Commonly a `u64` key hash; exactly the key for small `Copy` keys. `'static` because
    /// groups are the one token that may cross invocations.
    type Group: Copy + Ord + 'static;
    /// The value token for the first input, scoped to one invocation.
    type Token0: Copy + Ord;
    /// The value token for the second input, scoped to one invocation.
    type Token1: Copy + Ord;
    /// Diff type presented for the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type presented for the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the tactic.
    type ROut: Semigroup;
    /// The output container built from matched value tokens.
    type Output;

    /// Per-unit resumption state, owned by the unit and interpreted only by the backend.
    ///
    /// Unit progress cannot live on `&mut self`: the driver holds units across scheduler
    /// activations and drains them under fuel, so several half-drained units (from both input
    /// queues) may interleave their windows against one shared backend. `Default` is the state
    /// of a fresh unit; a typical backend records a position per batch per side.
    type Cursor: Default;

    /// Per-unit output staging, owned by the unit — the write-side twin of [`Cursor`](Self::Cursor).
    ///
    /// Staging cannot live on `&mut self` for the same reason unit progress cannot: two
    /// half-drained units interleave `absorb` calls against one shared backend, and a
    /// shared buffer would ship one unit's matches in another unit's containers — under
    /// the wrong capability. `Default` is empty staging; a typical backend uses its
    /// partially-built output container.
    type Sink: Default;

    /// Produce the next window of the join unit, and advance `cursor`.
    ///
    /// Windows must cover contiguous, strictly ascending group ranges, and together must cover
    /// every group appearing in the `fresh` side's data; groups absent from either side should
    /// be omitted (they produce no matches, and driving the unit by the fresh side's groups is
    /// what makes a small fresh batch against a large trace cost `O(fresh)` presentations
    /// rather than `O(trace)`). Note: the harness is data-oblivious and *cannot check* the
    /// coverage clause — it checks ordering, progress, and sortedness; missing a fresh group
    /// silently loses that group's matches. Both bridges **must be sorted and consolidated**
    /// by `((group, token), time)`. The backend sizes windows to amortize harness crossings;
    /// both bridges are live at once, so tighter windows mean less state.
    ///
    /// `reuse` returns the previous, fully-processed window: reclaim its bridge capacity
    /// (`clear()` and refill) rather than allocating fresh, so steady-state windowing does
    /// not churn allocation proportional to data volume.
    fn next_window(&mut self, instance: &JoinInstance<'_, B0, B1>, fresh: Fresh, cursor: &mut Self::Cursor, reuse: Option<JoinWindow<Self::Group, Self::Token0, Self::Token1, B0::Time, Self::R0, Self::R1>>) -> Option<JoinWindow<Self::Group, Self::Token0, Self::Token1, B0::Time, Self::R0, Self::R1>>;

    /// Absorb one match: left and right tokens, joined time, multiplied diff.
    ///
    /// The backend accumulates matches into `sink` (this unit's staging) and returns a
    /// finished container whenever its own size target is reached (the backend, not the
    /// harness, decides container granularity). Matches arrive grouped by key, keys in
    /// window order. Called at most once per match: there is no staging in the harness,
    /// so this is the only time the match exists outside the sink. Container boundaries
    /// must be semantically invisible — the concatenation of all yielded containers
    /// (including `flush`'s) must equal the single-container output.
    fn absorb(&mut self, instance: &JoinInstance<'_, B0, B1>, sink: &mut Self::Sink, left: (Self::Group, Self::Token0), right: (Self::Group, Self::Token1), time: B0::Time, diff: Self::ROut) -> Option<Self::Output>;

    /// Yield `sink`'s final partial container, if any. Called once, after the last window.
    fn flush(&mut self, instance: &JoinInstance<'_, B0, B1>, sink: &mut Self::Sink) -> Option<Self::Output>;
}

/// A proxy-space [`JoinTactic`]: matches records of the presented windows by group token,
/// crosses matched pairs with joined times and multiplied diffs, and defers all value
/// semantics to the backend.
///
/// The backend sits behind an `Rc<RefCell<_>>` because prepared units are *lazy*: the join
/// driver holds each unit's iterator across scheduler activations and drains it under fuel,
/// so several half-drained units (from both input queues) can be live at once, each needing
/// the backend when polled. Unit progress therefore lives in the unit (the `cursor` token),
/// never on the backend.
pub struct ProxyJoinTactic<B0, B1, Bk> {
    backend: std::rc::Rc<std::cell::RefCell<Bk>>,
    _marker: std::marker::PhantomData<(B0, B1)>,
}

impl<B0, B1, Bk> ProxyJoinTactic<B0, B1, Bk> {
    /// A join tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyJoinTactic { backend: std::rc::Rc::new(std::cell::RefCell::new(backend)), _marker: std::marker::PhantomData }
    }
}

impl<B0, B1, Bk> JoinTactic<B0, B1, Bk::Output> for ProxyJoinTactic<B0, B1, Bk>
where
    B0: BatchReader + 'static,
    B1: BatchReader<Time = B0::Time> + 'static,
    Bk: ProxyJoinBackend<B0, B1> + 'static,
    Bk::Output: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(JoinUnit {
            task: Task { input0, input1, fresh, lower: Antichain::from_elem(meet) },
            backend: std::rc::Rc::clone(&self.backend),
            cursor: Bk::Cursor::default(),
            sink: Bk::Sink::default(),
            phase: Phase::Fetch,
            high: None,
            spent: None,
            h0: IdHistory::new(),
            h1: IdHistory::new(),
            ready: std::collections::VecDeque::new(),
        })
    }
}

/// The bridge type a backend `Bk` presents for batches `B0`/`B1` — [`JoinWindow`] at
/// the backend's tokens and the batches' time. Named to keep the unit's fields legible.
type WindowFor<B0, B1, Bk> = JoinWindow<
    <Bk as ProxyJoinBackend<B0, B1>>::Group,
    <Bk as ProxyJoinBackend<B0, B1>>::Token0,
    <Bk as ProxyJoinBackend<B0, B1>>::Token1,
    <B0 as BatchReader>::Time,
    <Bk as ProxyJoinBackend<B0, B1>>::R0,
    <Bk as ProxyJoinBackend<B0, B1>>::R1,
>;

/// The immutable description of a unit's work: both batch lists, which side is fresh,
/// and the capability's time (a lower bound on the fresh side's times, so the
/// accumulated side loads compacted — see [`JoinInstance`]; every output ships under
/// that capability, so advancing loaded times by it leaves the output unchanged).
struct Task<B0: BatchReader, B1> {
    input0: Vec<B0>,
    input1: Vec<B1>,
    fresh: Fresh,
    lower: Antichain<B0::Time>,
}

impl<B0: BatchReader, B1: BatchReader<Time = B0::Time>> Task<B0, B1> {
    /// The borrowed view of the task that every backend call receives.
    fn instance(&self) -> JoinInstance<'_, B0, B1> {
        JoinInstance { batches0: &self.input0, batches1: &self.input1, lower: self.lower.borrow() }
    }
}

/// Where a unit stands in its march through the backend's windows.
enum Phase<W> {
    /// No window in progress; ask the backend for the next one.
    Fetch,
    /// Merging through a window, one key per step; the `usize`s are the merge
    /// positions into its two bridges.
    Merge(W, usize, usize),
    /// The backend returned `None`: windows are exhausted, final `flush` not yet taken.
    Drained,
    /// `flush` taken; the iterator is spent and yields only `None`.
    Spent,
}

/// One lazy join unit: owns its batches and streams outputs a key at a time.
///
/// Each `next` first drains `ready`; when empty it advances `phase` by one step —
/// fetching the next window, or merge-matching **one** key from the current one,
/// feeding each match to the backend's `absorb` (which yields containers at its own
/// granularity; the final remainder comes from `flush` at `Drained`). Matches are
/// never staged in the harness: peak state is one window's presentations, plus one
/// key's yielded containers in `ready` (a fanout-F key at container target t enqueues
/// F/t containers in one step), plus the sink's partial.
///
/// The fields group by owner. The *task* is the immutable work description. The
/// *backend* is shared by every live unit of the operator, so the two state halves it
/// interprets — `cursor` (read resumption) and `sink` (output staging) — live here,
/// per unit; a shared sink would ship one unit's matches in another's containers (see
/// [`ProxyJoinBackend::Sink`]). The harness owns the window *machine*: `phase`,
/// the `high` watermark enforcing ascending windows, and the `spent` window awaiting
/// return to the backend for buffer reuse. `h0`/`h1` are merge *scratch*, reloaded
/// per key (only the >=16/>=16 wave path touches them) so high-fanout keys pay no
/// per-key allocation. `ready` is the *output* queue toward the driver.
struct JoinUnit<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
{
    // The work.
    task: Task<B0, B1>,
    // The shared backend, and the two per-unit state halves it interprets.
    backend: std::rc::Rc<std::cell::RefCell<Bk>>,
    cursor: Bk::Cursor,
    sink: Bk::Sink,
    // The harness's window machine.
    phase: Phase<WindowFor<B0, B1, Bk>>,
    high: Option<Bk::Group>,
    spent: Option<WindowFor<B0, B1, Bk>>,
    // Per-key merge scratch, reused across keys and windows.
    h0: IdHistory<Bk::Token0, B0::Time, Bk::R0>,
    h1: IdHistory<Bk::Token1, B0::Time, Bk::R1>,
    // Outputs not yet handed to the driver: at most one key's containers.
    ready: std::collections::VecDeque<Bk::Output>,
}

impl<B0, B1, Bk> Iterator for JoinUnit<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
{
    type Item = Bk::Output;
    fn next(&mut self) -> Option<Bk::Output> {
        while self.ready.is_empty() {
            let backend = self.backend.clone();
            let mut backend = backend.borrow_mut();
            let instance = self.task.instance();

            match &mut self.phase {
                Phase::Spent => return None,

                // Take this unit's final partial container, exactly once.
                Phase::Drained => {
                    self.phase = Phase::Spent;
                    return backend.flush(&instance, &mut self.sink);
                }

                // Ask for the next window, returning the spent one for buffer reuse.
                Phase::Fetch => {
                    match backend.next_window(&instance, self.task.fresh, &mut self.cursor, self.spent.take()) {
                        None => self.phase = Phase::Drained,
                        Some(window) => {
                            super::debug_assert_sorted_bridge(&window.input0, "next_window.input0");
                            super::debug_assert_sorted_bridge(&window.input1, "next_window.input1");
                            // Progress guard: an empty window cannot advance the watermark, so a
                            // backend emitting them repeatedly would spin this loop forever. Skip
                            // fully-cancelled ranges internally, or return `None`.
                            assert!(
                                !window.input0.is_empty() || !window.input1.is_empty(),
                                "next_window: windows must present at least one record",
                            );
                            let first = window.input0.first().map(|r| r.0.0).into_iter().chain(window.input1.first().map(|r| r.0.0)).min();
                            let last = window.input0.last().map(|r| r.0.0).into_iter().chain(window.input1.last().map(|r| r.0.0)).max();
                            super::assert_ascending_window(&mut self.high, first, last, "join");
                            self.phase = Phase::Merge(window, 0, 0);
                        }
                    }
                }

                // Merge-match at most ONE key, so `ready` holds at most one key's
                // containers. Each match goes straight to the backend's `absorb` — no
                // staging — which yields containers at its own granularity, including
                // *within* a high-fanout key's wave.
                Phase::Merge(window, i, j) => {
                    let p0 = &window.input0;
                    let p1 = &window.input1;
                    let (h0, h1) = (&mut self.h0, &mut self.h1);
                    let ready = &mut self.ready;
                    let sink = &mut self.sink;
                    let mut emit = |l: (Bk::Group, Bk::Token0), r: (Bk::Group, Bk::Token1), t: B0::Time, d: Bk::ROut| {
                        if let Some(out) = backend.absorb(&instance, sink, l, r, t, d) {
                            ready.push_back(out);
                        }
                    };
                    let mut matched = false;
                    while !matched && *i < p0.len() && *j < p1.len() {
                        let (ki, kj) = (p0[*i].0.0, p1[*j].0.0);
                        if ki < kj {
                            *i += 1;
                        } else if kj < ki {
                            *j += 1;
                        } else {
                            let mut e0 = *i;
                            while e0 < p0.len() && p0[e0].0.0 == ki { e0 += 1; }
                            let mut e1 = *j;
                            while e1 < p1.len() && p1[e1].0.0 == ki { e1 += 1; }
                            join_key(ki, p0, *i..e0, p1, *j..e1, h0, h1, &mut emit);
                            *i = e0;
                            *j = e1;
                            matched = true;
                        }
                    }
                    if *i >= p0.len() || *j >= p1.len() {
                        match std::mem::replace(&mut self.phase, Phase::Fetch) {
                            Phase::Merge(window, _, _) => self.spent = Some(window),
                            _ => unreachable!("phase was Merge above"),
                        }
                    }
                }
            }
        }
        self.ready.pop_front()
    }
}

/// Match one key's records across the two presented runs, emitting each match.
///
/// If either history is small, this performs a simple cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
fn join_key<G, I0, I1, T, R0, R1, RO, F>(
    kh: G,
    p0: &ProxyBridge<G, I0, T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<G, I1, T, R1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<I0, T, R0>,
    h1: &mut IdHistory<I1, T, R1>,
    emit: &mut F,
) where
    G: Copy + Ord,
    I0: Copy + Ord,
    I1: Copy + Ord,
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
    F: FnMut((G, I0), (G, I1), T, RO),
{
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                emit((kh, p0[a].0.1), (kh, p1[b].0.1), p0[a].1.join(&p1[b].1), p0[a].2.clone().multiply(&p1[b].2));
            }
        }
        return;
    }

    // Reusable replay scratch, reloaded per key (`load_iter` clears + rebuilds, keeping capacity);
    // the caller holds `h0`/`h1` across the unit so a high-fanout join allocates no per-key history.
    h0.load_iter(r0.map(|i| (p0[i].0.1, p0[i].1.clone(), p0[i].2.clone())), None);
    h1.load_iter(r1.map(|i| (p1[i].0.1, p1[i].1.clone(), p1[i].2.clone())), None);

    crate::operators::common::bilinear_wave(h0, h1, |v0, v1, t, d| {
        emit((kh, v0), (kh, v1), t, d);
    });
}
