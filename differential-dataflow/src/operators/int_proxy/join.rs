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
/// inputs, matched by group, and calls `cross` (any number of times per window, including zero)
/// to produce outputs. Windows are produced lazily as the join driver's fuel allows, so at most
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

    /// Produce the next window of the join unit, and advance `cursor`.
    ///
    /// Windows must cover contiguous, strictly ascending group ranges, and together must cover
    /// every group appearing in the `fresh` side's data; groups absent from either side may be
    /// omitted (they produce no matches). Both bridges **must be sorted and consolidated** by
    /// `((group, token), time)`. The backend sizes windows to amortize harness crossings; both
    /// bridges are live at once, so tighter windows mean less state.
    fn next_window(&mut self, instance: &JoinInstance<'_, B0, B1>, fresh: Fresh, cursor: &mut Self::Cursor) -> Option<JoinWindow<Self::Group, Self::Token0, Self::Token1, B0::Time, Self::R0, Self::R1>>;
    /// From a list of left and right tokens, and corresponding times and diffs, the output.
    fn cross(&mut self, instance: &JoinInstance<'_, B0, B1>, left: &[(Self::Group, Self::Token0)], right: &[(Self::Group, Self::Token1)], times: Vec<B0::Time>, diffs: Vec<Self::ROut>) -> Self::Output;
}

/// A proxy-space [`JoinTactic`]: matches records of the presented windows by group token,
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
            backend: std::rc::Rc::clone(&self.backend),
            input0,
            input1,
            fresh,
            lower: Antichain::from_elem(meet),
            cursor: Bk::Cursor::default(),
            high: None,
            done: false,
            h0: IdHistory::new(),
            h1: IdHistory::new(),
            ready: std::collections::VecDeque::new(),
        })
    }
}

/// Update count threshold used to return to the backend with join output to instantiate.
const JOIN_CHUNK: usize = 1 << 20;

/// One lazy join unit: owns its batches and streams outputs window by window.
///
/// Each `next` first drains `ready`; when empty it asks the backend for the next window,
/// merge-matches the window's two presented runs by group, and cross-products matched
/// records into output containers — one per ~[`JOIN_CHUNK`] matches, flushed wherever the
/// batch fills (including mid-key; parity: their concatenation is the single-container
/// output). Peak state is one window's presentations plus the containers it produced.
/// `lower` is the capability's time, a lower bound on the fresh side's times, so the
/// accumulated side loads compacted (see [`JoinInstance`]); every output is produced under
/// that capability, so advancing loaded times by it leaves the output unchanged.
struct JoinUnit<B0: BatchReader, B1: BatchReader<Time = B0::Time>, Bk: ProxyJoinBackend<B0, B1>> {
    backend: std::rc::Rc<std::cell::RefCell<Bk>>,
    input0: Vec<B0>,
    input1: Vec<B1>,
    fresh: Fresh,
    lower: Antichain<B0::Time>,
    /// Backend-interpreted resumption state; `Default` at the unit's start.
    cursor: Bk::Cursor,
    /// Greatest group presented so far, to enforce ascending windows.
    high: Option<Bk::Group>,
    done: bool,
    /// Per-key replay histories, held across the unit and reloaded per key (only the >=16/>=16
    /// replay-wave path touches them), so a high-fanout join pays no per-key `IdHistory` alloc.
    h0: IdHistory<Bk::Token0, B0::Time, Bk::R0>,
    h1: IdHistory<Bk::Token1, B0::Time, Bk::R1>,
    /// Outputs produced by the current window, not yet handed to the driver.
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
        while self.ready.is_empty() && !self.done {
            let mut backend = self.backend.borrow_mut();
            let instance = JoinInstance { batches0: &self.input0, batches1: &self.input1, lower: self.lower.borrow() };
            match backend.next_window(&instance, self.fresh, &mut self.cursor) {
                None => { self.done = true; }
                Some(window) => {
                    let p0 = &window.input0;
                    let p1 = &window.input1;
                    super::debug_assert_sorted_bridge(p0, "next_window.input0");
                    super::debug_assert_sorted_bridge(p1, "next_window.input1");
                    debug_assert!(
                        self.high.map_or(true, |h| p0.first().map_or(true, |r| r.0.0 > h) && p1.first().map_or(true, |r| r.0.0 > h)),
                        "next_window: windows must present strictly ascending group ranges",
                    );
                    if let Some(hi) = p0.last().map(|r| r.0.0).into_iter().chain(p1.last().map(|r| r.0.0)).max() {
                        self.high = Some(hi);
                    }

                    // Merge-join the window's runs on group token (both sorted) and cross matched
                    // pairs. `flush` ships the accumulated batch as a container and resets the
                    // buffers; `join_key` calls it at each `JOIN_CHUNK` boundary — including
                    // *within* a high-fanout key's wave — so no single key materializes more than
                    // a chunk of its cross product.
                    let ready = &mut self.ready;
                    let mut flush = |li: &mut Vec<(Bk::Group, Bk::Token0)>, ri: &mut Vec<(Bk::Group, Bk::Token1)>, ot: &mut Vec<B0::Time>, od: &mut Vec<Bk::ROut>| {
                        ready.push_back(backend.cross(&instance, li.as_slice(), ri.as_slice(), std::mem::take(ot), std::mem::take(od)));
                        li.clear();
                        ri.clear();
                    };

                    let (mut li, mut ri) = (Vec::new(), Vec::new());
                    let (mut ot, mut od) = (Vec::new(), Vec::new());
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
                            join_key(ki, p0, i..e0, p1, j..e1, &mut self.h0, &mut self.h1, &mut li, &mut ri, &mut ot, &mut od, &mut flush);
                            i = e0;
                            j = e1;
                        }
                    }
                    if !li.is_empty() {
                        flush(&mut li, &mut ri, &mut ot, &mut od);
                    }
                }
            }
        }
        self.ready.pop_front()
    }
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, this performs a simple cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
#[allow(clippy::too_many_arguments)]
fn join_key<G, I0, I1, T, R0, R1, RO, F>(
    kh: G,
    p0: &ProxyBridge<G, I0, T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<G, I1, T, R1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<I0, T, R0>,
    h1: &mut IdHistory<I1, T, R1>,
    li: &mut Vec<(G, I0)>,
    ri: &mut Vec<(G, I1)>,
    ot: &mut Vec<T>,
    od: &mut Vec<RO>,
    flush: &mut F,
) where
    G: Copy + Ord,
    I0: Copy + Ord,
    I1: Copy + Ord,
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
    F: FnMut(&mut Vec<(G, I0)>, &mut Vec<(G, I1)>, &mut Vec<T>, &mut Vec<RO>),
{
    // `flush` ships the accumulated batch once it reaches `JOIN_CHUNK` and resets the buffers. It's
    // checked after every produced pair — *including inside the replay wave below* — so even a key
    // whose fanout dwarfs `JOIN_CHUNK` never materializes more than a chunk of its cross product.
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                li.push((kh, p0[a].0.1));
                ri.push((kh, p1[b].0.1));
                ot.push(p0[a].1.join(&p1[b].1));
                od.push(p0[a].2.clone().multiply(&p1[b].2));
                if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
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
        if li.len() >= JOIN_CHUNK {
            flush(li, ri, ot, od);
        }
    });
}
