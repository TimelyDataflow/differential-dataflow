//! The proxy join framework.
//!
//! A conventional differential join against `(u64, u64)` values, which are provided by
//! and then interpreted by a backend, who is relieved of lattice-time reasoning.

use std::cell::RefCell;
use std::rc::Rc;

use timely::progress::Timestamp;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use super::ProxyBridge;
use crate::operators::join::{Fresh, JoinTactic, KEY_WORK_LIMIT};
use crate::operators::history::ValueHistory;

use super::history::IdHistory;

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
        bridge0: &mut ProxyBridge<T, Self::R0>,
        bridge1: &mut ProxyBridge<T, Self::R1>,
    );

    /// Interpret matches derived from the preceding [`Self::advance`] call and place them in
    /// `output`. A block is matched in bounded steps, so `cross` may be called several times for
    /// one `advance`, and is skipped for a step that produced no matches. The iterator always makes
    /// those calls before the next `advance`, so a backend may keep block-local interpretation state
    /// across them, and the next `advance` may overwrite it.
    fn cross(
        &mut self,
        instance: &JoinInstance<T, B0, B1>,
        matches: &mut JoinMatches<T, Self::ROut>,
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

impl<T, B0, B1, Bk> JoinTactic<T, B0, B1, Bk::Output> for ProxyJoinTactic<B0, B1, Bk>
where
    T: Timestamp + Lattice + 'static,
    B0: 'static,
    B1: 'static,
    Bk: ProxyJoinBackend<T, B0, B1> + 'static,
    Bk::Output: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, _fresh: Fresh, meet: T) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(ProxyJoinIter {
            backend: Rc::clone(&self.backend),
            instance: JoinInstance { batches0: input0, batches1: input1, lower: meet },
            from: Some(0),
            p0: Vec::new(),
            p1: Vec::new(),
            h0: IdHistory::new(),
            h1: IdHistory::new(),
            at0: 0,
            at1: 0,
            resume: None,
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
///
/// A block is not matched in one go. `advance` reports a key entirely within the block that first
/// mentions it, so a block is at least one whole key, and one key's cross product is bounded by
/// nothing; matching a whole block would buffer it all before the back-end saw any of it. Matching
/// therefore stops once the buffered matches reach [`KEY_WORK_LIMIT`], even part-way through a key,
/// and `at0`/`at1`/`resume` record where to pick up. The state is all identifiers, times and diffs
/// the iterator owns, so unlike the cursor tactic it needs nothing reloaded to survive a suspension.
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
    p0: ProxyBridge<T, Bk::R0>,
    p1: ProxyBridge<T, Bk::R1>,
    /// Per-key replay histories, held across the iterator and reloaded per key when needed.
    h0: IdHistory<T, Bk::R0>,
    h1: IdHistory<T, Bk::R1>,
    /// How far into the current block's two runs the matching has come.
    at0: usize,
    at1: usize,
    /// Where in the open key's replay the matching suspended; `None` when no key is open.
    resume: Option<Resume>,
    /// The block's matched records, held across blocks to keep their allocations.
    matches: JoinMatches<T, Bk::ROut>,
    /// The last block's containers, in reverse, served from the back one `next` at a time.
    ready: Vec<Bk::Output>,
}

impl<T, B0, B1, Bk> Iterator for ProxyJoinIter<T, B0, B1, Bk>
where
    T: Timestamp + Lattice,
    Bk: ProxyJoinBackend<T, B0, B1>,
{
    type Item = Bk::Output;

    /// Serve a ready container, else match and cross until one yields any.
    fn next(&mut self) -> Option<Bk::Output> {
        while self.ready.is_empty() {
            if self.block_done() {
                if self.from.is_none() { break; }
                self.refill();
            }
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
    /// Whether the current block has been matched in full.
    fn block_done(&self) -> bool {
        self.resume.is_none() && (self.at0 >= self.p0.len() || self.at1 >= self.p1.len())
    }

    /// Draw the next block from the backend.
    fn refill(&mut self) {
        debug_assert!(self.block_done(), "a block is drawn only once the last one is matched");
        self.p0.clear();
        self.p1.clear();
        self.at0 = 0;
        self.at1 = 0;
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

    /// Match the current block into the match buffers, until it is drained or the buffers are full.
    fn work(&mut self) {
        // Disjoint field borrows, as `join_key` holds the bridges and the buffers at once.
        let (p0, p1) = (&self.p0, &self.p1);
        let (h0, h1) = (&mut self.h0, &mut self.h1);

        while self.at0 < p0.len() && self.at1 < p1.len() {
            let ki = p0[self.at0].0.0;
            debug_assert_eq!(ki, p1[self.at1].0.0, "advance must report common keys");
            let mut e0 = self.at0;
            while e0 < p0.len() && p0[e0].0.0 == ki { e0 += 1; }
            let mut e1 = self.at1;
            while e1 < p1.len() && p1[e1].0.0 == ki { e1 += 1; }
            let retired = join_key(ki, p0, self.at0..e0, p1, self.at1..e1, h0, h1, &mut self.matches, &mut self.resume);
            // A key that suspended stays open, so its runs must still be the ones `at0`/`at1` name.
            if !retired { return; }
            self.at0 = e0;
            self.at1 = e1;
            if self.matches.ids.len() >= KEY_WORK_LIMIT { return; }
        }
        debug_assert!(self.at0 == p0.len() && self.at1 == p1.len(), "both bridges must drain together");
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

/// Progress through one key, for the matching that suspends within a key.
///
/// The variants mirror the two strategies [`join_key`] chooses between: the direct cross product it
/// uses when a run is short, and the time-ordered wave it uses when both are long. A wave step
/// advances one history's buffer before emitting against it, so the step it is part-way through is
/// part of the state: resuming must not advance that buffer a second time, which would consolidate
/// it and invalidate the offset resumed from.
#[derive(Clone, Copy)]
enum Resume {
    /// The direct cross product, at offsets `(offset0, offset1)` into the key's two runs.
    Nested(usize, usize),
    /// The wave, between steps; the next step follows from the histories' least times.
    WaveChoose,
    /// The wave, emitting `h1`'s buffer against `h0`'s next edit, from this offset.
    WaveStep0(usize),
    /// The wave, emitting `h0`'s buffer against `h1`'s next edit, from this offset.
    WaveStep1(usize),
}

/// Match one key's records across the two presented runs, reporting whether the key is retired.
///
/// If either history is small, this performs a direct cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
///
/// A key whose cross product fits [`KEY_WORK_LIMIT`] is matched in one uninterrupted pass. A larger
/// one stops once `matches` reaches the limit, records where it stopped in `resume`, and returns
/// `false`; a later call with the same runs and the same `resume` picks up from there. Both
/// strategies address their inputs by offset, so nothing has to be reloaded across a suspension.
fn join_key<T, R0, R1, RO>(
    kh: u64,
    p0: &ProxyBridge<T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<T, R1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<T, R0>,
    h1: &mut IdHistory<T, R1>,
    matches: &mut JoinMatches<T, RO>,
    resume: &mut Option<Resume>,
) -> bool
where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    let nested = r0.len() < 16 || r1.len() < 16;

    if resume.is_none() {
        if r0.len().saturating_mul(r1.len()) <= KEY_WORK_LIMIT {
            if nested {
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
                bilinear_wave(h0, h1, |v0, v1, t, d| {
                    matches.ids.push((kh, (v0, v1)));
                    matches.times.push(t);
                    matches.diffs.push(d);
                });
            }
            return true;
        }

        // The key can outrun the budget, so open it for the matching that suspends within it. The
        // strategy is the one the uninterrupted pass would have taken, so both visit a key's
        // matches in the same order.
        *resume = Some(if nested {
            Resume::Nested(r0.start, r1.start)
        }
        else {
            h0.load_iter(r0.clone().map(|i| (p0[i].0.1, p0[i].1.clone(), p0[i].2.clone())), None);
            h1.load_iter(r1.clone().map(|i| (p1[i].0.1, p1[i].1.clone(), p1[i].2.clone())), None);
            Resume::WaveChoose
        });
    }

    let mut state = resume.expect("key is open");
    loop {
        match state {
            Resume::Nested(mut offset0, mut offset1) => {
                while offset0 < r0.end {
                    while offset1 < r1.end {
                        matches.ids.push((kh, (p0[offset0].0.1, p1[offset1].0.1)));
                        matches.times.push(p0[offset0].1.join(&p1[offset1].1));
                        matches.diffs.push(p0[offset0].2.clone().multiply(&p1[offset1].2));
                        offset1 += 1;
                        if matches.ids.len() >= KEY_WORK_LIMIT {
                            *resume = Some(Resume::Nested(offset0, offset1));
                            return false;
                        }
                    }
                    offset1 = r1.start;
                    offset0 += 1;
                }
                *resume = None;
                return true;
            }
            Resume::WaveChoose => {
                // Step whichever history holds the earlier un-replayed time, and once one is spent,
                // whichever remains. A tie steps the second, as `bilinear_wave`'s comparison does.
                let step0 = match (h0.time(), h1.time()) {
                    (Some(time0), Some(time1)) => time0 < time1,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => { *resume = None; return true; }
                };
                if step0 {
                    h1.advance_buffer_by(h0.meet().unwrap());
                    state = Resume::WaveStep0(0);
                }
                else {
                    h0.advance_buffer_by(h1.meet().unwrap());
                    state = Resume::WaveStep1(0);
                }
            }
            Resume::WaveStep0(mut offset) => {
                while offset < h1.buffer().len() {
                    let (val0, time0, diff0) = h0.edit().unwrap();
                    let ((val1, time1), diff1) = &h1.buffer()[offset];
                    matches.ids.push((kh, (val0, *val1)));
                    matches.times.push(time0.join(time1));
                    matches.diffs.push(diff0.clone().multiply(diff1));
                    offset += 1;
                    if matches.ids.len() >= KEY_WORK_LIMIT {
                        *resume = Some(Resume::WaveStep0(offset));
                        return false;
                    }
                }
                h0.step();
                state = Resume::WaveChoose;
            }
            Resume::WaveStep1(mut offset) => {
                while offset < h0.buffer().len() {
                    let ((val0, time0), diff0) = &h0.buffer()[offset];
                    let (val1, time1, diff1) = h1.edit().unwrap();
                    matches.ids.push((kh, (*val0, val1)));
                    matches.times.push(time0.join(time1));
                    matches.diffs.push(diff0.clone().multiply(diff1));
                    offset += 1;
                    if matches.ids.len() >= KEY_WORK_LIMIT {
                        *resume = Some(Resume::WaveStep1(offset));
                        return false;
                    }
                }
                h1.step();
                state = Resume::WaveChoose;
            }
        }
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
fn bilinear_wave<V, T, R0, R1, RO>(
    h0: &mut ValueHistory<V, T, R0>,
    h1: &mut ValueHistory<V, T, R1>,
    mut emit: impl FnMut(V, V, T, RO),
) where
    V: Copy + Ord,
    T: Ord + Clone + Lattice,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                emit(v0, *v1, t0.join(t1), d0.clone().multiply(d1));
            }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                emit(*v0, v1, t0.join(t1), d0.clone().multiply(d1));
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            emit(v0, *v1, t0.join(t1), d0.clone().multiply(d1));
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            emit(*v0, v1, t0.join(t1), d0.clone().multiply(d1));
        }
        h1.step();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// One `(key hash, id0, id1)` match, at a time and with a diff.
    type Match = ((u64, u64, u64), u64, isize);

    /// A backend that presents its two sides as a single block, and hands matches back verbatim.
    ///
    /// The single block is the shape the suspension has to cope with: `advance` must report a key
    /// entirely within one block, so a hot key cannot be split across blocks however a backend
    /// draws them.
    struct OneBlock {
        side0: ProxyBridge<u64, isize>,
        side1: ProxyBridge<u64, isize>,
    }

    impl ProxyJoinBackend<u64, (), ()> for OneBlock {
        type R0 = isize;
        type R1 = isize;
        type ROut = isize;
        type Output = Vec<Match>;

        fn advance(
            &mut self,
            _instance: &JoinInstance<u64, (), ()>,
            from: &mut Option<u64>,
            bridge0: &mut ProxyBridge<u64, Self::R0>,
            bridge1: &mut ProxyBridge<u64, Self::R1>,
        ) {
            if from.is_some() {
                bridge0.clone_from(&self.side0);
                bridge1.clone_from(&self.side1);
                *from = None;
            }
        }

        fn cross(
            &mut self,
            _instance: &JoinInstance<u64, (), ()>,
            matches: &mut JoinMatches<u64, Self::ROut>,
            output: &mut Vec<Self::Output>,
        ) {
            let crossed = (0..matches.ids.len())
                .map(|at| {
                    let (key, (id0, id1)) = matches.ids[at];
                    ((key, id0, id1), matches.times[at], matches.diffs[at])
                })
                .collect();
            output.push(crossed);
        }
    }

    /// One side of a join: `vals` values under the single key hash `0`, each at `times` distinct
    /// times, with diffs that differ per value and time so a dropped or duplicated match shows up.
    fn side(vals: u64, times: u64) -> ProxyBridge<u64, isize> {
        let mut bridge = ProxyBridge::new();
        for val in 0..vals {
            for time in 0..times {
                bridge.push(((0, val), time, 1 + isize::try_from(val + time).unwrap() % 3));
            }
        }
        bridge
    }

    /// Joins two single-key sides, returning the containers the tactic yields.
    fn join(side0: ProxyBridge<u64, isize>, side1: ProxyBridge<u64, isize>) -> Vec<Vec<Match>> {
        let mut tactic = ProxyJoinTactic::new(OneBlock { side0, side1 });
        tactic.prep(vec![], vec![], Fresh::Input1, 0).collect()
    }

    /// Accumulates matches into a sorted, consolidated list, dropping those that cancel.
    fn consolidate(mut matches: Vec<Match>) -> Vec<Match> {
        matches.sort();
        let mut out: Vec<Match> = Vec::new();
        for (data, time, diff) in matches {
            match out.last_mut() {
                Some(last) if (last.0, last.1) == (data, time) => last.2 += diff,
                _ => out.push((data, time, diff)),
            }
        }
        out.retain(|update| update.2 != 0);
        out
    }

    /// The cross product of two sides, as the join should produce it.
    fn cross(side0: &ProxyBridge<u64, isize>, side1: &ProxyBridge<u64, isize>) -> Vec<Match> {
        let mut out = Vec::new();
        for ((_, id0), time0, diff0) in side0 {
            for ((_, id1), time1, diff1) in side1 {
                out.push(((0, *id0, *id1), *time0.max(time1), diff0 * diff1));
            }
        }
        out
    }

    /// A key large enough to outrun the driver's budget suspends within itself, rather than
    /// matching its whole cross product before the backend is handed any of it. Both sides carry
    /// enough records for the time-ordered wave.
    #[test]
    fn wave_suspends_within_a_key() {
        let vals = 1_100;
        assert!((vals * vals) as usize > KEY_WORK_LIMIT);

        let containers = join(side(vals, 1), side(vals, 1));
        assert!(containers.len() > 1, "the key was matched in one go");
        assert!(containers[0].len() <= KEY_WORK_LIMIT);
        assert_eq!(containers.iter().map(Vec::len).sum::<usize>(), (vals * vals) as usize);
    }

    /// The same, for the direct cross product used when one side's run is short: few records on a
    /// side does not bound the work, only the other side's run length does.
    #[test]
    fn nested_suspends_within_a_key() {
        let (vals0, vals1) = (5, 250_000);
        assert!((vals0 * vals1) as usize > KEY_WORK_LIMIT);

        let containers = join(side(vals0, 1), side(vals1, 1));
        assert!(containers.len() > 1, "the key was matched in one go");
        assert!(containers[0].len() <= KEY_WORK_LIMIT);
        assert_eq!(containers.iter().map(Vec::len).sum::<usize>(), (vals0 * vals1) as usize);
    }

    /// A key with many distinct times exercises the wave's buffer advance and consolidation, which
    /// a suspension must resume without repeating. The matches must consolidate to the same
    /// collection as the cross product of the two sides, with times joined and diffs multiplied.
    #[test]
    fn suspended_wave_matches_the_cross_product() {
        let (vals, times) = (200, 8);
        assert!(((vals * times) * (vals * times)) as usize > KEY_WORK_LIMIT);

        let (side0, side1) = (side(vals, times), side(vals, times));
        let expected = consolidate(cross(&side0, &side1));
        let produced = consolidate(join(side0, side1).concat());

        // The collections run to millions of matches, so report the first difference rather than
        // both of them in full.
        let differs = produced.iter().zip(expected.iter()).position(|(x, y)| x != y);
        assert_eq!(
            differs.map(|at| (at, produced[at], expected[at])),
            None,
            "consolidated matches differ from the cross product",
        );
        assert_eq!(produced.len(), expected.len());
    }
}
