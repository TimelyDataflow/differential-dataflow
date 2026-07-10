//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers.

use std::collections::{BTreeMap, BTreeSet};

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use super::ProxyBridge;
use crate::operators::reduce::{ReduceTactic, sort_dedup};

use super::history::{IdHistory, TimeHistory};

/// A unit of proxied reduce work, presented to the backend.
pub struct ReduceInstance<'a, B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// The accumulated input history.
    pub source_batches: &'a [B1],
    /// The freshly arrived input delta.
    pub input_batches: &'a [B1],
    /// The accumulated output history.
    pub output_batches: &'a [B2],
    /// The compaction frontier for loading (the retire's lower bound).
    pub lower: AntichainRef<'a, B1::Time>,
}

/// One window of a retire's changed keys: a bounded, hash-contiguous snip the backend sizes.
///
/// The window has the input (old and new) and output histories, restricted to the window's keys.
pub struct ReduceWindow<T, RIn, ROut> {
    /// The window's key hashes: a contiguous, ascending slice of the retire's `changed` keys.
    pub keys: Vec<u64>,
    /// Input presentation for `keys`, sorted & consolidated by `((key_hash, value_id), time)`.
    pub input: ProxyBridge<T, RIn>,
    /// Output-history presentation for `keys`, same ordering.
    pub output: ProxyBridge<T, ROut>,
}

/// The reduce backend: value semantics for a proxy-space reduction, driven by [`ProxyReduceTactic`].
///
/// The protocol is currently (temporarily) for each round of invocation:
/// `seed_times begin [ next_window reduce_correction* emit ]* finish`
/// This should be improved to put the `seed_times` in the per-window loop, or remove it entirely.
pub trait ProxyReduceBackend<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output.
    type ROut: Semigroup + 'static;

    /// Hash keys and associated times in the instance's novel input batches.
    ///
    /// This is used (with held times) to seed the interesting times for each key.
    fn seed_times(&self, instance: &ReduceInstance<'_, B1, B2>) -> Vec<(u64, B1::Time)>;

    /// Initiate a session to create batches for these descriptions, which span `[lower, upper)`.
    ///
    /// It is the backend's job to prepare output batches for each of these descriptions.
    /// The computation proceeds in windows of keys, where only the backend maintains this
    /// work in progress, until `finish()` is called.
    fn begin(&mut self, tiles: &[Description<B1::Time>]);

    /// Produce the next window, resticted to `changed[cursor..]`, and update `cursor` to track.
    ///
    /// The size of the window is up to the backend, where the window should be large enough to
    /// amortize the crossings between the harness and the backend. The proxy bridges for the
    /// whole window will be active at the same time, so tighter windows reduce the required state.
    fn next_window(&mut self, instance: &ReduceInstance<'_, B1, B2>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<B1::Time, Self::RIn, Self::ROut>>;

    /// A wave of input-output reconciliation, in which the backend supplies necessary edits.
    ///
    /// Multiple keys are provided concurrently, for each an accumulated input and tentative output.
    /// The backend should provide for each key the necessary output updates to bring the output in
    /// with its desires. The `usize` integers upper bound the range for the corresponding key.
    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, Self::RIn)],
        out_ends: &[usize],
        output: &[(u64, Self::ROut)],
    ) -> (Vec<(u64, Self::ROut)>, Vec<usize>);

    /// Commit to a collection of updates at a specific batch in progress.
    ///
    /// The `tile: usize` indexes the list of descriptions provided to `begin()`, and these updates
    /// are aimed at that batch in progress.
    fn emit(&mut self, tile: usize, records: &[((u64, u64), B1::Time, Self::ROut)]);

    /// Complete the session matching `begin`. The outputs correspond to the descriptions it was provided.
    fn finish(&mut self) -> Vec<B2>;
}

/// A proxy-space [`ReduceTactic`]: matches input and output records by `key_hash`.
pub struct ProxyReduceTactic<T, Bk> {
    backend: Bk,
    /// Pending interesting times beyond the upper frontier, keyed by key hash.
    pending: BTreeMap<u64, Vec<T>>,
}

impl<T, Bk> ProxyReduceTactic<T, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyReduceTactic { backend, pending: BTreeMap::new() }
    }
}

impl<B1, B2, Bk> ReduceTactic<B1, B2> for ProxyReduceTactic<B1::Time, Bk>
where
    B1: BatchReader,
    B2: BatchReader<Time = B1::Time>,
    Bk: ProxyReduceBackend<B1, B2>,
{
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<B1::Time>,
        upper: &Antichain<B1::Time>,
        held: &Antichain<B1::Time>,
    ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>) {
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            return (Vec::new(), held.clone());
        }

        let instance = ReduceInstance {
            source_batches: &source_batches,
            input_batches: &input_batches,
            output_batches: &output_batches,
            lower: lower.borrow(),
        };

        let seeds = self.backend.seed_times(&instance);
        debug_assert!(seeds.windows(2).all(|w| w[0].0 <= w[1].0), "seed_times must be sorted by key_hash");
        let mut changed: BTreeSet<u64> = seeds.iter().map(|(k, _)| *k).collect();
        changed.extend(self.pending.keys().copied());
        if changed.is_empty() {
            self.pending.clear();
            return (Vec::new(), Antichain::new());
        }
        let changed: Vec<u64> = changed.into_iter().collect();

        // The output tiling (identical to the Abelian tactic): one tile per held time, keeping
        // non-degenerate intervals; `tile_of[i]` maps held time `i` to its tile.
        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let mut tile_descs: Vec<Description<B1::Time>> = Vec::new();
        let mut tile_held: Vec<B1::Time> = Vec::new();
        let mut tile_of: Vec<Option<usize>> = vec![None; held_elems.len()];
        {
            let mut out_lower = lower.clone();
            for index in 0..held_elems.len() {
                let mut out_upper = upper.clone();
                for t in &held_elems[index + 1..] {
                    out_upper.insert(t.clone());
                }
                if out_upper != out_lower {
                    tile_of[index] = Some(tile_descs.len());
                    tile_descs.push(Description::new(out_lower.clone(), out_upper.clone(), Antichain::from_elem(<B1::Time as Timestamp>::minimum())));
                    tile_held.push(held_elems[index].clone());
                    out_lower = out_upper;
                }
            }
        }
        self.backend.begin(&tile_descs);

        let mut new_pending: BTreeMap<u64, Vec<B1::Time>> = BTreeMap::new();

        let mut cursor = 0usize;
        let mut ns = 0usize;

        // Retire-wide reusable scratch (cleared per window/round/moment, not reallocated). See the
        // profiling note on `DiscoverScratch`: fresh per-key/per-round `Vec`s were the dominant cost.
        let mut discover_scratch: DiscoverScratch<B1::Time, Bk::RIn> = DiscoverScratch::new();
        let mut states: Vec<KeyState<B1::Time, Bk::RIn, Bk::ROut>> = Vec::new();
        let mut tile_deltas: Vec<Vec<((u64, u64), B1::Time, Bk::ROut)>> = (0..held_elems.len()).map(|_| Vec::new()).collect();
        let mut batch_keys: Vec<u64> = Vec::new();
        let mut in_ends: Vec<usize> = Vec::new();
        let mut in_all: Vec<(u64, Bk::RIn)> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::new();
        let mut out_all: Vec<(u64, Bk::ROut)> = Vec::new();
        let mut active: Vec<(usize, B1::Time)> = Vec::new();
        let mut in_accum: Vec<(u64, Bk::RIn)> = Vec::new();
        let mut cur_out: Vec<(u64, Bk::ROut)> = Vec::new();
        let mut moments_scratch: Vec<B1::Time> = Vec::new();
        let mut pended_scratch: Vec<B1::Time> = Vec::new();

        while let Some(window) = self.backend.next_window(&instance, &changed, &mut cursor) {
            let p_in = &window.input;
            let p_out = &window.output;
            super::debug_assert_sorted_bridge(p_in, "next_window.input");
            super::debug_assert_sorted_bridge(p_out, "next_window.output");

            for deltas in tile_deltas.iter_mut() { deltas.clear(); }

            // Phase 1 (determination): for every key in the window, discover its interesting times
            // (times only — no accumulation) and stand up its per-moment replays. Peak state is
            // O(window presentation), bounded by the window `next_window` already materialized.
            // `states` is a long-lived buffer reloaded slot-by-slot (not cleared/rebuilt): a slot's
            // `Vec`s and replays are allocated once and reused, so keys cost no per-key alloc/free.
            // `n_states` is the live prefix this window; higher slots persist (retaining capacity).
            let mut n_states = 0usize;
            let (mut is, mut os) = (0usize, 0usize);
            for &key in &window.keys {
                while is < p_in.len() && p_in[is].0.0 < key { is += 1; }
                let i0 = is;
                while is < p_in.len() && p_in[is].0.0 == key { is += 1; }
                let i1 = is;
                while os < p_out.len() && p_out[os].0.0 < key { os += 1; }
                let o0 = os;
                while os < p_out.len() && p_out[os].0.0 == key { os += 1; }
                let o1 = os;
                while ns < seeds.len() && seeds[ns].0 < key { ns += 1; }
                let n0 = ns;
                while ns < seeds.len() && seeds[ns].0 == key { ns += 1; }
                let n1 = ns;

                moments_scratch.clear();
                pended_scratch.clear();
                {
                    let pending = self.pending.get(&key).map(|p| &p[..]).unwrap_or(&[]);
                    let seed_times = seeds[n0..n1].iter().map(|(_, t)| t.clone());
                    let out_times = (o0..o1).map(|o| p_out[o].1.clone());
                    discover_times(
                        KeyView { p_in, i0, i1, pending },
                        seed_times, out_times, upper,
                        &mut discover_scratch,
                        &mut moments_scratch, &mut pended_scratch,
                    );
                }
                if !pended_scratch.is_empty() {
                    new_pending.insert(key, std::mem::take(&mut pended_scratch));
                }
                if moments_scratch.is_empty() {
                    continue;
                }

                // Reload slot `n_states` in place (grow the buffer by one only when a window is wider
                // than any before). `drain` moves the discovered moments in without copy or realloc.
                if n_states == states.len() {
                    states.push(KeyState::empty());
                }
                let st = &mut states[n_states];
                st.key = key;
                st.cursor = 0;
                st.produced.clear();
                st.moments.clear();
                st.moments.extend(moments_scratch.drain(..));
                st.meets.clear();
                st.meets.extend(st.moments.iter().cloned());
                for i in (1..st.meets.len()).rev() {
                    let m = st.meets[i].clone();
                    st.meets[i - 1].meet_assign(&m);
                }
                st.in_replay.load_iter((i0..i1).map(|i| (p_in[i].0.1, p_in[i].1.clone(), p_in[i].2.clone())), st.meets.first());
                st.out_replay.load_iter((o0..o1).map(|o| (p_out[o].0.1, p_out[o].1.clone(), p_out[o].2.clone())), st.meets.first());
                n_states += 1;
            }

            // Phase 2 (application): walk all keys' moments in ROUNDS. Each round assembles every
            // active key's one-moment-deep input and current-output accumulations and crosses them in
            // a SINGLE `reduce_corrections` — batching across keys (a key's own moments stay
            // sequential, each seeing its earlier corrections via `produced`). This caps the backend
            // call count at O(max moments over keys), not O(sum of moments), with peak materialization
            // one moment deep per key. `produced` is meet-collapsed each round, exactly like the
            // reference — bounded, not the O(times × values) delta history.
            loop {
                batch_keys.clear();
                in_ends.clear();
                in_all.clear();
                out_ends.clear();
                out_all.clear();
                active.clear();
                let mut advanced = false;
                for (si, st) in states[..n_states].iter_mut().enumerate() {
                    if st.cursor >= st.moments.len() {
                        continue;
                    }
                    advanced = true;
                    let j = st.cursor;
                    st.cursor += 1;
                    let t = st.moments[j].clone();
                    st.in_replay.step_through(&t);
                    st.out_replay.step_through(&t);
                    st.in_replay.advance_buffer_by(&st.meets[j]);
                    st.out_replay.advance_buffer_by(&st.meets[j]);
                    for ((_, et), _) in st.produced.iter_mut() {
                        *et = et.join(&st.meets[j]);
                    }
                    crate::consolidation::consolidate(&mut st.produced);

                    in_accum.clear();
                    for ((vid, et), d) in st.in_replay.buffer().iter() {
                        if et.less_equal(&t) {
                            in_accum.push((*vid, d.clone()));
                        }
                    }
                    crate::consolidation::consolidate(&mut in_accum);
                    cur_out.clear();
                    for ((vid, et), d) in st.out_replay.buffer().iter().chain(st.produced.iter()) {
                        if et.less_equal(&t) {
                            cur_out.push((*vid, d.clone()));
                        }
                    }
                    crate::consolidation::consolidate(&mut cur_out);

                    if in_accum.is_empty() && cur_out.is_empty() {
                        continue;
                    }
                    batch_keys.push(st.key);
                    in_all.extend(in_accum.drain(..));
                    in_ends.push(in_all.len());
                    out_all.extend(cur_out.drain(..));
                    out_ends.push(out_all.len());
                    active.push((si, t));
                }
                // Terminate only when every key is EXHAUSTED — not merely when this round produced no
                // crossing. A round can be empty because every key's current moment is empty-gated
                // while keys still have later (non-empty) moments; breaking here would drop them.
                if !advanced {
                    break;
                }
                if batch_keys.is_empty() {
                    continue;
                }

                let (corr, corr_ends) = self.backend.reduce_corrections(&batch_keys, &in_ends, &in_all, &out_ends, &out_all);
                let mut cstart = 0usize;
                for (bi, (si, t)) in active.iter().enumerate() {
                    let cend = corr_ends[bi];
                    if cstart != cend {
                        let idx = held_elems.iter().rposition(|h| h.less_equal(t)).expect("no held capability <= active time");
                        for (vid, d) in &corr[cstart..cend] {
                            states[*si].produced.push(((*vid, t.clone()), d.clone()));
                            tile_deltas[idx].push(((states[*si].key, *vid), t.clone(), d.clone()));
                        }
                    }
                    cstart = cend;
                }
            }

            for (held_index, deltas) in tile_deltas.iter_mut().enumerate() {
                if deltas.is_empty() {
                    continue;
                }
                if let Some(tile) = tile_of[held_index] {
                    crate::consolidation::consolidate_updates(deltas);
                    self.backend.emit(tile, &deltas[..]);
                }
            }
        }

        self.pending = new_pending;
        let produced: Vec<(B1::Time, B2)> = tile_held.into_iter().zip(self.backend.finish()).collect();
        let mut frontier = Antichain::new();
        for times in self.pending.values() {
            for t in times {
                frontier.insert_ref(t);
            }
        }
        (produced, frontier)
    }
}

/// Per-key application state for [`ProxyReduceTactic`]'s round-batched walk: the key's ordered
/// interesting `moments` and their suffix `meets`, its input and output replays (meet-collapsed),
/// the corrections `produced` this round so far, and a `cursor` into `moments`. Held for all of a
/// window's keys at once so each round's crossing batches across keys — a key's own moments stay
/// sequential (each sees its earlier corrections via `produced`), but distinct keys are independent.
struct KeyState<T, RIn, ROut> {
    key: u64,
    moments: Vec<T>,
    meets: Vec<T>,
    in_replay: IdHistory<T, RIn>,
    out_replay: IdHistory<T, ROut>,
    produced: Vec<((u64, T), ROut)>,
    cursor: usize,
}

impl<T: Timestamp + Lattice, RIn: Semigroup, ROut: Semigroup> KeyState<T, RIn, ROut> {
    /// An empty slot, to be filled by [`ProxyReduceTactic`]'s phase 1 (`reload`-style). The `states`
    /// vector holds these across windows and reloads them in place, so a key's buffers are allocated
    /// once (per slot) and reused — never dropped per key (which was ~18% of load in `free`).
    fn empty() -> Self {
        KeyState { key: 0, moments: Vec::new(), meets: Vec::new(), in_replay: IdHistory::new(), out_replay: IdHistory::new(), produced: Vec::new(), cursor: 0 }
    }
}

/// Reusable per-key scratch for [`discover_times`], held once per retire and threaded through every
/// key so the replays and time buffers are cleared-and-refilled (`load`/`load_iter` reset while
/// keeping capacity) rather than reallocated. Mirrors the reference `HistoryReplayer`'s field-held
/// scratch; without it each of ~n keys paid ~7 fresh allocations per call — profiled at ~54% of the
/// hash-reduce load in `malloc`, against the cursor reduce's ~10% (see DESIGN.md F7).
struct DiscoverScratch<T, RIn> {
    batch_replay: TimeHistory<T>,
    input_replay: IdHistory<T, RIn>,
    output_replay: TimeHistory<T>,
    synth: Vec<T>,
    times_current: Vec<T>,
    temporary: Vec<T>,
    meets: Vec<T>,
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone> DiscoverScratch<T, RIn> {
    fn new() -> Self {
        DiscoverScratch {
            batch_replay: TimeHistory::new(),
            input_replay: IdHistory::new(),
            output_replay: TimeHistory::new(),
            synth: Vec::new(),
            times_current: Vec::new(),
            temporary: Vec::new(),
            meets: Vec::new(),
        }
    }
}

/// A one-key view into the input presentation: the read-only arguments [`discover_times`] needs
/// about a single key — its slice `[i0, i1)` of the merged input run `p_in` and the carried
/// `pending` times.
struct KeyView<'a, T, RIn> {
    p_in: &'a ProxyBridge<T, RIn>,
    i0: usize,
    i1: usize,
    pending: &'a [T],
}

/// Updates an optional meet by an optional time.
fn update_meet<T: Lattice + Clone>(meet: &mut Option<T>, other: Option<&T>) {
    if let Some(time) = other {
        match meet.as_mut() {
            Some(m) => m.meet_assign(time),
            None => *meet = Some(time.clone()),
        }
    }
}

/// Phase A: discover a key's interesting times in `[lower, upper)` (pending those at/after `upper`)
/// **without** accumulating input brackets. It mirrors the reference's DETERMINATION step (times
/// only, ref `reduce.rs` :671–713): it replays the key's novel (`seed_times`) and `pending` times
/// in ascending order, marking those carrying novel/pending updates interesting and synthesizing the
/// joins thereof — joins against the input/output histories and the reached times. Nothing
/// materializes an input collection, so peak memory is O(times), never O(times × values); the
/// application walk re-assembles each moment's accumulation on the fly, one moment deep — the whole
/// point of the tactic (see DESIGN.md F8). Buffers are advanced by the meet of the times still to
/// come and consolidated, keeping a key with many distinct times linear rather than quadratic.
#[allow(clippy::too_many_arguments)]
fn discover_times<T, RIn>(
    key: KeyView<'_, T, RIn>,
    seed_times: impl Iterator<Item = T>,
    out_times: impl Iterator<Item = T>,
    upper: &Antichain<T>,
    scratch: &mut DiscoverScratch<T, RIn>,
    moments: &mut Vec<T>,
    pended: &mut Vec<T>,
) where
    T: Timestamp + Lattice,
    RIn: Semigroup + Clone,
{
    // Reuse the retire's scratch: `load`/`load_iter` reset the replays (keeping capacity); the plain
    // buffers are cleared here. `meets_slice` reborrows `meets` immutably; the rest stay disjoint.
    let DiscoverScratch { batch_replay, input_replay, output_replay, synth, times_current, temporary, meets } = scratch;
    synth.clear();
    times_current.clear();
    temporary.clear();

    batch_replay.load(seed_times, None);

    meets.clear();
    meets.extend(key.pending.iter().cloned());
    for i in (1..meets.len()).rev() {
        let m = meets[i].clone();
        meets[i - 1].meet_assign(&m);
    }

    let mut meet: Option<T> = None;
    update_meet(&mut meet, meets.first());
    update_meet(&mut meet, batch_replay.meet());

    // The merged (history ⊎ novel) run — replayed for its TIMES only (join base), never
    // accumulated. Output times likewise: base joins, never seeds.
    input_replay.load_iter(
        (key.i0..key.i1).map(|i| (key.p_in[i].0.1, key.p_in[i].1.clone(), key.p_in[i].2.clone())),
        meet.as_ref(),
    );
    output_replay.load(out_times, meet.as_ref());

    let mut times_slice = key.pending;
    let mut meets_slice = &meets[..];

    while let Some(next_time) = [batch_replay.time(), times_slice.first(), input_replay.time(), output_replay.time(), synth.last()]
        .into_iter()
        .flatten()
        .min()
        .cloned()
    {
        input_replay.step_while_time_is(&next_time);
        output_replay.step_while_time_is(&next_time);
        let mut interesting = batch_replay.step_while_time_is(&next_time);
        if interesting {
            if let Some(m) = meet.as_ref() {
                batch_replay.advance_buffer_by(m);
            }
        }
        while synth.last() == Some(&next_time) {
            times_current.push(synth.pop().expect("nonempty"));
            interesting = true;
        }
        while times_slice.first() == Some(&next_time) {
            times_current.push(times_slice[0].clone());
            times_slice = &times_slice[1..];
            meets_slice = &meets_slice[1..];
            interesting = true;
        }
        interesting = interesting || batch_replay.buffer().iter().any(|t| t.less_equal(&next_time));
        interesting = interesting || times_current.iter().any(|t| t.less_equal(&next_time));

        if !upper.less_equal(&next_time) {
            if interesting {
                // Synthesize joins against the input/output histories (times only — no
                // accumulation), then record `next_time` as an interesting moment.
                if let Some(m) = meet.as_ref() {
                    input_replay.advance_buffer_by(m);
                }
                for ((_, t), _) in input_replay.buffer().iter() {
                    if !t.less_equal(&next_time) {
                        temporary.push(next_time.join(t));
                    }
                }
                if let Some(m) = meet.as_ref() {
                    output_replay.advance_buffer_by(m);
                }
                for t in output_replay.buffer().iter() {
                    if !t.less_equal(&next_time) {
                        temporary.push(next_time.join(t));
                    }
                }
                moments.push(next_time.clone());
            }
            temporary.extend(batch_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            temporary.extend(times_current.iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            sort_dedup(temporary);
            let synth_len = synth.len();
            for time in temporary.drain(..) {
                if upper.less_equal(&time) {
                    pended.push(time);
                } else {
                    synth.push(time);
                }
            }
            if synth.len() > synth_len {
                synth.sort_by(|x, y| y.cmp(x));
                synth.dedup();
            }
        } else if interesting {
            pended.push(next_time.clone());
        }

        meet = None;
        update_meet(&mut meet, batch_replay.meet());
        update_meet(&mut meet, input_replay.meet());
        update_meet(&mut meet, output_replay.meet());
        for t in synth.iter() {
            update_meet(&mut meet, Some(t));
        }
        update_meet(&mut meet, meets_slice.first());
        if let Some(m) = meet.as_ref() {
            for t in times_current.iter_mut() {
                *t = t.join(m);
            }
        }
        sort_dedup(times_current);
    }
    sort_dedup(pended);
}
