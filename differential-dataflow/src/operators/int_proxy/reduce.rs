//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers.

use std::collections::BTreeMap;

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use super::ProxyBridge;
use crate::operators::reduce::ReduceTactic;

use super::history::IdHistory;
use crate::operators::common::{discover_times, tile_descriptions, DiscoverScratch, KeyView};

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
/// The window has the input (old and new) and output histories, restricted to the window's keys,
/// and the times the new input makes interesting for them.
pub struct ReduceWindow<T, RIn, ROut> {
    /// The window's key hashes: a contiguous, ascending slice of the retire's changed keys.
    pub keys: Vec<u64>,
    /// Input presentation for `keys`, sorted & consolidated by `((key_hash, value_id), time)`.
    pub input: ProxyBridge<T, RIn>,
    /// Output-history presentation for `keys`, same ordering.
    pub output: ProxyBridge<T, ROut>,
    /// Times the instance's *new* input batches carry for `keys`, sorted by key hash.
    ///
    /// These seed each key's interesting times, together with the times held over from earlier
    /// retires. A key needs an entry here or in those held times to belong in `keys` at all.
    pub seeds: Vec<(u64, T)>,
}

impl<T, RIn, ROut> Default for ReduceWindow<T, RIn, ROut> {
    fn default() -> Self {
        Self { keys: vec![], input: vec![], output: vec![], seeds: vec![] }
    }
}

impl<T, RIn, ROut> ReduceWindow<T, RIn, ROut> {
    /// Empties the window, retaining its allocations for the next one.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.input.clear();
        self.output.clear();
        self.seeds.clear();
    }
}

/// One round of reconciliation: for each of several keys, an accumulated input and tentative output.
///
/// The lists are segmented, parallel to `keys`: key `keys[i]` has input `input[..]` over the range
/// ending at `in_ends[i]` and starting at `in_ends[i-1]` (at zero for `i == 0`), and output `output[..]`
/// over the range likewise delimited by `out_ends`. So `in_ends` and `out_ends` have `keys`' length,
/// ascend, and end at the lengths of `input` and `output`. Within a key's range the `u64` are value
/// ids, ascending and consolidated.
pub struct ReduceRound<RIn, ROut> {
    /// The keys reconciled this round, ascending.
    pub keys: Vec<u64>,
    /// One past each key's last input record.
    pub in_ends: Vec<usize>,
    /// The accumulated input, by key.
    pub input: Vec<(u64, RIn)>,
    /// One past each key's last output record.
    pub out_ends: Vec<usize>,
    /// The tentative accumulated output, by key.
    pub output: Vec<(u64, ROut)>,
}

impl<RIn, ROut> Default for ReduceRound<RIn, ROut> {
    fn default() -> Self {
        Self { keys: vec![], in_ends: vec![], input: vec![], out_ends: vec![], output: vec![] }
    }
}

impl<RIn, ROut> ReduceRound<RIn, ROut> {
    /// Empties the round, retaining its allocations for the next one.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.in_ends.clear();
        self.input.clear();
        self.out_ends.clear();
        self.output.clear();
    }
}

/// The output updates a backend supplies for a [`ReduceRound`], as one segmented list.
///
/// `ends` is parallel to the round's `keys`: key `keys[i]`'s corrections are the `updates` ending at
/// `ends[i]` and starting at `ends[i-1]` (at zero for `i == 0`). A key needing no correction gets an
/// empty range, not an omitted one, so `ends` has `keys`' length and ends at `updates`' length.
pub struct ReduceCorrections<ROut> {
    /// The corrections, by key. The `u64` are value ids.
    pub updates: Vec<(u64, ROut)>,
    /// One past each key's last correction.
    pub ends: Vec<usize>,
}

impl<ROut> Default for ReduceCorrections<ROut> {
    fn default() -> Self { Self { updates: vec![], ends: vec![] } }
}

impl<ROut> ReduceCorrections<ROut> {
    /// Empties the corrections, retaining their allocations for the next round.
    pub fn clear(&mut self) {
        self.updates.clear();
        self.ends.clear();
    }
}

/// The reduce backend: value semantics for a proxy-space reduction, driven by [`ProxyReduceTactic`].
///
/// The harness repeatedly invokes [`advance`](Self::advance) to draw a window of changed keys, then
/// reconciles that window's keys against their output with [`reduce_corrections`](Self::reduce_corrections),
/// handing the resulting updates back with [`emit`](Self::emit), until `advance` reports the key space
/// exhausted. The output session brackets whatever windows have work:
/// `advance ( begin [ reduce_corrections* emit* advance ]* finish )?`
/// A retire with no changed keys draws one window, finds it empty, and never opens the session.
pub trait ProxyReduceBackend<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output.
    type ROut: Semigroup;

    /// Initiate a session to create batches for these descriptions, which span `[lower, upper)`.
    ///
    /// It is the backend's job to prepare output batches for each of these descriptions.
    /// The computation proceeds in windows of keys, where only the backend maintains this
    /// work in progress, until `finish()` is called.
    fn begin(&mut self, tiles: &[Description<B1::Time>]);

    /// Populates `window` with the next range of changed keys, and everything known about them.
    ///
    /// The `from` indicates an inclusive lower bound on key hash, and should be updated by the
    /// implementor to an exclusive upper bound for the range of keys it covers in this call. The
    /// `None` value indicates the keys are exhausted. A key is *changed* if the instance's new input
    /// batches carry a time for it, or if it appears in `pending`: the interesting times earlier
    /// retires withheld, ascending by key hash, which the implementor moves through alongside its own
    /// input. The window must hold every changed key in the range, with all of both inputs' updates
    /// for those keys, and it must be non-empty unless `from` is returned as `None`.
    ///
    /// The size of the window is up to the backend, where the window should be large enough to
    /// amortize the crossings between the harness and the backend. The proxy bridges for the
    /// whole window will be active at the same time, so tighter windows reduce the required state.
    fn advance(
        &mut self,
        instance: &ReduceInstance<'_, B1, B2>,
        from: &mut Option<u64>,
        pending: &[(u64, B1::Time)],
        window: &mut ReduceWindow<B1::Time, Self::RIn, Self::ROut>,
    );

    /// A wave of input-output reconciliation, in which the backend supplies necessary edits.
    ///
    /// Multiple keys are provided concurrently, for each an accumulated input and tentative output.
    /// The backend should populate `corrections`, which arrives empty, with for each key the output
    /// updates that bring the output in line with its desires.
    fn reduce_corrections(
        &mut self,
        round: &ReduceRound<Self::RIn, Self::ROut>,
        corrections: &mut ReduceCorrections<Self::ROut>,
    );

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

        // The output tiling (identical to the Abelian tactic): one tile per held time, keeping
        // non-degenerate intervals; `tile_of[i]` maps held time `i` to its tile.
        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let (tile_descs, tile_held, tile_of) = tile_descriptions(lower, upper, &held_elems);
        let tiles: Vec<(B1::Time, Option<usize>)> = held_elems.into_iter().zip(tile_of).collect();

        // The withheld times as the backend sees them: one flat, key-ordered list to move through.
        // The harness keeps its own map, which it indexes by key as it determines each one.
        let pending_flat: Vec<(u64, B1::Time)> = self.pending.iter()
            .flat_map(|(key, times)| times.iter().map(move |t| (*key, t.clone())))
            .collect();

        let mut retire = Retire::new(&mut self.backend, instance, upper, tiles, &self.pending, &pending_flat);
        let began = retire.run(&tile_descs);
        let new_pending = retire.new_pending;

        self.pending = new_pending;
        // No window had work, so no session was opened and there is nothing to finish or to withhold.
        if !began {
            debug_assert!(self.pending.is_empty(), "a retire that determined no key cannot withhold a time");
            return (Vec::new(), Antichain::new());
        }
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

/// One retire in progress: the backend session it drives, the times it reasons against, and the
/// scratch its two phases reuse.
///
/// The retire proceeds window by window (see [`run`](Self::run)), each window determined then
/// applied then flushed. All buffers below are cleared per window, round, or moment rather than
/// reallocated. See the profiling note on [`DiscoverScratch`]: fresh per-key and per-round `Vec`s
/// were the dominant cost.
struct Retire<'a, B1, B2, Bk>
where
    B1: BatchReader,
    B2: BatchReader<Time = B1::Time>,
    Bk: ProxyReduceBackend<B1, B2>,
{
    /// The backend, whose output session [`run`](Self::run) opens and the caller closes.
    backend: &'a mut Bk,
    /// The batches under retirement, as presented to the backend.
    instance: ReduceInstance<'a, B1, B2>,
    /// The retire's upper bound: times at or beyond it are withheld rather than applied.
    upper: &'a Antichain<B1::Time>,
    /// The held capability times, ascending, each with the tile it commits to.
    ///
    /// A held time whose interval was degenerate has no tile; `tile_descriptions` skipped it.
    tiles: Vec<(B1::Time, Option<usize>)>,
    /// Interesting times withheld by earlier retires, by key hash, and the flat presentation of
    /// the same times the backend moves through.
    pending: &'a BTreeMap<u64, Vec<B1::Time>>,
    pending_flat: &'a [(u64, B1::Time)],
    /// Interesting times this retire withholds, by key hash.
    new_pending: BTreeMap<u64, Vec<B1::Time>>,
    /// Output updates by held index, accumulated across a window and emitted at its end.
    tile_deltas: Vec<Vec<((u64, u64), B1::Time, Bk::ROut)>>,

    /// Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
    /// once the backend reports the keys exhausted.
    from: Option<u64>,
    /// The window last drawn, held across draws to keep its allocations.
    window: ReduceWindow<B1::Time, Bk::RIn, Bk::ROut>,

    /// Per-key application state, in slots [`determine`](Self::determine) refills window by window.
    /// It reports how many it filled; higher slots persist, retaining their capacity for a later,
    /// wider window.
    states: Vec<KeyState<B1::Time, Bk::RIn, Bk::ROut>>,
    /// Time-discovery scratch.
    discover_scratch: DiscoverScratch<B1::Time, Bk::RIn>,
    /// One key's discovered times inside `upper`, before they move into its state slot.
    ///
    /// A field rather than a local because `append` drains it and leaves its capacity behind, so the
    /// next key reuses it. Its counterpart — the times at or beyond `upper` — is a local, because
    /// `new_pending` takes that allocation whole and leaves nothing to reuse.
    moments: Vec<B1::Time>,

    /// One round's crossing, assembled across all active keys, and the corrections it draws.
    round: ReduceRound<Bk::RIn, Bk::ROut>,
    corrections: ReduceCorrections<Bk::ROut>,
    /// The state slot and moment each of the round's keys was assembled for, parallel to its `keys`.
    active: Vec<(usize, B1::Time)>,
    /// One key's accumulations, before `append` drains them into the round and leaves their capacity.
    in_accum: Vec<(u64, Bk::RIn)>,
    out_accum: Vec<(u64, Bk::ROut)>,
}

impl<'a, B1, B2, Bk> Retire<'a, B1, B2, Bk>
where
    B1: BatchReader,
    B2: BatchReader<Time = B1::Time>,
    Bk: ProxyReduceBackend<B1, B2>,
{
    /// A retire at the start of the key space, with empty scratch and no session yet opened.
    fn new(
        backend: &'a mut Bk,
        instance: ReduceInstance<'a, B1, B2>,
        upper: &'a Antichain<B1::Time>,
        tiles: Vec<(B1::Time, Option<usize>)>,
        pending: &'a BTreeMap<u64, Vec<B1::Time>>,
        pending_flat: &'a [(u64, B1::Time)],
    ) -> Self {
        let tile_deltas = (0..tiles.len()).map(|_| Vec::new()).collect();
        Retire {
            backend, instance, upper, tiles, pending, pending_flat,
            new_pending: BTreeMap::new(),
            tile_deltas,
            from: Some(0),
            window: ReduceWindow::default(),
            states: Vec::new(),
            discover_scratch: DiscoverScratch::new(),
            moments: Vec::new(),
            round: ReduceRound::default(),
            corrections: ReduceCorrections::default(),
            active: Vec::new(),
            in_accum: Vec::new(),
            out_accum: Vec::new(),
        }
    }

    /// Retire every window the backend offers, in ascending key-hash order, opening the output
    /// session on the first window that has work. Reports whether the session was opened.
    fn run(&mut self, tiles: &[Description<B1::Time>]) -> bool {
        let mut began = false;
        while self.from.is_some() {
            self.draw();
            // The backend should only report an empty window when it reports exhaustion, but skipping
            // one costs nothing, where terminating on one would abandon the keys beyond it.
            if self.window.keys.is_empty() { continue; }
            if !began {
                self.backend.begin(tiles);
                began = true;
            }
            let live = self.determine();
            self.apply(live);
            self.flush();
        }
        began
    }

    /// Draw the next window from the backend.
    fn draw(&mut self) {
        self.window.clear();
        let before = self.from;
        self.backend.advance(&self.instance, &mut self.from, self.pending_flat, &mut self.window);
        // Without progress the retire would never terminate, so this guards liveness as well as contract.
        debug_assert!(
            self.from.is_none() || self.from > before,
            "advance must either strictly increase `from` or report the keys exhausted",
        );
        debug_assert!(
            self.from.is_none() || !self.window.keys.is_empty(),
            "advance must draw a non-empty window unless it reports the keys exhausted",
        );
        super::debug_assert_sorted_bridge(&self.window.input, "advance (input)");
        super::debug_assert_sorted_bridge(&self.window.output, "advance (output)");
        debug_assert!(self.window.keys.windows(2).all(|w| w[0] < w[1]), "a window's keys must ascend");
        debug_assert!(self.window.seeds.windows(2).all(|w| w[0].0 <= w[1].0), "a window's seeds must be sorted by key hash");
        // A changed key outside `[before, from)` is either one an earlier window already retired, or
        // one a later window may yet report: both split a key's times across windows, which reconciles
        // it against an input it is not yet whole.
        debug_assert!(
            {
                let mut keys = self.window.keys.iter().copied()
                    .chain(self.window.input.iter().map(|r| r.0.0))
                    .chain(self.window.output.iter().map(|r| r.0.0))
                    .chain(self.window.seeds.iter().map(|s| s.0));
                keys.all(|k| before.is_none_or(|b| b <= k) && self.from.is_none_or(|f| k < f))
            },
            "advance must report a key hash entirely within the window that first mentions it",
        );
    }

    /// Phase 1 (determination): for every key in the window, discover its interesting times (times
    /// only — no accumulation) and stand up its per-moment replays. Returns how many `states` slots
    /// it filled, which is what [`apply`](Self::apply) then walks.
    ///
    /// Peak state is O(window presentation), bounded by the window `advance` already materialized.
    /// `states` is a long-lived buffer reloaded slot-by-slot (not cleared/rebuilt): a slot's `Vec`s
    /// and replays are allocated once and reused, so keys cost no per-key alloc/free.
    fn determine(&mut self) -> usize {
        let p_in = &self.window.input;
        let p_out = &self.window.output;
        let seeds = &self.window.seeds;

        // The times at or beyond `upper`, which `new_pending` takes whole; see the `moments` field.
        let mut withheld: Vec<B1::Time> = Vec::new();
        let mut live = 0usize;
        let (mut is, mut os, mut ns) = (0usize, 0usize, 0usize);
        for &key in &self.window.keys {
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

            self.moments.clear();
            withheld.clear();
            {
                let pending = self.pending.get(&key).map(|p| &p[..]).unwrap_or(&[]);
                let seed_times = seeds[n0..n1].iter().map(|(_, t)| t.clone());
                let out_times = (o0..o1).map(|o| p_out[o].1.clone());
                discover_times(
                    KeyView { p_in: &p_in[..], i0, i1, pending },
                    seed_times, out_times, self.upper,
                    &mut self.discover_scratch,
                    &mut self.moments, &mut withheld,
                );
            }
            if !withheld.is_empty() {
                self.new_pending.insert(key, std::mem::take(&mut withheld));
            }
            if self.moments.is_empty() {
                continue;
            }

            // Reload slot `live` in place (grow the buffer by one only when a window is wider than
            // any before). `append` moves the discovered moments in without copy or realloc.
            if live == self.states.len() {
                self.states.push(KeyState::empty());
            }
            let st = &mut self.states[live];
            st.key = key;
            st.cursor = 0;
            st.produced.clear();
            st.moments.clear();
            st.moments.append(&mut self.moments);
            st.meets.clear();
            st.meets.extend(st.moments.iter().cloned());
            for i in (1..st.meets.len()).rev() {
                let m = st.meets[i].clone();
                st.meets[i - 1].meet_assign(&m);
            }
            st.in_replay.load_iter((i0..i1).map(|i| (p_in[i].0.1, p_in[i].1.clone(), p_in[i].2.clone())), st.meets.first());
            st.out_replay.load_iter((o0..o1).map(|o| (p_out[o].0.1, p_out[o].1.clone(), p_out[o].2.clone())), st.meets.first());
            live += 1;
        }
        live
    }

    /// Phase 2 (application): walk all keys' moments in ROUNDS, accumulating output updates into
    /// `tile_deltas`.
    ///
    /// Each round assembles every active key's one-moment-deep input and current-output accumulations
    /// and crosses them in a SINGLE `reduce_corrections` — batching across keys (a key's own moments
    /// stay sequential, each seeing its earlier corrections via `produced`). This caps the backend
    /// call count at O(max moments over keys), not O(sum of moments), with peak materialization one
    /// moment deep per key. `produced` is meet-collapsed each round, exactly like the reference —
    /// bounded, not the O(times × values) delta history.
    fn apply(&mut self, live: usize) {
        for deltas in self.tile_deltas.iter_mut() { deltas.clear(); }

        loop {
            self.round.clear();
            self.active.clear();
            let mut advanced = false;
            for (si, st) in self.states[..live].iter_mut().enumerate() {
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

                self.in_accum.clear();
                for ((vid, et), d) in st.in_replay.buffer().iter() {
                    if et.less_equal(&t) {
                        self.in_accum.push((*vid, d.clone()));
                    }
                }
                crate::consolidation::consolidate(&mut self.in_accum);
                self.out_accum.clear();
                for ((vid, et), d) in st.out_replay.buffer().iter().chain(st.produced.iter()) {
                    if et.less_equal(&t) {
                        self.out_accum.push((*vid, d.clone()));
                    }
                }
                crate::consolidation::consolidate(&mut self.out_accum);

                if self.in_accum.is_empty() && self.out_accum.is_empty() {
                    continue;
                }
                self.round.keys.push(st.key);
                self.round.input.append(&mut self.in_accum);
                self.round.in_ends.push(self.round.input.len());
                self.round.output.append(&mut self.out_accum);
                self.round.out_ends.push(self.round.output.len());
                self.active.push((si, t));
            }
            // Terminate only when every key is EXHAUSTED — not merely when this round produced no
            // crossing. A round can be empty because every key's current moment is empty-gated
            // while keys still have later (non-empty) moments; breaking here would drop them.
            if !advanced {
                break;
            }
            if self.round.keys.is_empty() {
                continue;
            }

            self.corrections.clear();
            self.backend.reduce_corrections(&self.round, &mut self.corrections);
            debug_assert_eq!(self.corrections.ends.len(), self.round.keys.len(), "corrections must delimit one run per key");
            let mut cstart = 0usize;
            for (bi, (si, t)) in self.active.iter().enumerate() {
                let cend = self.corrections.ends[bi];
                if cstart != cend {
                    let idx = self.tiles.iter().rposition(|(h, _)| h.less_equal(t)).expect("no held capability <= active time");
                    for (vid, d) in &self.corrections.updates[cstart..cend] {
                        self.states[*si].produced.push(((*vid, t.clone()), d.clone()));
                        self.tile_deltas[idx].push(((self.states[*si].key, *vid), t.clone(), d.clone()));
                    }
                }
                cstart = cend;
            }
        }
    }

    /// Hand the window's output updates to the backend, one call per tile they land in.
    fn flush(&mut self) {
        for (held_index, deltas) in self.tile_deltas.iter_mut().enumerate() {
            if deltas.is_empty() {
                continue;
            }
            if let Some(tile) = self.tiles[held_index].1 {
                crate::consolidation::consolidate_updates(deltas);
                self.backend.emit(tile, &deltas[..]);
            }
        }
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

