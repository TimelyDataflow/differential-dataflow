//! The proxy reduce: all interesting-time logic in proxy space, value work by callback.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasherDefault, Hasher};

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};

use crate::difference::{Abelian, IsZero, Semigroup};
use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use crate::trace::chunk::int_proxy::ProxyChunk;
use crate::operators::reduce::ReduceTactic;

use super::history::{IdHistory, TimeHistory};

/// Identity hasher for `value_id`-keyed scratch maps: `value_id`s are already 64-bit content hashes,
/// so re-hashing them is waste. Reused across moments (cleared, not reallocated) to avoid a
/// per-moment map alloc/free.
#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline] fn write_u64(&mut self, i: u64) { self.0 = i; }
    #[inline] fn write(&mut self, _: &[u8]) { unreachable!("value_id keys are u64") }
    #[inline] fn finish(&self) -> u64 { self.0 }
}
type IdMap<R> = HashMap<u64, R, BuildHasherDefault<IdHasher>>;

/// The reduce backend: value semantics for a proxy-space reduction.
///
/// Protocol (per `retire`, driven by [`ProxyReduceTactic`]): one `seed_times` (over the
/// new input batches — the batch's `(key, time)` support and the changed keys), one
/// `present_input`, one `present_output`, ONE batched `reduce_many` call carrying every
/// `(key, interesting time)` moment of the retire — with indices referring to the input
/// presentation — then one `materialize` per output batch. Within
/// one retire the backend must be able to resolve every presented `key_hash` to its key
/// and every presented or minted `value_id` to its value.
pub trait ProxyReduceBackend<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output; the desired-vs-current delta needs negation.
    type ROut: Abelian + 'static;

    /// (read) The freshly arrived batches' `(key_hash, time)` support: one entry per
    /// novel record, sorted by `key_hash`. RAW — no merge with stored history, no
    /// consolidation, no value work (no `value_id`, no diffs). Also how the tactic learns
    /// the changed keys (their distinct `key_hash`es).
    ///
    /// Seeding interesting times from the batch's OWN times — never from a time-filter
    /// over the merged accumulation — is load-bearing for CORRECTNESS: legal compaction
    /// may advance a stored record onto a novel record's exact `(value, time)`,
    /// cancelling it out of the merged view while the batch still owes output changes at
    /// that time. The model pins this: seeds are `b.support ∪ pending` (`Model.lean`'s
    /// `seedSet`; `scenario1_cancels` is the compaction-cancellation case).
    ///
    /// Cheap by design: seeds need only key hashes and times. Interesting-time
    /// over-derivation is sound (a non-changing seed yields a zero delta), so the raw
    /// novel times — a superset of `b.support` — suffice, and the tactic reads only times
    /// from them. So this method does NOT hash values, assign `value_id`s, sort by value,
    /// or consolidate; a full value-present of the delta happens once, in `present_input`.
    fn seed_times(&self, novel: &[B1]) -> Vec<(u64, B1::Time)>;
    /// (read) Flatten the input history (`history` ∪ `novel`, the accumulated trace and
    /// the freshly arrived batches) into one sorted, consolidated proxy run, restricted to
    /// the sorted `keys` — the ACCUMULATION run (consolidation across history and novel
    /// is fine here: compaction is accumulation-preserving; only seeding must not read
    /// this view). The restriction is load-bearing: presenting only changed keys is
    /// what keeps small-delta recursion from re-reading the accumulated trace each round.
    fn present_input(&mut self, history: &[B1], novel: &[B1], keys: &[u64]) -> ProxyChunk<B1::Time, Self::RIn>;
    /// (read) As `present_input`, for the operator's own output trace. The `value_id`s
    /// here must agree, on equal output values, with the ids [`reduce`](Self::reduce)
    /// mints later in this retire — desired and current output cancel by id. Content
    /// hashing achieves this statelessly; an exact per-retire value→id map achieves it
    /// with no collision risk.
    fn present_output(&mut self, batches: &[B2], keys: &[u64]) -> ProxyChunk<B1::Time, Self::ROut>;
    /// (value callback) Apply the reduction to one key's accumulated input: `input` holds
    /// one entry per value with a nonzero accumulation — a representative record index
    /// into the current input presentation, and the accumulated diff. Never called with
    /// `input` empty. Returns the reduced output as `(value_id, diff)`, minting ids for
    /// produced values — by any scheme that agrees with
    /// [`present_output`](Self::present_output) on equal values within this retire — and
    /// recording `id → value` for `materialize`.
    fn reduce(&mut self, key_hash: u64, input: &[(usize, Self::RIn)]) -> Vec<(u64, Self::ROut)>;
    /// (value callback, batched) As [`reduce`](Self::reduce), for a run of brackets in
    /// one call: bracket `i` is the accumulated input `input[ends[i-1]..ends[i]]` (with
    /// `ends[-1] = 0`, every bracket non-empty) for `keys[i]` — and a key may appear
    /// several times, once per moment the tactic evaluates it at (the bracket is the
    /// unit, not the key). Returns the concatenated per-bracket outputs together with
    /// their own bracket ends, aligned with `keys`.
    ///
    /// This is the boundary's bulk crossing: the tactic calls this method once per
    /// retire, with every `(key, interesting time)` moment as a bracket — desired
    /// outputs depend only on input accumulations, so no time ordering constrains the
    /// batch — and a backend whose value logic crosses into interpreted or columnar
    /// execution (where per-call overhead dominates) does the whole retire's value work
    /// in one crossing. The default implementation loops [`reduce`](Self::reduce);
    /// backends with cheap native per-key logic need not override it.
    fn reduce_many(&mut self, keys: &[u64], ends: &[usize], input: &[(usize, Self::RIn)]) -> (Vec<(u64, Self::ROut)>, Vec<usize>) {
        let mut outs = Vec::new();
        let mut out_ends = Vec::with_capacity(keys.len());
        let mut start = 0;
        for (i, &key) in keys.iter().enumerate() {
            outs.extend(self.reduce(key, &input[start..ends[i]]));
            out_ends.push(outs.len());
            start = ends[i];
        }
        (outs, out_ends)
    }
    /// (egress) Seal proxy-space output records into a real output batch: resolve each
    /// record's `key_hash` and `value_id` to real data, order by the backend's own record
    /// order, and build the batch with the given description. `records` may be empty (the
    /// description still advances the output trace).
    fn materialize(&mut self, records: ProxyChunk<B1::Time, Self::ROut>, description: Description<B1::Time>) -> B2;
}

/// A proxy-space [`ReduceTactic`]: owns all interesting-time logic over
/// `(key_hash, value_id, time, diff)`, calling the backend only for value semantics.
///
/// Per changed key (keys touched by new input, plus keys carrying pending times) it
/// derives the active times in `[lower, upper)` — joins of input times, seeded by the
/// novel times — consolidates each `(key, active time)` moment's input by `value_id`
/// (presence), and hands ALL moments to the backend in one batched value callback per
/// retire ([`ProxyReduceBackend::reduce_many`]): desired outputs depend only on input
/// accumulations, so the batch needs no time ordering. Each moment's desired output is
/// then diffed against its current output (committed history plus deltas emitted at
/// earlier moments — the order-sensitive part, kept in proxy space) and the difference
/// emitted into the held-time bucket. Times at or beyond `upper` are pended, keyed by
/// the stable `key_hash`, so they survive retires without any reference to backend
/// state.
pub struct ProxyReduceTactic<T, Bk> {
    backend: Bk,
    /// Outstanding interesting `(key, time)` moments at or beyond the last interval,
    /// keyed by the stable key hash (a `BTreeMap` for deterministic iteration).
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
        // Only work if we hold a time we can actually ship in [.., upper).
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            return (Vec::new(), held.clone());
        }

        // Only keys touched by the new input delta or carrying pending times can change
        // output in [lower, upper); everything else is untouched. This restriction keeps
        // per-retire cost proportional to the delta, not the accumulation.
        let seeds = self.backend.seed_times(&input_batches);
        debug_assert!(seeds.windows(2).all(|w| w[0].0 <= w[1].0), "seed_times must be sorted by key_hash");
        let mut changed: BTreeSet<u64> = seeds.iter().map(|(k, _)| *k).collect();
        changed.extend(self.pending.keys().copied());
        if changed.is_empty() {
            // No input delta and no carried pending: no key's output can change, and
            // nothing remains to hold. Downgrade to the EMPTY frontier — returning `held`
            // would keep the capability forever and deadlock a recursive scope.
            self.pending.clear();
            return (Vec::new(), Antichain::new());
        }
        let changed: Vec<u64> = changed.into_iter().collect();

        // The two read-side presentations, restricted to the changed keys: the full input
        // history (accumulated ∪ new) and the output history. Sorted by (key, val, time).
        let p_in = self.backend.present_input(&source_batches, &input_batches, &changed);
        let p_out = self.backend.present_output(&output_batches, &changed);

        // One output-record bucket per held time; a delta at `t` routes to the largest
        // held time `<= t`.
        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let mut buckets: Vec<Vec<((u64, u64), B1::Time, Bk::ROut)>> = vec![Vec::new(); held_elems.len()];

        let mut new_pending: BTreeMap<u64, Vec<B1::Time>> = BTreeMap::new();

        // Per changed key, phase A: the time-replay loop of the cursor tactic
        // (`history_replay::compute`), ported to proxy space with the value callback
        // deferred — it discovers the interesting times in `[lower, upper)`, assembles
        // each one's consolidated input accumulation as a bracket, and pends times at or
        // beyond `upper`. Both presentations hold only changed keys, in hash order, so
        // the ranges advance monotonically. Keys are independent: their brackets share
        // the retire's single batched callback, and phase B below replays each key's
        // output side over its own (ascending) moments.
        let mut keys_v: Vec<u64> = Vec::new();
        let mut ends: Vec<usize> = Vec::new();
        let mut entries: Vec<(usize, Bk::RIn)> = Vec::new();
        struct KeyWork<T> {
            key: u64,
            o0: usize,
            o1: usize,
            /// The key's interesting in-interval moments, ascending, each with its
            /// bracket index (`None`: empty accumulation — the callback is only
            /// consulted for non-empty input, but current output may need retracting).
            moments: Vec<(T, Option<usize>)>,
        }
        let mut work: Vec<KeyWork<B1::Time>> = Vec::new();
        // Reused across keys (cleared each iteration) so per-key work doesn't reallocate.
        let mut rep: Vec<(u64, usize)> = Vec::new();
        let mut raw_moments: Vec<(B1::Time, (usize, usize))> = Vec::new();
        let (mut is, mut os, mut ns) = (0usize, 0usize, 0usize);
        for &key in &changed {
            while is < p_in.len() && p_in.key_hashes()[is] < key { is += 1; }
            let i0 = is;
            while is < p_in.len() && p_in.key_hashes()[is] == key { is += 1; }
            let i1 = is;
            while os < p_out.len() && p_out.key_hashes()[os] < key { os += 1; }
            let o0 = os;
            while os < p_out.len() && p_out.key_hashes()[os] == key { os += 1; }
            let o1 = os;
            while ns < seeds.len() && seeds[ns].0 < key { ns += 1; }
            let n0 = ns;
            while ns < seeds.len() && seeds[ns].0 == key { ns += 1; }
            let n1 = ns;

            // value_id → representative record index (the first of the vid's run; the
            // key's records are sorted by (value_id, time)).
            rep.clear();
            for i in i0..i1 {
                if rep.last().is_none_or(|(v, _)| *v != p_in.value_ids()[i]) {
                    rep.push((p_in.value_ids()[i], i));
                }
            }
            let pending = self.pending.get(&key).map(|p| &p[..]).unwrap_or(&[]);

            raw_moments.clear();
            let mut pended: Vec<B1::Time> = Vec::new();
            let seed_times = seeds[n0..n1].iter().map(|(_, t)| t.clone());
            discover_and_accumulate(
                &p_in, i0, i1, seed_times, &rep, pending, upper,
                &mut raw_moments, &mut entries, &mut pended,
            );
            if !pended.is_empty() {
                new_pending.insert(key, pended);
            }
            if raw_moments.is_empty() {
                continue;
            }
            let moments = raw_moments
                .drain(..)
                .map(|(t, (lo, hi))| {
                    if lo < hi {
                        keys_v.push(key);
                        ends.push(hi);
                        (t, Some(keys_v.len() - 1))
                    } else {
                        (t, None)
                    }
                })
                .collect();
            work.push(KeyWork { key, o0, o1, moments });
        }

        // The one crossing for this retire.
        let (outs, out_ends) = if keys_v.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            self.backend.reduce_many(&keys_v, &ends, &entries)
        };

        // Phase B, per key: replay the output side over the key's moments in ascending
        // time order — `Ord` extends the partial order, so earlier deltas are always
        // emitted before a later moment reads them — with the same meet-advancement
        // keeping the output buffer and this pass's deltas consolidated.
        // Reused across keys: `IdHistory::load` clears+refills (retaining capacity), and `meets`/
        // `emitted` are cleared each key — so per-key replay doesn't reallocate.
        let mut meets: Vec<B1::Time> = Vec::new();
        let mut out_replay = IdHistory::new();
        let mut emitted: Vec<((u64, B1::Time), Bk::ROut)> = Vec::new();
        // Reused `value_id → diff` accumulator, cleared each moment (retains capacity — no per-moment
        // map alloc/free).
        let mut acc: IdMap<Bk::ROut> = IdMap::default();
        for w in work {
            meets.clear();
            meets.extend(w.moments.iter().map(|(t, _)| t.clone()));
            for i in (1..meets.len()).rev() {
                let m = meets[i].clone();
                meets[i - 1].meet_assign(&m);
            }
            out_replay.load(
                (w.o0..w.o1).map(|o| (p_out.value_ids()[o], p_out.times()[o].clone(), p_out.diffs()[o].clone())),
                meets.first(),
            );
            // Deltas emitted for this key earlier in this pass; part of "current output".
            emitted.clear();
            for (j, (t, bracket)) in w.moments.iter().enumerate() {
                out_replay.step_through(t);
                out_replay.advance_buffer_by(&meets[j]);
                for ((_, et), _) in emitted.iter_mut() {
                    *et = et.join(&meets[j]);
                }
                crate::consolidation::consolidate(&mut emitted);

                // Current output at `t`: committed history plus this pass's deltas.
                acc.clear();
                for ((vid, et), d) in out_replay.buffer().iter().chain(emitted.iter()) {
                    if et.less_equal(t) {
                        match acc.entry(*vid) {
                            std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().plus_equals(d),
                            std::collections::hash_map::Entry::Vacant(e) => { e.insert(d.clone()); }
                        }
                    }
                }
                acc.retain(|_, d| !d.is_zero());

                // delta = desired − current, in proxy space (negate current, add desired).
                for d in acc.values_mut() {
                    d.negate();
                }
                if let Some(b) = *bracket {
                    let lo = if b == 0 { 0 } else { out_ends[b - 1] };
                    for (vid, d) in outs[lo..out_ends[b]].iter().cloned() {
                        match acc.entry(vid) {
                            std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().plus_equals(&d),
                            std::collections::hash_map::Entry::Vacant(e) => { e.insert(d); }
                        }
                    }
                }
                acc.retain(|_, d| !d.is_zero());
                if acc.is_empty() {
                    continue;
                }

                let idx = held_elems.iter().rposition(|h| h.less_equal(t)).expect("no held capability <= active time");
                for (vid, d) in acc.drain() {
                    emitted.push(((vid, t.clone()), d.clone()));
                    buckets[idx].push(((w.key, vid), t.clone(), d));
                }
            }
        }

        self.pending = new_pending;

        // Build one output batch per held time, tiling [lower, upper): batch `i`'s upper
        // is `upper` joined with the later held times, so the descriptions abut.
        let mut produced = Vec::new();
        let mut out_lower = lower.clone();
        for (index, rows) in buckets.into_iter().enumerate() {
            let mut out_upper = upper.clone();
            for t in &held_elems[index + 1..] {
                out_upper.insert(t.clone());
            }
            if out_upper != out_lower {
                let description = Description::new(out_lower.clone(), out_upper.clone(), Antichain::from_elem(<B1::Time as Timestamp>::minimum()));
                let (mut ks, mut vs) = (Vec::with_capacity(rows.len()), Vec::with_capacity(rows.len()));
                let (mut ts, mut ds) = (Vec::with_capacity(rows.len()), Vec::with_capacity(rows.len()));
                for ((k, v), t, d) in rows {
                    ks.push(k);
                    vs.push(v);
                    ts.push(t);
                    ds.push(d);
                }
                let (records, _reps) = ProxyChunk::from_unsorted(ks, vs, ts, ds);
                let batch = self.backend.materialize(records, description);
                produced.push((held_elems[index].clone(), batch));
                out_lower = out_upper;
            }
        }

        // New held frontier: the lower envelope of all carried pending times.
        let mut frontier = Antichain::new();
        for times in self.pending.values() {
            for t in times {
                frontier.insert_ref(t);
            }
        }
        (produced, frontier)
    }
}

/// Phase A of one key's retire: the time-replay loop of the cursor tactic
/// (`history_replay::compute`), ported to proxy space with the value callback deferred.
///
/// It replays the key's NOVEL batch times (`seed_times` — the batch's own `(key, time)`
/// support, raw, apart from the stored history) together with the carried `pending`
/// times, in ascending time order, discovering the interesting times: those carrying
/// novel or pending updates, and joins thereof (synthesized as replay proceeds). The
/// merged input presentation (`p_in[i0..i1]`, which already contains the novel records)
/// is replayed alongside purely for ACCUMULATION: at each interesting time within the
/// interval its buffer supplies the consolidated input, appended to `entries` as a
/// bracket (recorded in `moments` with its entry range; an empty range marks a moment
/// whose accumulation vanished but whose current output may need retracting). Times at
/// or beyond `upper` are `pended`.
///
/// Seeding from the batch's own times — never from a time-filter over the merged
/// presentation — is the formal model's `seedSet = b.support ∪ pending`: compaction can
/// legally advance a stored record onto a novel one and cancel it out of the MERGED
/// view, but it still owes changes at that time (`scenario1_cancels`). The seed times are
/// raw (a superset of `b.support`); the spurious extras are sound over-derivation.
///
/// The replay buffers are repeatedly advanced by the meet of the times still to come and
/// consolidated — the collapse that keeps a key with many distinct times linear rather
/// than quadratic, exactly as in the cursor tactic.
#[allow(clippy::too_many_arguments)]
fn discover_and_accumulate<T, RIn>(
    p_in: &ProxyChunk<T, RIn>,
    i0: usize,
    i1: usize,
    seed_times: impl Iterator<Item = T>,
    rep: &[(u64, usize)],
    pending: &[T],
    upper: &Antichain<T>,
    moments: &mut Vec<(T, (usize, usize))>,
    entries: &mut Vec<(usize, RIn)>,
    pended: &mut Vec<T>,
) where
    T: Timestamp + Lattice,
    RIn: Semigroup + Clone,
{
    // The batch's times seed interestingness and generate the synthetic joins; the batch
    // carries no values here — accumulation comes wholly from the merged run below.
    let mut batch_replay = TimeHistory::new();
    batch_replay.load(seed_times, None);

    // Suffix meets of the carried pending times (ascending, so meets accumulate from the end).
    let mut meets: Vec<T> = pending.to_vec();
    for i in (1..meets.len()).rev() {
        let m = meets[i].clone();
        meets[i - 1].meet_assign(&m);
    }

    let mut meet: Option<T> = None;
    update_meet(&mut meet, meets.first());
    update_meet(&mut meet, batch_replay.meet());

    // The merged (history ⊎ novel, consolidated) run: the accumulation source.
    let mut input_replay = IdHistory::new();
    input_replay.load(
        (i0..i1).map(|i| (p_in.value_ids()[i], p_in.times()[i].clone(), p_in.diffs()[i].clone())),
        meet.as_ref(),
    );

    let mut synth: Vec<T> = Vec::new(); // sorted descending: pop ascending
    let mut times_current: Vec<T> = Vec::new();
    let mut temporary: Vec<T> = Vec::new();
    let mut times_slice = pending;
    let mut meets_slice = &meets[..];

    while let Some(next_time) = [batch_replay.time(), times_slice.first(), input_replay.time(), synth.last()]
        .into_iter()
        .flatten()
        .min()
        .cloned()
    {
        input_replay.step_while_time_is(&next_time);
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
                if let Some(m) = meet.as_ref() {
                    input_replay.advance_buffer_by(m);
                }
                // Assemble the accumulation at `next_time` from the merged run's buffer
                // (the batch's records are already in it — the batch buffer must not
                // contribute diffs twice); buffered times beyond it contribute joins.
                let mut acc: Vec<(u64, RIn)> = Vec::new();
                for ((vid, t), d) in input_replay.buffer().iter() {
                    if t.less_equal(&next_time) {
                        acc.push((*vid, d.clone()));
                    } else {
                        temporary.push(next_time.join(t));
                    }
                }
                crate::consolidation::consolidate(&mut acc);
                let lo = entries.len();
                for (vid, d) in acc {
                    let r = rep[rep.binary_search_by_key(&vid, |x| x.0).expect("vid presented")].1;
                    entries.push((r, d));
                }
                moments.push((next_time.clone(), (lo, entries.len())));
            }
            // Synthetic interesting times: joins with the remaining batch times and the
            // times seen so far.
            temporary.extend(batch_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            temporary.extend(times_current.iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            sort_dedup(&mut temporary);
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

        // Track the meet of every remaining source of times, and keep `times_current`
        // advanced by it (the same collapse as the buffers).
        meet = None;
        update_meet(&mut meet, batch_replay.meet());
        update_meet(&mut meet, input_replay.meet());
        for t in synth.iter() {
            update_meet(&mut meet, Some(t));
        }
        update_meet(&mut meet, meets_slice.first());
        if let Some(m) = meet.as_ref() {
            for t in times_current.iter_mut() {
                *t = t.join(m);
            }
        }
        sort_dedup(&mut times_current);
    }
    sort_dedup(pended);
}

fn sort_dedup<T: Ord>(list: &mut Vec<T>) {
    list.sort();
    list.dedup();
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
