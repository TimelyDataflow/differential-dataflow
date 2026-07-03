//! The proxy reduce: all interesting-time logic in proxy space, value work by callback.

use std::collections::{BTreeMap, BTreeSet};

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::{Abelian, IsZero, Semigroup};
use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use crate::trace::chunk::int_proxy::ProxyChunk;
use crate::operators::reduce::ReduceTactic;

/// The reduce backend: value semantics for a proxy-space reduction.
///
/// Protocol (per `retire`, driven by [`ProxyReduceTactic`]): one `key_hashes` (over the
/// new input batches), one `present_input`, one `present_output`, one `reduce_many` call
/// per distinct interesting time — batching every key active at that time, with indices
/// referring to the input presentation — then one `materialize` per output batch. Within
/// one retire the backend must be able to resolve every presented `key_hash` to its key
/// and every presented or minted `value_id` to its value.
pub trait ProxyReduceBackend<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output; the desired-vs-current delta needs negation.
    type ROut: Abelian + 'static;

    /// The `key_hash`es appearing in `batches`, sorted and deduplicated. Used by the
    /// tactic to find the keys whose output can change this interval.
    fn key_hashes(&self, batches: &[B1]) -> Vec<u64>;
    /// (read) Flatten the input history (`history` ∪ `novel`, the accumulated trace and
    /// the freshly arrived batches) into one sorted, consolidated proxy run, restricted to
    /// the sorted `keys`. The restriction is load-bearing: presenting only changed keys is
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
    /// (value callback, batched) As [`reduce`](Self::reduce), for a run of keys in one
    /// call: `keys[i]`'s accumulated input is `input[ends[i-1]..ends[i]]` (with
    /// `ends[-1] = 0`), every bracket non-empty. Returns the concatenated per-key outputs
    /// together with their own bracket ends, aligned with `keys`.
    ///
    /// This is the boundary's bulk crossing: the tactic calls only this method, batching
    /// all keys that share an interesting time, so a backend whose value logic crosses
    /// into interpreted or columnar execution (where per-call overhead dominates) can do
    /// a whole wave's work per crossing. The default implementation loops
    /// [`reduce`](Self::reduce); backends with cheap native per-key logic need not
    /// override it.
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
/// novel times — and groups the work into *waves*: at each distinct time, every active
/// key's input is consolidated by `value_id` (presence) and the whole wave is handed to
/// the backend in one batched value callback ([`ProxyReduceBackend::reduce_many`]) — one
/// crossing into the backend's value logic per wave. Each key's desired output is then
/// diffed against its current output (committed history plus deltas emitted this pass)
/// and the difference emitted into the held-time bucket. Times at or beyond `upper` are
/// pended, keyed by the stable `key_hash`, so they survive retires without any reference
/// to backend state.
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

/// The active times for an interval `[lower, upper)`: joins of input times (`novel` +
/// `prior`) reachable from the `novel` seeds that are not at or beyond `upper`; those at
/// or beyond are `pended` for a future interval. Over-derivation is sound (a non-changing
/// time yields a zero delta).
fn active_times<T: Lattice + Ord + Clone>(
    upper: AntichainRef<T>,
    prior: &[T],
    novel: &[T],
    active: &mut BTreeSet<T>,
    pended: &mut BTreeSet<T>,
) {
    let mut todo: BTreeSet<T> = novel.iter().cloned().collect();
    while let Some(next) = todo.pop_first() {
        if upper.less_equal(&next) {
            pended.insert(next);
        } else if active.insert(next.clone()) {
            for t in novel.iter().chain(prior.iter()) {
                let join = next.join(t);
                if !active.contains(&join) {
                    todo.insert(join);
                }
            }
        }
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
        let mut changed: BTreeSet<u64> = self.backend.key_hashes(&input_batches).into_iter().collect();
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

        // Pass 1: march through the changed keys — both presentations hold only changed
        // keys, in hash order, so the ranges advance monotonically — derive each key's
        // active times, and group the work into WAVES: all keys active at a time are
        // handled by one batched value callback, so a whole wave costs one crossing into
        // the backend's value logic.
        struct Plan {
            key: u64,
            i0: usize,
            i1: usize,
            o0: usize,
            o1: usize,
        }
        let mut plans: Vec<Plan> = Vec::new();
        let mut waves: BTreeMap<B1::Time, Vec<usize>> = BTreeMap::new();
        let (mut is, mut os) = (0usize, 0usize);
        for &key in &changed {
            while is < p_in.len() && p_in.key_hashes()[is] < key { is += 1; }
            let i0 = is;
            while is < p_in.len() && p_in.key_hashes()[is] == key { is += 1; }
            let i1 = is;
            while os < p_out.len() && p_out.key_hashes()[os] < key { os += 1; }
            let o0 = os;
            while os < p_out.len() && p_out.key_hashes()[os] == key { os += 1; }
            let o1 = os;

            // Seeds: input times in [lower, upper) plus carried pending times; join
            // partners: input times before `lower`.
            let mut novel: Vec<B1::Time> = p_in.times()[i0..i1]
                .iter()
                .filter(|t| lower.less_equal(t) && !upper.less_equal(t))
                .cloned()
                .collect();
            if let Some(p) = self.pending.get(&key) {
                novel.extend(p.iter().cloned());
            }
            novel.sort();
            novel.dedup();
            let prior: Vec<B1::Time> = p_in.times()[i0..i1]
                .iter()
                .filter(|t| !lower.less_equal(t))
                .cloned()
                .collect();

            let mut active = BTreeSet::new();
            let mut pended = BTreeSet::new();
            active_times(upper.borrow(), &prior, &novel, &mut active, &mut pended);
            if !pended.is_empty() {
                new_pending.entry(key).or_default().extend(pended);
            }
            if active.is_empty() {
                continue;
            }
            let index = plans.len();
            plans.push(Plan { key, i0, i1, o0, o1 });
            for t in active {
                waves.entry(t).or_default().push(index);
            }
        }

        // Deltas emitted per key earlier in this pass; part of "current output".
        let mut emitted: Vec<Vec<(u64, B1::Time, Bk::ROut)>> = (0..plans.len()).map(|_| Vec::new()).collect();

        // Pass 2: play the waves in ascending time order — `Ord` extends the partial
        // order, so a key's earlier deltas are always emitted before a later time
        // consults them. Each wave makes at most one (batched) value callback.
        for (t, members) in waves {
            // Assemble the batch: per member, the input accumulation at `t` consolidated
            // by value id (each key's records are sorted by (value_id, time), so each id
            // is one contiguous run) and the current output at `t` (committed history
            // plus this pass's deltas). Members with empty accumulation skip the callback
            // — the row reduce contract consults logic only for non-empty input — and go
            // straight to retracting `cur`.
            let mut keys_v: Vec<u64> = Vec::new();
            let mut ends: Vec<usize> = Vec::new();
            let mut entries: Vec<(usize, Bk::RIn)> = Vec::new();
            let mut batched: Vec<(usize, BTreeMap<u64, Bk::ROut>)> = Vec::new();
            let mut retract_only: Vec<(usize, BTreeMap<u64, Bk::ROut>)> = Vec::new();
            for &pi in &members {
                let plan = &plans[pi];
                let mut acc: Vec<(usize, Bk::RIn)> = Vec::new();
                let mut a = plan.i0;
                while a < plan.i1 {
                    let vid = p_in.value_ids()[a];
                    let rep = a;
                    let mut net: Option<Bk::RIn> = None;
                    while a < plan.i1 && p_in.value_ids()[a] == vid {
                        if p_in.times()[a].less_equal(&t) {
                            match net.as_mut() {
                                Some(n) => n.plus_equals(&p_in.diffs()[a]),
                                None => net = Some(p_in.diffs()[a].clone()),
                            }
                        }
                        a += 1;
                    }
                    if let Some(n) = net {
                        if !n.is_zero() {
                            acc.push((rep, n));
                        }
                    }
                }

                let mut cur: BTreeMap<u64, Bk::ROut> = BTreeMap::new();
                for o in plan.o0..plan.o1 {
                    if p_out.times()[o].less_equal(&t) {
                        match cur.entry(p_out.value_ids()[o]) {
                            std::collections::btree_map::Entry::Occupied(mut e) => e.get_mut().plus_equals(&p_out.diffs()[o]),
                            std::collections::btree_map::Entry::Vacant(e) => { e.insert(p_out.diffs()[o].clone()); }
                        }
                    }
                }
                for (vid, et, d) in emitted[pi].iter() {
                    if et.less_equal(&t) {
                        match cur.entry(*vid) {
                            std::collections::btree_map::Entry::Occupied(mut e) => e.get_mut().plus_equals(d),
                            std::collections::btree_map::Entry::Vacant(e) => { e.insert(d.clone()); }
                        }
                    }
                }
                cur.retain(|_, d| !d.is_zero());

                if acc.is_empty() && cur.is_empty() {
                    continue;
                }
                if acc.is_empty() {
                    retract_only.push((pi, cur));
                } else {
                    keys_v.push(plan.key);
                    entries.extend(acc);
                    ends.push(entries.len());
                    batched.push((pi, cur));
                }
            }

            // One crossing for the whole wave.
            let (outs, out_ends) = if keys_v.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                self.backend.reduce_many(&keys_v, &ends, &entries)
            };

            // delta = desired − current, in proxy space, routed to the held-time bucket.
            let mut start = 0;
            let desired_of = |lo: usize, hi: usize| outs[lo..hi].iter().cloned();
            for (m, (pi, cur)) in batched.into_iter().enumerate() {
                let end = out_ends[m];
                let mut delta: BTreeMap<u64, Bk::ROut> = cur;
                for d in delta.values_mut() {
                    d.negate();
                }
                for (vid, d) in desired_of(start, end) {
                    match delta.entry(vid) {
                        std::collections::btree_map::Entry::Occupied(mut e) => e.get_mut().plus_equals(&d),
                        std::collections::btree_map::Entry::Vacant(e) => { e.insert(d); }
                    }
                }
                start = end;
                delta.retain(|_, d| !d.is_zero());
                if delta.is_empty() {
                    continue;
                }
                let idx = held_elems.iter().rposition(|h| h.less_equal(&t)).expect("no held capability <= active time");
                for (vid, d) in delta {
                    emitted[pi].push((vid, t.clone(), d.clone()));
                    buckets[idx].push(((plans[pi].key, vid), t.clone(), d));
                }
            }
            for (pi, cur) in retract_only {
                let mut delta = cur;
                for d in delta.values_mut() {
                    d.negate();
                }
                delta.retain(|_, d| !d.is_zero());
                if delta.is_empty() {
                    continue;
                }
                let idx = held_elems.iter().rposition(|h| h.less_equal(&t)).expect("no held capability <= active time");
                for (vid, d) in delta {
                    emitted[pi].push((vid, t.clone(), d.clone()));
                    buckets[idx].push(((plans[pi].key, vid), t.clone(), d));
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
