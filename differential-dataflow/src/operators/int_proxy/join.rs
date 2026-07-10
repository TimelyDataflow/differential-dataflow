//! The proxy join: equi-join by `key_hash`, value work by matched index lists.

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::BatchReader;
use super::ProxyBridge;
use crate::operators::join::{Fresh, JoinTactic};

use super::history::IdHistory;

/// The work-unit context the tactic shares with the backend: the two input batch lists, and
/// a `lower` frontier below which loaded times may be advanced and consolidated.
///
/// The batches are live for the whole unit, so a backend may resolve `value_id`s against them
/// (rather than copying data out at presentation — a later refinement). `lower` is the unit's
/// capability time: every output is produced under that capability, so no output time falls
/// below it, and advancing loaded times by it leaves the output unchanged while collapsing the
/// accumulated tail.
pub struct JoinInstance<'a, B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// The first input's batches.
    pub batches0: &'a [B0],
    /// The second input's batches.
    pub batches1: &'a [B1],
    /// The compaction frontier for loading (the unit's capability time).
    pub lower: AntichainRef<'a, B0::Time>,
}

/// A type that can interpret and retire pairs of batches, joined by key hashes.
///
/// The protocol is one call to each `present*` method, followed by at most one
/// call to `cross`. The `value_id`s named in `cross` are exactly those the backend
/// supplied in the `present*` bridges (the harness returns them unchanged), so the
/// backend resolves them to real data by whatever scheme it minted them under.
pub trait ProxyJoinBackend<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// Diff type presented for the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type presented for the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the tactic.
    type ROut: Semigroup;
    /// The output container built from matched value ids.
    type Output;

    /// Prepare a proxy bridge from the instance's first input, optionally key-hash restricted.
    /// The returned bridge **must be sorted and consolidated** by `((key_hash, value_id), time)`
    /// — the tactic merge-joins the two runs on `key_hash`, so an unsorted or unconsolidated run
    /// silently drops matches. Advancing loaded times by `instance.lower` first is optional; the
    /// sort/consolidate is not.
    fn present0(&mut self, instance: &JoinInstance<'_, B0, B1>, filter: Option<&[u64]>) -> ProxyBridge<B0::Time, Self::R0>;
    /// Prepare a proxy bridge from the instance's second input, optionally key-hash restricted.
    /// **Must be sorted and consolidated** by `((key_hash, value_id), time)` (as `present0`);
    /// advancing by `instance.lower` is optional.
    fn present1(&mut self, instance: &JoinInstance<'_, B0, B1>, filter: Option<&[u64]>) -> ProxyBridge<B0::Time, Self::R1>;
    /// For matched pairs from the most recent bridges — each identified by its full
    /// `(key_hash, value_id)` bridge key — with times and diffs, produce an output. A bare
    /// `value_id` is only meaningful within a `key_hash` (it is an intra-hash identifier), so
    /// the pair is what pins a record; the backend resolves each against its presentation.
    fn cross(&mut self, instance: &JoinInstance<'_, B0, B1>, left: &[(u64, u64)], right: &[(u64, u64)], times: Vec<B0::Time>, diffs: Vec<Self::ROut>) -> Self::Output;
}

/// A proxy-space [`JoinTactic`]: matches records of the two presented runs by `key_hash`,
/// cross-products matched records with DD-computed times (lattice join) and diffs
/// (product), and hands the backend the matched value ids for the value work.
///
/// The tactic is pure data-to-data: `prep` maps two batch lists to output containers, holding no
/// capabilities and no fuel — the driver owns the queues, pairs each unit with its shipping
/// capability, and meters fuel. `prep` chunks the unit's output at `key_hash` boundaries (a
/// container per ~[`JOIN_CHUNK`] matches), so a large unit ships in bounded pieces the driver meters
/// fuel against. Bounding the *presentation* of a single huge unit (both sides in full) is F7's
/// hash-native windowing; see DESIGN.md F8.
pub struct ProxyJoinTactic<B0, B1, Bk> {
    backend: Bk,
    _marker: std::marker::PhantomData<(B0, B1)>,
}

impl<B0, B1, Bk> ProxyJoinTactic<B0, B1, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyJoinTactic { backend, _marker: std::marker::PhantomData }
    }
}

impl<B0, B1, Bk> JoinTactic<B0, B1, Bk::Output> for ProxyJoinTactic<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
    Bk::Output: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(join_prep(&mut self.backend, input0, input1, fresh, meet).into_iter())
    }
}

/// Target matched-pair count per emitted container. The merge-join accumulates matches and ships a
/// container once the batch reaches this size — so the unit's output leaves in bounded pieces
/// (downstream backpressure and the driver's fuel granularity) rather than one container, and
/// delta-sized units keep a small working set. The flush fires wherever the batch fills, **including
/// mid-key inside a high-fanout key's cross-product wave**, so no single key materializes more than a
/// chunk (a match is an independent output record, so splitting anywhere is sound). Tunable.
/// (Bounding the *presentation* of a single huge unit — both sides in full — is a separate matter,
/// gated on hash-native storage; see DESIGN.md F8/F7.)
const JOIN_CHUNK: usize = 1 << 18;

/// Prepare one join unit: present both sides, merge-match key runs over the sorted hashes, and
/// cross-product matched records into a sequence of output containers — one per ~[`JOIN_CHUNK`]
/// matches, flushed wherever the batch fills (including mid-key; parity: their concatenation is the
/// single-container output). `meet` is a lower bound on the fresh side's times, so the accumulated
/// side loads compacted (see [`JoinInstance`]); every output is produced under the capability at
/// `meet`, so advancing loaded times by it leaves the output unchanged.
fn join_prep<B0, B1, Bk>(backend: &mut Bk, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Vec<Bk::Output>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
{
    let lower = Antichain::from_elem(meet);
    let instance = JoinInstance { batches0: &input0, batches1: &input1, lower: lower.borrow() };

    // Prepare work using the keys of the just-received side.
    // FIXME: Use the smaller of the two instead.
    let (p0, p1) = match fresh {
        Fresh::Input0 => {
            let p0 = backend.present0(&instance, None);
            if p0.is_empty() { return Vec::new(); }
            let mut keys: Vec<u64> = p0.iter().map(|r| r.0.0).collect();
            keys.dedup();
            let p1 = backend.present1(&instance, Some(&keys));
            (p0, p1)
        }
        Fresh::Input1 => {
            let p1 = backend.present1(&instance, None);
            if p1.is_empty() { return Vec::new(); }
            let mut keys: Vec<u64> = p1.iter().map(|r| r.0.0).collect();
            keys.dedup();
            let p0 = backend.present0(&instance, Some(&keys));
            (p0, p1)
        }
    };
    if p0.is_empty() || p1.is_empty() { return Vec::new(); }
    super::debug_assert_sorted_bridge(&p0, "present0");
    super::debug_assert_sorted_bridge(&p1, "present1");

    // Merge-join the two presented runs on `key_hash` (both sorted) and cross matched pairs. `flush`
    // ships the accumulated batch as a container and resets the buffers; `join_key` calls it at each
    // `JOIN_CHUNK` boundary — including *within* a high-fanout key's wave — so no single key
    // materializes more than a chunk of its cross product. The closure scope releases its
    // `backend`/`out` borrow before `out` is returned.
    let mut out: Vec<Bk::Output> = Vec::new();
    {
        let mut flush = |li: &mut Vec<(u64, u64)>, ri: &mut Vec<(u64, u64)>, ot: &mut Vec<B0::Time>, od: &mut Vec<Bk::ROut>| {
            out.push(backend.cross(&instance, li.as_slice(), ri.as_slice(), std::mem::take(ot), std::mem::take(od)));
            li.clear();
            ri.clear();
        };

        let (mut li, mut ri) = (Vec::new(), Vec::new());
        let (mut ot, mut od) = (Vec::new(), Vec::new());
        // Per-key replay histories, held across the unit and reloaded per key (only the >=16/>=16
        // replay-wave path touches them), so a high-fanout join pays no per-key `IdHistory` alloc.
        let mut h0 = IdHistory::new();
        let mut h1 = IdHistory::new();
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
                join_key(ki, &p0, i..e0, &p1, j..e1, &mut h0, &mut h1, &mut li, &mut ri, &mut ot, &mut od, &mut flush);
                i = e0;
                j = e1;
            }
        }
        if !li.is_empty() {
            flush(&mut li, &mut ri, &mut ot, &mut od);
        }
    }
    out
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, this performs a simple cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
///
/// All records here share the key hash `kh` (the merge-join matched on it), so matches are
/// reported to `li`/`ri` as their full `(kh, value_id)` bridge keys — equal keys denote equal
/// records — which the backend resolves; the harness never needs a representative.
#[allow(clippy::too_many_arguments)]
fn join_key<T, R0, R1, RO, F>(
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
    flush: &mut F,
) where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
    F: FnMut(&mut Vec<(u64, u64)>, &mut Vec<(u64, u64)>, &mut Vec<T>, &mut Vec<RO>),
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

    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                li.push((kh, v0));
                ri.push((kh, *v1));
                ot.push(t0.join(t1));
                od.push(d0.clone().multiply(d1));
                if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
            }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                li.push((kh, *v0));
                ri.push((kh, v1));
                ot.push(t0.join(t1));
                od.push(d0.clone().multiply(d1));
                if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            li.push((kh, v0));
            ri.push((kh, *v1));
            ot.push(t0.join(t1));
            od.push(d0.clone().multiply(d1));
            if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            li.push((kh, *v0));
            ri.push((kh, v1));
            ot.push(t0.join(t1));
            od.push(d0.clone().multiply(d1));
            if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
        }
        h1.step();
    }
}
