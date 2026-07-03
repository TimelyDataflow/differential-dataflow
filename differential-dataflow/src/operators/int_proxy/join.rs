//! The proxy join: equi-match by `key_hash`, value work by matched index lists.

use std::collections::VecDeque;

use timely::ContainerBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputBuilderSession;

use timely::progress::Timestamp;

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::BatchReader;
use crate::trace::chunk::int_proxy::ProxyChunk;
use crate::operators::join::{EffortBuilder, Fresh, JoinTactic};

use super::history::IdHistory;

/// The join backend: value semantics for a proxy-space equijoin.
///
/// Protocol (per deferred work unit, driven by [`ProxyJoinTactic`]): one `present0`, one
/// `present1`, then at most one `cross` whose indices refer to those two presentations.
/// The backend must keep each presentation's alignment (run index → real record) until
/// the `cross` call.
pub trait ProxyJoinBackend<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// Diff type presented for the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type presented for the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the tactic.
    type ROut: Semigroup;
    /// The output container built from matched index lists.
    type Output;

    /// (read) Flatten `batches` into one sorted, consolidated proxy run. When `filter` is
    /// set (sorted key hashes), restrict to records of those keys: the tactic presents a
    /// unit's fresh side unfiltered and passes its key set when presenting the
    /// accumulated side, so per-unit work tracks the fresh batch, not the accumulated
    /// trace — the join analogue of reduce's changed-key restriction, and just as
    /// load-bearing (without it, a small update against a large trace re-reads the
    /// trace).
    fn present0(&mut self, batches: &[B0], filter: Option<&[u64]>) -> ProxyChunk<B0::Time, Self::R0>;
    /// (read) As `present0`, for the second input.
    fn present1(&mut self, batches: &[B1], filter: Option<&[u64]>) -> ProxyChunk<B0::Time, Self::R1>;
    /// (value work) The projection. `left[i]`/`right[i]` index the current presentations;
    /// each pair matched on `key_hash`. `times[i]`/`diffs[i]` are the DD-computed lattice
    /// join of the pair's times and product of its diffs, to carry through per record.
    fn cross(&mut self, left: &[usize], right: &[usize], times: Vec<B0::Time>, diffs: Vec<Self::ROut>) -> Self::Output;
}

/// A proxy-space [`JoinTactic`]: matches records of the two presented runs by `key_hash`,
/// cross-products matched records with DD-computed times (lattice join) and diffs
/// (product), and hands the backend the matched index lists for the value work.
///
/// Work units are queued by arrival direction (as in the cursor tactic) and currently
/// drained eagerly; fuel budgeting is a later refinement.
pub struct ProxyJoinTactic<B0: BatchReader, B1: BatchReader<Time = B0::Time>, Bk> {
    backend: Bk,
    todo0: VecDeque<JoinUnit<B0, B1>>,
    todo1: VecDeque<JoinUnit<B0, B1>>,
}

/// A deferred bilinear join unit: a fresh batch list against the accumulated other side.
struct JoinUnit<B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    left: Vec<B0>,
    right: Vec<B1>,
    /// Which side carried the fresh batch; the other side is presented restricted to the
    /// fresh side's keys.
    fresh: Fresh,
    capability: Capability<B0::Time>,
}

impl<B0: BatchReader, B1: BatchReader<Time = B0::Time>, Bk> ProxyJoinTactic<B0, B1, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyJoinTactic { backend, todo0: VecDeque::new(), todo1: VecDeque::new() }
    }
}

impl<B0, B1, Bk, CB> JoinTactic<B0, B1, CB> for ProxyJoinTactic<B0, B1, Bk>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
    CB: ContainerBuilder<Container = Bk::Output>,
{
    fn defer(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, capability: Capability<B0::Time>) {
        let queue = match fresh {
            Fresh::Input0 => &mut self.todo0,
            Fresh::Input1 => &mut self.todo1,
        };
        queue.push_back(JoinUnit { left: input0, right: input1, fresh, capability });
    }

    fn work(&mut self, fuel: &mut isize, output: &mut OutputBuilderSession<B0::Time, EffortBuilder<CB>>) {
        while let Some(unit) = self.todo0.pop_front() {
            join_unit(&mut self.backend, unit, output);
        }
        while let Some(unit) = self.todo1.pop_front() {
            join_unit(&mut self.backend, unit, output);
        }
        *fuel = 0;
    }
}

/// Join one deferred unit: present both sides, merge-match key runs over the sorted
/// hashes, cross-product matched records, and hand the backend the index lists.
fn join_unit<B0, B1, Bk, CB>(backend: &mut Bk, unit: JoinUnit<B0, B1>, output: &mut OutputBuilderSession<B0::Time, EffortBuilder<CB>>)
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
    CB: ContainerBuilder<Container = Bk::Output>,
{
    // Present the fresh side first (delta-sized), then the accumulated side restricted
    // to the fresh side's keys, so a unit's work tracks the fresh batch.
    let (p0, p1) = match unit.fresh {
        Fresh::Input0 => {
            let p0 = backend.present0(&unit.left, None);
            if p0.is_empty() { return; }
            let mut keys = p0.key_hashes().to_vec();
            keys.dedup();
            let p1 = backend.present1(&unit.right, Some(&keys));
            (p0, p1)
        }
        Fresh::Input1 => {
            let p1 = backend.present1(&unit.right, None);
            if p1.is_empty() { return; }
            let mut keys = p1.key_hashes().to_vec();
            keys.dedup();
            let p0 = backend.present0(&unit.left, Some(&keys));
            (p0, p1)
        }
    };
    if p0.is_empty() || p1.is_empty() { return; }
    let (k0, k1) = (p0.key_hashes(), p1.key_hashes());

    let (mut li, mut ri) = (Vec::new(), Vec::new());
    let (mut ot, mut od) = (Vec::new(), Vec::new());
    let (mut i, mut j) = (0usize, 0usize);
    while i < k0.len() && j < k1.len() {
        if k0[i] < k1[j] {
            i += 1;
        } else if k1[j] < k0[i] {
            j += 1;
        } else {
            let key = k0[i];
            let mut e0 = i;
            while e0 < k0.len() && k0[e0] == key { e0 += 1; }
            let mut e1 = j;
            while e1 < k1.len() && k1[e1] == key { e1 += 1; }
            join_key(&p0, i..e0, &p1, j..e1, &mut li, &mut ri, &mut ot, &mut od);
            i = e0;
            j = e1;
        }
    }
    if li.is_empty() { return; }
    let mut container = backend.cross(&li, &ri, ot, od);
    output.session_with_builder(&unit.capability).give_container(&mut container);
}

/// Match one key's records across the two presented runs.
///
/// For reasonably sized histories, the dead-simple cross product. For larger ones, the
/// [`JoinThinker`]'s replay (ported to proxy space): both sides' edits are replayed in
/// ascending time order, each edit joined against the *other* side's accumulated buffer,
/// which is repeatedly advanced by the meet of the times still to come and consolidated —
/// so a key whose two histories carry many distinct times costs the histories' sum times
/// the buffer width, not their product. Emitted times are identical either way
/// (`t0 ∨ (t1 ∨ meet) = t0 ∨ t1` since `meet ≤ t0`), and consolidation only pre-sums
/// diffs whose product terms would have consolidated downstream.
///
/// Matched records reach the backend as representative indices per `value_id` (equal ids
/// denote equal values, so any representative serves the projection).
///
/// [`JoinThinker`]: crate::operators::join (private; see the cursor tactic)
#[allow(clippy::too_many_arguments)]
fn join_key<T, R0, R1, RO>(
    p0: &ProxyChunk<T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyChunk<T, R1>,
    r1: std::ops::Range<usize>,
    li: &mut Vec<usize>,
    ri: &mut Vec<usize>,
    ot: &mut Vec<T>,
    od: &mut Vec<RO>,
) where
    T: Lattice + Timestamp,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    if r0.len() < 16 || r1.len() < 16 {
        for a in r0 {
            for b in r1.clone() {
                li.push(a);
                ri.push(b);
                ot.push(p0.times()[a].join(&p1.times()[b]));
                od.push(p0.diffs()[a].clone().multiply(&p1.diffs()[b]));
            }
        }
        return;
    }

    // value_id → representative record index, per side (each side's records are sorted
    // by (value_id, time) within the key).
    let rep = |p_vids: &[u64], range: std::ops::Range<usize>| {
        let mut rep: Vec<(u64, usize)> = Vec::new();
        for i in range {
            if rep.last().is_none_or(|(v, _)| *v != p_vids[i]) {
                rep.push((p_vids[i], i));
            }
        }
        rep
    };
    let rep0 = rep(p0.value_ids(), r0.clone());
    let rep1 = rep(p1.value_ids(), r1.clone());
    let find = |rep: &[(u64, usize)], vid: u64| rep[rep.binary_search_by_key(&vid, |x| x.0).expect("vid presented")].1;

    let mut h0: IdHistory<T, R0> = IdHistory::new();
    h0.load(r0.map(|i| (p0.value_ids()[i], p0.times()[i].clone(), p0.diffs()[i].clone())), None);
    let mut h1: IdHistory<T, R1> = IdHistory::new();
    h1.load(r1.map(|i| (p1.value_ids()[i], p1.times()[i].clone(), p1.diffs()[i].clone())), None);

    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                li.push(find(&rep0, v0));
                ri.push(find(&rep1, *v1));
                ot.push(t0.join(t1));
                od.push(d0.clone().multiply(d1));
            }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                li.push(find(&rep0, *v0));
                ri.push(find(&rep1, v1));
                ot.push(t0.join(t1));
                od.push(d0.clone().multiply(d1));
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            li.push(find(&rep0, v0));
            ri.push(find(&rep1, *v1));
            ot.push(t0.join(t1));
            od.push(d0.clone().multiply(d1));
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            li.push(find(&rep0, *v0));
            ri.push(find(&rep1, v1));
            ot.push(t0.join(t1));
            od.push(d0.clone().multiply(d1));
        }
        h1.step();
    }
}
