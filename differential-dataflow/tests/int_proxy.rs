//! The integer-proxy chunk framework against the row-based operators.
//!
//! An equi-join and reductions (count, distinct) run twice over identical multi-round
//! inputs with retractions: once through the proxy-space tactics over the in-memory
//! reference backend (`operators::int_proxy`), once through the stock row-based operators.
//! The update streams must agree exactly. A scripted `retire` sequence over
//! partially-ordered `Product` times exercises the synthetic-time and pending machinery
//! that totally-ordered dataflow rounds cannot reach.

use std::collections::BTreeMap;
use std::sync::mpsc::channel;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::Capture;
use timely::order::Product;
use timely::PartialOrder;
use timely::progress::Antichain;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};

mod support;
use support::vec_backend::{RefBatch, VecJoinBackend, VecReduceBackend};
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::operators::reduce::{reduce_with_tactic, ReduceTactic};
use differential_dataflow::trace::chunk::vec::{ChunkBatcher, ChunkBuilder, ChunkSpine, VecChunk};
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::{Builder, Description, Navigable};

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

/// Rounds of `((key, val), diff)` updates: fresh insertions plus retractions of
/// previously inserted records, so histories cancel and consolidate.
fn gen_rounds(seed: u64, rounds: usize, per_round: usize) -> Vec<Vec<((u64, u64), isize)>> {
    let mut s = seed;
    let mut live: Vec<(u64, u64)> = Vec::new();
    (0..rounds)
        .map(|_| {
            let mut round = Vec::new();
            for _ in 0..per_round {
                if !live.is_empty() && xorshift(&mut s) % 4 == 0 {
                    let idx = (xorshift(&mut s) as usize) % live.len();
                    round.push((live.swap_remove(idx), -1));
                } else {
                    let rec = (xorshift(&mut s) % 8, xorshift(&mut s) % 4);
                    live.push(rec);
                    round.push((rec, 1));
                }
            }
            round
        })
        .collect()
}

/// Flatten captured events into a consolidated `(data, time) → diff` map.
fn consolidated<D: Ord>(events: Vec<Event<u64, Vec<(D, u64, isize)>>>) -> BTreeMap<(D, u64), isize> {
    let mut acc = BTreeMap::new();
    for event in events {
        if let Event::Messages(_, data) = event {
            for (d, t, r) in data {
                *acc.entry((d, t)).or_insert(0) += r;
            }
        }
    }
    acc.retain(|_, r| *r != 0);
    acc
}

type Chunker<K, V> = ContainerChunker<VecChunk<K, V, u64, isize>>;
type Batcher<K, V> = ChunkBatcher<K, V, u64, isize>;
type Bldr<K, V> = ChunkBuilder<K, V, u64, isize>;
type Spine<K, V> = ChunkSpine<K, V, u64, isize>;

type JoinOut = (u64, (u64, u64));

#[test]
fn proxy_join_matches_row_join() {
    let rounds0 = gen_rounds(0x853C49E6748FEA9B, 5, 12);
    let rounds1 = gen_rounds(0xDA3E39CB94B95BDB, 5, 12);

    // Id path: arranged chunk spines, joined by the id tactic over the reference backend.
    let (tx, rx) = channel();
    {
        let (rounds0, rounds1) = (rounds0.clone(), rounds1.clone());
        timely::execute_directly(move |worker| {
            let (mut in0, mut in1) = worker.dataflow::<u64, _, _>(|scope| {
                let (in0, c0) = scope.new_collection::<(u64, u64), isize>();
                let (in1, c1) = scope.new_collection::<(u64, u64), isize>();
                let a0 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c0.inner, Pipeline, "Arrange0");
                let a1 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c1.inner, Pipeline, "Arrange1");
                let tactic = ProxyJoinTactic::new(VecJoinBackend::new(|k: &u64, v0: &u64, v1: &u64| (*k, (*v0, *v1))));
                let joined = join_with_tactic::<_, _, _, Vec<(JoinOut, u64, isize)>>(a0, a1, tactic);
                joined.capture_into(tx);
                (in0, in1)
            });
            for (t, (r0, r1)) in rounds0.iter().zip(rounds1.iter()).enumerate() {
                for (rec, diff) in r0 { in0.update(*rec, *diff); }
                for (rec, diff) in r1 { in1.update(*rec, *diff); }
                in0.advance_to(t as u64 + 1);
                in1.advance_to(t as u64 + 1);
                in0.flush();
                in1.flush();
                worker.step();
            }
        });
    }
    let got = consolidated(rx.into_iter().collect());

    // Row path: the stock join over the same inputs.
    let (tx, rx) = channel();
    {
        timely::execute_directly(move |worker| {
            let (mut in0, mut in1) = worker.dataflow::<u64, _, _>(|scope| {
                let (in0, c0) = scope.new_collection::<(u64, u64), isize>();
                let (in1, c1) = scope.new_collection::<(u64, u64), isize>();
                c0.join(c1).inner.capture_into(tx);
                (in0, in1)
            });
            for (t, (r0, r1)) in rounds0.iter().zip(rounds1.iter()).enumerate() {
                for (rec, diff) in r0 { in0.update(*rec, *diff); }
                for (rec, diff) in r1 { in1.update(*rec, *diff); }
                in0.advance_to(t as u64 + 1);
                in1.advance_to(t as u64 + 1);
                in0.flush();
                in1.flush();
                worker.step();
            }
        });
    }
    let want = consolidated(rx.into_iter().collect());

    assert!(!want.is_empty(), "trivial test: row join produced nothing");
    assert_eq!(got, want);
}

/// Drive the same rounds through the proxy-space reduce and the row reduce with the same
/// user logic, and compare the output update streams.
fn check_reduce<V2, L>(logic: L)
where
    V2: differential_dataflow::Data + std::hash::Hash + Send + Sync,
    L: FnMut(&u64, &[(&u64, isize)], &mut Vec<(V2, isize)>) + Clone + Send + Sync + 'static,
{
    let rounds = gen_rounds(0xC0FFEE0DDBA11, 6, 10);

    // Id path.
    let (tx, rx) = channel();
    {
        let rounds = rounds.clone();
        let logic = logic.clone();
        timely::execute_directly(move |worker| {
            let mut input = worker.dataflow::<u64, _, _>(|scope| {
                let (input, c) = scope.new_collection::<(u64, u64), isize>();
                let arranged = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c.inner, Pipeline, "Arrange");
                let tactic = ProxyReduceTactic::new(VecReduceBackend::new(logic));
                let reduced = reduce_with_tactic::<_, Spine<u64, V2>, _>(arranged, "IdReduce", tactic);
                reduced.as_collection(|k, v| (*k, v.clone())).inner.capture_into(tx);
                input
            });
            for (t, round) in rounds.iter().enumerate() {
                for (rec, diff) in round { input.update(*rec, *diff); }
                input.advance_to(t as u64 + 1);
                input.flush();
                worker.step();
            }
        });
    }
    let got = consolidated(rx.into_iter().collect());

    // Row path.
    let (tx, rx) = channel();
    {
        let rounds = rounds.clone();
        let logic = logic.clone();
        timely::execute_directly(move |worker| {
            let mut input = worker.dataflow::<u64, _, _>(|scope| {
                let (input, c) = scope.new_collection::<(u64, u64), isize>();
                let mut logic = logic.clone();
                c.reduce(move |k, input, output| logic(k, input, output)).inner.capture_into(tx);
                input
            });
            for (t, round) in rounds.iter().enumerate() {
                for (rec, diff) in round { input.update(*rec, *diff); }
                input.advance_to(t as u64 + 1);
                input.flush();
                worker.step();
            }
        });
    }
    let want = consolidated(rx.into_iter().collect());

    assert!(!want.is_empty(), "trivial test: row reduce produced nothing");
    assert_eq!(got, want);
}

#[test]
fn proxy_reduce_count_matches_row_reduce() {
    check_reduce::<isize, _>(|_k, input, output| {
        let count: isize = input.iter().map(|(_, d)| *d).sum();
        if count > 0 {
            output.push((count, 1));
        }
    });
}

#[test]
fn proxy_reduce_distinct_matches_row_reduce() {
    check_reduce::<(), _>(|_k, input, output| {
        if input.iter().any(|(_, d)| *d > 0) {
            output.push(((), 1));
        }
    });
}

#[test]
fn proxy_reduce_min_matches_row_reduce() {
    // Order-sensitive reduction: `value_id`s are hash-ordered, so min MUST come from the
    // value callback (the design's ordering decision); this pins that path.
    check_reduce::<u64, _>(|_k, input, output| {
        if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| **v).min() {
            output.push((m, 1));
        }
    });
}

// ---------------------------------------------------------------------------
// Partially ordered times: a scripted retire sequence over `Product<u64, u64>`.
// ---------------------------------------------------------------------------

type PT = Product<u64, u64>;
type PBatch<V> = RefBatch<u64, V, PT, isize>;

fn pt(a: u64, b: u64) -> PT {
    Product::new(a, b)
}

/// Build a reference-backend batch from rows (sorted+consolidated internally).
fn pbatch<V: Ord + Clone + 'static>(mut rows: Vec<((u64, V), PT, isize)>, lower: Vec<PT>, upper: Vec<PT>) -> PBatch<V> {
    use timely::container::PushInto;
    consolidate_updates(&mut rows);
    let mut chunk = VecChunk::default();
    for row in rows {
        chunk.push_into(row);
    }
    let description = Description::new(
        Antichain::from_iter(lower),
        Antichain::from_iter(upper),
        Antichain::from_elem(pt(0, 0)),
    );
    let mut builder = ChunkBuilder::<u64, V, PT, isize>::with_capacity(0, 0, 0);
    builder.push(&mut chunk);
    builder.done(description)
}

fn pbatch_rows<V: Ord + Clone + 'static>(batch: &PBatch<V>) -> Vec<((u64, V), PT, isize)> {
    let mut rows = Vec::new();
    let mut cursor = batch.cursor();
    while let Some(k) = cursor.get_key(batch) {
        while let Some(v) = cursor.get_val(batch) {
            cursor.map_times(batch, |t, d| rows.push(((*k, v.clone()), t.clone(), *d)));
            cursor.step_val(batch);
        }
        cursor.step_key(batch);
    }
    consolidate_updates(&mut rows);
    rows
}

/// Updates at `(0, 1)` and `(1, 0)` must produce a correction at their join `(1, 1)` —
/// synthesized by the tactic in one interval when `(1, 1)` lies inside it, and *pended*
/// across retires when it does not. Three scripted retires walk a count reduction
/// through arrival at `(0, 1)`, arrival at `(1, 0)` with `(1, 1)` beyond the interval
/// (so it pends), and the pending-driven correction with no new input at all.
#[test]
fn proxy_reduce_synthesizes_and_pends_product_times() {
    let count = |_k: &u64, input: &[(&u64, isize)], output: &mut Vec<(isize, isize)>| {
        let count: isize = input.iter().map(|(_, d)| *d).sum();
        if count > 0 {
            output.push((count, 1));
        }
    };
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(count));

    // Retire 1: value 10 arrives at (0, 1); interval [(0,0), {(0,2),(1,0)}).
    let batch1 = pbatch(vec![((1u64, 10u64), pt(0, 1), 1)], vec![pt(0, 0)], vec![pt(0, 2), pt(1, 0)]);
    let lower1 = Antichain::from_elem(pt(0, 0));
    let upper1 = Antichain::from_iter([pt(0, 2), pt(1, 0)]);
    let held1 = Antichain::from_elem(pt(0, 0));
    let (produced1, frontier1) = tactic.retire(vec![], vec![], vec![batch1.clone()], &lower1, &upper1, &held1);
    assert_eq!(produced1.len(), 1);
    assert_eq!(produced1[0].0, pt(0, 0));
    assert_eq!(pbatch_rows(&produced1[0].1), vec![((1, 1isize), pt(0, 1), 1)]);
    assert!(frontier1.is_empty()); // nothing pended

    // Retire 2: value 20 arrives at (1, 0); interval [{(0,2),(1,0)}, {(1,1)}). The
    // synthetic join (0,1) ∨ (1,0) = (1,1) is at the upper bound, so it must PEND.
    let batch2 = pbatch(vec![((1u64, 20u64), pt(1, 0), 1)], vec![pt(0, 2), pt(1, 0)], vec![pt(1, 1)]);
    let lower2 = upper1.clone();
    let upper2 = Antichain::from_elem(pt(1, 1));
    let held2 = Antichain::from_elem(pt(1, 0));
    let (produced2, frontier2) = tactic.retire(
        vec![batch1.clone()],
        vec![produced1[0].1.clone()],
        vec![batch2.clone()],
        &lower2,
        &upper2,
        &held2,
    );
    assert_eq!(produced2.len(), 1);
    assert_eq!(produced2[0].0, pt(1, 0));
    // At (1, 0) only value 20 is visible ((0,1) is incomparable): count 1.
    assert_eq!(pbatch_rows(&produced2[0].1), vec![((1, 1isize), pt(1, 0), 1)]);
    assert_eq!(frontier2, Antichain::from_elem(pt(1, 1))); // (1,1) pended

    // Retire 3: no new input; the pended (1,1) drives the correction. Both values are
    // now visible (count 2), while the accumulated output claims two counts of 1.
    let lower3 = upper2.clone();
    let upper3 = Antichain::new(); // the closed interval: nothing lies beyond
    let held3 = Antichain::from_elem(pt(1, 1));
    let (produced3, frontier3) = tactic.retire(
        vec![batch1, batch2],
        vec![produced1[0].1.clone(), produced2[0].1.clone()],
        vec![],
        &lower3,
        &upper3,
        &held3,
    );
    assert_eq!(produced3.len(), 1);
    assert_eq!(produced3[0].0, pt(1, 1));
    assert_eq!(
        pbatch_rows(&produced3[0].1),
        vec![((1, 1isize), pt(1, 1), -2), ((1, 2isize), pt(1, 1), 1)]
    );
    assert!(frontier3.is_empty());
}

// ---------------------------------------------------------------------------
// Asymptotics: presented work must track the delta, not the accumulated trace.
// ---------------------------------------------------------------------------
//
// The cursor tactics touch an arrangement only at the keys a fresh batch names
// (plus, for reduce, pending keys), at logarithmic seek cost. The proxy tactics
// must not be asymptotically worse: the changed-key filter (reduce) and the
// fresh-side filter (join) exist precisely so a backend can seek rather than
// scan. These tests pin that: with `N` arranged keys and rounds touching one
// key, the number of *presented* records — a deterministic work measure the
// backend observes directly — must stay far below `N`, where a scanning
// implementation would present `Ω(rounds · N)`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use differential_dataflow::operators::int_proxy::{ProxyBridge, ProxyJoinBackend, JoinInstance, ProxyReduceBackend, ReduceInstance, ReduceWindow};
use differential_dataflow::trace::BatchReader;

/// A reduce backend wrapper counting records presented by the inner backend.
struct CountingReduce<B> {
    inner: B,
    presented: Arc<AtomicUsize>,
}

impl<B1, B2, B> ProxyReduceBackend<B1, B2> for CountingReduce<B>
where
    B1: BatchReader,
    B2: BatchReader<Time = B1::Time>,
    B: ProxyReduceBackend<B1, B2>,
{
    type RIn = B::RIn;
    type ROut = B::ROut;

    fn seed_times(&self, instance: &ReduceInstance<'_, B1, B2>) -> Vec<(u64, B1::Time)> {
        let seeds = self.inner.seed_times(instance);
        self.presented.fetch_add(seeds.len(), AtomicOrdering::SeqCst);
        seeds
    }
    fn begin(&mut self, tiles: &[Description<B1::Time>]) {
        self.inner.begin(tiles);
    }
    fn next_window(&mut self, instance: &ReduceInstance<'_, B1, B2>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<B1::Time, B::RIn, B::ROut>> {
        let window = self.inner.next_window(instance, changed, cursor);
        if let Some(w) = &window {
            self.presented.fetch_add(w.input.len() + w.output.len(), AtomicOrdering::SeqCst);
        }
        window
    }
    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, B::RIn)], out_ends: &[usize], output: &[(u64, B::ROut)]) -> (Vec<(u64, B::ROut)>, Vec<usize>) {
        self.inner.reduce_corrections(keys, in_ends, input, out_ends, output)
    }
    fn emit(&mut self, tile: usize, records: &[((u64, u64), B1::Time, B::ROut)]) {
        self.inner.emit(tile, records);
    }
    fn finish(&mut self) -> Vec<B2> {
        self.inner.finish()
    }
}

/// A join backend wrapper counting records presented by the inner backend.
struct CountingJoin<B> {
    inner: B,
    presented: Arc<AtomicUsize>,
}

impl<B0, B1, B> ProxyJoinBackend<B0, B1> for CountingJoin<B>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    B: ProxyJoinBackend<B0, B1>,
{
    type R0 = B::R0;
    type R1 = B::R1;
    type ROut = B::ROut;
    type Output = B::Output;

    fn present0(&mut self, instance: &JoinInstance<'_, B0, B1>, filter: Option<&[u64]>) -> ProxyBridge<B0::Time, B::R0> {
        let chunk = self.inner.present0(instance, filter);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn present1(&mut self, instance: &JoinInstance<'_, B0, B1>, filter: Option<&[u64]>) -> ProxyBridge<B0::Time, B::R1> {
        let chunk = self.inner.present1(instance, filter);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn cross(&mut self, instance: &JoinInstance<'_, B0, B1>, left: &[(u64, u64)], right: &[(u64, u64)], times: Vec<B0::Time>, diffs: Vec<B::ROut>) -> B::Output {
        self.inner.cross(instance, left, right, times, diffs)
    }
}

#[test]
fn proxy_reduce_work_is_delta_proportional() {
    use timely::dataflow::operators::probe::{Handle, Probe};

    const N: u64 = 20_000;
    const ROUNDS: u64 = 5;
    let presented = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&presented);

    timely::execute_directly(move |worker| {
        let probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, c) = scope.new_collection::<(u64, u64), isize>();
            let arranged = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c.inner, Pipeline, "Arrange");
            let backend = CountingReduce {
                inner: VecReduceBackend::new(|_k: &u64, input: &[(&u64, isize)], output: &mut Vec<(isize, isize)>| {
                    let count: isize = input.iter().map(|(_, d)| *d).sum();
                    if count > 0 {
                        output.push((count, 1));
                    }
                }),
                presented: counter,
            };
            let reduced = reduce_with_tactic::<_, Spine<u64, isize>, _>(arranged, "ProxyReduce", ProxyReduceTactic::new(backend));
            reduced.stream.probe_with(&probe);
            input
        });

        // Round 0: a large arrangement. This work is inherently O(N); let it drain.
        for k in 0..N {
            input.update((k, k % 5), 1);
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        let after_load = presented.load(AtomicOrdering::SeqCst);

        // Rounds 1..: touch ONE key per round. Presented work must track that key's
        // history, independent of N; a scanning backend would present ≥ ROUNDS · N.
        for t in 1..=ROUNDS {
            input.update((7, 100 + t), 1);
            input.advance_to(t + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        let total = presented.load(AtomicOrdering::SeqCst);
        let incremental = total - after_load;
        assert!(incremental > 0, "the incremental rounds presented nothing");
        assert!(
            incremental < (N as usize) / 4,
            "presented {incremental} records over {ROUNDS} single-key rounds against {N} keys: not delta-proportional"
        );
    });
}

#[test]
fn proxy_join_work_is_delta_proportional() {
    use timely::dataflow::operators::probe::{Handle, Probe};

    const N: u64 = 20_000;
    const ROUNDS: u64 = 5;
    let presented = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&presented);

    timely::execute_directly(move |worker| {
        let probe = Handle::new();
        let (mut left, mut right) = worker.dataflow::<u64, _, _>(|scope| {
            let (left, c0) = scope.new_collection::<(u64, u64), isize>();
            let (right, c1) = scope.new_collection::<(u64, u64), isize>();
            let a0 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c0.inner, Pipeline, "Arrange0");
            let a1 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c1.inner, Pipeline, "Arrange1");
            let backend = CountingJoin {
                inner: VecJoinBackend::new(|k: &u64, v0: &u64, v1: &u64| (*k, (*v0, *v1))),
                presented: counter,
            };
            let joined = join_with_tactic::<_, _, _, Vec<(JoinOut, u64, isize)>>(a0, a1, ProxyJoinTactic::new(backend));
            joined.probe_with(&probe);
            (left, right)
        });

        // Round 0: a large accumulated right side. Inherently O(N); let it drain.
        for k in 0..N {
            right.update((k, k), 1);
        }
        left.advance_to(1);
        right.advance_to(1);
        left.flush();
        right.flush();
        while probe.less_than(left.time()) {
            worker.step();
        }
        let after_load = presented.load(AtomicOrdering::SeqCst);

        // Rounds 1..: one fresh left record per round. The unit joins it against the
        // accumulated right trace; the fresh-side filter must keep presented work at
        // the matched key's records, independent of N.
        for t in 1..=ROUNDS {
            left.update((7, 100 + t), 1);
            left.advance_to(t + 1);
            right.advance_to(t + 1);
            left.flush();
            right.flush();
            while probe.less_than(left.time()) {
                worker.step();
            }
        }
        let total = presented.load(AtomicOrdering::SeqCst);
        let incremental = total - after_load;
        assert!(incremental > 0, "the incremental rounds presented nothing");
        assert!(
            incremental < (N as usize) / 4,
            "presented {incremental} records over {ROUNDS} single-record rounds against {N} keys: not delta-proportional"
        );
    });
}

// ---------------------------------------------------------------------------
// The minimal self-contained assessment: the framework closed over itself.
// ---------------------------------------------------------------------------
//
// The tactics are operator-side: they demand only `BatchReader` of the trace, so the
// minimal arrangement is a batch of `IdentityChunk` (test-local storage) — the proxy data IS the data.
// The `IdentityReduce` backend makes the loop close: values are `u64`s and the id
// function is the identity, so there is no hashing, no resolution machinery, and no
// collision possibility. What remains under test is exactly the framework's own
// contribution — the interesting-time logic, desired-vs-current deltas, pending, and
// held-time routing — which a brute-force oracle checks at EVERY point of a
// partially-ordered time grid: the accumulated output must equal the reduction of the
// accumulated input, everywhere, under randomized inputs and randomized retire
// boundaries (diagonal frontiers, so synthetic joins pend across retires).

mod identity {
    use std::rc::Rc;
    use differential_dataflow::operators::int_proxy::{ProxyBridge, ProxyReduceBackend, ReduceInstance, ReduceWindow};
    use differential_dataflow::trace::Description;
    use differential_dataflow::trace::chunk::ChunkBatch;
    use differential_dataflow::lattice::Lattice;
    use timely::progress::Timestamp;
    use crate::support::identity_chunk::IdentityChunk;
    use differential_dataflow::consolidation::consolidate_updates;

    pub type Batch<T> = Rc<ChunkBatch<IdentityChunk<T, isize>>>;

    /// The identity backend: batches store proxy records (via the test-local
    /// `IdentityChunk`), values are the ids themselves.
    pub struct IdentityReduce<T, L> {
        pub logic: L,
        /// The output session: per-tile description and accumulated (proxy = real) records.
        tiles: Vec<(Description<T>, ProxyBridge<T, isize>)>,
    }

    impl<T: Timestamp + Lattice, L> IdentityReduce<T, L> {
        pub fn new(logic: L) -> Self {
            IdentityReduce { logic, tiles: Vec::new() }
        }
    }

    /// Changed keys presented per window in the identity oracle — small so the fuzz exercises
    /// the multi-window path.
    const IDENT_WINDOW_KEYS: usize = 512;

    fn records<T: Timestamp + Lattice>(batches: &[Batch<T>], keys: Option<&[u64]>) -> (Vec<u64>, Vec<u64>, Vec<T>, Vec<isize>) {
        let (mut ks, mut vs) = (Vec::new(), Vec::new());
        let (mut ts, mut ds) = (Vec::new(), Vec::new());
        for batch in batches {
            for chunk in &batch.chunks {
                for i in 0..chunk.len() {
                    let k = chunk.key_hashes()[i];
                    if keys.is_none_or(|f| f.binary_search(&k).is_ok()) {
                        ks.push(k);
                        vs.push(chunk.value_ids()[i]);
                        ts.push(chunk.times()[i].clone());
                        ds.push(chunk.diffs()[i]);
                    }
                }
            }
        }
        (ks, vs, ts, ds)
    }

    impl<T, L> ProxyReduceBackend<Batch<T>, Batch<T>> for IdentityReduce<T, L>
    where
        T: Timestamp + Lattice,
        L: FnMut(&[(u64, isize)]) -> Vec<(u64, isize)>,
    {
        type RIn = isize;
        type ROut = isize;

        fn seed_times(&self, instance: &ReduceInstance<'_, Batch<T>, Batch<T>>) -> Vec<(u64, T)> {
            // Raw (key_hash, time) support, sorted by key_hash — no value work.
            let (ks, _vs, ts, _ds) = records(instance.input_batches, None);
            let mut out: Vec<(u64, T)> = ks.into_iter().zip(ts).collect();
            out.sort_by_key(|(k, _)| *k);
            out
        }

        fn begin(&mut self, tiles: &[Description<T>]) {
            self.tiles = tiles.iter().map(|d| (d.clone(), Vec::new())).collect();
        }

        // This backend ignores `instance.lower` — advancing is a valid efficiency choice, not a
        // requirement; leaving times raw is always correct (just less compact), which is what a
        // fuzz oracle wants. Presents input + output for one window of keys at a time.
        fn next_window(&mut self, instance: &ReduceInstance<'_, Batch<T>, Batch<T>>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<T, isize, isize>> {
            if *cursor >= changed.len() {
                return None;
            }
            let end = (*cursor + IDENT_WINDOW_KEYS).min(changed.len());
            let window = &changed[*cursor..end];
            let (mut ks, mut vs, mut ts, mut ds) = records(instance.source_batches, Some(window));
            let (k2, v2, t2, d2) = records(instance.input_batches, Some(window));
            ks.extend(k2);
            vs.extend(v2);
            ts.extend(t2);
            ds.extend(d2);
            let mut input: ProxyBridge<T, isize> =
                ks.into_iter().zip(vs).zip(ts).zip(ds).map(|(((k, v), t), d)| ((k, v), t, d)).collect();
            consolidate_updates(&mut input);
            let (ks, vs, ts, ds) = records(instance.output_batches, Some(window));
            let mut output: ProxyBridge<T, isize> =
                ks.into_iter().zip(vs).zip(ts).zip(ds).map(|(((k, v), t), d)| ((k, v), t, d)).collect();
            consolidate_updates(&mut output);
            *cursor = end;
            Some(ReduceWindow { input, output, keys: window.to_vec() })
        }

        fn reduce_corrections(
            &mut self,
            keys: &[u64],
            in_ends: &[usize],
            input: &[(u64, isize)],
            out_ends: &[usize],
            output: &[(u64, isize)],
        ) -> (Vec<(u64, isize)>, Vec<usize>) {
            let mut corrections = Vec::new();
            let mut ends = Vec::with_capacity(keys.len());
            let (mut istart, mut ostart) = (0usize, 0usize);
            for k in 0..keys.len() {
                let (iend, oend) = (in_ends[k], out_ends[k]);
                // The ids ARE the values, so difference desired against current keyed by id.
                let mut delta = (self.logic)(&input[istart..iend]);
                for (vid, d) in &output[ostart..oend] {
                    delta.push((*vid, -*d));
                }
                differential_dataflow::consolidation::consolidate(&mut delta);
                corrections.extend(delta.drain(..));
                ends.push(corrections.len());
                istart = iend;
                ostart = oend;
            }
            (corrections, ends)
        }

        fn emit(&mut self, tile: usize, records: &[((u64, u64), T, isize)]) {
            // The ids ARE the values, so the emitted proxies are already the output records.
            self.tiles[tile].1.extend(records.iter().cloned());
        }

        fn finish(&mut self) -> Vec<Batch<T>> {
            std::mem::take(&mut self.tiles)
                .into_iter()
                .map(|(description, mut bridge)| {
                    consolidate_updates(&mut bridge);
                    let chunks = if bridge.is_empty() {
                        Vec::new()
                    } else {
                        let (mut ks, mut vs, mut ts, mut ds) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                        for ((k, v), t, d) in bridge {
                            ks.push(k);
                            vs.push(v);
                            ts.push(t);
                            ds.push(d);
                        }
                        vec![IdentityChunk::from_sorted(ks, vs, ts, ds)]
                    };
                    Rc::new(ChunkBatch::new(chunks, description))
                })
                .collect()
        }
    }
}

/// Drive `ProxyReduceTactic` over the identity backend through a sequence of retires,
/// emulating the driver protocol: `lower` chases `upper`, `held` is the previous
/// returned frontier joined with the round's input times (the batch capabilities),
/// and source/output batch lists accumulate.
/// Drive `ProxyReduceTactic` over the identity backend through a sequence of retires,
/// emulating the driver protocol. With `compact`, plays the COMPACTION ADVERSARY: before
/// each retire, every accumulated source and output batch has its times advanced to the
/// interval's lower bound and is re-consolidated — accumulation-preserving (the model's
/// `acc_mapDomain`), and exactly the move that can cancel a stored record against a
/// novel one in a merged view. A correct tactic's output must not change.
fn drive_identity_reduce_compacting<Tac>(
    mut tactic: Tac,
    rounds: &[(Antichain<PT>, Vec<((u64, u64), PT, isize)>)],
    compact: bool,
) -> Vec<((u64, u64), PT, isize)>
where
    Tac: ReduceTactic<identity::Batch<PT>, identity::Batch<PT>>,
{
    use crate::support::identity_chunk::IdentityChunk;
    use differential_dataflow::trace::chunk::ChunkBatch;

    #[allow(unused_imports)]
    use differential_dataflow::trace::BatchReader as _;

    let mut lower = Antichain::from_elem(pt(0, 0));
    let mut frontier: Antichain<PT> = Antichain::new();
    let mut source: Vec<identity::Batch<PT>> = Vec::new();
    let mut outputs: Vec<identity::Batch<PT>> = Vec::new();
    let mut produced_records = Vec::new();

    for (upper, updates) in rounds {
        let input: Vec<identity::Batch<PT>> = if updates.is_empty() {
            Vec::new()
        } else {
            let (ks, vs): (Vec<u64>, Vec<u64>) = updates.iter().map(|((k, v), _, _)| (*k, *v)).unzip();
            let ts: Vec<PT> = updates.iter().map(|(_, t, _)| t.clone()).collect();
            let ds: Vec<isize> = updates.iter().map(|(_, _, d)| *d).collect();
            let (chunk, _) = IdentityChunk::from_unsorted(ks, vs, ts, ds);
            let desc = Description::new(lower.clone(), upper.clone(), Antichain::from_elem(pt(0, 0)));
            let chunks = if chunk.is_empty() { Vec::new() } else { vec![chunk] };
            vec![std::rc::Rc::new(ChunkBatch::new(chunks, desc))]
        };

        // Held capabilities: what the driver would hold — previously returned frontier
        // plus the capabilities that arrived with this round's batches.
        let mut held = frontier.clone();
        for (_, t, _) in updates {
            held.insert(t.clone());
        }

        if compact {
            use differential_dataflow::lattice::Lattice;
            let advance = |batches: &mut Vec<identity::Batch<PT>>| {
                for batch in batches.iter_mut() {
                    let (mut ks, mut vs) = (Vec::new(), Vec::new());
                    let (mut ts, mut ds) = (Vec::new(), Vec::new());
                    for chunk in &batch.chunks {
                        for i in 0..chunk.len() {
                            ks.push(chunk.key_hashes()[i]);
                            vs.push(chunk.value_ids()[i]);
                            let mut t = chunk.times()[i].clone();
                            t.advance_by(lower.borrow());
                            ts.push(t);
                            ds.push(chunk.diffs()[i]);
                        }
                    }
                    let (chunk, _) = IdentityChunk::from_unsorted(ks, vs, ts, ds);
                    let chunks = if chunk.is_empty() { Vec::new() } else { vec![chunk] };
                    *batch = std::rc::Rc::new(ChunkBatch::new(chunks, batch.description().clone()));
                }
            };
            advance(&mut source);
            advance(&mut outputs);
        }

        let (produced, new_frontier) = tactic.retire(source.clone(), outputs.clone(), input.clone(), &lower, upper, &held);
        for (_, batch) in produced {
            for chunk in &batch.chunks {
                for i in 0..chunk.len() {
                    produced_records.push((
                        (chunk.key_hashes()[i], chunk.value_ids()[i]),
                        chunk.times()[i].clone(),
                        chunk.diffs()[i],
                    ));
                }
            }
            outputs.push(batch);
        }
        source.extend(input);
        frontier = new_frontier;
        lower = upper.clone();
    }
    assert!(frontier.is_empty(), "everything retired, but times remain pending");
    produced_records
}


/// Randomized closure test of the reduce tactic alone, over partially ordered times:
/// random updates on a `Product` grid, retired through random diagonal frontiers (so
/// synthetic joins arise inside and across intervals and must pend), checked against a
/// brute-force oracle at EVERY grid point — the accumulated output there must equal the
/// reduction of the accumulated input.
#[test]
fn proxy_reduce_identity_fuzz_matches_grid_oracle() {
    const T: u64 = 3; // times range over the (T+1)×(T+1) grid
    let mut seed = 0x853C49E6748FEA9Bu64;

    // Reducers over ids (values are ids; the id order is the value order here).
    type Logic = fn(&[(u64, isize)]) -> Vec<(u64, isize)>;
    let reducers: Vec<Logic> = vec![
        // count: the number of present records, as a value.
        |acc| {
            let s: isize = acc.iter().map(|(_, d)| *d).sum();
            if s > 0 { vec![(s as u64, 1)] } else { Vec::new() }
        },
        // distinct: the unit value if anything is present.
        |acc| {
            if acc.iter().any(|(_, d)| *d > 0) { vec![(0, 1)] } else { Vec::new() }
        },
        // min: the least present id.
        |acc| {
            acc.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).min().map(|m| vec![(m, 1)]).unwrap_or_default()
        },
    ];

    for iteration in 0..300 {
        let logic = reducers[iteration % reducers.len()];

        // Random updates over a small key/value space and the time grid.
        let n = (xorshift(&mut seed) % 12) as usize + 1;
        let updates: Vec<((u64, u64), PT, isize)> = (0..n)
            .map(|_| {
                let k = xorshift(&mut seed) % 2;
                let v = xorshift(&mut seed) % 3;
                let t = pt(xorshift(&mut seed) % (T + 1), xorshift(&mut seed) % (T + 1));
                let d = if xorshift(&mut seed) % 3 == 0 { -1 } else { 1 };
                ((k, v), t, d)
            })
            .collect();

        // Random increasing diagonal frontiers: upper_r = { p : |p| = s_r } for an
        // increasing random set of diagonals, closed by the empty frontier (retire all).
        let mut cuts: Vec<u64> = (1..=2 * T).filter(|_| xorshift(&mut seed) % 2 == 0).collect();
        cuts.dedup();
        let mut rounds: Vec<(Antichain<PT>, Vec<((u64, u64), PT, isize)>)> = Vec::new();
        let mut prev = 0u64;
        for &s in &cuts {
            let upper = Antichain::from_iter((0..=s).map(|x| pt(x, s - x)));
            let batch = updates.iter().filter(|(_, t, _)| {
                let sum = t.outer + t.inner;
                prev <= sum && sum < s
            }).cloned().collect();
            rounds.push((upper, batch));
            prev = s;
        }
        let tail = updates.iter().filter(|(_, t, _)| t.outer + t.inner >= prev).cloned().collect();
        rounds.push((Antichain::new(), tail));

        let compact = iteration % 2 == 1;
        // The reduce tactic under test, driven through the identity backend, checked below against
        // the brute-force grid oracle at every grid point.
        let produced = drive_identity_reduce_compacting(
            ProxyReduceTactic::new(identity::IdentityReduce::new(logic)),
            &rounds,
            compact,
        );

        // The oracle: at every grid point, per key, the accumulated output equals the
        // reduction of the accumulated input.
        for x in 0..=T {
            for y in 0..=T {
                let p = pt(x, y);
                for key in 0..2u64 {
                    let mut acc: BTreeMap<u64, isize> = BTreeMap::new();
                    for ((k, v), t, d) in &updates {
                        if *k == key && t.less_equal(&p) {
                            *acc.entry(*v).or_insert(0) += d;
                        }
                    }
                    acc.retain(|_, d| *d != 0);
                    let pairs: Vec<(u64, isize)> = acc.into_iter().collect();
                    let mut want: BTreeMap<u64, isize> = BTreeMap::new();
                    if !pairs.is_empty() {
                        for (v, d) in (logic)(&pairs) {
                            *want.entry(v).or_insert(0) += d;
                        }
                        want.retain(|_, d| *d != 0);
                    }
                    let mut got: BTreeMap<u64, isize> = BTreeMap::new();
                    for ((k, v), t, d) in &produced {
                        if *k == key && t.less_equal(&p) {
                            *got.entry(*v).or_insert(0) += d;
                        }
                    }
                    got.retain(|_, d| *d != 0);
                    assert_eq!(
                        got, want,
                        "iteration {iteration}, key {key}, point {p:?}\nupdates: {updates:?}\nrounds: {:?}\nproduced: {produced:?}",
                        rounds.iter().map(|(u, b)| (u.elements().to_vec(), b.len())).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Wall clock: the proxy stack against the stock row stack.
// ---------------------------------------------------------------------------
//
// Not run by default. Run with:
//   cargo test --release -p differential-dataflow --test int_proxy -- --ignored --nocapture
//
// Compares full stacks end to end — the stock row operators (their own arrangements
// included) against chunk arrangements plus the proxy tactics over the reference
// backend — for a bulk load of `n` keys and an incremental phase of single-key rounds.
// The counting tests above pin the asymptotics; this reports the constants.

// F7 spike: a "boring" hash-native reduce backend. Reads `VecChunk<u64,u64>` arrangements whose key
// IS the key_hash and whose value IS the value_id (values are ids — the identity model), so `present`
// is an in-order scan: no hashing, no re-sort into seam order, no real-`(K,V)` resolution. Value-only
// logic; real keys are dropped, so `hash(K)` collisions merge groups (risked). The exemplar for
// "values are already ids"; see DESIGN.md F7.
struct HashReduce<L> {
    logic: L,
    tiles: Vec<(Description<u64>, Vec<((u64, u64), u64, isize)>)>,
}
impl<L> HashReduce<L> {
    fn new(logic: L) -> Self { HashReduce { logic, tiles: Vec::new() } }
}

// Collect `(key_hash, value_id, time, diff)` from hash-native batches (key IS key_hash, value IS
// value_id — no hashing), restricted to `keys` if given. The reference `VecChunk` is a flat sorted
// slice of `((key_hash, value_id), time, diff)` — already the bridge tuple in seam order — so we read
// its columns directly (`as_slice`) instead of walking the generic galloping cursor. The full-scan
// copy is a `memcpy` (`extend_from_slice`); the filtered read is a two-pointer against the (sorted)
// keys. `ChunkBatch.chunks` are ordered sorted runs whose concatenation is the batch.
fn read_hash(batches: &[RefBatch<u64, u64, u64, isize>], keys: Option<&[u64]>, out: &mut Vec<((u64, u64), u64, isize)>) {
    for batch in batches {
        for chunk in &batch.chunks {
            let slice = chunk.as_slice();
            match keys {
                None => out.extend_from_slice(slice),
                Some(ks) => {
                    // Two-pointer over the sorted slice (by key_hash) and the sorted filter keys. A
                    // key may straddle chunks, so each chunk is scanned independently.
                    let (mut ri, mut ki) = (0usize, 0usize);
                    while ri < slice.len() && ki < ks.len() {
                        let kh = (slice[ri].0).0;
                        if kh < ks[ki] {
                            ri += slice[ri..].partition_point(|r| (r.0).0 < ks[ki]);
                        } else if kh > ks[ki] {
                            // Gallop `ki` too — a chunk entirely above the window would otherwise
                            // step through every filter key one at a time (quadratic across chunks).
                            ki += ks[ki..].partition_point(|&k| k < kh);
                        } else {
                            let start = ri;
                            let end = start + slice[start..].partition_point(|r| (r.0).0 <= kh);
                            out.extend_from_slice(&slice[start..end]);
                            ri = end;
                        }
                    }
                }
            }
        }
    }
}

impl<L> ProxyReduceBackend<RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>> for HashReduce<L>
where L: FnMut(&[(u64, isize)]) -> Vec<(u64, isize)>
{
    type RIn = isize;
    type ROut = isize;

    fn seed_times(&self, instance: &ReduceInstance<'_, RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>>) -> Vec<(u64, u64)> {
        let mut recs = Vec::new();
        read_hash(instance.input_batches, None, &mut recs);
        let mut out: Vec<(u64, u64)> = recs.into_iter().map(|((kh, _), t, _)| (kh, t)).collect();
        out.sort_by_key(|(k, _)| *k);
        out
    }

    fn begin(&mut self, tiles: &[Description<u64>]) {
        self.tiles = tiles.iter().map(|d| (d.clone(), Vec::new())).collect();
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<u64, isize, isize>> {
        use differential_dataflow::lattice::Lattice;
        if *cursor >= changed.len() { return None; }
        let end = (*cursor + 1024).min(changed.len());
        let window = &changed[*cursor..end];
        // Present is a SCAN: read the window's records (already in (key_hash, value_id) order), advance
        // to `lower`, consolidate. No hashing, no re-sort into seam order (the store already is).
        let mut input = Vec::new();
        read_hash(instance.source_batches, Some(window), &mut input);
        read_hash(instance.input_batches, Some(window), &mut input);
        for r in input.iter_mut() { r.1.advance_by(instance.lower); }
        consolidate_updates(&mut input);
        let mut output = Vec::new();
        read_hash(instance.output_batches, Some(window), &mut output);
        for r in output.iter_mut() { r.1.advance_by(instance.lower); }
        consolidate_updates(&mut output);
        *cursor = end;
        Some(ReduceWindow { input, output, keys: window.to_vec() })
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, isize)], out_ends: &[usize], output: &[(u64, isize)]) -> (Vec<(u64, isize)>, Vec<usize>) {
        let mut corrections = Vec::new();
        let mut ends = Vec::with_capacity(keys.len());
        let (mut istart, mut ostart) = (0usize, 0usize);
        for k in 0..keys.len() {
            let (iend, oend) = (in_ends[k], out_ends[k]);
            let mut delta = (self.logic)(&input[istart..iend]);
            for (vid, d) in &output[ostart..oend] { delta.push((*vid, -*d)); }
            differential_dataflow::consolidation::consolidate(&mut delta);
            corrections.extend(delta.drain(..));
            ends.push(corrections.len());
            istart = iend;
            ostart = oend;
        }
        (corrections, ends)
    }

    fn emit(&mut self, tile: usize, records: &[((u64, u64), u64, isize)]) {
        self.tiles[tile].1.extend(records.iter().cloned());
    }

    fn finish(&mut self) -> Vec<RefBatch<u64, u64, u64, isize>> {
        std::mem::take(&mut self.tiles)
            .into_iter()
            .map(|(description, mut rows)| {
                consolidate_updates(&mut rows);
                let mut builder = ChunkBuilder::<u64, u64, u64, isize>::with_capacity(0, 0, 0);
                let mut rows = rows.into_iter().peekable();
                while rows.peek().is_some() {
                    let mut chunk = VecChunk::default();
                    for row in rows.by_ref().take(<VecChunk<u64, u64, u64, isize> as differential_dataflow::trace::chunk::Chunk>::TARGET) {
                        use timely::container::PushInto;
                        chunk.push_into(row);
                    }
                    builder.push(&mut chunk);
                }
                builder.done(description)
            })
            .collect()
    }
}

fn bench_reduce(n: u64, rounds: u64, mode: u8) -> (std::time::Duration, std::time::Duration) {
    use std::time::Instant;
    use timely::dataflow::operators::probe::{Handle, Probe};

    timely::execute_directly(move |worker| {
        let probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, c) = scope.new_collection::<(u64, u64), isize>();
            let count = |_k: &u64, input: &[(&u64, isize)], output: &mut Vec<(isize, isize)>| {
                let count: isize = input.iter().map(|(_, d)| *d).sum();
                if count > 0 {
                    output.push((count, 1));
                }
            };
            if mode == 1 {
                let arranged = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c.inner, Pipeline, "Arrange");
                let reduced = reduce_with_tactic::<_, Spine<u64, isize>, _>(arranged, "ProxyReduce", ProxyReduceTactic::new(VecReduceBackend::new(count)));
                reduced.stream.probe_with(&probe);
            } else if mode == 2 {
                // Hash-native: pre-map the key to its hash (values already are ids), so the arrangement
                // is stored in seam order and `present` scans instead of re-sorting. Count over ids.
                let hashed = c.map(|(k, v)| (k.wrapping_mul(0x9E3779B97F4A7C15), v));
                let arranged = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(hashed.inner, Pipeline, "ArrangeHash");
                let count_ids = |vals: &[(u64, isize)]| { let s: isize = vals.iter().map(|(_, d)| *d).sum(); if s > 0 { vec![(s as u64, 1)] } else { Vec::new() } };
                let reduced = reduce_with_tactic::<_, Spine<u64, u64>, _>(arranged, "HashReduce", ProxyReduceTactic::new(HashReduce::new(count_ids)));
                reduced.stream.probe_with(&probe);
            } else {
                c.reduce(count).inner.probe_with(&probe);
            }
            input
        });

        let start = Instant::now();
        for k in 0..n {
            input.update((k, k % 5), 1);
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        let load = start.elapsed();

        // Warm up with unmeasured incremental rounds: the load leaves amortized
        // trace-maintenance debt (compaction-driven merges) that the first couple
        // thousand rounds pay down; the measured phase is steady state.
        let warmup = 5_000;
        let mut timer = Instant::now();
        for t in 1..=(warmup + rounds) {
            if t == warmup + 1 { timer = Instant::now(); }
            input.update((t % 97, 100 + t), 1);
            input.advance_to(t + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        (load, timer.elapsed())
    })
}

// F7 spike: the join twin of `HashReduce`. Both inputs are pre-mapped to `(hash(K), V)`, so the key
// IS the key_hash and the value IS the value_id. `present` is the `read_hash` scan (stored order ==
// bridge order), and `cross` needs NO resolution map — the value_id it is handed is the value, so it
// pairs `(l.value, r.value)` directly. Real keys are dropped; the output carries the key_hash in
// their place. `hash(K)` collisions would cross unrelated groups (risked; the bench's hash is
// bijective, so none). See DESIGN.md F7.
struct HashJoin;

impl ProxyJoinBackend<RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>> for HashJoin {
    type R0 = isize;
    type R1 = isize;
    type ROut = isize;
    type Output = Vec<(JoinOut, u64, isize)>;

    fn present0(&mut self, instance: &JoinInstance<'_, RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>>, filter: Option<&[u64]>) -> ProxyBridge<u64, isize> {
        present_hash(instance.batches0, filter, instance.lower)
    }

    fn present1(&mut self, instance: &JoinInstance<'_, RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>>, filter: Option<&[u64]>) -> ProxyBridge<u64, isize> {
        present_hash(instance.batches1, filter, instance.lower)
    }

    fn cross(&mut self, _instance: &JoinInstance<'_, RefBatch<u64, u64, u64, isize>, RefBatch<u64, u64, u64, isize>>, left: &[(u64, u64)], right: &[(u64, u64)], times: Vec<u64>, diffs: Vec<isize>) -> Self::Output {
        let mut out = Vec::with_capacity(left.len());
        for (((&l, &r), t), d) in left.iter().zip(right).zip(times).zip(diffs) {
            // value_id IS the value; the key_hash stands in for the (dropped) real key.
            out.push(((l.0, (l.1, r.1)), t, d));
        }
        out
    }
}

// Scan a hash-native side into a sorted+consolidated bridge: advance loaded times to `lower`, then
// consolidate (which restores `((key_hash, value_id), time)` order). No hashing, no re-sort.
fn present_hash(batches: &[RefBatch<u64, u64, u64, isize>], filter: Option<&[u64]>, lower: timely::progress::frontier::AntichainRef<'_, u64>) -> ProxyBridge<u64, isize> {
    use differential_dataflow::lattice::Lattice;
    let mut recs = Vec::new();
    read_hash(batches, filter, &mut recs);
    for r in recs.iter_mut() { r.1.advance_by(lower); }
    consolidate_updates(&mut recs);
    recs
}

fn bench_join(n: u64, rounds: u64, mode: u8) -> (std::time::Duration, std::time::Duration) {
    use std::time::Instant;
    use timely::dataflow::operators::probe::{Handle, Probe};

    timely::execute_directly(move |worker| {
        let probe = Handle::new();
        let (mut left, mut right) = worker.dataflow::<u64, _, _>(|scope| {
            let (left, c0) = scope.new_collection::<(u64, u64), isize>();
            let (right, c1) = scope.new_collection::<(u64, u64), isize>();
            if mode == 1 {
                let a0 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c0.inner, Pipeline, "Arrange0");
                let a1 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c1.inner, Pipeline, "Arrange1");
                let tactic = ProxyJoinTactic::new(VecJoinBackend::new(|k: &u64, v0: &u64, v1: &u64| (*k, (*v0, *v1))));
                let joined = join_with_tactic::<_, _, _, Vec<(JoinOut, u64, isize)>>(a0, a1, tactic);
                joined.probe_with(&probe);
            } else if mode == 2 {
                // Hash-native: pre-map both sides' keys to their hash (values already are ids), so the
                // arrangements are stored in bridge order and `present` scans instead of re-sorting.
                let h0 = c0.map(|(k, v)| (k.wrapping_mul(0x9E3779B97F4A7C15), v));
                let h1 = c1.map(|(k, v)| (k.wrapping_mul(0x9E3779B97F4A7C15), v));
                let a0 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(h0.inner, Pipeline, "ArrangeHash0");
                let a1 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(h1.inner, Pipeline, "ArrangeHash1");
                let joined = join_with_tactic::<_, _, _, Vec<(JoinOut, u64, isize)>>(a0, a1, ProxyJoinTactic::new(HashJoin));
                joined.probe_with(&probe);
            } else {
                c0.join(c1).inner.probe_with(&probe);
            }
            (left, right)
        });

        let start = Instant::now();
        for k in 0..n {
            left.update((k, k), 1);
            right.update((k, 2 * k), 1);
        }
        left.advance_to(1);
        right.advance_to(1);
        left.flush();
        right.flush();
        while probe.less_than(left.time()) {
            worker.step();
        }
        let load = start.elapsed();

        // Warm up with unmeasured incremental rounds (see bench_reduce).
        let warmup = 5_000;
        let mut timer = Instant::now();
        for t in 1..=(warmup + rounds) {
            if t == warmup + 1 { timer = Instant::now(); }
            left.update((t % 97, 100 + t), 1);
            left.advance_to(t + 1);
            right.advance_to(t + 1);
            left.flush();
            right.flush();
            while probe.less_than(left.time()) {
                worker.step();
            }
        }
        (load, timer.elapsed())
    })
}

// Profiling target: the hash-reduce LOAD only (no warmup/steady), repeated so a sampler gathers
// enough samples to attribute the ~2.9x residual. `mode`: 0=row, 2=hash. Run under samply:
//   cargo test --release --test int_proxy prof_reduce_load -- --ignored --nocapture
#[test]
#[ignore = "profiling target; run under a sampler"]
fn prof_reduce_load() {
    use std::time::Instant;
    use timely::dataflow::operators::probe::{Handle, Probe};
    let mode: u8 = std::env::var("PROF_MODE").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    let n: u64 = std::env::var("PROF_N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let iters: u64 = std::env::var("PROF_ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(40);
    let mut total = std::time::Duration::ZERO;
    for _ in 0..iters {
        let elapsed = timely::execute_directly(move |worker| {
            let probe = Handle::new();
            let mut input = worker.dataflow::<u64, _, _>(|scope| {
                let (input, c) = scope.new_collection::<(u64, u64), isize>();
                let count = |_k: &u64, input: &[(&u64, isize)], output: &mut Vec<(isize, isize)>| {
                    let count: isize = input.iter().map(|(_, d)| *d).sum();
                    if count > 0 { output.push((count, 1)); }
                };
                if mode == 2 {
                    let hashed = c.map(|(k, v)| (k.wrapping_mul(0x9E3779B97F4A7C15), v));
                    let arranged = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(hashed.inner, Pipeline, "ArrangeHash");
                    let count_ids = |vals: &[(u64, isize)]| { let s: isize = vals.iter().map(|(_, d)| *d).sum(); if s > 0 { vec![(s as u64, 1)] } else { Vec::new() } };
                    let reduced = reduce_with_tactic::<_, Spine<u64, u64>, _>(arranged, "HashReduce", ProxyReduceTactic::new(HashReduce::new(count_ids)));
                    reduced.stream.probe_with(&probe);
                } else {
                    c.reduce(count).inner.probe_with(&probe);
                }
                input
            });
            let start = Instant::now();
            for k in 0..n { input.update((k, k % 5), 1); }
            input.advance_to(1);
            input.flush();
            while probe.less_than(input.time()) { worker.step(); }
            start.elapsed()
        });
        total += elapsed;
    }
    eprintln!("prof_reduce_load mode={mode} n={n} iters={iters}: total {total:.2?}, per-load {:.2?}", total / iters as u32);
}

// Profiling target: the hash-JOIN load only (no warmup/steady), repeated. `mode`: 0=row, 2=hash.
//   cargo test --release --test int_proxy prof_join_load -- --ignored --nocapture
#[test]
#[ignore = "profiling target; run under a sampler"]
fn prof_join_load() {
    use std::time::Instant;
    use timely::dataflow::operators::probe::{Handle, Probe};
    let mode: u8 = std::env::var("PROF_MODE").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    let n: u64 = std::env::var("PROF_N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let iters: u64 = std::env::var("PROF_ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(40);
    // Records per key per side. >=16 on both sides fires `join_key`'s replay wave (the per-key
    // `IdHistory` path); 1 (default) takes the naive cross and never allocates per key.
    let fanout: u64 = std::env::var("PROF_FANOUT").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let groups = (n / fanout).max(1);
    let mut total = std::time::Duration::ZERO;
    for _ in 0..iters {
        let elapsed = timely::execute_directly(move |worker| {
            let probe = Handle::new();
            let (mut left, mut right) = worker.dataflow::<u64, _, _>(|scope| {
                let (left, c0) = scope.new_collection::<(u64, u64), isize>();
                let (right, c1) = scope.new_collection::<(u64, u64), isize>();
                if mode == 2 {
                    let h0 = c0.map(|(k, v)| (k.wrapping_mul(0x9E3779B97F4A7C15), v));
                    let h1 = c1.map(|(k, v)| (k.wrapping_mul(0x9E3779B97F4A7C15), v));
                    let a0 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(h0.inner, Pipeline, "ArrangeHash0");
                    let a1 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(h1.inner, Pipeline, "ArrangeHash1");
                    let joined = join_with_tactic::<_, _, _, Vec<(JoinOut, u64, isize)>>(a0, a1, ProxyJoinTactic::new(HashJoin));
                    joined.probe_with(&probe);
                } else {
                    c0.join(c1).inner.probe_with(&probe);
                }
                (left, right)
            });
            let start = Instant::now();
            // key = k % groups (so each key gets `fanout` distinct values per side); value = k (distinct).
            for k in 0..n { left.update((k % groups, k), 1); right.update((k % groups, n + k), 1); }
            left.advance_to(1);
            right.advance_to(1);
            left.flush();
            right.flush();
            while probe.less_than(left.time()) { worker.step(); }
            start.elapsed()
        });
        total += elapsed;
    }
    eprintln!("prof_join_load mode={mode} n={n} iters={iters}: total {total:.2?}, per-load {:.2?}", total / iters as u32);
}

#[test]
#[ignore = "wall-clock benchmark; run --release with --ignored --nocapture"]
fn bench_wall_clock_vs_row() {
    for &n in &[10_000u64, 100_000, 1_000_000] {
        let rounds = 1_000;
        let (row_l, row_i) = bench_reduce(n, rounds, 0);
        let (px_l, px_i) = bench_reduce(n, rounds, 1);
        let (hx_l, hx_i) = bench_reduce(n, rounds, 2);
        eprintln!(
            "reduce n={n:>9}: load row {row_l:>10.2?} proxy {px_l:>10.2?} ({:>5.2}x) hash {hx_l:>10.2?} ({:>5.2}x) | {rounds} steady rounds row {row_i:>9.2?} proxy {px_i:>9.2?} hash {hx_i:>9.2?}",
            px_l.as_secs_f64() / row_l.as_secs_f64(),
            hx_l.as_secs_f64() / row_l.as_secs_f64(),
        );
        let (row_l, row_i) = bench_join(n, rounds, 0);
        let (px_l, px_i) = bench_join(n, rounds, 1);
        let (hx_l, hx_i) = bench_join(n, rounds, 2);
        eprintln!(
            "join   n={n:>9}: load row {row_l:>10.2?} proxy {px_l:>10.2?} ({:>5.2}x) hash {hx_l:>10.2?} ({:>5.2}x) | {rounds} steady rounds row {row_i:>9.2?} proxy {px_i:>9.2?} hash {hx_i:>9.2?}",
            px_l.as_secs_f64() / row_l.as_secs_f64(),
            hx_l.as_secs_f64() / row_l.as_secs_f64(),
        );
    }
}

// ---------------------------------------------------------------------------
// Scaling: one key, many distinct times, in a single batch (the shapes of the
// row suite's `reduce_scaling` / `join_scaling` tests). These are linear-ish
// only if the tactics replay histories with meet-advancement; a per-moment
// rescan or a naive per-key cross product is quadratic and would hang here.
// ---------------------------------------------------------------------------

#[test]
fn proxy_reduce_scaling() {
    use timely::dataflow::operators::ToStream;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::vec::Map;
    use differential_dataflow::AsCollection;

    let scale = 100_000u64;
    let data = timely::example(move |scope| {
        let arranged = arrange_core::<_, _, Chunker<(), ()>, Batcher<(), ()>, Bldr<(), ()>, Spine<(), ()>>(
            (0..1)
                .to_stream(scope)
                .flat_map(move |_| (0..scale).map(|i| ((((), ())), i, 1)))
                .as_collection()
                .inner,
            Pipeline,
            "Arrange",
        );
        let count = |_k: &(), input: &[(&(), isize)], output: &mut Vec<(isize, isize)>| {
            let c: isize = input.iter().map(|(_, d)| *d).sum();
            if c > 0 {
                output.push((c, 1));
            }
        };
        let tactic = ProxyReduceTactic::new(VecReduceBackend::new(count));
        let reduced = reduce_with_tactic::<_, Spine<(), isize>, _>(arranged, "ProxyReduce", tactic);
        reduced.as_collection(|_k, v| *v).inner.capture()
    });
    let extracted: Vec<_> = data.extract();
    assert_eq!(extracted.len(), 1);
}

#[test]
fn proxy_join_scaling() {
    use timely::dataflow::operators::ToStream;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::vec::Map;
    use differential_dataflow::AsCollection;

    let scale = 100_000u64;
    let data = timely::example(move |scope| {
        let counts = (0..1)
            .to_stream(scope)
            .flat_map(move |_| (0..scale).map(|i| ((), i, 1)))
            .as_collection()
            .count();
        let odds = counts.clone().filter(|x| x.1 % 2 == 1).inner;
        let evens = counts.filter(|x| x.1 % 2 == 0).inner;
        let a0 = arrange_core::<_, _, Chunker<(), isize>, Batcher<(), isize>, Bldr<(), isize>, Spine<(), isize>>(odds, Pipeline, "Odds");
        let a1 = arrange_core::<_, _, Chunker<(), isize>, Batcher<(), isize>, Bldr<(), isize>, Spine<(), isize>>(evens, Pipeline, "Evens");
        let tactic = ProxyJoinTactic::new(VecJoinBackend::new(|_k: &(), v0: &isize, v1: &isize| (*v0, *v1)));
        join_with_tactic::<_, _, _, Vec<((isize, isize), u64, isize)>>(a0, a1, tactic).capture()
    });
    // Odd and even counts are never simultaneously present: the join is empty.
    let total: isize = data
        .extract()
        .iter()
        .flat_map(|(_, v)| v.iter().map(|(_, _, d)| *d))
        .sum();
    assert_eq!(total, 0);
}

/// The formal model's `scenario1_cancels`, realized against a compacted trace (the SCC
/// field bug): compaction legally advances a stored `+1` onto a novel `−1`'s exact
/// `(value, time)`, so the two cancel in the merged input presentation — but the batch
/// still owes a change there (the standing output must be retracted). Interesting-time
/// seeds must come from the batch's own support (`seedSet = b.support ∪ pending`), never
/// from a time-filter over the merged view.
#[test]
fn proxy_reduce_seeds_survive_compaction_cancellation() {
    let count = |_k: &u64, input: &[(&u64, isize)], output: &mut Vec<(isize, isize)>| {
        let c: isize = input.iter().map(|(_, d)| *d).sum();
        if c > 0 {
            output.push((c, 1));
        }
    };
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(count));

    // Retire 1: (key 1, value 10) +1 at (0,1); interval [(0,0), {(1,1)}).
    let batch1 = pbatch(vec![((1u64, 10u64), pt(0, 1), 1)], vec![pt(0, 0)], vec![pt(1, 1)]);
    let lower1 = Antichain::from_elem(pt(0, 0));
    let upper1 = Antichain::from_elem(pt(1, 1));
    let held1 = Antichain::from_elem(pt(0, 1));
    let (produced1, _) = tactic.retire(vec![], vec![], vec![batch1], &lower1, &upper1, &held1);
    assert_eq!(produced1.len(), 1);
    assert_eq!(pbatch_rows(&produced1[0].1), vec![((1, 1isize), pt(0, 1), 1)]);

    // Between retires the trace compacts to since = {(1,1)} — legal, since every later
    // evaluation time is at or beyond it. advance_by maps (0,1) → (1,1) in both the
    // source and the output trace.
    let source_compacted = pbatch(vec![((1u64, 10u64), pt(1, 1), 1)], vec![pt(0, 0)], vec![pt(1, 1)]);
    let output_compacted = pbatch(vec![((1u64, 1isize), pt(1, 1), 1)], vec![pt(0, 0)], vec![pt(1, 1)]);

    // Retire 2: the novel batch retracts the input at exactly (1,1) — the time the
    // compacted stored record now occupies. Merged and consolidated, they cancel; the
    // batch support still holds (1,1), and the owed output retraction hangs on it.
    let batch2 = pbatch(vec![((1u64, 10u64), pt(1, 1), -1)], vec![pt(1, 1)], vec![]);
    let lower2 = upper1.clone();
    let upper2 = Antichain::new();
    let held2 = Antichain::from_elem(pt(1, 1));
    let (produced2, frontier2) = tactic.retire(
        vec![source_compacted],
        vec![output_compacted],
        vec![batch2],
        &lower2,
        &upper2,
        &held2,
    );
    let rows: Vec<_> = produced2.iter().flat_map(|(_, b)| pbatch_rows(b)).collect();
    assert_eq!(
        rows,
        vec![((1, 1isize), pt(1, 1), -1)],
        "the standing count must be retracted at (1,1); an empty result means the seed was cancelled out of the merged presentation"
    );
    assert!(frontier2.is_empty());
}

// ---------------------------------------------------------------------------
// SCC: the non-Abelian reduce tactic inside a doubly-nested product-timed fixpoint —
// our best exemplar for smoking out interesting-time logic. The inner label-propagation
// `reduce` is run via the proxy tactic and via the stock row reduce (the trusted oracle),
// and the two whole-SCC outputs are asserted equal over incremental edge rounds.
// ---------------------------------------------------------------------------
mod scc_proxy {
    use super::VecReduceBackend;
    use differential_dataflow::operators::int_proxy::ProxyReduceTactic;
    use differential_dataflow::operators::arrange::arrangement::arrange_core;
    use differential_dataflow::operators::reduce::reduce_with_tactic;
    use differential_dataflow::trace::chunk::vec::{ChunkBatcher, ChunkBuilder, ChunkSpine, VecChunk};
    use differential_dataflow::trace::implementations::chunker::ContainerChunker;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::input::Input;
    use differential_dataflow::VecCollection;
    use differential_dataflow::operators::*;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::progress::Timestamp;
    use rand::{Rng, SeedableRng, StdRng};

    type Node = usize;
    type Edge = (Node, Node);

    type PChunker<T> = ContainerChunker<VecChunk<Node, Node, T, isize>>;
    type PBatcher<T> = ChunkBatcher<Node, Node, T, isize>;
    type PBldr<T> = ChunkBuilder<Node, Node, T, isize>;
    type PSpine<T> = ChunkSpine<Node, Node, T, isize>;

    fn min_label(_k: &Node, s: &[(&Node, isize)], t: &mut Vec<(Node, isize)>) {
        // Empty-safe (the reduce contract may call logic with empty input when output stands —
        // the retract case — which the proxy tactic exercises and the stock path here happens not to).
        if let Some(first) = s.first() {
            t.push((*first.0, 1));
        }
    }

    // Forward min-label propagation; the inner `reduce` is the tactic under test.
    fn propagate<'scope, T>(edges: VecCollection<'scope, T, Edge>, nodes: VecCollection<'scope, T, (Node, Node)>, proxy: bool) -> VecCollection<'scope, T, (Node, Node)>
    where
        T: Timestamp + Lattice + Ord,
    {
        let seed = nodes.clone();
        nodes.filter(|_| false).iterate(move |scope, inner| {
            let edges = edges.enter(scope);
            let seed = seed.enter(scope);
            let combined = inner.join_map(edges, |&_n, &l, &d| (d, l)).concat(seed);
            if proxy {
                let arranged = arrange_core::<_, _, PChunker<_>, PBatcher<_>, PBldr<_>, PSpine<_>>(combined.inner, Pipeline, "prop-arrange");
                let reduced = reduce_with_tactic::<_, PSpine<_>, _>(arranged, "PropProxy", ProxyReduceTactic::new(VecReduceBackend::new(min_label)));
                reduced.as_collection(|&k, &v| (k, v))
            } else {
                combined.reduce(min_label)
            }
        })
    }

    // Keep only edges whose endpoints share a label; twice (forward then transposed) converges on SCC edges.
    fn trim_edges<'scope, T>(cycle: VecCollection<'scope, T, Edge>, edges: VecCollection<'scope, T, Edge>, proxy: bool) -> VecCollection<'scope, T, Edge>
    where
        T: Timestamp + Lattice + Ord,
    {
        let nodes = edges.clone().map(|(_a, b)| (b, b));
        let labels = propagate(cycle, nodes, proxy);
        edges.join_map(labels.clone(), |&src, &dst, &l1| (dst, (src, l1)))
             .join_map(labels, |&dst, &(src, l1), &l2| ((src, dst), (l1, l2)))
             .filter(|(_, (l1, l2))| l1 == l2)
             .map(|((src, dst), _)| (dst, src))
    }

    fn scc<'scope, T>(graph: VecCollection<'scope, T, Edge>, proxy: bool) -> VecCollection<'scope, T, Edge>
    where
        T: Timestamp + Lattice + Ord,
    {
        let graph2 = graph.clone();
        graph.iterate(move |scope, inner| {
            let edges = graph2.enter(scope);
            let trans = edges.clone().map(|(a, b)| (b, a));
            let trimmed = trim_edges(inner.clone(), edges, proxy);
            trim_edges(trimmed, trans, proxy)
        })
    }

    fn scc_differential(nodes: usize, edges: usize, rounds: usize, seed: &[usize]) {
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);
        let mut edge_list: Vec<((usize, usize), usize, isize)> = Vec::new();
        for _ in 0..edges { edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1)); }
        for round in 1..rounds {
            edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
            edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round, -1));
        }
        timely::execute_directly(move |worker| {
            let mut edges_list = edge_list.clone();
            let mut edges = worker.dataflow::<usize, _, _>(|scope| {
                let (edge_input, edges) = scope.new_collection();
                let via_stock = scc(edges.clone(), false);
                let via_proxy = scc(edges, true);
                via_stock.assert_eq(via_proxy);
                edge_input
            });
            edges_list.sort_by(|x, y| y.1.cmp(&x.1));
            let mut round = 0;
            while !edges_list.is_empty() {
                while edges_list.last().map(|x| x.1) == Some(round) { let ((s, t), _, d) = edges_list.pop().unwrap(); edges.update((s, t), d); }
                round += 1;
                edges.advance_to(round);
            }
        });
    }

    #[test] fn scc_proxy_tiny_a() { scc_differential(6, 12, 4, &[1, 2, 3, 4]); }
    #[test] fn scc_proxy_tiny_b() { scc_differential(8, 16, 4, &[5, 6, 7, 8]); }
    #[test] fn scc_proxy_12_24()  { scc_differential(12, 24, 5, &[2, 4, 6, 8]); }
}
