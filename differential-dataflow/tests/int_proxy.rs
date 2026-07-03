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

use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::Capture;
use timely::order::Product;
use timely::PartialOrder;
use timely::progress::Antichain;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::int_proxy::reference::{RefBatch, VecJoinBackend, VecReduceBackend};
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};
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
                let joined = join_with_tactic::<_, _, _, CapacityContainerBuilder<Vec<(JoinOut, u64, isize)>>>(a0, a1, tactic);
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

use differential_dataflow::operators::int_proxy::{ProxyChunk, ProxyJoinBackend, ProxyReduceBackend};
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

    fn present_novel(&mut self, novel: &[B1]) -> ProxyChunk<B1::Time, B::RIn> {
        let chunk = self.inner.present_novel(novel);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn present_input(&mut self, history: &[B1], novel: &[B1], keys: &[u64]) -> ProxyChunk<B1::Time, B::RIn> {
        let chunk = self.inner.present_input(history, novel, keys);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn present_output(&mut self, batches: &[B2], keys: &[u64]) -> ProxyChunk<B1::Time, B::ROut> {
        let chunk = self.inner.present_output(batches, keys);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn reduce(&mut self, key_hash: u64, input: &[(usize, B::RIn)]) -> Vec<(u64, B::ROut)> {
        self.inner.reduce(key_hash, input)
    }
    fn reduce_many(&mut self, keys: &[u64], ends: &[usize], input: &[(usize, B::RIn)]) -> (Vec<(u64, B::ROut)>, Vec<usize>) {
        self.inner.reduce_many(keys, ends, input)
    }
    fn materialize(&mut self, records: ProxyChunk<B1::Time, B::ROut>, description: Description<B1::Time>) -> B2 {
        self.inner.materialize(records, description)
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

    fn present0(&mut self, batches: &[B0], filter: Option<&[u64]>) -> ProxyChunk<B0::Time, B::R0> {
        let chunk = self.inner.present0(batches, filter);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn present1(&mut self, batches: &[B1], filter: Option<&[u64]>) -> ProxyChunk<B0::Time, B::R1> {
        let chunk = self.inner.present1(batches, filter);
        self.presented.fetch_add(chunk.len(), AtomicOrdering::SeqCst);
        chunk
    }
    fn cross(&mut self, left: &[usize], right: &[usize], times: Vec<B0::Time>, diffs: Vec<B::ROut>) -> B::Output {
        self.inner.cross(left, right, times, diffs)
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
            let joined = join_with_tactic::<_, _, _, CapacityContainerBuilder<Vec<(JoinOut, u64, isize)>>>(a0, a1, ProxyJoinTactic::new(backend));
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
// minimal arrangement is a batch of `ProxyChunk` itself — the proxy data IS the data.
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
    use differential_dataflow::operators::int_proxy::{ProxyChunk, ProxyReduceBackend};
    use differential_dataflow::trace::Description;
    use differential_dataflow::trace::chunk::ChunkBatch;
    use differential_dataflow::lattice::Lattice;
    use timely::progress::Timestamp;

    pub type Batch<T> = Rc<ChunkBatch<ProxyChunk<T, isize>>>;

    /// The identity backend: batches are proxy chunks, values are the ids themselves.
    pub struct IdentityReduce<T, L> {
        pub logic: L,
        /// The live input presentation, for representative-index → id resolution.
        current: ProxyChunk<T, isize>,
    }

    impl<T: Timestamp + Lattice, L> IdentityReduce<T, L> {
        pub fn new(logic: L) -> Self {
            IdentityReduce { logic, current: ProxyChunk::default() }
        }
    }

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

        fn present_novel(&mut self, novel: &[Batch<T>]) -> ProxyChunk<T, isize> {
            let (ks, vs, ts, ds) = records(novel, None);
            ProxyChunk::from_unsorted(ks, vs, ts, ds).0
        }

        fn present_input(&mut self, history: &[Batch<T>], novel: &[Batch<T>], keys: &[u64]) -> ProxyChunk<T, isize> {
            let (mut ks, mut vs, mut ts, mut ds) = records(history, Some(keys));
            let (k2, v2, t2, d2) = records(novel, Some(keys));
            ks.extend(k2);
            vs.extend(v2);
            ts.extend(t2);
            ds.extend(d2);
            let (chunk, _reps) = ProxyChunk::from_unsorted(ks, vs, ts, ds);
            self.current = chunk.clone();
            chunk
        }

        fn present_output(&mut self, batches: &[Batch<T>], keys: &[u64]) -> ProxyChunk<T, isize> {
            let (ks, vs, ts, ds) = records(batches, Some(keys));
            ProxyChunk::from_unsorted(ks, vs, ts, ds).0
        }

        fn reduce(&mut self, _key_hash: u64, input: &[(usize, isize)]) -> Vec<(u64, isize)> {
            // Resolve representative indices to ids; the ids are the values.
            let pairs: Vec<(u64, isize)> = input.iter().map(|&(i, d)| (self.current.value_ids()[i], d)).collect();
            (self.logic)(&pairs)
        }

        // Override the batched form (a stand-in for one bulk crossing into columnar
        // logic), asserting the bracket protocol from the backend's seat, so the fuzz
        // exercises the wave-batched path rather than the per-key default.
        fn reduce_many(&mut self, keys: &[u64], ends: &[usize], input: &[(usize, isize)]) -> (Vec<(u64, isize)>, Vec<usize>) {
            assert_eq!(keys.len(), ends.len(), "one bracket end per key");
            assert!(ends.windows(2).all(|w| w[0] < w[1]), "brackets non-empty and increasing");
            assert_eq!(ends.last().copied().unwrap_or(0), input.len(), "brackets cover the input");
            let mut outs = Vec::new();
            let mut out_ends = Vec::with_capacity(keys.len());
            let mut start = 0;
            for &end in ends {
                let pairs: Vec<(u64, isize)> = input[start..end].iter().map(|&(i, d)| (self.current.value_ids()[i], d)).collect();
                outs.extend((self.logic)(&pairs));
                out_ends.push(outs.len());
                start = end;
            }
            (outs, out_ends)
        }

        fn materialize(&mut self, chunk: ProxyChunk<T, isize>, description: Description<T>) -> Batch<T> {
            // The proxy records ARE the output records: the output arrangement is proxy
            // chunks (incidentally exercising `ChunkBatch<ProxyChunk>` as a real batch).
            let chunks = if chunk.is_empty() { Vec::new() } else { vec![chunk] };
            Rc::new(ChunkBatch::new(chunks, description))
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
fn drive_identity_reduce_compacting<L>(
    rounds: &[(Antichain<PT>, Vec<((u64, u64), PT, isize)>)],
    logic: L,
    compact: bool,
) -> Vec<((u64, u64), PT, isize)>
where
    L: FnMut(&[(u64, isize)]) -> Vec<(u64, isize)>,
{
    use differential_dataflow::operators::int_proxy::ProxyChunk;
    use differential_dataflow::trace::chunk::ChunkBatch;

    #[allow(unused_imports)]
    use differential_dataflow::trace::BatchReader as _;

    let mut tactic = ProxyReduceTactic::new(identity::IdentityReduce::new(logic));
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
            let (chunk, _) = ProxyChunk::from_unsorted(ks, vs, ts, ds);
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
                    let (chunk, _) = ProxyChunk::from_unsorted(ks, vs, ts, ds);
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
        let produced = drive_identity_reduce_compacting(&rounds, logic, compact);

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

fn bench_reduce(n: u64, rounds: u64, proxy: bool) -> (std::time::Duration, std::time::Duration) {
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
            if proxy {
                let arranged = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c.inner, Pipeline, "Arrange");
                let reduced = reduce_with_tactic::<_, Spine<u64, isize>, _>(arranged, "ProxyReduce", ProxyReduceTactic::new(VecReduceBackend::new(count)));
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

fn bench_join(n: u64, rounds: u64, proxy: bool) -> (std::time::Duration, std::time::Duration) {
    use std::time::Instant;
    use timely::dataflow::operators::probe::{Handle, Probe};

    timely::execute_directly(move |worker| {
        let probe = Handle::new();
        let (mut left, mut right) = worker.dataflow::<u64, _, _>(|scope| {
            let (left, c0) = scope.new_collection::<(u64, u64), isize>();
            let (right, c1) = scope.new_collection::<(u64, u64), isize>();
            if proxy {
                let a0 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c0.inner, Pipeline, "Arrange0");
                let a1 = arrange_core::<_, _, Chunker<u64, u64>, Batcher<u64, u64>, Bldr<u64, u64>, Spine<u64, u64>>(c1.inner, Pipeline, "Arrange1");
                let tactic = ProxyJoinTactic::new(VecJoinBackend::new(|k: &u64, v0: &u64, v1: &u64| (*k, (*v0, *v1))));
                let joined = join_with_tactic::<_, _, _, CapacityContainerBuilder<Vec<(JoinOut, u64, isize)>>>(a0, a1, tactic);
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

#[test]
#[ignore = "wall-clock benchmark; run --release with --ignored --nocapture"]
fn bench_wall_clock_vs_row() {
    for &n in &[10_000u64, 100_000, 1_000_000] {
        let rounds = 1_000;
        let (row_l, row_i) = bench_reduce(n, rounds, false);
        let (px_l, px_i) = bench_reduce(n, rounds, true);
        eprintln!(
            "reduce n={n:>9}: load row {row_l:>10.2?} proxy {px_l:>10.2?} ({:>5.2}x) | {rounds} steady single-key rounds row {row_i:>10.2?} proxy {px_i:>10.2?} ({:>5.2}x)",
            px_l.as_secs_f64() / row_l.as_secs_f64(),
            px_i.as_secs_f64() / row_i.as_secs_f64(),
        );
        let (row_l, row_i) = bench_join(n, rounds, false);
        let (px_l, px_i) = bench_join(n, rounds, true);
        eprintln!(
            "join   n={n:>9}: load row {row_l:>10.2?} proxy {px_l:>10.2?} ({:>5.2}x) | {rounds} steady single-key rounds row {row_i:>10.2?} proxy {px_i:>10.2?} ({:>5.2}x)",
            px_l.as_secs_f64() / row_l.as_secs_f64(),
            px_i.as_secs_f64() / row_i.as_secs_f64(),
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
        join_with_tactic::<_, _, _, CapacityContainerBuilder<Vec<((isize, isize), u64, isize)>>>(a0, a1, tactic).capture()
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
