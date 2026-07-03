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

    fn key_hashes(&self, batches: &[B1]) -> Vec<u64> {
        self.inner.key_hashes(batches)
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
        let mut probe = Handle::new();
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
        let mut probe = Handle::new();
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
