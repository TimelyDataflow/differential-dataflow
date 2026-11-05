//! The proxy half-join tactic over the vec backend: in a dataflow, against the cursor half join on
//! the same random inputs, strict and not, at one and two workers, with the backend presenting
//! blocks of several sizes; alone, against the definition over product times; and wrapped in
//! differential's `BackendFrontier`, against the definition over arranged times advanced to the
//! frontier, at product times and over the pentagon N5.

use std::sync::{Arc, Mutex};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::ToStream;
use timely::progress::Antichain;

use differential_dataflow::AsCollection;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::vec::{ChunkBatcher as VChunkBatcher, ChunkSpine as VChunkSpine, VecChunk};

use differential_dogs3::operators::half_join::half_join_with_tactic;
use differential_dogs3::operators::half_join::cursors::{half_join, BlobList};
use differential_dogs3::operators::half_join::proxy::ProxyHalfJoinTactic;
use differential_dogs3::operators::half_join::vec_backend::{VBatch, VecHalfJoinBackend};

type Out = (((u64, u64, u64), u64), u64, i64);

/// Both half joins over the same inputs; their consolidated outputs.
fn both(stream: Vec<((u64, u64, u64), u64, i64)>, arranged: Vec<((u64, u64), u64, i64)>, strict: bool, workers: usize, window: usize) -> (Vec<Out>, Vec<Out>) {
    let cursor_out = Arc::new(Mutex::new(Vec::<Out>::new()));
    let proxy_out = Arc::new(Mutex::new(Vec::<Out>::new()));
    let (cs, ps) = (Arc::clone(&cursor_out), Arc::clone(&proxy_out));
    timely::execute(timely::Config::process(workers), move |worker| {
        let (cs, ps) = (Arc::clone(&cs), Arc::clone(&ps));
        let (stream, arranged) = if worker.index() == 0 { (stream.clone(), arranged.clone()) } else { (vec![], vec![]) };
        worker.dataflow::<u64, _, _>(|scope| {
            // Hold compaction back one step, so that "strictly before" and "at the same time" stay distinct.
            let frontier_func = |time: &u64, antichain: &mut Antichain<u64>| { antichain.insert(time.saturating_sub(1)); };
            let stream = stream.to_stream(scope).as_collection();
            let arranged = arranged.to_stream(scope).as_collection();

            half_join(stream.clone(), arranged.clone().arrange_by_key(), frontier_func, strict, |k: &u64, v: &u64, v2: &u64| (*k, *v, *v2))
                .inspect(move |(d, t, r)| cs.lock().unwrap().push((*d, *t, *r)));

            let hashed = arranged.map(|(k, v)| (k.hashed(), (k, v)));
            let arr = arrange_core::<_, Vec<((u64, (u64, u64)), u64, i64)>, _, VChunkSpine<u64, (u64, u64), u64, i64>>(
                hashed.inner, Exchange::new(|((h, _), _, _): &((u64, _), _, _)| *h), "Arrange", VChunkBatcher::new);
            let backend = VecHalfJoinBackend::with_window(|k: &u64, v: &u64, v2: &u64| (*k, *v, *v2), window);
            let pact = Exchange::new(|((k, _, _), _, _): &((u64, u64, u64), u64, i64)| k.hashed());
            half_join_with_tactic(stream.inner, arr, pact, frontier_func, |_, _| false, BlobList::new(strict), ProxyHalfJoinTactic::new(backend, strict))
                .as_collection()
                .inspect(move |(d, t, r)| ps.lock().unwrap().push((*d, *t, *r)));
        });
    }).unwrap().join();
    let (mut cursor, mut proxy) = (cursor_out.lock().unwrap().clone(), proxy_out.lock().unwrap().clone());
    consolidate_updates(&mut cursor);
    consolidate_updates(&mut proxy);
    (cursor, proxy)
}

#[test]
fn proxy_half_join_matches_cursor_half_join() {
    let mut random = 11u64;
    let mut next = move |n: u64| { random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (random >> 33) % n };
    for seed in 0..12 {
        let arranged: Vec<((u64, u64), u64, i64)> = (0..(1 + next(60))).map(|_| ((next(6), next(50)), next(5), if next(3) == 0 { -1 } else { 1 })).collect();
        let stream: Vec<((u64, u64, u64), u64, i64)> = (0..(1 + next(60))).map(|_| {
            let t = next(5);
            ((next(8), next(10), t + next(3)), t, if next(3) == 0 { -1 } else { 1 })
        }).collect();
        for strict in [false, true] {
            for (workers, window) in [(1, usize::MAX), (1, 2), (2, 3)] {
                let (cursor, proxy) = both(stream.clone(), arranged.clone(), strict, workers, window);
                assert!(!cursor.is_empty() || seed > 2, "seed {seed}: the inputs meet");
                assert_eq!(proxy, cursor, "seed {seed}, strict {strict}, {workers} workers, window {window}");
            }
        }
    }
}

/// Build a hash-keyed input batch from `((K, V), T, R)` rows: `((hash(K), (K, V)), T, R)`.
fn hbatch<K, V, T, R>(rows: Vec<((K, V), T, R)>) -> VBatch<(K, V), T, R>
where
    K: Hashable<Output: Into<u64>> + Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: differential_dataflow::lattice::Lattice + timely::progress::Timestamp,
    R: Ord + differential_dataflow::difference::Semigroup + 'static,
{
    use timely::container::PushInto;
    let mut hrows: Vec<((u64, (K, V)), T, R)> = rows.into_iter().map(|((k, v), t, r)| ((k.hashed().into(), (k, v)), t, r)).collect();
    consolidate_updates(&mut hrows);
    let mut chunk = VecChunk::default();
    for u in hrows { chunk.push_into(u); }
    std::rc::Rc::new(ChunkBatch::new(vec![chunk]))
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Collide(u64);
/// Every `Collide` hashes alike, so they share a key proxy and exercise the collision paths.
impl std::hash::Hash for Collide {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { state.write_u64(0); }
}

/// The proxy half-join tactic over the vec backend, against the definition: an update meets the
/// arranged rows of its key whose time precedes its own in the total (lexicographic) order on
/// product times, strictly or not, and carries its data time joined with the arranged time. Over
/// several block sizes, keys whose rows are many enough for the sorted path, and colliding hashes.
#[test]
fn half_join_meets_what_the_definition_meets() {
    use differential_dogs3::operators::half_join::HalfJoinTactic;
    type T = timely::order::Product<u64, u64>;
    let time = T::new;
    let mut random = 7u64;
    let mut next = move |n: u64| { random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (random >> 33) % n };
    for seed in 0..30 {
        // Keys below 6 collide pairwise through `Collide`; key 6 is dense enough for the sorted path.
        let key = |k: u64| Collide(k);
        let n_arr = 1 + next(40) as usize + if seed % 3 == 0 { 40 } else { 0 };
        let arranged: Vec<((Collide, u64), T, i64)> = (0..n_arr).map(|i| {
            let k = if seed % 3 == 0 && i >= n_arr - 40 { 6 } else { next(6) };
            ((key(k), next(100)), time(next(4), next(3)), if next(3) == 0 { -1 } else { 1 })
        }).collect();
        let n_str = 1 + next(40) as usize + if seed % 3 == 0 { 30 } else { 0 };
        let stream: Vec<((Collide, u64, T), T, i64)> = (0..n_str).map(|i| {
            let k = if seed % 3 == 0 && i >= n_str - 30 { 6 } else { next(8) };
            let t = time(next(4), next(3));
            let data = t.join(&time(next(4), next(3)));
            ((key(k), next(10), data), t, if next(3) == 0 { -1 } else { 1 })
        }).collect();
        // The batches: the arranged rows split in three, hash-keyed.
        let batches: Vec<_> = arranged.chunks(1 + n_arr / 3).map(|rows| hbatch(rows.to_vec())).collect();
        // The released containers: the stream split in a few.
        let chunks: Vec<Vec<_>> = stream.chunks(1 + n_str / 4).map(|rows| rows.to_vec()).collect();
        let lower: Antichain<T> = stream.iter().map(|u| u.1).collect();
        for strict in [false, true] {
            let mut expected: Vec<(((Collide, u64, u64), T), T, i64)> = Vec::new();
            for ((k, v, data), t, d) in &stream {
                for ((k2, v2), t2, d2) in &arranged {
                    if k != k2 { continue; }
                    if !(t2 < t || (!strict && t2 == t)) { continue; }
                    expected.push((((k.clone(), *v, *v2), data.join(t2)), *t, d * d2));
                }
            }
            consolidate_updates(&mut expected);
            for window in [1, 3, usize::MAX] {
                let backend = VecHalfJoinBackend::with_window(|k: &Collide, v: &u64, v2: &u64| (k.clone(), *v, *v2), window);
                let mut tactic = ProxyHalfJoinTactic::new(backend, strict);
                let mut got: Vec<_> = tactic.prep(chunks.clone(), batches.clone(), lower.clone()).flatten().collect();
                assert!(got.iter().all(|(_, t, _)| lower.less_equal(t)), "every output is covered by lower");
                consolidate_updates(&mut got);
                assert_eq!(got, expected, "seed {seed}, strict {strict}, window {window}");
            }
        }
    }
}

/// The wrapped backend: arranged rows stored before `since` are read advanced to it, which can
/// change their place in the total order against stream times beyond `since`.
mod frontier {
    use super::*;
    use differential_dataflow::operators::int_proxy::BackendFrontier;
    use differential_dogs3::operators::half_join::HalfJoinTactic;
    use timely::order::Product;
    use timely::progress::{Antichain, PathSummary, Timestamp};
    use timely::PartialOrder;

    type T2 = Product<u64, u64>;

    /// The definition, over arranged times advanced to `since`: `(((key, val, val2), data ∨ t2'), t, d · d2)`
    /// for each pair with `t2' < t` (or `≤`), consolidated.
    fn definition<T: Lattice + Timestamp>(
        stream: &[((Collide, u64, T), T, i64)],
        arranged: &[((Collide, u64), T, i64)],
        since: &Antichain<T>,
        strict: bool,
    ) -> Vec<(((Collide, u64, u64), T), T, i64)> {
        let mut expected = Vec::new();
        for ((k, v, data), t, d) in stream {
            for ((k2, v2), t2, d2) in arranged {
                if k != k2 { continue; }
                let mut t2 = t2.clone();
                t2.advance_by(since.borrow());
                if !(t2 < *t || (!strict && t2 == *t)) { continue; }
                expected.push((((k.clone(), *v, *v2), data.join(&t2)), t.clone(), d * d2));
            }
        }
        consolidate_updates(&mut expected);
        expected
    }

    /// The wrapped tactic's output for one unit, consolidated.
    fn wrapped<T: Lattice + Timestamp>(
        stream: &[((Collide, u64, T), T, i64)],
        arranged: &[((Collide, u64), T, i64)],
        since: &Antichain<T>,
        strict: bool,
        window: usize,
    ) -> Vec<(((Collide, u64, u64), T), T, i64)> {
        let backend = VecHalfJoinBackend::with_window(|k: &Collide, v: &u64, v2: &u64| (k.clone(), *v, *v2), window);
        let mut tactic = ProxyHalfJoinTactic::new(BackendFrontier::new(backend, since.clone()), strict);
        let batches: Vec<_> = arranged.chunks(1 + arranged.len() / 3).map(|rows| hbatch(rows.to_vec())).collect();
        let chunks: Vec<Vec<_>> = stream.chunks(1 + stream.len() / 4).map(|rows| rows.to_vec()).collect();
        let lower: Antichain<T> = stream.iter().map(|u| u.1.clone()).collect();
        let mut got: Vec<_> = tactic.prep(chunks, batches, lower).flatten().collect();
        consolidate_updates(&mut got);
        got
    }

    /// Advancing flips the total order: stored, `(0, 5)` precedes the stream time `(1, 0)`;
    /// advanced to `since = {(1, 0)}` it is `(1, 5)`, which follows it. So the stored reading
    /// meets, and the as-of reading does not, and the wrapper gives the latter in every unit,
    /// although the stream time is beyond `since`.
    #[test]
    fn advancing_flips_the_total_order() {
        let time = T2::new;
        let arranged = vec![((Collide(1), 7), time(0, 5), 1i64)];
        let stream = vec![((Collide(1), 3, time(1, 0)), time(1, 0), 1i64)];
        let since = Antichain::from_elem(time(1, 0));
        for strict in [true, false] {
            assert_eq!(wrapped(&stream, &arranged, &Antichain::new(), strict, usize::MAX).len(), 1, "stored: meets");
            assert_eq!(wrapped(&stream, &arranged, &since, strict, usize::MAX).len(), 0, "as of (1, 0): does not");
            assert_eq!(definition(&stream, &arranged, &since, strict).len(), 0);
        }
        // An arranged row before `since` that still precedes once advanced is met, and its data
        // time is joined with the advanced time: `(0, 5)` as of `{(1, 0)}` is `(1, 5)`.
        let later = vec![((Collide(1), 3, time(2, 0)), time(2, 0), 1i64)];
        let got = wrapped(&later, &arranged, &since, true, usize::MAX);
        assert_eq!(got, vec![(((Collide(1), 3, 7), time(2, 5)), time(2, 0), 1)]);
        assert_eq!(got, definition(&later, &arranged, &since, true));
    }

    /// Against the definition over product times: arranged rows stored at times on both sides of
    /// `since`, stream rows beyond it, strict and not, several block sizes, colliding hashes.
    #[test]
    fn reads_the_arrangement_as_of_since() {
        let time = T2::new;
        let mut random = 23u64;
        let mut next = move |n: u64| { random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (random >> 33) % n };
        for seed in 0..40 {
            let since = Antichain::from_elem(time(2, 1 + next(3)));
            let n_arr = 1 + next(50) as usize;
            let arranged: Vec<((Collide, u64), T2, i64)> = (0..n_arr).map(|_| {
                ((Collide(next(5)), next(6)), time(next(5), next(4)), if next(3) == 0 { -1 } else { 1 })
            }).collect();
            let n_str = 1 + next(30) as usize;
            let stream: Vec<((Collide, u64, T2), T2, i64)> = (0..n_str).map(|_| {
                // Beyond `since`: at or past its element.
                let t = since[0].join(&time(2 + next(3), next(4)));
                let data = t.join(&time(next(5), next(4)));
                ((Collide(next(6)), next(4), data), t, if next(3) == 0 { -1 } else { 1 })
            }).collect();
            for strict in [false, true] {
                let expected = definition(&stream, &arranged, &since, strict);
                for window in [1, 4, usize::MAX] {
                    assert_eq!(wrapped(&stream, &arranged, &since, strict, window), expected, "seed {seed}, strict {strict}, window {window}");
                }
            }
        }
    }

    /// The pentagon `0 < a < b < 1`, `0 < c < 1`, with `a`, `b` incomparable to `c`: the smallest
    /// non-distributive lattice. The derived `Ord` is a linear extension of the partial order.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, serde::Serialize, serde::Deserialize)]
    struct N5(u8);
    impl N5 {
        const ALL: [N5; 5] = [N5(0), N5(1), N5(2), N5(3), N5(4)];
        fn bounds(self, other: N5, upper: bool) -> impl Iterator<Item = N5> {
            N5::ALL.into_iter().filter(move |x| if upper { self.less_equal(x) && other.less_equal(x) } else { x.less_equal(&self) && x.less_equal(&other) })
        }
    }
    impl PartialOrder for N5 {
        fn less_equal(&self, other: &Self) -> bool {
            self == other || self.0 == 0 || other.0 == 4 || (self.0 == 1 && other.0 == 2)
        }
    }
    impl Lattice for N5 {
        fn join(&self, other: &Self) -> Self {
            self.bounds(*other, true).find(|x| self.bounds(*other, true).all(|y| x.less_equal(&y))).expect("a lattice")
        }
        fn meet(&self, other: &Self) -> Self {
            self.bounds(*other, false).find(|x| self.bounds(*other, false).all(|y| y.less_equal(x))).expect("a lattice")
        }
    }
    #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
    struct Stay;
    impl PartialOrder for Stay { fn less_equal(&self, _: &Self) -> bool { true } }
    impl PathSummary<N5> for Stay {
        fn results_in(&self, time: &N5) -> Option<N5> { Some(*time) }
        fn followed_by(&self, _: &Self) -> Option<Self> { Some(Stay) }
    }
    impl Timestamp for N5 {
        type Summary = Stay;
        fn minimum() -> Self { N5(0) }
    }

    /// Over N5, every frontier of one or two elements, arranged rows at every time, and stream
    /// rows at every time beyond the frontier: the wrapper agrees with the definition over
    /// advanced times exactly. Nothing here uses distributivity, and N5 has none.
    #[test]
    fn reads_as_of_a_frontier_in_n5() {
        let mut frontiers: Vec<Antichain<N5>> = N5::ALL.iter().map(|t| Antichain::from_elem(*t)).collect();
        for x in N5::ALL { for y in N5::ALL {
            if x < y && !x.less_equal(&y) && !y.less_equal(&x) { frontiers.push([x, y].into_iter().collect()); }
        } }
        let mut units = 0;
        for since in &frontiers {
            let arranged: Vec<((Collide, u64), N5, i64)> = N5::ALL.iter().enumerate()
                .flat_map(|(i, &t)| [((Collide(1), i as u64), t, 1i64), ((Collide(1), 9), t, if i % 2 == 0 { 1 } else { -1 }), ((Collide(2), i as u64), t, 1)])
                .collect();
            let stream: Vec<((Collide, u64, N5), N5, i64)> = N5::ALL.iter().filter(|t| since.less_equal(t))
                .flat_map(|&t| [((Collide(1), 0, t), t, 1i64), ((Collide(1), 1, t.join(&N5(3))), t, -1), ((Collide(2), 0, t), t, 1), ((Collide(3), 0, t), t, 1)])
                .collect();
            for strict in [false, true] {
                let expected = definition(&stream, &arranged, since, strict);
                for window in [1, usize::MAX] {
                    assert_eq!(wrapped(&stream, &arranged, since, strict, window), expected, "since {since:?}, strict {strict}, window {window}");
                    units += 1;
                }
            }
        }
        assert_eq!(units, 4 * frontiers.len());
    }
}
