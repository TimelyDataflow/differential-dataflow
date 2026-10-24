//! The proxy half-join tactic over the vec backend: in a dataflow, against the cursor half join on
//! the same random inputs, strict and not, at one and two workers, with the backend presenting
//! blocks of several sizes; and alone, against the definition over product times.

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
