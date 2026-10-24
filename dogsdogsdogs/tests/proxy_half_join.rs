//! The proxy half-join tactic over differential's vec backend, in a dataflow, against the cursor
//! half join on the same random inputs: strict and not, at one and two workers, with the backend
//! presenting blocks of several sizes.

use std::sync::{Arc, Mutex};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::ToStream;
use timely::progress::Antichain;

use differential_dataflow::AsCollection;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::int_proxy::ProxyHalfJoinTactic;
use differential_dataflow::operators::int_proxy::vec_backend::VecHalfJoinBackend;
use differential_dataflow::trace::chunk::vec::{ChunkBatcher as VChunkBatcher, ChunkSpine as VChunkSpine};

use differential_dogs3::operators::half_join::half_join_with_tactic;
use differential_dogs3::operators::half_join::cursors::{half_join, BlobList};

type Out = (((u64, u64, u64), u64), u64, i64);

/// Both half joins over the same inputs; their consolidated outputs.
fn both(stream: Vec<((u64, u64, u64), u64, i64)>, arranged: Vec<((u64, u64), u64, i64)>, strict: bool, workers: usize, window: usize) -> (Vec<Out>, Vec<Out>) {
    let cursor_out = Arc::new(Mutex::new(Vec::<Out>::new()));
    let proxy_out = Arc::new(Mutex::new(Vec::<Out>::new()));
    let (cs, ps) = (cursor_out.clone(), proxy_out.clone());
    timely::execute(timely::Config::process(workers), move |worker| {
        let (cs, ps) = (cs.clone(), ps.clone());
        let (stream, arranged) = if worker.index() == 0 { (stream.clone(), arranged.clone()) } else { (vec![], vec![]) };
        worker.dataflow::<u64, _, _>(|scope| {
            // Hold compaction back one step, so that "strictly before" and "at the same time" stay distinct.
            let frontier_func = |time: &u64, antichain: &mut Antichain<u64>| { antichain.insert(time.saturating_sub(1)); };
            let stream = stream.to_stream(scope).as_collection();
            let arranged = arranged.to_stream(scope).as_collection();

            half_join(stream.clone(), arranged.clone().arrange_by_key(), frontier_func, strict, |k: &u64, v: &u64, v2: &u64| (*k, *v, *v2))
                .inspect(move |(d, t, r)| cs.lock().unwrap().push((d.clone(), *t, *r)));

            let hashed = arranged.map(|(k, v)| (k.hashed(), (k, v)));
            let arr = arrange_core::<_, Vec<((u64, (u64, u64)), u64, i64)>, _, VChunkSpine<u64, (u64, u64), u64, i64>>(
                hashed.inner, Exchange::new(|((h, _), _, _): &((u64, _), _, _)| *h), "Arrange", VChunkBatcher::new);
            let backend = VecHalfJoinBackend::with_window(|k: &u64, v: &u64, v2: &u64| (*k, *v, *v2), window);
            let pact = Exchange::new(|((k, _, _), _, _): &((u64, u64, u64), u64, i64)| k.hashed());
            half_join_with_tactic(stream.inner, arr, pact, frontier_func, |_, _| false, BlobList::new(strict), ProxyHalfJoinTactic::new(backend, strict))
                .as_collection()
                .inspect(move |(d, t, r)| ps.lock().unwrap().push((d.clone(), *t, *r)));
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
