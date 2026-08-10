//! Tests for the proxy reduce tactic over the [`VecReduceBackend`] reference backend.
//!
//! The backend reads a HASH-KEYED arrangement (`VecChunk<u64, (K, V), _, _>`, key = hash(K)), so
//! the caller maps `(k, v) -> (hash(k), (k, v))` before arranging. Every dataflow test compares
//! against the cursor-tactic reduce on the same input; run in debug so the tactic's
//! `debug_assert_sorted_bridge` and window-contract assertions are live.

use std::rc::Rc;
use std::sync::{Arc, Mutex};

use timely::container::PushInto;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::ToStream;
use timely::order::Product;
use timely::progress::{Antichain, Timestamp};

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
use differential_dataflow::operators::int_proxy::vec_backend::VecReduceBackend;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::reduce::{reduce_with_tactic, ReduceTactic};
use differential_dataflow::trace::chunk::vec::{
    ChunkBatcher as VChunkBatcher, ChunkBuilder as VChunkBuilder, ChunkSpine as VChunkSpine, VecChunk,
};
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ContainerChunker;
use differential_dataflow::trace::{Description, Navigable};
use differential_dataflow::AsCollection;

type Batch<K, V, T, R> = Rc<ChunkBatch<VecChunk<K, V, T, R>>>;

/// Read a `u64`-keyed batch, dropping the hash key; returns `(value, time, diff)`.
fn hread<KV, T, R>(batches: &[Batch<u64, KV, T, R>]) -> Vec<(KV, T, R)>
where
    KV: Ord + Clone + 'static,
    T: Lattice + Timestamp,
    R: Ord + Semigroup + 'static,
{
    let mut out = Vec::new();
    for b in batches {
        for chunk in &b.chunks {
            let mut c = chunk.cursor();
            while c.key_valid(chunk) {
                while c.val_valid(chunk) {
                    let kv = c.val(chunk).clone();
                    c.map_times(chunk, |t, d| out.push((kv.clone(), t.clone(), d.clone())));
                    c.step_val(chunk);
                }
                c.step_key(chunk);
            }
        }
    }
    consolidate_updates(&mut out);
    out
}

/// Build a HASH-KEYED input batch from `((K, V), T, R)` rows: `((hash(K), (K, V)), T, R)`.
fn hbatch<K, V, T, R>(rows: Vec<((K, V), T, R)>, lower: T, upper: T) -> Batch<u64, (K, V), T, R>
where
    K: Hashable + Ord + Clone + 'static,
    K::Output: Into<u64>,
    V: Ord + Clone + 'static,
    T: Lattice + Timestamp,
    R: Ord + Semigroup + 'static,
{
    let mut hrows: Vec<((u64, (K, V)), T, R)> =
        rows.into_iter().map(|((k, v), t, r)| ((k.hashed().into(), (k, v)), t, r)).collect();
    consolidate_updates(&mut hrows);
    let mut chunk = VecChunk::default();
    for u in hrows {
        chunk.push_into(u);
    }
    let desc = Description::new(Antichain::from_elem(lower), Antichain::from_elem(upper), Antichain::from_elem(T::minimum()));
    Rc::new(ChunkBatch::new(vec![chunk], desc))
}

fn max_logic(_k: &u64, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>) {
    if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() {
        updates.push((m, 1));
    }
    for (w, d) in current.iter() {
        updates.push((*w, -*d));
    }
}

#[test]
fn reduce_one_retire() {
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));
    let input = hbatch::<u64, u64, u64, i64>(vec![((7, 3), 0, 1), ((7, 5), 0, 1), ((7, 4), 0, 1), ((9, 2), 0, 1)], 0, 1);
    let (produced, frontier) = tactic.retire(
        vec![], vec![], vec![input],
        &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64),
    );
    assert!(frontier.is_empty());
    let out: Vec<_> = produced.into_iter().flat_map(|(_t, b)| hread(&[b])).collect();
    // Output values are `(key, max)`: (7,5) and (9,2).
    assert_eq!(out, vec![((7u64, 5u64), 0u64, 1i64), ((9, 2), 0, 1)]);
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Collide(u64);
impl Hashable for Collide {
    type Output = u64;
    fn hashed(&self) -> u64 { 0 }
}

#[test]
fn reduce_collision_correct() {
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(
        |_k: &Collide, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>| {
            if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() {
                updates.push((m, 1));
            }
            for (w, d) in current.iter() { updates.push((*w, -*d)); }
        },
    ));
    let input = hbatch::<Collide, u64, u64, i64>(vec![((Collide(1), 9), 0, 1), ((Collide(1), 4), 0, 1), ((Collide(2), 9), 0, 1)], 0, 1);
    let (produced, _f) = tactic.retire(
        vec![], vec![], vec![input],
        &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64),
    );
    let out: Vec<_> = produced.into_iter().flat_map(|(_t, b)| hread(&[b])).collect();
    assert_eq!(out, vec![((Collide(1), 9u64), 0u64, 1i64), ((Collide(2), 9), 0, 1)], "collision must not merge keys");
}

/// Run the proxy reduce (with the given backend window size) and the cursor reduce over the same
/// updates; both outputs must agree.
fn proxy_matches_mainline(updates: Vec<((u64, u64), u64, i64)>, window: usize) {
    let t_out = Arc::new(Mutex::new(Vec::<((u64, u64), u64, i64)>::new()));
    let r_out = Arc::new(Mutex::new(Vec::<((u64, u64), u64, i64)>::new()));
    let (ts, rs) = (t_out.clone(), r_out.clone());
    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let coll = updates.clone().to_stream(scope).as_collection();
            let hashed = coll.clone().map(|(k, v)| (k.hashed(), (k, v)));
            let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), u64, i64)>, ContainerChunker<VecChunk<u64, (u64, u64), u64, i64>>, VChunkBatcher<u64, (u64, u64), u64, i64>, VChunkBuilder<u64, (u64, u64), u64, i64>, VChunkSpine<u64, (u64, u64), u64, i64>>(hashed.inner, Pipeline, "Arrange");
            reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), u64, i64>, _>(arr, "VecReduce", ProxyReduceTactic::new(VecReduceBackend::with_window(max_logic, window)))
                .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
                .inspect(move |(d, t, r)| ts.lock().unwrap().push((*d, *t, *r)));
            coll.reduce(|_k, input, output| {
                if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() { output.push((*m, 1)); }
            })
            .inspect(move |(d, t, r)| rs.lock().unwrap().push((*d, *t, *r)));
        });
    });
    let (mut got, mut want) = (t_out.lock().unwrap().clone(), r_out.lock().unwrap().clone());
    got.sort();
    want.sort();
    assert_eq!(got, want, "proxy-reduce must match the cursor reduce");
}

#[test]
fn reduce_dataflow_matches_mainline() {
    proxy_matches_mainline(
        vec![((7, 3), 0, 1), ((7, 5), 0, 1), ((7, 4), 0, 1), ((9, 2), 0, 1), ((7, 5), 1, -1)],
        usize::MAX,
    );
}

#[test]
fn reduce_multiwindow_matches_mainline() {
    // One key per window: every window-loop edge (the `from` cursor, the window-key merge, and the
    // in-window `changed` consumption) runs many times, under the harness's contract asserts.
    let mut updates = Vec::new();
    for k in 0..64u64 {
        updates.push(((k, k), 0u64, 1i64));
        updates.push(((k, 100 + k), 0, 1));
        updates.push(((k, 100 + k), 1, -1));
    }
    proxy_matches_mainline(updates, 1);
}

#[test]
fn reduce_multimoment_matches_mainline() {
    // Four distinct times in ONE batch: multi-moment keys exercise the round loop, including
    // produced-corrections feedback (the max changes at t1, reverts at t2, changes at t3).
    proxy_matches_mainline(
        vec![
            ((7, 3), 0, 1), ((7, 5), 1, 1), ((7, 5), 2, -1), ((7, 9), 3, 1),
            ((9, 2), 0, 1), ((9, 1), 2, 1), ((9, 2), 3, -1),
        ],
        usize::MAX,
    );
}

#[test]
fn reduce_string_values_matches_mainline() {
    let t_out = Arc::new(Mutex::new(Vec::<((u64, String), u64, i64)>::new()));
    let r_out = Arc::new(Mutex::new(Vec::<((u64, String), u64, i64)>::new()));
    let (ts, rs) = (t_out.clone(), r_out.clone());
    let updates: Vec<((u64, String), u64, i64)> = vec![
        ((7, "b".into()), 0, 1), ((7, "d".into()), 0, 1), ((7, "c".into()), 0, 1), ((9, "a".into()), 0, 1),
        ((7, "d".into()), 1, -1), ((9, "z".into()), 1, 1),
    ];
    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let coll = updates.clone().to_stream(scope).as_collection();
            let hashed = coll.clone().map(|(k, v): (u64, String)| (k.hashed(), (k, v)));
            let arr = arrange_core::<Pipeline, Vec<((u64, (u64, String)), u64, i64)>, ContainerChunker<VecChunk<u64, (u64, String), u64, i64>>, VChunkBatcher<u64, (u64, String), u64, i64>, VChunkBuilder<u64, (u64, String), u64, i64>, VChunkSpine<u64, (u64, String), u64, i64>>(hashed.inner, Pipeline, "Arrange");
            reduce_with_tactic::<_, VChunkSpine<u64, (u64, String), u64, i64>, _>(arr, "VecReduceStr", ProxyReduceTactic::new(VecReduceBackend::with_window(
                |_k: &u64, input: &[(String, i64)], current: &mut Vec<(String, i64)>, updates: &mut Vec<(String, i64)>| {
                    if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| v.clone()).max() { updates.push((m, 1)); }
                    for (w, d) in current.iter() { updates.push((w.clone(), -*d)); }
                },
                1,
            )))
                .as_collection(|_h, kw: &(u64, String)| (kw.0, kw.1.clone()))
                .inspect(move |(d, t, r)| ts.lock().unwrap().push((d.clone(), *t, *r)));
            coll.reduce(|_k, input, output| {
                if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| (*v).clone()).max() { output.push((m, 1)); }
            })
            .inspect(move |(d, t, r)| rs.lock().unwrap().push((d.clone(), *t, *r)));
        });
    });
    let (mut got, mut want) = (t_out.lock().unwrap().clone(), r_out.lock().unwrap().clone());
    got.sort();
    want.sort();
    assert_eq!(got, want, "proxy-reduce must match the cursor reduce for String values");
}

#[test]
fn reduce_inside_iterate() {
    let out = Arc::new(Mutex::new(Vec::<((u64, u64), u64, i64)>::new()));
    let os = out.clone();
    let updates: Vec<((u64, u64), u64, i64)> =
        vec![((7, 3), 0, 1), ((7, 5), 0, 1), ((7, 4), 0, 1), ((9, 2), 0, 1), ((9, 8), 0, 1)];

    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let input = updates.clone().to_stream(scope).as_collection();
            let result = input.iterate(|_scope, inner| {
                let hashed = inner.map(|(k, v)| (k.hashed(), (k, v)));
                let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), Product<u64, u64>, i64)>, ContainerChunker<VecChunk<u64, (u64, u64), Product<u64, u64>, i64>>, VChunkBatcher<u64, (u64, u64), Product<u64, u64>, i64>, VChunkBuilder<u64, (u64, u64), Product<u64, u64>, i64>, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>>(hashed.inner, Pipeline, "ArrIter");
                reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>, _>(arr, "IterReduce", ProxyReduceTactic::new(VecReduceBackend::with_window(max_logic, 1)))
                    .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
            });
            result.inspect(move |(d, t, r)| os.lock().unwrap().push((*d, *t, *r)));
        });
    });

    let mut got = out.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![((7u64, 5u64), 0u64, 1i64), ((9, 8), 0, 1)], "max-per-key fixpoint");
}
