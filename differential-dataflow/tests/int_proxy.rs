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
use differential_dataflow::trace::{Navigable};
use differential_dataflow::AsCollection;

type Span<K, V, T, R> = Rc<ChunkBatch<VecChunk<K, V, T, R>>>;

/// Read a `u64`-keyed batch, dropping the hash key; returns `(value, time, diff)`.
fn hread<KV, T, R>(batches: &[Span<u64, KV, T, R>]) -> Vec<(KV, T, R)>
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
fn hbatch<K, V, T, R>(rows: Vec<((K, V), T, R)>, _lower: T, _upper: T) -> Span<u64, (K, V), T, R>
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
    Rc::new(ChunkBatch::new(vec![chunk]))
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
    let out: Vec<_> = produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
    // Output values are `(key, max)`: (7,5) and (9,2).
    assert_eq!(out, vec![((7u64, 5u64), 0u64, 1i64), ((9, 2), 0, 1)]);
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Collide(u64);
/// Every `Collide` hashes alike, so they share a `key_hash` and exercise the collision paths.
/// Written as `Hash` rather than `Hashable` because the latter has a blanket impl for `T: Hash`,
/// and the backend's id interning needs `Hash` too.
impl std::hash::Hash for Collide {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { state.write_u64(0); }
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
    let out: Vec<_> = produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
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
            let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), u64, i64)>, ContainerChunker<VecChunk<u64, (u64, u64), u64, i64>>, _, VChunkBuilder<u64, (u64, u64), u64, i64>, VChunkSpine<u64, (u64, u64), u64, i64>>(hashed.inner, Pipeline, "Arrange", VChunkBatcher::new);
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
            let arr = arrange_core::<Pipeline, Vec<((u64, (u64, String)), u64, i64)>, ContainerChunker<VecChunk<u64, (u64, String), u64, i64>>, _, VChunkBuilder<u64, (u64, String), u64, i64>, VChunkSpine<u64, (u64, String), u64, i64>>(hashed.inner, Pipeline, "Arrange", VChunkBatcher::new);
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
                let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), Product<u64, u64>, i64)>, ContainerChunker<VecChunk<u64, (u64, u64), Product<u64, u64>, i64>>, _, VChunkBuilder<u64, (u64, u64), Product<u64, u64>, i64>, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>>(hashed.inner, Pipeline, "ArrIter", VChunkBatcher::new);
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

/// Two colliding keys, but the collision spans the HISTORY/NOVEL boundary: retire 1 inserts both
/// keys, retire 2 touches only the lower one. Input ids are ordinals minted history-first, so the
/// bracket reads `[C1, C2, C1]` by id and its endpoints agree even though its interior does not.
#[test]
fn reduce_collision_across_retires() {
    let logic = |_k: &Collide, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>| {
        if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() {
            updates.push((m, 1));
        }
        for (w, d) in current.iter() { updates.push((*w, -*d)); }
    };
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(logic));

    // Retire 1: both colliding keys arrive.
    let b0 = hbatch::<Collide, u64, u64, i64>(vec![((Collide(1), 5), 0, 1), ((Collide(2), 7), 0, 1)], 0, 1);
    let (p0, _f) = tactic.retire(
        vec![], vec![], vec![b0.clone()],
        &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64),
    );
    let out0: Vec<_> = p0.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
    assert_eq!(out0, vec![((Collide(1), 5u64), 0u64, 1i64), ((Collide(2), 7), 0, 1)], "retire 1");

    // Retire 2: a novel update to the LOWER key only, so the id order is [C1(hist), C2(hist), C1(novel)].
    let out_batches: Vec<_> = {
        let mut t2 = ProxyReduceTactic::new(VecReduceBackend::new(logic));
        let b = hbatch::<Collide, u64, u64, i64>(vec![((Collide(1), 5), 0, 1), ((Collide(2), 7), 0, 1)], 0, 1);
        let (p, _) = t2.retire(vec![], vec![], vec![b], &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64));
        p.into_iter().filter_map(|b| b.inner).collect()
    };
    let b1 = hbatch::<Collide, u64, u64, i64>(vec![((Collide(1), 9), 1, 1)], 1, 2);
    let (p1, _f) = tactic.retire(
        vec![b0], out_batches, vec![b1],
        &Antichain::from_elem(1u64), &Antichain::from_elem(2u64), &Antichain::from_elem(1u64),
    );
    let out1: Vec<_> = p1.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
    // C1's max rises 5 -> 9; C2 is untouched and must NOT be disturbed.
    assert_eq!(out1, vec![((Collide(1), 5u64), 1u64, -1i64), ((Collide(1), 9), 1, 1)], "retire 2 must not disturb C2");
}

/// Colliding hashes inside an `iterate`, where a correction can be deferred past the retire's
/// upper bound and applied in a later one.
///
/// The hash is `k % 2` rather than a real hash, so every bucket holds several real keys throughout.
/// A round's reduction retracts the values it replaces, so a real key's input can cancel in one
/// retire while the output it no longer justifies is corrected in another — the window where a
/// hash's input mentions one real key and its output another. Under `Product` times a synthesized
/// time can land at or beyond `upper`, which is what defers the correction to produce that window.
#[test]
fn reduce_collision_inside_iterate() {
    let out = Arc::new(Mutex::new(Vec::<((u64, u64), u64, i64)>::new()));
    let os = out.clone();
    // Key `k` holds `{10k, 10k + 7, 10k + 3}`, so its maximum is `10k + 7`; a key borrowing from
    // the neighbour it shares a bucket with would land on a different, visibly wrong maximum.
    let updates: Vec<((u64, u64), u64, i64)> = (0..6u64)
        .flat_map(|k| [((k, 10 * k), 0u64, 1i64), ((k, 10 * k + 7), 0, 1), ((k, 10 * k + 3), 0, 1)])
        .collect();

    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let input = updates.clone().to_stream(scope).as_collection();
            let result = input.iterate(|_scope, inner| {
                let hashed = inner.map(|(k, v)| (k % 2, (k, v)));
                let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), Product<u64, u64>, i64)>, ContainerChunker<VecChunk<u64, (u64, u64), Product<u64, u64>, i64>>, _, VChunkBuilder<u64, (u64, u64), Product<u64, u64>, i64>, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>>(hashed.inner, Pipeline, "ArrCollide", VChunkBatcher::new);
                reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>, _>(arr, "CollideReduce", ProxyReduceTactic::new(VecReduceBackend::with_window(max_logic, 1)))
                    .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
            });
            result.inspect(move |(d, t, r)| os.lock().unwrap().push((*d, *t, *r)));
        });
    });

    let mut got = out.lock().unwrap().clone();
    got.sort();
    let want: Vec<_> = (0..6u64).map(|k| ((k, 10 * k + 7), 0u64, 1i64)).collect();
    assert_eq!(got, want, "each real key reaches its own maximum");
}

/// A key landing in one of two hash buckets, so several real keys share each hash and several
/// hashes share a window. `Collide` puts everything in one bucket, which cannot show that the
/// backend rebuilds its collision bookkeeping per window rather than carrying it between them.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Bucket(u64);
impl std::hash::Hash for Bucket {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { state.write_u64(self.0 % 2) }
}

/// Two colliding hashes, each holding several real keys, presented across several windows.
///
/// The window budget of one record forces a window per hash, so the collision set is built, used
/// and discarded twice within the retire. A set that leaked between windows, or a representative
/// misaligned with the window's key list, merges two real keys' values into one reduction — which
/// shows up here as a key taking a neighbour's maximum.
#[test]
fn reduce_collision_multiwindow() {
    let logic = |_k: &Bucket, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>| {
        if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() {
            updates.push((m, 1));
        }
        for (w, d) in current.iter() { updates.push((*w, -*d)); }
    };
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::with_window(logic, 1));
    // Key `k` carries values `10k` and `10k + 1`, so its maximum is `10k + 1` and borrowing from a
    // neighbour would be visible. Keys 0, 2, 4 share one hash; keys 1, 3 share the other.
    let rows: Vec<((Bucket, u64), u64, i64)> = (0..5u64)
        .flat_map(|k| [((Bucket(k), 10 * k), 0u64, 1i64), ((Bucket(k), 10 * k + 1), 0, 1)])
        .collect();
    let input = hbatch::<Bucket, u64, u64, i64>(rows, 0, 1);
    let (produced, _f) = tactic.retire(
        vec![], vec![], vec![input],
        &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64),
    );
    let mut out: Vec<_> = produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
    out.sort();
    let want: Vec<_> = (0..5u64).map(|k| ((Bucket(k), 10 * k + 1), 0u64, 1i64)).collect();
    assert_eq!(out, want, "each real key keeps its own maximum");
}

/// A reduction that emits only for `Collide(1)`, so `Collide(2)` has input but never any output.
fn only_first(k: &Collide, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>) {
    if k.0 == 1 {
        if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() {
            updates.push((m, 1));
        }
    }
    for (w, d) in current.iter() { updates.push((*w, -*d)); }
}

/// Input ids are ordinals minted history-first, so the id order across a hash bracket is
/// `[history by key, then novel by key]` — NOT globally key-sorted. Here that makes the bracket
/// read `[C1, C2, C1]` by id while its endpoints agree, and the output bracket holds only `C1`
/// (C2 never emits), so the endpoint test concludes the bracket is a single key.
#[test]
fn reduce_collision_fastpath_endpoints() {
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(only_first));
    let b0 = hbatch::<Collide, u64, u64, i64>(vec![((Collide(1), 5), 0, 1), ((Collide(2), 900), 0, 1)], 0, 1);
    let (p0, _) = tactic.retire(
        vec![], vec![], vec![b0.clone()],
        &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64),
    );
    let outs: Vec<_> = p0.into_iter().filter_map(|b| b.inner).collect();
    assert_eq!(outs.iter().flat_map(|b| hread(std::slice::from_ref(b))).collect::<Vec<_>>(),
               vec![((Collide(1), 5u64), 0u64, 1i64)], "retire 1: only C1 emits");

    let b1 = hbatch::<Collide, u64, u64, i64>(vec![((Collide(1), 6), 1, 1)], 1, 2);
    let (p1, _) = tactic.retire(
        vec![b0], outs, vec![b1],
        &Antichain::from_elem(1u64), &Antichain::from_elem(2u64), &Antichain::from_elem(1u64),
    );
    let out1: Vec<_> = p1.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
    // C1's values are {5, 6}: the max becomes 6. C2's 900 belongs to a different real key.
    assert_eq!(out1, vec![((Collide(1), 5u64), 1u64, -1i64), ((Collide(1), 6), 1, 1)],
               "C2's value must not enter C1's reduction");
}

#[test]
fn reduce_cancelling_keys_matches_mainline() {
    // Every key's input cancels completely by time 2, so from then on it has no records in any of
    // the three presentations while still being a key the retire must consider — its stale output
    // has to be retracted. With one key per window this is also the case where a window's key list
    // and the `changed` set disagree.
    let mut updates = Vec::new();
    for k in 0..16u64 {
        updates.push(((k, 10 + k), 0u64, 1i64));
        updates.push(((k, 20 + k), 1, 1));
        updates.push(((k, 10 + k), 2, -1));
        updates.push(((k, 20 + k), 2, -1));
    }
    proxy_matches_mainline(updates, 1);
}

/// The cancellation case that forces seeds to travel apart from records: retire 2's novel update
/// exactly cancels the compaction-advanced prior record, so the merged input presentation for the
/// key is EMPTY — yet its time must still seed, because the key's stale output has to be
/// retracted. Deriving seeds from the merged view would lose the time and keep the stale output.
#[test]
fn reduce_seed_survives_cancellation() {
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));

    // Retire 1: key 7 gets value 3; the output history records max = 3.
    let b0 = hbatch::<u64, u64, u64, i64>(vec![((7, 3), 0, 1)], 0, 1);
    let (p0, _f) = tactic.retire(
        vec![], vec![], vec![b0.clone()],
        &Antichain::from_elem(0u64), &Antichain::from_elem(1u64), &Antichain::from_elem(0u64),
    );
    let outs: Vec<_> = p0.into_iter().filter_map(|b| b.inner).collect();
    assert_eq!(
        outs.iter().flat_map(|b| hread(std::slice::from_ref(b))).collect::<Vec<_>>(),
        vec![((7u64, 3u64), 0u64, 1i64)],
        "retire 1 establishes the output",
    );

    // Retire 2: the novel batch retracts (7, 3) at time 1. The prior record is advanced to the
    // compaction frontier (lower = 1) as it is drawn, so it lands at time 1 too, and the pair nets
    // to zero: the merged input presents NOTHING for the key. Only the seed carries time 1.
    let b1 = hbatch::<u64, u64, u64, i64>(vec![((7, 3), 1, -1)], 1, 2);
    let (p1, _f) = tactic.retire(
        vec![b0], outs, vec![b1],
        &Antichain::from_elem(1u64), &Antichain::from_elem(2u64), &Antichain::from_elem(1u64),
    );
    let out1: Vec<_> = p1.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect();
    assert_eq!(out1, vec![((7u64, 3u64), 1u64, -1i64)], "the stale output must be retracted");
}
