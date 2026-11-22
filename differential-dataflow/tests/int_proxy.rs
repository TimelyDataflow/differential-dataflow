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
    ChunkBatcher as VChunkBatcher, ChunkSpine as VChunkSpine, VecChunk,
};
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::cursor::Cursor;
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
            let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), u64, i64)>, _, VChunkSpine<u64, (u64, u64), u64, i64>>(hashed.inner, Pipeline, "Arrange", VChunkBatcher::new);
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
            let arr = arrange_core::<Pipeline, Vec<((u64, (u64, String)), u64, i64)>, _, VChunkSpine<u64, (u64, String), u64, i64>>(hashed.inner, Pipeline, "Arrange", VChunkBatcher::new);
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
                let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), Product<u64, u64>, i64)>, _, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>>(hashed.inner, Pipeline, "ArrIter", VChunkBatcher::new);
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
                let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), Product<u64, u64>, i64)>, _, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>>(hashed.inner, Pipeline, "ArrCollide", VChunkBatcher::new);
                reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>, _>(arr, "CollideReduce", ProxyReduceTactic::new(VecReduceBackend::with_window(max_logic, usize::MAX)).with_key_batch_size(1))
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

/// Reusing slots must preserve incomparable times, pending-only retires, and
/// cancellations against compacted history across multiple groups in one window.
#[test]
fn bounded_sweeps_match_partial_order_oracle() {
    type T = Product<u64, u64>;
    let time = T::new;
    let mut initial = Vec::new();
    let mut novel = Vec::new();
    let mut expected = Vec::new();
    for key in 0..1031u64 {
        let mut rows = vec![((key, 3), time(0, 0), 1i64),
                            ((key, 7), time(1, 0), 1),
                            ((key, 9), time(0, 1), 1)];
        initial.extend(rows.iter().cloned());
        let mut changes = Vec::new();
        if key % 2 == 0 { changes.push(((key, 9), time(2, 2), -1)); }
        if key % 3 == 0 { changes.push(((key, 7), time(2, 2), -1)); }
        // Some keys cancel entirely after compaction: only the raw novel seeds
        // and stale output remain to trigger their final retraction.
        if key % 6 == 0 { changes.push(((key, 3), time(2, 2), -1)); }
        novel.extend(changes.iter().cloned());
        rows.extend(changes);

        // Recompute the desired maximum at each time from the original updates,
        // then subtract the output in its partial-order past. No sweep machinery.
        let mut prior: Vec<((u64, u64), T, i64)> = Vec::new();
        for at in [time(0, 0), time(0, 1), time(1, 0), time(1, 1), time(2, 2)] {
            let mut counts = std::collections::BTreeMap::new();
            for ((_, value), t, diff) in &rows {
                if timely::PartialOrder::less_equal(t, &at) {
                    *counts.entry(*value).or_insert(0i64) += diff;
                }
            }
            let mut correction = std::collections::BTreeMap::new();
            if let Some((&value, _)) = counts.iter().rev().find(|(_, d)| **d > 0) {
                correction.insert(value, 1i64);
            }
            for ((_, value), t, diff) in &prior {
                if timely::PartialOrder::less_equal(t, &at) {
                    *correction.entry(*value).or_default() -= diff;
                }
            }
            prior.extend(correction.into_iter().filter(|(_, d)| *d != 0)
                .map(|(value, diff)| ((key, value), at, diff)));
        }
        expected.extend(prior);
    }
    consolidate_updates(&mut expected);
    for limit in [1, 7, 256, 1024, usize::MAX] {
        let backend = VecReduceBackend::with_window(max_logic, usize::MAX);
        let mut tactic = ProxyReduceTactic::new(backend).with_key_batch_size(limit);
        let source = hbatch(initial.clone(), time(0, 0), time(1, 1));
        let lower = Antichain::from_elem(time(0, 0));
        let middle = Antichain::from_elem(time(1, 1));
        let upper = Antichain::from_elem(time(2, 2));
        let (first, pending) = tactic.retire(vec![], vec![], vec![source.clone()], &lower, &middle, &lower);
        assert_eq!(pending, middle, "limit {limit}: synthesized crossing must remain carried");
        let mut all: Vec<_> = first.into_iter().filter_map(|s| s.inner).collect();
        // No new records: every key is revisited solely because of its pending join.
        let (second, pending) = tactic.retire(vec![source.clone()], all.clone(), vec![], &middle, &upper, &pending);
        assert!(pending.is_empty(), "limit {limit}: pending-only retire");
        all.extend(second.into_iter().filter_map(|s| s.inner));
        let input = hbatch(novel.clone(), time(2, 2), time(3, 3));
        let (third, pending) = tactic.retire(vec![source], all.clone(), vec![input], &upper, &Antichain::from_elem(time(3, 3)), &upper);
        assert!(pending.is_empty(), "limit {limit}: cancellations");
        all.extend(third.into_iter().filter_map(|s| s.inner));
        assert_eq!(hread(&all), expected, "limit {limit}");
    }
}

/// A single seed stays pending beyond upper, then evaluates with no novel input.
#[test]
fn reduce_single_seed_deferred_then_due() {
    let time = Product::<u64, u64>::new;
    let lower = Antichain::from_elem(time(0, 0));
    let middle = Antichain::from_elem(time(1, 1));
    let seed = time(2, 2);
    let input = hbatch(vec![((7u64, 3u64), seed, 1i64)], time(0, 0), time(3, 3));
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));
    let (first, pending) = tactic.retire(
        vec![], vec![], vec![input.clone()], &lower, &middle, &lower,
    );
    let outputs: Vec<_> = first.into_iter().filter_map(|s| s.inner).collect();
    assert!(hread(&outputs).is_empty(), "a seed beyond upper must not evaluate");
    assert_eq!(pending, Antichain::from_elem(seed));
    let (second, pending) = tactic.retire(
        vec![input], outputs, vec![], &middle, &Antichain::from_elem(time(3, 3)), &pending,
    );
    let outputs: Vec<_> = second.into_iter().filter_map(|s| s.inner).collect();
    assert_eq!(hread(&outputs), vec![((7, 3), seed, 1)], "the due seed needs no novel support");
    assert!(pending.is_empty());
}

/// Input and prior output at exactly the seed time participate in direct reconciliation.
#[test]
fn reduce_single_seed_includes_equal_times() {
    let time = Product::<u64, u64>::new;
    let zero = time(0, 0);
    let seed = time(1, 1);
    let prior = hbatch(vec![((7u64, 3u64), zero, 1i64)], zero, seed);
    let input = hbatch(vec![((7u64, 9u64), seed, 1i64)], seed, time(2, 2));
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));
    // The backend advances both the prior input and output to lower = seed.
    let (result, pending) = tactic.retire(
        vec![prior.clone()], vec![prior], vec![input],
        &Antichain::from_elem(seed), &Antichain::from_elem(time(2, 2)), &Antichain::from_elem(seed),
    );
    let outputs: Vec<_> = result.into_iter().filter_map(|s| s.inner).collect();
    assert_eq!(hread(&outputs), vec![((7, 3), seed, -1), ((7, 9), seed, 1)]);
    assert!(pending.is_empty());
}

/// One incomparable record requires both the seed evaluation and its later join.
#[test]
fn reduce_single_seed_with_incomparable_record() {
    let time = Product::<u64, u64>::new;
    let zero = time(0, 0);
    let seed = time(2, 0);
    let mut lower = Antichain::from_elem(time(0, 2));
    lower.insert(seed);
    let prior = hbatch(vec![((7u64, 9u64), time(1, 1), 1i64)], zero, time(1, 1));
    let input = hbatch(vec![((7u64, 3u64), seed, 1i64)], zero, time(2, 2));
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));
    let (result, pending) = tactic.retire(
        vec![prior.clone()], vec![prior], vec![input],
        &lower, &Antichain::from_elem(time(3, 3)), &lower,
    );
    let outputs: Vec<_> = result.into_iter().filter_map(|s| s.inner).collect();
    assert_eq!(hread(&outputs), vec![((7, 3), seed, 1), ((7, 3), time(2, 1), -1)],
        "including the incomparable value at the seed, or skipping the join, is incorrect");
    assert!(pending.is_empty());
}

/// With partially ordered times, advancing a prior record to `lower` can move it past a novel
/// record of the same value: prior `(0, 5)` advances to `(1, 5)`, after novel `(1, 0)`. The input
/// must still be presented sorted by `(id, time)`, and the prior record must net against the
/// novel retraction at `(1, 5)`.
#[test]
fn reduce_prior_advanced_past_novel() {
    let time = Product::<u64, u64>::new;
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));
    let prior = hbatch(vec![((7u64, 3u64), time(0, 5), 1i64)], time(0, 0), time(1, 0));
    let (first, _) = tactic.retire(
        vec![], vec![], vec![prior.clone()],
        &Antichain::from_elem(time(0, 0)), &Antichain::from_elem(time(1, 0)), &Antichain::from_elem(time(0, 0)),
    );
    let outputs: Vec<_> = first.into_iter().filter_map(|s| s.inner).collect();
    assert_eq!(hread(&outputs), vec![((7, 3), time(0, 5), 1)]);

    let input = hbatch(vec![((7u64, 3u64), time(1, 0), 1i64), ((7, 3), time(1, 5), -1)], time(1, 0), time(2, 0));
    let (second, pending) = tactic.retire(
        vec![prior], outputs, vec![input],
        &Antichain::from_elem(time(1, 0)), &Antichain::from_elem(time(2, 0)), &Antichain::from_elem(time(1, 0)),
    );
    let outputs: Vec<_> = second.into_iter().filter_map(|s| s.inner).collect();
    assert_eq!(hread(&outputs), vec![((7, 3), time(1, 0), 1), ((7, 3), time(1, 5), -1)]);
    assert!(pending.is_empty());
}

/// Advancing a prior record by the meet of the seeds can move it past `upper` while its join with
/// a seed is still owed. Prior `(0,1,0)` advances by the meet `(0,0,1)` of seeds `(0,0,2)` and
/// `(1,0,1)` to `(0,1,1)`, which is past `upper`. Its join with `(0,0,2)`, at `(0,1,2)`, must be
/// pended, as the value is present twice there. Two dimensions cannot produce this case.
#[test]
fn reduce_unreached_time_past_upper_pends_joins() {
    type T = Product<u64, Product<u64, u64>>;
    let time = |a, b, c| -> T { Product::new(a, Product::new(b, c)) };
    let updates = vec![((7u64, 3u64), time(0, 1, 0), 1i64), ((7, 3), time(0, 0, 2), 1), ((7, 3), time(1, 0, 1), 1)];
    let prior = hbatch(updates[.. 1].to_vec(), time(0, 0, 0), time(0, 0, 1));
    let novel = hbatch(updates[1 ..].to_vec(), time(0, 0, 1), time(0, 1, 1));
    let frontier = Antichain::from_elem;
    let mut tactic = ProxyReduceTactic::new(VecReduceBackend::new(max_logic));
    let (first, pending) = tactic.retire(
        vec![], vec![], vec![prior.clone()],
        &frontier(time(0, 0, 0)), &frontier(time(0, 0, 1)), &frontier(time(0, 0, 0)),
    );
    assert!(pending.is_empty());
    let mut outputs: Vec<_> = first.into_iter().filter_map(|s| s.inner).collect();
    let (second, pending) = tactic.retire(
        vec![prior.clone()], outputs.clone(), vec![novel.clone()],
        &frontier(time(0, 0, 1)), &frontier(time(0, 1, 1)), &frontier(time(0, 0, 1)),
    );
    outputs.extend(second.into_iter().filter_map(|s| s.inner));
    // No new records: the rest of the work is the pending times.
    let (third, pending) = tactic.retire(
        vec![prior, novel], outputs.clone(), vec![],
        &frontier(time(0, 1, 1)), &frontier(time(3, 3, 3)), &pending,
    );
    assert!(pending.is_empty());
    outputs.extend(third.into_iter().filter_map(|s| s.inner));
    let outputs = hread(&outputs);
    for (a, b, c) in (0 .. 3).flat_map(|a| (0 .. 3).flat_map(move |b| (0 .. 3).map(move |c| (a, b, c)))) {
        let at = time(a, b, c);
        let count: i64 = updates.iter().filter(|(_, t, _)| timely::PartialOrder::less_equal(t, &at)).map(|(_, _, d)| d).sum();
        let output: i64 = outputs.iter().filter(|(_, t, _)| timely::PartialOrder::less_equal(t, &at)).map(|(_, _, d)| d).sum();
        assert_eq!(output, if count > 0 { 1 } else { 0 }, "at {at:?}");
    }
}

// ===== Reading as of a time =====
//
// A trace's accumulations are correct only at or beyond its compaction frontier: compaction can
// leave a retraction advanced to the frontier beside the insertion it retracts. A reader that
// starts after that reads every time advanced to the frontier, as a cursor through `TraceFrontier`
// does. The proxy tactics read through backends, so `BackendFrontier` reads what a backend draws as
// of a time. The reference is the same tactic over the rows with every time advanced by hand.

use differential_dataflow::operators::int_proxy::join::{JoinInstance, JoinMatches, ProxyJoinBackend, ProxyJoinTactic};
use differential_dataflow::operators::int_proxy::{BackendFrontier, KeyPosition, ProxyBridge};
use differential_dataflow::operators::join::{Fresh, JoinTactic};

/// The max-reduce's logic, as a plain function, so that its backends name one type.
type MaxLogic = fn(&u64, &[(u64, i64)], &mut Vec<(u64, i64)>, &mut Vec<(u64, i64)>);
/// The max-reduce's backend.
type MaxBackend = VecReduceBackend<u64, u64, u64, u64, i64, MaxLogic>;
fn max_backend() -> MaxBackend { VecReduceBackend::new(max_logic as MaxLogic) }

/// `rows` with every time advanced to `since`, as a cursor through `TraceFrontier` reads them.
fn advanced(rows: Vec<((u64, u64), u64, i64)>, since: u64) -> Vec<((u64, u64), u64, i64)> {
    rows.into_iter().map(|(d, t, r)| (d, t.max(since), r)).collect()
}

/// The history the frontier hides: (7, 3) inserted at 1 and retracted at 5, beside (7, 4) at 1;
/// (8, 1) inserted at 1 and retracted at 5, so key 8 is empty as of 5; and (9, 2) inserted at 1
/// and retracted at 6. Read as of 5, nothing happens before 5.
fn history() -> Vec<((u64, u64), u64, i64)> {
    vec![((7, 3), 1, 1), ((7, 3), 5, -1), ((7, 4), 1, 1), ((8, 1), 1, 1), ((8, 1), 5, -1), ((9, 2), 1, 1), ((9, 2), 6, -1)]
}

/// One retire of a reduce tactic over `input`, between `lower` and `upper`, its output read.
fn retire<B1>(mut tactic: impl ReduceTactic<u64, B1, Span<u64, (u64, u64), u64, i64>>, input: B1, lower: u64, upper: u64) -> Vec<((u64, u64), u64, i64)> {
    let (produced, _) = tactic.retire(vec![], vec![], vec![input], &Antichain::from_elem(lower), &Antichain::from_elem(upper), &Antichain::from_elem(lower));
    produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect()
}

#[test]
fn reduce_reads_as_of_since() {
    let got = retire(ProxyReduceTactic::new(BackendFrontier::new(max_backend(), Antichain::from_elem(5))), hbatch(history(), 0, 0), 0, 10);
    let want = retire(ProxyReduceTactic::new(max_backend()), hbatch(advanced(history(), 5), 0, 0), 0, 10);
    assert_eq!(got, want);
    assert!(got.iter().all(|(_, t, _)| *t >= 5), "nothing before since: {got:?}");
    // Read as stored, the history reduces at times before since.
    let raw = retire(ProxyReduceTactic::new(max_backend()), hbatch(history(), 0, 0), 0, 10);
    assert!(raw.iter().any(|(_, t, _)| *t < 5));
}

/// A join backend over hash-keyed `u64` pairs, which proxy themselves: it draws every update of
/// both inputs in one block, at the times stored (it does not consolidate on load), and crosses
/// matches into `((key, (val0, val1)), time, diff)` rows.
struct PairJoin;
type Pairs = Span<u64, (u64, u64), u64, i64>;
type Joined = Vec<((u64, ((u64, u64), (u64, u64))), u64, i64)>;
impl ProxyJoinBackend<u64, Pairs, Pairs> for PairJoin {
    type Key = u64;
    type V0 = (u64, u64);
    type V1 = (u64, u64);
    type R0 = i64;
    type R1 = i64;
    type ROut = i64;
    type Output = Joined;
    fn advance(&mut self, instance: &JoinInstance<u64, Pairs, Pairs>, from: &mut KeyPosition<u64>, bridge0: &mut ProxyBridge<u64, i64, u64, (u64, u64)>, bridge1: &mut ProxyBridge<u64, i64, u64, (u64, u64)>) {
        fn draw(batches: &[Pairs]) -> ProxyBridge<u64, i64, u64, (u64, u64)> {
            let mut rows: Vec<_> = hread(batches).into_iter().map(|(kv, t, r)| ((kv.0.hashed(), kv), t, r)).collect();
            consolidate_updates(&mut rows);
            rows
        }
        let (b0, b1) = (draw(&instance.batches0), draw(&instance.batches1));
        bridge0.extend(b0.iter().filter(|r| b1.iter().any(|s| s.0.0 == r.0.0)).cloned());
        bridge1.extend(b1.iter().filter(|r| b0.iter().any(|s| s.0.0 == r.0.0)).cloned());
        *from = KeyPosition::End;
    }
    fn cross(&mut self, _instance: &JoinInstance<u64, Pairs, Pairs>, matches: &mut JoinMatches<u64, i64, u64, (u64, u64), (u64, u64)>, output: &mut Vec<Self::Output>) {
        let rows = matches.ids.iter().zip(&matches.times).zip(&matches.diffs).map(|((id, t), r)| (*id, *t, *r)).collect();
        output.push(rows);
    }
}

/// The join's output, keyed by its two values and consolidated.
fn joined(rows: Box<dyn Iterator<Item = Joined>>) -> Vec<(((u64, u64), (u64, u64)), u64, i64)> {
    let mut out: Vec<_> = rows.flatten().map(|((_, (a, b)), t, r)| ((a, b), t, r)).collect();
    consolidate_updates(&mut out);
    out
}

#[test]
fn join_reads_as_of_since() {
    // The other input: a row for each key, at `at`. Key 8 has nothing to meet as of 5.
    let other_at = |at| hbatch::<u64, u64, u64, i64>(vec![((7, 70), at, 1), ((8, 80), at, 1), ((9, 90), at, 1)], 0, 0);
    let since = 5;
    let as_of = || Antichain::from_elem(since);
    // The wrapper reads both inputs as of since; the reference advances only the history, which
    // joins to the same output. The driver's bound is at or below the fresh input's times: with
    // the other input at 2 the bound is before since, and the wrapper reads; at 5 it is at since,
    // and the wrapper passes the inner backend's raw read through, which joins to the same output.
    for (meet, label) in [(0, "the bound before since"), (5, "the bound at since")] {
        let other = other_at(meet.max(2));
        let got = joined(ProxyJoinTactic::new(BackendFrontier::new(PairJoin, as_of())).prep(vec![hbatch(history(), 0, 0)], vec![Rc::clone(&other)], Fresh::Input0, meet));
        let want = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(advanced(history(), since), 0, 0)], vec![Rc::clone(&other)], Fresh::Input0, meet));
        assert_eq!(got, want, "{label}");
        assert!(got.iter().all(|(_, t, _)| *t >= since), "nothing before since: {got:?}");
        // The same with the history on the second input.
        let got_second = joined(ProxyJoinTactic::new(BackendFrontier::new(PairJoin, as_of())).prep(vec![Rc::clone(&other)], vec![hbatch(history(), 0, 0)], Fresh::Input0, meet));
        let want_second = joined(ProxyJoinTactic::new(PairJoin).prep(vec![Rc::clone(&other)], vec![hbatch(advanced(history(), since), 0, 0)], Fresh::Input0, meet));
        assert_eq!(got_second, want_second, "{label}, second input");
    }
    // Read as stored, the history joins at times before since.
    let raw = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(history(), 0, 0)], vec![other_at(2)], Fresh::Input0, 0));
    assert!(raw.iter().any(|(_, t, _)| *t < since));
    // Two histories, both read as of since: the join of the two advanced histories.
    let second = vec![((7, 70), 2, 1), ((8, 80), 2, 1), ((8, 80), 4, -1), ((9, 90), 6, 1)];
    let got = joined(ProxyJoinTactic::new(BackendFrontier::new(PairJoin, as_of())).prep(vec![hbatch(history(), 0, 0)], vec![hbatch(second.clone(), 0, 0)], Fresh::Input0, 0));
    let want = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(advanced(history(), since), 0, 0)], vec![hbatch(advanced(second, since), 0, 0)], Fresh::Input0, 0));
    assert_eq!(got, want, "both inputs");
    assert!(got.iter().all(|(_, t, _)| *t >= since), "nothing before since: {got:?}");
}

// ===== Reading as of a frontier, in a non-distributive lattice =====
//
// The wrapper's reasoning uses only the defining property of `advance_by`: for `t'` beyond `F`,
// `t ≤ t' ⟺ advance_F(t) ≤ t'`. What a non-distributive lattice breaks is advancing commuting
// with joins and meets. These tests run the wrapper over N5, the pentagon `0 < a < b < 1`,
// `0 < c < 1`, with `c` incomparable to `a` and `b`: `a ∨ (b ∧ c) = a` but `(a ∨ b) ∧ (a ∨ c) = b`.
// The reference is the same tactic over the rows advanced by `advance_by` by hand. The claims:
// the output agrees with the reference at every time beyond `F` always; it is identical when `F`
// has one element, or when `F` is beyond the unit's bound (as in every first unit, whose bound
// is the minimum). Where the inner backend consolidates on load by a bound that crosses `F`, the
// records it presents can differ from the reference's, and the test reports whether the outputs do.

use serde::{Deserialize, Serialize};
use timely::order::PartialOrder;
use timely::progress::PathSummary;

/// The pentagon, by index: 0 is the bottom, 1 is `a`, 2 is `b`, 3 is `c`, 4 is the top. The
/// derived `Ord` is a linear extension of the partial order, as the tactics require.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize)]
struct N5(u8);

impl N5 {
    const ALL: [N5; 5] = [N5(0), N5(1), N5(2), N5(3), N5(4)];
    fn upper_bounds(self, other: N5) -> impl Iterator<Item = N5> {
        N5::ALL.into_iter().filter(move |x| self.less_equal(x) && other.less_equal(x))
    }
}

impl PartialOrder for N5 {
    fn less_equal(&self, other: &Self) -> bool {
        self == other || self.0 == 0 || other.0 == 4 || (self.0 == 1 && other.0 == 2)
    }
}

impl Lattice for N5 {
    fn join(&self, other: &Self) -> Self {
        self.upper_bounds(*other).find(|x| self.upper_bounds(*other).all(|y| x.less_equal(&y))).expect("a lattice")
    }
    fn meet(&self, other: &Self) -> Self {
        let lower = |x: &N5| x.less_equal(self) && x.less_equal(other);
        N5::ALL.into_iter().filter(lower).find(|x| N5::ALL.iter().filter(|y| lower(y)).all(|y| y.less_equal(x))).expect("a lattice")
    }
}

/// The one path summary: a step that leaves a time where it is.
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

fn beyond(frontier: &Antichain<N5>, t: N5) -> bool { frontier.less_equal(&t) }
fn advanced_n5(mut t: N5, frontier: &Antichain<N5>) -> N5 { t.advance_by(frontier.borrow()); t }
/// The antichains of N5 with one or two elements, the empty one aside.
fn antichains() -> Vec<Antichain<N5>> {
    let mut out: Vec<Antichain<N5>> = N5::ALL.iter().map(|t| Antichain::from_elem(*t)).collect();
    for x in N5::ALL { for y in N5::ALL {
        if x < y && !x.less_equal(&y) && !y.less_equal(&x) { out.push([x, y].into_iter().collect()); }
    } }
    out
}

/// A join backend over hash-keyed pairs at any time, which either presents stored times or
/// consolidates on load by `instance.lower`, as the contract allows either.
///
/// With `complete` set it completes every key that has one update on some side, unless asked to
/// present all: it holds the key's output, every pair of its updates, for the following `cross`.
/// `completed` counts the keys it completed.
struct PairJoinAt<T> { apply_lower: bool, complete: bool, done: JoinedAt<T>, completed: Rc<std::cell::Cell<usize>> }
impl<T> PairJoinAt<T> {
    fn new(apply_lower: bool, complete: bool) -> Self { PairJoinAt { apply_lower, complete, done: Vec::new(), completed: Rc::default() } }
}
type PairsAt<T> = Span<u64, (u64, u64), T, i64>;
type JoinedAt<T> = Vec<((u64, ((u64, u64), (u64, u64))), T, i64)>;
impl<T: Lattice + Timestamp> ProxyJoinBackend<T, PairsAt<T>, PairsAt<T>> for PairJoinAt<T> {
    type Key = u64;
    type V0 = (u64, u64);
    type V1 = (u64, u64);
    type R0 = i64;
    type R1 = i64;
    type ROut = i64;
    type Output = JoinedAt<T>;
    fn advance(&mut self, instance: &JoinInstance<T, PairsAt<T>, PairsAt<T>>, from: &mut KeyPosition<u64>, bridge0: &mut ProxyBridge<T, i64, u64, (u64, u64)>, bridge1: &mut ProxyBridge<T, i64, u64, (u64, u64)>) {
        let draw = |batches: &[PairsAt<T>]| {
            let mut rows: Vec<_> = hread(batches).into_iter().map(|(kv, mut t, r)| {
                if self.apply_lower { t.join_assign(&instance.lower); }
                ((kv.0.hashed(), kv), t, r)
            }).collect();
            consolidate_updates(&mut rows);
            rows
        };
        let (b0, b1) = (draw(&instance.batches0), draw(&instance.batches1));
        let rows = |bridge: &ProxyBridge<T, i64, u64, (u64, u64)>, key: u64| bridge.iter().filter(|r| r.0.0 == key).count();
        let complete = self.complete && !instance.present_all;
        let completes = |key: u64| complete && (rows(&b0, key) == 1 || rows(&b1, key) == 1);
        for r in b0.iter() {
            for s in b1.iter().filter(|s| s.0.0 == r.0.0 && completes(r.0.0)) {
                self.done.push(((r.0.0, (r.0.1, s.0.1)), r.1.join(&s.1), r.2 * s.2));
            }
        }
        let mut keys: Vec<u64> = b0.iter().map(|r| r.0.0).filter(|key| completes(*key) && rows(&b1, *key) > 0).collect();
        keys.dedup();
        self.completed.set(self.completed.get() + keys.len());
        bridge0.extend(b0.iter().filter(|r| !completes(r.0.0) && b1.iter().any(|s| s.0.0 == r.0.0)).cloned());
        bridge1.extend(b1.iter().filter(|r| !completes(r.0.0) && b0.iter().any(|s| s.0.0 == r.0.0)).cloned());
        *from = KeyPosition::End;
    }
    fn cross(&mut self, _instance: &JoinInstance<T, PairsAt<T>, PairsAt<T>>, matches: &mut JoinMatches<T, i64, u64, (u64, u64), (u64, u64)>, output: &mut Vec<Self::Output>) {
        if !self.done.is_empty() { output.push(std::mem::take(&mut self.done)); }
        output.push(matches.ids.iter().zip(&matches.times).zip(&matches.diffs).map(|((id, t), r)| (*id, t.clone(), *r)).collect());
    }
}

/// A collection's accumulation at `t`: each datum's total over the records at or before `t`.
fn accumulation<D: Ord + Clone>(rows: &[(D, N5, i64)], t: N5) -> Vec<(D, i64)> {
    let mut out: Vec<(D, i64)> = rows.iter().filter(|(_, time, _)| time.less_equal(&t)).map(|(d, _, r)| (d.clone(), *r)).collect();
    differential_dataflow::consolidation::consolidate(&mut out);
    out
}

/// Both readings agree at every time beyond `frontier`; returns whether they are identical.
fn agree_beyond<D: Ord + Clone + std::fmt::Debug>(got: &[(D, N5, i64)], want: &[(D, N5, i64)], frontier: &Antichain<N5>, what: &str) -> bool {
    for t in N5::ALL {
        if beyond(frontier, t) {
            assert_eq!(accumulation(got, t), accumulation(want, t), "{what}: accumulations differ at {t:?} beyond {frontier:?}\n got {got:?}\nwant {want:?}");
        }
    }
    got == want
}

#[test]
fn join_as_of_frontier_in_n5() {
    let (a, b, c) = (N5(1), N5(2), N5(3));
    assert_ne!(a.join(&b.meet(&c)), a.join(&b).meet(&a.join(&c)), "N5 is not distributive");
    // The case the search for differences found: with `F = {b, c}` and a bound `a`, a record at 0
    // is presented at `advance_F(0 ∨ a) = b` by a backend that consolidates on load, where the
    // as-of reading has `advance_F(0) = 0`.
    let f_bc: Antichain<N5> = [b, c].into_iter().collect();
    assert_eq!(advanced_n5(N5(0).join(&a), &f_bc), b);
    assert_eq!(advanced_n5(N5(0), &f_bc), N5(0));

    let (mut units, mut exact, mut crossing, mut differ) = (0, 0, 0, Vec::new());
    for frontier in antichains() {
        for lower in N5::ALL {
            if beyond(&frontier, lower) { continue; }   // a unit the wrapper leaves alone
            for t1 in N5::ALL.into_iter().filter(|t| lower.less_equal(t)) {   // the fresh input's time
                for t0 in N5::ALL {
                    for apply_lower in [false, true] {
                        let backend = || PairJoinAt::<N5>::new(apply_lower, false);
                        // The history: one key with two values, one of them retracted at `t1`, so that
                        // consolidation after advancing has something to do; the other input at `t1`.
                        let history = vec![((7u64, 3u64), t0, 1i64), ((7, 3), t1, -1), ((7, 4), t0, 1)];
                        let other = vec![((7u64, 70u64), t1, 1i64)];
                        let as_of = |rows: &Vec<((u64, u64), N5, i64)>| rows.iter().map(|(d, t, r)| (*d, advanced_n5(*t, &frontier), *r)).collect::<Vec<_>>();
                        let got = joined_at(ProxyJoinTactic::new(BackendFrontier::new(backend(), frontier.clone())).prep(vec![hbatch(history.clone(), N5(0), N5(0))], vec![hbatch(other.clone(), N5(0), N5(0))], Fresh::Input1, lower));
                        let want = joined_at(ProxyJoinTactic::new(backend()).prep(vec![hbatch(as_of(&history), N5(0), N5(0))], vec![hbatch(as_of(&other), N5(0), N5(0))], Fresh::Input1, lower));
                        units += 1;
                        let identical = agree_beyond(&got, &want, &frontier, "join");
                        // Exact when the backend presents stored times, when `F` has one element, or
                        // when `F` is beyond the bound.
                        let frontier_beyond_lower = frontier.iter().all(|s| lower.less_equal(s));
                        if !apply_lower || frontier.len() == 1 || frontier_beyond_lower {
                            assert!(identical, "join: expected identical outputs for F {frontier:?}, lower {lower:?}, t0 {t0:?}, t1 {t1:?}, apply_lower {apply_lower}\n got {got:?}\nwant {want:?}");
                            exact += 1;
                        } else {
                            crossing += 1;
                            if !identical { differ.push((frontier.clone(), lower, t0, t1)); }
                        }
                    }
                }
            }
        }
    }
    println!("join over N5: {units} units; {exact} exact by the claims; {crossing} where the bound crosses F, of which {} differ exactly: {differ:?}", differ.len());
}

/// The join's output, keyed by its two values and consolidated.
fn joined_at(rows: Box<dyn Iterator<Item = JoinedAt<N5>>>) -> Vec<(((u64, u64), (u64, u64)), N5, i64)> {
    let mut out: Vec<_> = rows.flatten().map(|((_, (x, y)), t, r)| ((x, y), t, r)).collect();
    consolidate_updates(&mut out);
    out
}

#[test]
fn reduce_as_of_frontier_in_n5() {
    type MaxN5 = VecReduceBackend<u64, u64, u64, N5, i64, MaxLogic>;
    let backend = || -> MaxN5 { VecReduceBackend::new(max_logic as MaxLogic) };
    let (mut units, mut exact, mut crossing, mut differ) = (0, 0, 0, Vec::new());
    for frontier in antichains() {
        for lower in N5::ALL {
            if beyond(&frontier, lower) { continue; }
            let lower_ac = Antichain::from_elem(lower);
            // Prior rows before the retire, and novel rows in it: a value the novel rows retract,
            // and a new maximum; a second prior value survives.
            for tp in N5::ALL {
                for tn in N5::ALL.into_iter().filter(|t| lower.less_equal(t)) {
                    for retract in [false, true] {
                        let prior = vec![((7u64, 3u64), tp, 1i64), ((7, 2), tp, 1)];
                        let mut novel = vec![((7u64, 9u64), tn, 1i64)];
                        if retract { novel.push(((7, 3), tn, -1)); }
                        let as_of = |rows: &Vec<((u64, u64), N5, i64)>| rows.iter().map(|(d, t, r)| (*d, advanced_n5(*t, &frontier), *r)).collect::<Vec<_>>();
                        let run = |tactic: &mut dyn ReduceTactic<N5, Span<u64, (u64, u64), N5, i64>, Span<u64, (u64, u64), N5, i64>>, prior_rows: Vec<((u64, u64), N5, i64)>, novel_rows: Vec<((u64, u64), N5, i64)>| {
                            let (produced, _) = tactic.retire(vec![hbatch(prior_rows, N5(0), N5(0))], vec![], vec![hbatch(novel_rows, N5(0), N5(0))], &lower_ac, &Antichain::new(), &lower_ac);
                            produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect::<Vec<_>>()
                        };
                        let got = run(&mut ProxyReduceTactic::new(BackendFrontier::new(backend(), frontier.clone())), prior.clone(), novel.clone());
                        let want = run(&mut ProxyReduceTactic::new(backend()), as_of(&prior), as_of(&novel));
                        units += 1;
                        let identical = agree_beyond(&got, &want, &frontier, "reduce");
                        let frontier_beyond_lower = frontier.iter().all(|s| lower.less_equal(s));
                        if frontier.len() == 1 || frontier_beyond_lower {
                            assert!(identical, "reduce: expected identical outputs for F {frontier:?}, lower {lower:?}, prior at {tp:?}, novel at {tn:?}, retract {retract}\n got {got:?}\nwant {want:?}");
                            exact += 1;
                        } else {
                            crossing += 1;
                            if !identical { differ.push((frontier.clone(), lower, tp, tn, retract)); }
                        }
                    }
                }
            }
        }
    }
    println!("reduce over N5: {units} retires; {exact} exact by the claims; {crossing} where the bound crosses F, of which {} differ exactly: {differ:?}", differ.len());
}

// ===== Keys the backend completes =====
//
// A backend may complete a key itself rather than present it. The join's completed output is every
// pair of the key's updates; the reduce's is one evaluation at the key's one time, which
// `int_proxy::reduce::direct` finds, and the times the key carries beyond `upper`. These tests
// check that a completing backend produces what a presenting one does, inside and outside
// `BackendFrontier`, and that completion happens where it may.

#[test]
fn join_completed_keys_match_presented() {
    let (mut completed_passed, mut completed_read) = (0, 0);
    for frontier in antichains() {
        for lower in N5::ALL {
            for t1 in N5::ALL.into_iter().filter(|t| lower.less_equal(t)) {
                for t0 in N5::ALL {
                    for apply_lower in [false, true] {
                        // Key 7 has one update on its second side, so the completing backend completes it;
                        // key 8 has two on each side, so it is presented.
                        let history = vec![((7u64, 3u64), t0, 1i64), ((7, 3), t1, -1), ((7, 4), t0, 1), ((8, 1), t0, 1), ((8, 2), t1, 1)];
                        let other = vec![((7u64, 70u64), t1, 1i64), ((8, 80), t0, 1), ((8, 81), t1, -1)];
                        let run = |backend: PairJoinAt<N5>, wrap: bool| {
                            let (b0, b1) = (vec![hbatch(history.clone(), N5(0), N5(0))], vec![hbatch(other.clone(), N5(0), N5(0))]);
                            if wrap { joined_at(ProxyJoinTactic::new(BackendFrontier::new(backend, frontier.clone())).prep(b0, b1, Fresh::Input1, lower)) }
                            else { joined_at(ProxyJoinTactic::new(backend).prep(b0, b1, Fresh::Input1, lower)) }
                        };
                        for wrap in [false, true] {
                            let completing = PairJoinAt::new(apply_lower, true);
                            let count = Rc::clone(&completing.completed);
                            let got = run(completing, wrap);
                            let want = run(PairJoinAt::new(apply_lower, false), wrap);
                            assert_eq!(got, want, "F {frontier:?}, lower {lower:?}, t0 {t0:?}, t1 {t1:?}, apply_lower {apply_lower}, wrapped {wrap}");
                            // The wrapper reads, and so asks for every key, exactly when the bound is not beyond `F`.
                            if wrap && !beyond(&frontier, lower) { completed_read += count.get(); } else { completed_passed += count.get(); }
                        }
                    }
                }
            }
        }
    }
    assert!(completed_passed > 0, "the completing backend completed keys");
    assert_eq!(completed_read, 0, "a unit read as of a frontier presents every key");
}

/// A reduce backend that completes the keys [`direct`](differential_dataflow::operators::int_proxy::reduce::direct)
/// finds, over any backend that presents.
///
/// It has the inner backend present the window, then evaluates each direct key that the tactic does
/// not owe a time through the inner backend's own `reduce_corrections`, and removes it from the
/// window. Its corrections are held, and committed with the window's presented keys' at the next
/// `emit`, or before the next window or the finish if no `emit` follows.
struct Completing<B, T> {
    inner: B,
    upper: Antichain<T>,
    held: Vec<((u64, u64), T, i64)>,
    /// The keys completed, and the times they carried.
    completed: Arc<Mutex<(usize, usize)>>,
}

impl<B, T> Completing<B, T> {
    fn new(inner: B) -> Self { Completing { inner, upper: Antichain::new(), held: Vec::new(), completed: Arc::default() } }
}

impl<T, B1, B2, B> differential_dataflow::operators::int_proxy::ProxyReduceBackend<T, B1, B2> for Completing<B, T>
where
    T: Timestamp + Lattice,
    B: differential_dataflow::operators::int_proxy::ProxyReduceBackend<T, B1, B2, Key = u64, VIn = u64, VOut = u64, RIn = i64, ROut = i64>,
{
    type Key = u64;
    type VIn = u64;
    type VOut = u64;
    type RIn = i64;
    type ROut = i64;

    fn begin(&mut self, description: differential_dataflow::trace::Description<T>) {
        self.upper = description.upper().clone();
        self.inner.begin(description)
    }

    fn next_window(
        &mut self,
        instance: &differential_dataflow::operators::int_proxy::ReduceInstance<'_, T, B1, B2>,
        changed: &[u64],
        from: &mut KeyPosition<u64>,
        window: &mut differential_dataflow::operators::int_proxy::ReduceWindow<T, i64, i64>,
    ) {
        use differential_dataflow::operators::int_proxy::reduce::direct;
        self.emit(&[]);
        self.inner.next_window(instance, changed, from, window);
        if instance.present_all { return; }
        let (input, seeds, output) = (std::mem::take(&mut window.input), std::mem::take(&mut window.seeds), std::mem::take(&mut window.output));
        let mut keys: Vec<u64> = input.iter().map(|r| r.0.0).chain(seeds.iter().map(|s| s.0)).chain(output.iter().map(|r| r.0.0)).collect();
        keys.sort();
        keys.dedup();
        let (mut done, mut ats, mut in_ends, mut in_all, mut out_ends, mut out_all) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut completed, mut carried) = (0, 0);
        for key in keys {
            let ins: Vec<_> = input.iter().filter(|r| r.0.0 == key).cloned().collect();
            let novel: Vec<_> = seeds.iter().filter(|s| s.0 == key).cloned().collect();
            let outs: Vec<_> = output.iter().filter(|r| r.0.0 == key).cloned().collect();
            let at = if changed.binary_search(&key).is_ok() { None } else {
                direct(novel.iter().map(|s| &s.1), ins.iter().map(|r| &r.1).chain(outs.iter().map(|r| &r.1)), self.upper.borrow()).cloned()
            };
            let Some(at) = at else {
                window.input.extend(ins);
                window.seeds.extend(novel);
                window.output.extend(outs);
                continue;
            };
            completed += 1;
            // The records not at or before `at` carry their joins with it.
            for record in ins.iter().map(|r| &r.1).chain(outs.iter().map(|r| &r.1)).filter(|t| !t.less_equal(&at)) {
                window.carried.push((record.join(&at), key));
                carried += 1;
            }
            let accumulate = |rows: &[((u64, u64), T, i64)], all: &mut Vec<(u64, i64)>, ends: &mut Vec<usize>| {
                let start = all.len();
                all.extend(rows.iter().filter(|r| r.1.less_equal(&at)).map(|r| (r.0.1, r.2)));
                differential_dataflow::consolidation::consolidate_from(all, start);
                ends.push(all.len());
            };
            accumulate(&ins, &mut in_all, &mut in_ends);
            accumulate(&outs, &mut out_all, &mut out_ends);
            done.push(key);
            ats.push(at);
        }
        let mut count = self.completed.lock().unwrap();
        count.0 += completed;
        count.1 += carried;
        drop(count);
        if !done.is_empty() {
            let (corrections, ends) = self.inner.reduce_corrections(&done, &in_ends, &in_all, &out_ends, &out_all);
            let mut start = 0;
            for ((key, at), end) in done.iter().zip(ats).zip(ends) {
                self.held.extend(corrections[start..end].iter().map(|(id, diff)| ((*key, *id), at.clone(), *diff)));
                start = end;
            }
        }
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, i64)], out_ends: &[usize], output: &[(u64, i64)]) -> (Vec<(u64, i64)>, Vec<usize>) {
        self.inner.reduce_corrections(keys, in_ends, input, out_ends, output)
    }

    fn emit(&mut self, records: &[((u64, u64), T, i64)]) {
        if self.held.is_empty() && records.is_empty() { return; }
        let mut all = std::mem::take(&mut self.held);
        all.extend_from_slice(records);
        consolidate_updates(&mut all);
        self.inner.emit(&all);
    }

    fn finish(&mut self) -> Option<B2> {
        self.emit(&[]);
        self.inner.finish()
    }
}

/// A key whose only seed is in the retire and whose other record joins with it beyond `upper` is
/// completed, and carries that join: the next retire must evaluate the key there.
#[test]
fn reduce_completed_key_carries_time_beyond_upper() {
    let time = Product::<u64, u64>::new;
    let seed = time(2, 0);
    let mut lower = Antichain::from_elem(time(0, 2));
    lower.insert(seed);
    // The prior record at (1, 1) is not at or before the seed, and joins with it at (2, 1).
    let first_upper = Antichain::from_elem(time(2, 1));
    let last_upper = Antichain::from_elem(time(3, 3));
    let prior = hbatch(vec![((7u64, 9u64), time(1, 1), 1i64)], time(0, 0), time(1, 1));
    let input = hbatch(vec![((7u64, 3u64), seed, 1i64)], time(0, 0), time(2, 2));
    let run = |backend: &mut dyn ReduceTactic<Product<u64, u64>, Span<u64, (u64, u64), Product<u64, u64>, i64>, Span<u64, (u64, u64), Product<u64, u64>, i64>>| {
        let (first, carried) = backend.retire(vec![prior.clone()], vec![prior.clone()], vec![input.clone()], &lower, &first_upper, &lower);
        let mut outputs: Vec<_> = vec![prior.clone()];
        outputs.extend(first.into_iter().filter_map(|s| s.inner));
        let (second, pending) = backend.retire(vec![prior.clone(), input.clone()], outputs.clone(), vec![], &first_upper, &last_upper, &carried);
        outputs.extend(second.into_iter().filter_map(|s| s.inner));
        (carried, pending, hread(&outputs[1..]))
    };
    let completing = Completing::new(max_backend_at());
    let count = Arc::clone(&completing.completed);
    let got = run(&mut ProxyReduceTactic::new(completing));
    let want = run(&mut ProxyReduceTactic::new(max_backend_at()));
    // The prior input record and the prior output record each carry (2, 1).
    assert_eq!(*count.lock().unwrap(), (1, 2), "the key is completed in the first retire, and carries its two records' joins");
    assert_eq!(got.0, Antichain::from_elem(time(2, 1)), "the carried time holds the first retire's frontier");
    assert_eq!(got, want);
    assert_eq!(got.2, vec![((7, 3), seed, 1), ((7, 3), time(2, 1), -1)]);
    assert!(got.1.is_empty());
}

/// The max-reduce's backend at product times.
fn max_backend_at() -> VecReduceBackend<u64, u64, u64, Product<u64, u64>, i64, MaxLogic> { VecReduceBackend::new(max_logic as MaxLogic) }

/// The partial-order oracle's three retires, with keys completed in the third.
#[test]
fn reduce_completed_keys_match_partial_order_oracle() {
    type T = Product<u64, u64>;
    let time = T::new;
    let (mut initial, mut novel) = (Vec::new(), Vec::new());
    for key in 0..64u64 {
        initial.extend([((key, 3), time(0, 0), 1i64), ((key, 7), time(1, 0), 1), ((key, 9), time(0, 1), 1)]);
        if key % 2 == 0 { novel.push(((key, 9), time(2, 2), -1)); }
        if key % 3 == 0 { novel.push(((key, 7), time(2, 2), -1)); }
        if key % 6 == 0 { novel.push(((key, 3), time(2, 2), -1)); }
    }
    let run = |tactic: &mut dyn ReduceTactic<T, Span<u64, (u64, u64), T, i64>, Span<u64, (u64, u64), T, i64>>| {
        let source = hbatch(initial.clone(), time(0, 0), time(1, 1));
        let lower = Antichain::from_elem(time(0, 0));
        let middle = Antichain::from_elem(time(1, 1));
        let upper = Antichain::from_elem(time(2, 2));
        let (first, pending1) = tactic.retire(vec![], vec![], vec![source.clone()], &lower, &middle, &lower);
        let mut all: Vec<_> = first.into_iter().filter_map(|s| s.inner).collect();
        let (second, pending2) = tactic.retire(vec![source.clone()], all.clone(), vec![], &middle, &upper, &pending1);
        all.extend(second.into_iter().filter_map(|s| s.inner));
        let input = hbatch(novel.clone(), time(2, 2), time(3, 3));
        let (third, pending3) = tactic.retire(vec![source], all.clone(), vec![input], &upper, &Antichain::from_elem(time(3, 3)), &upper);
        all.extend(third.into_iter().filter_map(|s| s.inner));
        (pending1, pending2, pending3, hread(&all))
    };
    for window in [1, 5, usize::MAX] {
        let completing = Completing::new(VecReduceBackend::with_window(max_logic as MaxLogic, window));
        let count = Arc::clone(&completing.completed);
        let got = run(&mut ProxyReduceTactic::new(completing));
        let want = run(&mut ProxyReduceTactic::new(VecReduceBackend::with_window(max_logic as MaxLogic, window)));
        assert_eq!(got, want, "window {window}");
        // Keys divisible by 2 or 3 change in the third retire, each at one time with all records before it.
        assert_eq!(count.lock().unwrap().0, (0..64).filter(|k| k % 2 == 0 || k % 3 == 0).count(), "window {window}");
    }
}

/// The reduce over N5, read as of each frontier and not, by a completing backend and a presenting one.
#[test]
fn reduce_completed_keys_as_of_frontier_in_n5() {
    type MaxN5 = VecReduceBackend<u64, u64, u64, N5, i64, MaxLogic>;
    let backend = || -> MaxN5 { VecReduceBackend::new(max_logic as MaxLogic) };
    let (mut completed_passed, mut completed_read) = (0, 0);
    for frontier in antichains() {
        for lower in N5::ALL {
            let lower_ac = Antichain::from_elem(lower);
            for tp in N5::ALL {
                for tn in N5::ALL.into_iter().filter(|t| lower.less_equal(t)) {
                    for retract in [false, true] {
                        let prior = vec![((7u64, 3u64), tp, 1i64), ((7, 2), tp, 1)];
                        let mut novel = vec![((7u64, 9u64), tn, 1i64)];
                        if retract { novel.push(((7, 3), tn, -1)); }
                        let run = |tactic: &mut dyn ReduceTactic<N5, Span<u64, (u64, u64), N5, i64>, Span<u64, (u64, u64), N5, i64>>| {
                            let (produced, carried) = tactic.retire(vec![hbatch(prior.clone(), N5(0), N5(0))], vec![], vec![hbatch(novel.clone(), N5(0), N5(0))], &lower_ac, &Antichain::new(), &lower_ac);
                            (carried, produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect::<Vec<_>>())
                        };
                        for wrap in [false, true] {
                            let completing = Completing::new(backend());
                            let count = Arc::clone(&completing.completed);
                            let (got, want) = if wrap {
                                (run(&mut ProxyReduceTactic::new(BackendFrontier::new(completing, frontier.clone()))),
                                 run(&mut ProxyReduceTactic::new(BackendFrontier::new(backend(), frontier.clone()))))
                            } else {
                                (run(&mut ProxyReduceTactic::new(completing)), run(&mut ProxyReduceTactic::new(backend())))
                            };
                            assert_eq!(got, want, "F {frontier:?}, lower {lower:?}, prior at {tp:?}, novel at {tn:?}, retract {retract}, wrapped {wrap}");
                            let completed = count.lock().unwrap().0;
                            if wrap && !beyond(&frontier, lower) { completed_read += completed; } else { completed_passed += completed; }
                        }
                    }
                }
            }
        }
    }
    assert!(completed_passed > 0, "the completing backend completed keys");
    assert_eq!(completed_read, 0, "a retire read as of a frontier presents every key");
}

/// Label propagation to a fixed point, over a graph that changes across outer times, one time at a
/// time, through a completing backend and a presenting one. Inside the loop a key's records from
/// earlier rounds can join its seed beyond the retire's upper bound, so completed keys carry times.
#[test]
fn reduce_completed_keys_inside_iterate() {
    use differential_dataflow::input::InputSession;
    use timely::dataflow::operators::probe::{Handle as ProbeHandle, Probe};
    let run = |complete: bool| {
        let out = Arc::new(Mutex::new(Vec::<((u64, u64), u64, i64)>::new()));
        let count = Arc::new(Mutex::new((0, 0)));
        let (os, cs) = (out.clone(), count.clone());
        // A chain 11 -> 10 -> ... -> 0, so node 0 learns label 11 eleven rounds in. At time 1 node 12
        // arrives with an edge to node 0, which learns 12 a round in: its records from round 11 at
        // time 0 join that seed beyond the retire.
        let mut nodes: Vec<((u64, u64), u64, i64)> = (0..12u64).map(|n| ((n, n), 0, 1)).collect();
        nodes.push(((12, 12), 1, 1));
        let mut edges: Vec<((u64, u64), u64, i64)> = (0..11u64).map(|n| ((n + 1, n), 0, 1)).collect();
        edges.extend([((12, 0), 1, 1), ((6, 5), 2, -1), ((3, 9), 3, 1), ((12, 0), 4, -1), ((6, 5), 5, 1)]);
        timely::execute_directly(move |worker| {
            let mut node_input: InputSession<u64, (u64, u64), i64> = InputSession::new();
            let mut edge_input: InputSession<u64, (u64, u64), i64> = InputSession::new();
            let probe = worker.dataflow::<u64, _, _>(|scope| {
                let nodes = node_input.to_collection(scope);
                let edges = edge_input.to_collection(scope);
                let mut probe = ProbeHandle::new();
                nodes.clone().iterate(|scope, inner| {
                    let edges = edges.enter(scope);
                    let nodes = nodes.enter(scope);
                    let hashed = inner.join_map(edges, |_src, label, dst| (*dst, *label)).concat(nodes).map(|(k, v)| (k.hashed(), (k, v)));
                    let arr = arrange_core::<Pipeline, Vec<((u64, (u64, u64)), Product<u64, u64>, i64)>, _, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>>(hashed.inner, Pipeline, "ArrLabels", VChunkBatcher::new);
                    let backend = VecReduceBackend::with_window(max_logic as MaxLogic, 4);
                    let reduced = if complete {
                        let mut completing = Completing::new(backend);
                        completing.completed = cs;
                        reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>, _>(arr, "Labels", ProxyReduceTactic::new(completing))
                    } else {
                        reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), Product<u64, u64>, i64>, _>(arr, "Labels", ProxyReduceTactic::new(backend))
                    };
                    reduced.as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
                })
                .inspect(move |(d, t, r)| os.lock().unwrap().push((*d, *t, *r)))
                .inner.probe_with(&mut probe);
                probe
            });
            for epoch in 0..6u64 {
                for (d, _, r) in nodes.iter().filter(|u| u.1 == epoch) { node_input.update(*d, *r); }
                for (d, _, r) in edges.iter().filter(|u| u.1 == epoch) { edge_input.update(*d, *r); }
                node_input.advance_to(epoch + 1);
                edge_input.advance_to(epoch + 1);
                node_input.flush();
                edge_input.flush();
                while probe.less_than(&(epoch + 1)) { worker.step(); }
            }
        });
        let mut got = out.lock().unwrap().clone();
        consolidate_updates(&mut got);
        let count = *count.lock().unwrap();
        (got, count)
    };
    let (got, (completed, carried)) = run(true);
    let (want, _) = run(false);
    assert_eq!(got, want);
    println!("inside iterate: {completed} keys completed, carrying {carried} times");
    assert!(completed > 0 && carried > 0, "keys were completed ({completed}) and carried times ({carried})");
}
