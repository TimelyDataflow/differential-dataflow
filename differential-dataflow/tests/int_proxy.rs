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

// ===== Reading wrapped batches =====
//
// The trace wrappers change only times. A batch entered into a nested scope (`BatchEnter`) reads
// each stored time entered; a batch of a trace read as of a frontier (`BatchFrontier`) reads each
// advanced by `since`, and none at or beyond `until`. Before `since` such a trace is not correct:
// compaction can leave a retraction advanced to `since` beside the insertion it retracts. The
// backend wrappers (`BackendEnter`, `BackendFrontier`) read the times their inner backends draw as
// the wrappers say. The reference is the same tactic over the batch rebuilt with every time read
// as a cursor through the wrappers reads it.

use differential_dataflow::operators::int_proxy::join::{JoinInstance, JoinMatches, ProxyJoinBackend, ProxyJoinTactic};
use differential_dataflow::operators::int_proxy::{BackendEnter, BackendFrontier, KeyPosition, ProxyBridge};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::trace::wrappers::enter::BatchEnter;
use differential_dataflow::trace::wrappers::frontier::BatchFrontier;

/// A time of the nested scope the tests enter: an outer time, and a round.
type Nested = Product<u64, u64>;
/// The max-reduce's logic, as a plain function, so that its backends name one type.
type MaxLogic = fn(&u64, &[(u64, i64)], &mut Vec<(u64, i64)>, &mut Vec<(u64, i64)>);
/// The max-reduce's backend, producing at times `T`.
type MaxBackend<T> = VecReduceBackend<u64, u64, u64, T, i64, MaxLogic>;

/// `rows` as a hash-keyed batch read as of `since` (and before `until`), and the same rows with
/// every time read as a cursor through the frontier reads it.
fn as_of_pair(rows: Vec<((u64, u64), u64, i64)>, since: u64, until: Option<u64>) -> (BatchFrontier<Span<u64, (u64, u64), u64, i64>, u64>, Vec<((u64, u64), u64, i64)>) {
    let until = until.map_or_else(Antichain::new, Antichain::from_elem);
    let wrapped = BatchFrontier::make_from(hbatch(rows.clone(), 0, 0), Antichain::from_elem(since).borrow(), until.borrow());
    let read = rows.into_iter().filter_map(|(d, mut t, r)| wrapped.advance_time(&mut t).then_some((d, t, r))).collect();
    (wrapped, read)
}

/// `rows` with each time entered into the nested scope, at round 0.
fn entered(rows: Vec<((u64, u64), u64, i64)>) -> Vec<((u64, u64), Nested, i64)> {
    rows.into_iter().map(|(d, t, r)| (d, Product::new(t, 0), r)).collect()
}

/// The history the frontier hides: (7, 3) inserted at 1 and retracted at 5, beside (7, 4) at 1;
/// (8, 1) inserted at 1 and retracted at 5, so key 8 is empty as of 5; and (9, 2) inserted at 1
/// and retracted at 6. Read as of 5, nothing happens before 5.
fn history() -> Vec<((u64, u64), u64, i64)> {
    vec![((7, 3), 1, 1), ((7, 3), 5, -1), ((7, 4), 1, 1), ((8, 1), 1, 1), ((8, 1), 5, -1), ((9, 2), 1, 1), ((9, 2), 6, -1)]
}

/// One retire of a reduce tactic over `input`, between `lower` and `upper`, its output read.
fn retire<B1, T: Lattice + Timestamp>(mut tactic: impl ReduceTactic<T, B1, Span<u64, (u64, u64), T, i64>>, input: B1, lower: T, upper: T) -> Vec<((u64, u64), T, i64)> {
    let (produced, _) = tactic.retire(vec![], vec![], vec![input], &Antichain::from_elem(lower.clone()), &Antichain::from_elem(upper), &Antichain::from_elem(lower));
    produced.into_iter().filter_map(|b| b.inner).flat_map(|b| hread(&[b])).collect()
}

/// The max-reduce's backend.
fn max_backend<T>() -> MaxBackend<T> { VecReduceBackend::new(max_logic as MaxLogic) }

#[test]
fn reduce_reads_as_of_since() {
    for until in [None, Some(6)] {
        let (wrapped, read) = as_of_pair(history(), 5, until);
        let got = retire(ProxyReduceTactic::new(BackendFrontier::new(max_backend())), wrapped, 0, 10);
        let want = retire(ProxyReduceTactic::new(max_backend()), hbatch(read, 0, 0), 0, 10);
        assert_eq!(got, want, "until {until:?}");
        assert!(got.iter().all(|(_, t, _)| *t >= 5), "nothing before since: {got:?}");
    }
}

#[test]
fn reduce_reads_entered_as_of_since() {
    let (zero, ten) = (Product::new(0, 0), Product::new(10, 0));
    for until in [None, Some(6)] {
        let (wrapped, read) = as_of_pair(history(), 5, until);
        let backend = BackendEnter::<_, u64>::new(BackendFrontier::new(max_backend::<Nested>()));
        let got = retire(ProxyReduceTactic::new(backend), BatchEnter::<_, Nested>::make_from(wrapped), zero, ten);
        let want = retire(ProxyReduceTactic::new(max_backend()), hbatch(entered(read), zero, zero), zero, ten);
        assert_eq!(got, want, "until {until:?}");
    }
    // Entering alone reads the history at its own times, entered.
    let backend = BackendEnter::<_, u64>::new(max_backend::<Nested>());
    let only_entered = retire(ProxyReduceTactic::new(backend), BatchEnter::<_, Nested>::make_from(hbatch(history(), 0, 0)), zero, ten);
    let reference = retire(ProxyReduceTactic::new(max_backend()), hbatch(entered(history()), zero, zero), zero, ten);
    assert_eq!(only_entered, reference);
}

/// A join backend over hash-keyed `u64` pairs, which proxy themselves, at any input times: it draws
/// every update of both inputs in one block, and crosses matches into `((key, (val0, val1)), time,
/// diff)` rows.
struct PairJoin;
type Pairs<T> = Span<u64, (u64, u64), T, i64>;
type Joined<T> = Vec<((u64, ((u64, u64), (u64, u64))), T, i64)>;
impl<T, T0, T1> ProxyJoinBackend<T, Pairs<T0>, Pairs<T1>, T0, T1> for PairJoin
where T: Lattice + Timestamp, T0: Lattice + Timestamp, T1: Lattice + Timestamp,
{
    type Key = u64;
    type V0 = (u64, u64);
    type V1 = (u64, u64);
    type R0 = i64;
    type R1 = i64;
    type ROut = i64;
    type Output = Joined<T>;
    fn advance(&mut self, instance: &JoinInstance<T0, Pairs<T0>, Pairs<T1>, T1>, from: &mut KeyPosition<u64>, bridge0: &mut ProxyBridge<T0, i64, u64, (u64, u64)>, bridge1: &mut ProxyBridge<T1, i64, u64, (u64, u64)>) {
        fn draw<T: Lattice + Timestamp>(batches: &[Pairs<T>]) -> ProxyBridge<T, i64, u64, (u64, u64)> {
            let mut rows: Vec<_> = hread(batches).into_iter().map(|(kv, t, r)| ((kv.0.hashed(), kv), t, r)).collect();
            consolidate_updates(&mut rows);
            rows
        }
        let (b0, b1) = (draw(&instance.batches0), draw(&instance.batches1));
        bridge0.extend(b0.iter().filter(|r| b1.iter().any(|s| s.0.0 == r.0.0)).cloned());
        bridge1.extend(b1.iter().filter(|r| b0.iter().any(|s| s.0.0 == r.0.0)).cloned());
        *from = KeyPosition::End;
    }
    fn cross(&mut self, _instance: &JoinInstance<T0, Pairs<T0>, Pairs<T1>, T1>, matches: &mut JoinMatches<T, i64, u64, (u64, u64), (u64, u64)>, output: &mut Vec<Self::Output>) {
        let rows = matches.ids.iter().zip(&matches.times).zip(&matches.diffs).map(|((id, t), r)| (*id, t.clone(), *r)).collect();
        output.push(rows);
    }
}

/// The join's output, keyed by its two values and consolidated.
fn joined<T: Ord>(rows: Box<dyn Iterator<Item = Joined<T>>>) -> Vec<(((u64, u64), (u64, u64)), T, i64)> {
    let mut out: Vec<_> = rows.flatten().map(|((_, (a, b)), t, r)| ((a, b), t, r)).collect();
    consolidate_updates(&mut out);
    out
}

#[test]
fn join_reads_as_of_since() {
    // The other input: a row for each key, at 2. Key 8 has nothing to meet as of 5.
    let other = hbatch::<u64, u64, u64, i64>(vec![((7, 70), 2, 1), ((8, 80), 2, 1), ((9, 90), 2, 1)], 0, 0);
    for until in [None, Some(6)] {
        let (wrapped, read) = as_of_pair(history(), 5, until);
        let got = joined(ProxyJoinTactic::new(BackendFrontier::new(PairJoin)).prep(vec![wrapped.clone()], vec![Rc::clone(&other)], Fresh::Input0, 0));
        let want = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(read.clone(), 0, 0)], vec![Rc::clone(&other)], Fresh::Input0, 0));
        assert_eq!(got, want, "until {until:?}");
        assert!(got.iter().all(|(_, t, _)| *t >= 5), "nothing before since: {got:?}");
        // The same on the second input.
        let got_right = joined(ProxyJoinTactic::new(BackendFrontier::right(PairJoin)).prep(vec![Rc::clone(&other)], vec![wrapped], Fresh::Input0, 0));
        let want_right = joined(ProxyJoinTactic::new(PairJoin).prep(vec![Rc::clone(&other)], vec![hbatch(read, 0, 0)], Fresh::Input0, 0));
        assert_eq!(got_right, want_right, "until {until:?}, second input");
        // Read as it is stored, the history would join at times before since.
        let raw = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(history(), 0, 0)], vec![Rc::clone(&other)], Fresh::Input0, 0));
        assert!(raw.iter().any(|(_, t, _)| *t < 5));
    }
}

#[test]
fn join_reads_entered_as_of_since() {
    // The other input lives in the nested scope, at round 1.
    let other = hbatch::<u64, u64, Nested, i64>(vec![((7, 70), Product::new(2, 1), 1), ((8, 80), Product::new(2, 1), 1), ((9, 90), Product::new(2, 1), 1)], Product::new(0, 0), Product::new(0, 0));
    let meet = Product::new(0, 0);
    for until in [None, Some(6)] {
        let (wrapped, read) = as_of_pair(history(), 5, until);
        let backend = BackendEnter::<_, u64>::new(BackendFrontier::new(PairJoin));
        let got = joined(ProxyJoinTactic::new(backend).prep(vec![BatchEnter::<_, Nested>::make_from(wrapped)], vec![Rc::clone(&other)], Fresh::Input0, meet));
        let want = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(entered(read), meet, meet)], vec![Rc::clone(&other)], Fresh::Input0, meet));
        assert_eq!(got, want, "until {until:?}");
    }
    // Entering alone, on the second input.
    let entering = BackendEnter::<_, u64, _>::right(PairJoin);
    let got = joined(ProxyJoinTactic::new(entering).prep(vec![Rc::clone(&other)], vec![BatchEnter::<_, Nested>::make_from(hbatch(history(), 0, 0))], Fresh::Input0, meet));
    let want = joined(ProxyJoinTactic::new(PairJoin).prep(vec![Rc::clone(&other)], vec![hbatch(entered(history()), meet, meet)], Fresh::Input0, meet));
    assert_eq!(got, want);
    // Both inputs entered and read as of a frontier.
    let (left, left_read) = as_of_pair(history(), 5, None);
    let (right, right_read) = as_of_pair(vec![((7, 70), 2, 1), ((8, 80), 2, 1), ((9, 90), 6, 1)], 4, None);
    let both = BackendEnter::<_, u64>::new(BackendEnter::<_, u64, _>::right(BackendFrontier::new(BackendFrontier::right(PairJoin))));
    let got_both = joined(ProxyJoinTactic::new(both).prep(vec![BatchEnter::<_, Nested>::make_from(left)], vec![BatchEnter::<_, Nested>::make_from(right)], Fresh::Input0, meet));
    let want_both = joined(ProxyJoinTactic::new(PairJoin).prep(vec![hbatch(entered(left_read), meet, meet)], vec![hbatch(entered(right_read), meet, meet)], Fresh::Input0, meet));
    assert_eq!(got_both, want_both, "both inputs");
}
