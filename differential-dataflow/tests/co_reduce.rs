//! Tests for `co_reduce_with_tactic` over the proxy reduce tactic and its reference backend.

use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use timely::dataflow::channels::pact::Pipeline;
use timely::order::Product;
use timely::progress::{Antichain, Timestamp};

use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::co_reduce::co_reduce_with_tactic;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
use differential_dataflow::operators::int_proxy::vec_backend::VecReduceBackend;
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::vec::{ChunkBatcher as VChunkBatcher, ChunkSpine as VChunkSpine, VecChunk};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::{Navigable, Span};

type Batch<T = u64> = Rc<ChunkBatch<VecChunk<u64, (u64, u64), T, i64>>>;
type Spine<T = u64> = VChunkSpine<u64, (u64, u64), T, i64>;

/// Reads tagged batches as one list of plain batches, which reduces the sum of the inputs. Counts
/// the handed batches that hold a time at or beyond `upper`.
struct Untag<Tac> {
    inner: Tac,
    ahead: Arc<AtomicUsize>,
}

impl<T: Timestamp + Lattice, Tac: ReduceTactic<T, Batch<T>, Batch<T>>> ReduceTactic<T, (usize, Batch<T>), Batch<T>> for Untag<Tac> {
    fn retire(
        &mut self,
        source_batches: Vec<(usize, Batch<T>)>,
        output_batches: Vec<Batch<T>>,
        input_batches: Vec<(usize, Batch<T>)>,
        lower: &Antichain<T>,
        upper: &Antichain<T>,
        held: &Antichain<T>,
    ) -> (Option<Span<T, Batch<T>>>, Antichain<T>) {
        for (_, batch) in &input_batches {
            let mut beyond = false;
            for chunk in &batch.chunks {
                let mut c = chunk.cursor();
                while c.key_valid(chunk) {
                    while c.val_valid(chunk) {
                        c.map_times(chunk, |t, _| beyond |= upper.less_equal(t));
                        c.step_val(chunk);
                    }
                    c.step_key(chunk);
                }
            }
            if beyond { self.ahead.fetch_add(1, Ordering::Relaxed); }
        }
        let untag = |batches: Vec<(usize, Batch<T>)>| batches.into_iter().map(|(_, b)| b).collect();
        self.inner.retire(untag(source_batches), output_batches, untag(input_batches), lower, upper, held)
    }
}

/// Arranges `c` by the hash of its key, as the reference backend reads it.
fn arrange<'s, T: Timestamp + Lattice>(c: VecCollection<'s, T, (u64, u64), i64>) -> Arranged<'s, TraceAgent<Spine<T>>> {
    let hashed = c.map(|(k, v)| (k.hashed(), (k, v)));
    arrange_core::<Pipeline, Vec<((u64, (u64, u64)), T, i64)>, _, Spine<T>>(hashed.inner, Pipeline, "Arrange", VChunkBatcher::new)
}

#[expect(clippy::ptr_arg, reason = "the signature the reduce logic takes")]
fn max_logic(_k: &u64, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>) {
    if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() {
        updates.push((m, 1));
    }
    for (w, d) in current.iter() {
        updates.push((*w, -*d));
    }
}

/// Input `a` runs twenty ticks ahead of input `b`, which advances one time at a time. The driver
/// hands over `a`'s batches, whose times lie beyond the interval it retires, and the output still
/// matches the reduce of the concatenation.
#[test]
fn input_ahead_matches_concat() {
    let got = Arc::new(Mutex::new(Vec::new()));
    let want = Arc::new(Mutex::new(Vec::new()));
    let ahead = Arc::new(AtomicUsize::new(0));
    let (g, w, seen) = (Arc::clone(&got), Arc::clone(&want), Arc::clone(&ahead));
    timely::execute_directly(move |worker| {
        let mut a = InputSession::<u64, (u64, u64), i64>::new();
        let mut b = InputSession::<u64, (u64, u64), i64>::new();
        let (g, w, seen) = (Arc::clone(&g), Arc::clone(&w), Arc::clone(&seen));
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let (ca, cb) = (a.to_collection(scope), b.to_collection(scope));
            let tactic = Untag { inner: ProxyReduceTactic::new(VecReduceBackend::with_window(max_logic, 3)), ahead: seen };
            co_reduce_with_tactic::<_, Spine, _>(vec![arrange(ca.clone()), arrange(cb.clone())], "CoReduce", tactic)
                .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
                .inspect(move |(d, t, r)| g.lock().unwrap().push((*d, *t, *r)));
            ca.concat(cb)
                .reduce(|_k, input, output| {
                    if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).max() { output.push((*m, 1)); }
                })
                .inspect(move |(d, t, r)| w.lock().unwrap().push((*d, *t, *r)))
                .probe()
                .0
        });

        // A deterministic stream of insertions over keys 0..5, each later retracted.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |bound: u64| { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state % bound };
        let mut live_a = Vec::new();
        for t in 0..20 {
            a.advance_to(t);
            for _ in 0..4 {
                let row = (next(5), next(30));
                a.update(row, 1);
                live_a.push(row);
            }
            if t % 3 == 2 { a.update(live_a.swap_remove(0), -1); }
        }
        a.advance_to(20);
        a.flush();
        let mut live_b = Vec::new();
        for t in 0..20 {
            b.advance_to(t);
            for _ in 0..2 {
                let row = (next(5), next(30));
                b.update(row, 1);
                live_b.push(row);
            }
            if t % 2 == 1 { b.update(live_b.swap_remove(0), -1); }
            b.advance_to(t + 1);
            b.flush();
            // `a` stays twenty ticks ahead, with a batch of its own each round.
            let row = (next(5), next(30));
            a.update(row, 1);
            live_a.push(row);
            if t % 4 == 3 { a.update(live_a.swap_remove(0), -1); }
            a.advance_to(21 + t);
            a.flush();
            for _ in 0..4 { worker.step(); }
        }
        a.close();
        b.close();
        while !probe.done() { worker.step(); }
    });
    assert!(ahead.load(Ordering::Relaxed) >= 10, "few handed batches held a time beyond the interval");
    let (mut got, mut want) = (got.lock().unwrap().clone(), want.lock().unwrap().clone());
    differential_dataflow::consolidation::consolidate_updates(&mut got);
    differential_dataflow::consolidation::consolidate_updates(&mut want);
    assert!(!want.is_empty());
    assert_eq!(got, want);
}

#[expect(clippy::ptr_arg, reason = "the signature the reduce logic takes")]
fn min_logic(_k: &u64, input: &[(u64, i64)], current: &mut Vec<(u64, i64)>, updates: &mut Vec<(u64, i64)>) {
    if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).min() {
        updates.push((m, 1));
    }
    for (w, d) in current.iter() {
        updates.push((*w, -*d));
    }
}

/// Inside an iterative scope, input `a` enters from outside and runs several outer epochs ahead
/// of the loop's feedback, which counts each key's least value down to zero. The driver hands
/// over `a`'s batches, whose times lie beyond the interval it retires while the loop iterates,
/// and the reduce's output at every iteration still matches the reduce of the concatenation.
#[test]
fn input_ahead_in_loop_matches_concat() {
    let got = Arc::new(Mutex::new(Vec::new()));
    let want = Arc::new(Mutex::new(Vec::new()));
    let ahead = Arc::new(AtomicUsize::new(0));
    let (g, w, seen) = (Arc::clone(&got), Arc::clone(&want), Arc::clone(&ahead));
    timely::execute_directly(move |worker| {
        let mut a = InputSession::<u64, (u64, u64), i64>::new();
        let mut b = InputSession::<u64, (u64, u64), i64>::new();
        let (g, w, seen) = (Arc::clone(&g), Arc::clone(&w), Arc::clone(&seen));
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let (ca, cb) = (a.to_collection(scope), b.to_collection(scope));
            let (la, lb) = (ca.clone(), cb.clone());
            let co = scope.iterate(move |inner, x| {
                // The loop's own input: `b` and each key's least value less one, down to zero,
                // which is the fixed point.
                let step = lb.enter(inner).concat(x.map(|(k, v): (u64, u64)| (k, v.saturating_sub(1))));
                let tactic = Untag { inner: ProxyReduceTactic::new(VecReduceBackend::with_window(min_logic, 3)), ahead: seen };
                co_reduce_with_tactic::<_, Spine<Product<u64, u64>>, _>(vec![arrange(la.enter(inner)), arrange(step)], "CoReduce", tactic)
                    .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
                    .inspect(move |(d, t, r)| g.lock().unwrap().push((*d, *t, *r)))
            }).probe().0;
            let reference = scope.iterate(move |inner, x| {
                let step = cb.enter(inner).concat(x.map(|(k, v): (u64, u64)| (k, v.saturating_sub(1))));
                ca.enter(inner).concat(step).reduce(|_k, input, output| {
                    if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).min() { output.push((*m, 1)); }
                })
                .inspect(move |(d, t, r)| w.lock().unwrap().push((*d, *t, *r)))
            }).probe().0;
            (co, reference)
        });

        // A deterministic stream of insertions over keys 0..5, each later retracted.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |bound: u64| { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state % bound };
        let mut live_a = Vec::new();
        for t in 0..6 {
            a.advance_to(t);
            for _ in 0..3 {
                let row = (next(5), 5 + next(20));
                a.update(row, 1);
                live_a.push(row);
            }
            if t % 2 == 1 { a.update(live_a.swap_remove(0), -1); }
        }
        a.advance_to(6);
        a.flush();
        let mut live_b = Vec::new();
        for t in 0..24 {
            b.advance_to(t);
            for _ in 0..2 {
                let row = (next(5), 5 + next(20));
                b.update(row, 1);
                live_b.push(row);
            }
            if t % 2 == 1 { b.update(live_b.swap_remove(0), -1); }
            b.advance_to(t + 1);
            b.flush();
            // `a` stays six epochs ahead, with a batch of its own each round.
            let row = (next(5), 5 + next(20));
            a.update(row, 1);
            live_a.push(row);
            if t % 3 == 2 { a.update(live_a.swap_remove(0), -1); }
            a.advance_to(7 + t);
            a.flush();
            // A few steps, so the loop iterates while `a` is ahead.
            for _ in 0..8 { worker.step(); }
        }
        a.close();
        b.close();
        while !probe.0.done() || !probe.1.done() { worker.step(); }
    });
    assert!(ahead.load(Ordering::Relaxed) >= 20, "few handed batches held a time beyond the interval");
    let (mut got, mut want) = (got.lock().unwrap().clone(), want.lock().unwrap().clone());
    differential_dataflow::consolidation::consolidate_updates(&mut got);
    differential_dataflow::consolidation::consolidate_updates(&mut want);
    assert!(!want.is_empty());
    assert_eq!(got, want);
}
