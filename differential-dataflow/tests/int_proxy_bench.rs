//! Benchmarks the cursor tactic against the proxy tactic over IDENTICAL storage.
//!
//! Both tactics drive a reduction over the same hash-keyed `ChunkSpine` arrangement — the cursor
//! tactic through [`Arranged::reduce_core`], the proxy tactic through `reduce_with_tactic` with the
//! [`VecReduceBackend`] — so their difference is the tactic (and its backend boundary), not the
//! storage. The inherent `reduce` (its own `OrdVal`-style arrangement) is carried as a familiar
//! reference point.
//!
//! Three workloads cover the regimes that have historically mattered:
//! * `churn` — bounded per-key history, one distinct time per flush: the streaming steady state.
//! * `multimoment` — several distinct times per flush: the batched/throughput regime.
//! * `propagate` — label propagation inside `iterate`: `Product` times and carried interesting
//!   times across retires, the shape on which visiting non-due keys was quadratic (PR #824).
//!
//! Run with:
//! ```text
//! cargo test --release --test int_proxy_bench -- --ignored --nocapture
//! ```

use std::sync::{Arc, Mutex};
use std::time::Instant;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::Probe;
use timely::order::Product;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
use differential_dataflow::operators::int_proxy::vec_backend::VecReduceBackend;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::reduce::reduce_with_tactic;
use differential_dataflow::trace::chunk::vec::{
    ChunkBatcher as VChunkBatcher, ChunkBuilder as VChunkBuilder, ChunkSpine as VChunkSpine,
    VecChunk, VecChunkCursor,
};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ContainerChunker;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// The inherent `reduce`: cursor tactic over its own conventional arrangement.
    Mainline,
    /// The cursor tactic over the hash-keyed chunk arrangement.
    CursorSame,
    /// The proxy tactic + `VecReduceBackend` over the hash-keyed chunk arrangement.
    Proxy,
}

/// Arrange `(k, v) -> (hash(k), (k, v))` into a hash-keyed `ChunkSpine`.
macro_rules! harrange {
    ($coll:expr, $t:ty, $name:expr) => {{
        let hashed = $coll.map(|(k, v)| (k.hashed(), (k, v)));
        arrange_core::<
            Pipeline,
            Vec<((u64, (u64, u64)), $t, isize)>,
            ContainerChunker<VecChunk<u64, (u64, u64), $t, isize>>,
            VChunkBatcher<u64, (u64, u64), $t, isize>,
            VChunkBuilder<u64, (u64, u64), $t, isize>,
            VChunkSpine<u64, (u64, u64), $t, isize>,
        >(hashed.inner, Pipeline, $name)
    }};
}

/// The cursor tactic over a hash-keyed chunk arrangement, with `$fold` computing the desired
/// output value from the accumulated `(key, val)` pairs.
macro_rules! cursor_reduce {
    ($arr:expr, $t:ty, $name:expr, $fold:expr) => {
        $arr.reduce_core::<
            _,
            VChunkBuilder<u64, (u64, u64), $t, isize>,
            VChunkSpine<u64, (u64, u64), $t, isize>,
            <VecChunkCursor<u64, (u64, u64), $t, isize> as Cursor>::KeyContainer,
            _,
        >(
            $name,
            |_h, input, current, updates| {
                if let Some(kw) = $fold(input.iter().filter(|(_, d)| *d > 0).map(|(kv, _)| **kv)) {
                    updates.push((kw, 1));
                }
                for (w, d) in current.iter() {
                    updates.push((w.clone(), -*d));
                }
            },
            |chunk, key, list| {
                use timely::container::PushInto;
                *chunk = Default::default();
                for (v, t, d) in list.drain(..) {
                    chunk.push_into(((*key, v), t, d));
                }
            },
        )
    };
}

/// The proxy tactic + backend over a hash-keyed chunk arrangement, same `$fold`.
macro_rules! proxy_reduce {
    ($arr:expr, $t:ty, $name:expr, $fold:expr) => {
        reduce_with_tactic::<_, VChunkSpine<u64, (u64, u64), $t, isize>, _>(
            $arr,
            $name,
            ProxyReduceTactic::new(VecReduceBackend::new(
                |k: &u64, input: &[(u64, isize)], current: &mut Vec<(u64, isize)>, updates: &mut Vec<(u64, isize)>| {
                    if let Some((_, w)) = $fold(input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| (*k, *v))) {
                        updates.push((w, 1));
                    }
                    for (w, d) in current.iter() {
                        updates.push((*w, -*d));
                    }
                },
            )),
        )
    };
}

fn max_fold(iter: impl Iterator<Item = (u64, u64)>) -> Option<(u64, u64)> {
    iter.max_by_key(|kv| kv.1)
}

fn min_fold(iter: impl Iterator<Item = (u64, u64)>) -> Option<(u64, u64)> {
    iter.min_by_key(|kv| kv.1)
}

const CHURN_KEYS: u64 = 50_000;
const MM_KEYS: u64 = 10_000;
const MM_SUB: u64 = 4;
const ROUNDS: u64 = 16;
const WARMUP: u64 = 6;

/// Bounded churn, one distinct time per flush; returns averaged microseconds per round.
fn run_churn(mode: Mode) -> f64 {
    run_flat(mode, CHURN_KEYS, 1)
}

/// Bounded churn, `MM_SUB` distinct times per flush.
fn run_multimoment(mode: Mode) -> f64 {
    run_flat(mode, MM_KEYS, MM_SUB)
}

fn run_flat(mode: Mode, keys: u64, sub: u64) -> f64 {
    timely::execute_directly(move |worker| {
        let mut input: InputSession<u64, (u64, u64), isize> = InputSession::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let coll = input.to_collection(scope);
            let mut ph = ProbeHandle::new();
            match mode {
                Mode::Mainline => {
                    coll.reduce(|_k, i: &[(&u64, isize)], o: &mut Vec<(u64, isize)>| {
                        if let Some(m) = i.iter().filter(|(_, d)| *d > 0).map(|(v, _)| **v).max() {
                            o.push((m, 1));
                        }
                    })
                    .inner
                    .probe_with(&mut ph);
                }
                Mode::CursorSame => {
                    let arr = harrange!(coll, u64, "Arrange");
                    cursor_reduce!(arr, u64, "CursorReduce", max_fold).stream.probe_with(&mut ph);
                }
                Mode::Proxy => {
                    let arr = harrange!(coll, u64, "Arrange");
                    proxy_reduce!(arr, u64, "ProxyReduce", max_fold).stream.probe_with(&mut ph);
                }
            }
            ph
        });
        input.advance_to(0);
        for k in 0..keys {
            input.insert((k, 0));
            input.insert((k, 1));
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(&1) {
            worker.step();
        }
        let mut times = Vec::new();
        for r in 0..ROUNDS {
            let base = 1 + r * sub;
            for s in 0..sub {
                let t = base + s;
                input.advance_to(t);
                for k in 0..keys {
                    if t > 1 {
                        input.remove((k, 1000 + t - 1));
                    }
                    input.insert((k, 1000 + t));
                }
            }
            input.advance_to(base + sub);
            input.flush();
            let start = Instant::now();
            while probe.less_than(&(base + sub)) {
                worker.step();
            }
            if r >= WARMUP {
                times.push(start.elapsed().as_micros());
            }
        }
        times.iter().sum::<u128>() as f64 / times.len() as f64
    })
}

const PROP_NODES: u64 = 2_000;
const PROP_EDGES: usize = 4_000;
const PROP_CHURN: usize = 50;
const PROP_ROUNDS: u64 = 40;
const PROP_WARMUP: u64 = 5;

fn edge(i: usize) -> (u64, u64) {
    let n = PROP_NODES;
    ((i as u64).wrapping_mul(2654435761) % n, (i as u64).wrapping_mul(40503).wrapping_add(7) % n)
}

/// Label propagation (min over self and in-neighbors) inside `iterate`, with edge churn: the
/// carried-interesting-times shape. Returns averaged microseconds per round and a checksum of the
/// produced label updates, which must agree across modes.
fn run_propagate(mode: Mode) -> (f64, i64) {
    let check = Arc::new(Mutex::new(0i64));
    let sum = check.clone();
    let avg = timely::execute_directly(move |worker| {
        let mut nodes: InputSession<u64, (u64, u64), isize> = InputSession::new();
        let mut edges: InputSession<u64, (u64, u64), isize> = InputSession::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let nodes = nodes.to_collection(scope);
            let edges = edges.to_collection(scope);
            let mut ph = ProbeHandle::new();
            let labels = nodes.clone().iterate(move |_scope, inner| {
                let scope = inner.scope();
                let nodes_in = nodes.enter(scope);
                let edges_in = edges.enter(scope);
                let prop = edges_in.join_map(inner, |_src, dst, lbl| (*dst, *lbl)).concat(nodes_in);
                match mode {
                    Mode::Mainline => prop.reduce(|_k, i: &[(&u64, isize)], o: &mut Vec<(u64, isize)>| {
                        if let Some(m) = i.iter().filter(|(_, d)| *d > 0).map(|(v, _)| **v).min() {
                            o.push((m, 1));
                        }
                    }),
                    Mode::CursorSame => {
                        let arr = harrange!(prop, Product<u64, u64>, "ArrProp");
                        cursor_reduce!(arr, Product<u64, u64>, "CursorProp", min_fold)
                            .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
                    }
                    Mode::Proxy => {
                        let arr = harrange!(prop, Product<u64, u64>, "ArrProp");
                        proxy_reduce!(arr, Product<u64, u64>, "ProxyProp", min_fold)
                            .as_collection(|_h, kw: &(u64, u64)| (kw.0, kw.1))
                    }
                }
            });
            labels
                .inspect(move |((_, lbl), _, d)| {
                    *sum.lock().unwrap() += (*lbl as i64) * (*d as i64);
                })
                .inner
                .probe_with(&mut ph);
            ph
        });
        nodes.advance_to(0);
        edges.advance_to(0);
        for k in 0..PROP_NODES {
            nodes.insert((k, k));
        }
        for i in 0..PROP_EDGES {
            let (s, d) = edge(i);
            edges.insert((s, d));
        }
        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();
        while probe.less_than(&1) {
            worker.step();
        }
        let mut times = Vec::new();
        for r in 0..PROP_ROUNDS {
            let t = r + 1;
            // Perturb a low-numbered label each round: low labels spread widely under `min`, so
            // withdrawing one forces its reach to relabel — real iterate work, with interesting
            // times carried across retires.
            let m = r % 50;
            if r > 0 {
                let p = (r - 1) % 50;
                nodes.insert((p, p));
            }
            nodes.remove((m, m));
            for c in 0..PROP_CHURN {
                let dead = (r as usize) * PROP_CHURN + c;
                let (s, d) = edge(dead);
                edges.remove((s, d));
                let (s, d) = edge(PROP_EDGES + dead);
                edges.insert((s, d));
            }
            nodes.advance_to(t + 1);
            nodes.flush();
            edges.advance_to(t + 1);
            edges.flush();
            let start = Instant::now();
            while probe.less_than(&(t + 1)) {
                worker.step();
            }
            if r >= PROP_WARMUP {
                times.push(start.elapsed().as_micros());
            }
        }
        times.iter().sum::<u128>() as f64 / times.len() as f64
    });
    let total = *check.lock().unwrap();
    (avg, total)
}

fn report(name: &str, mainline: f64, cursor: f64, proxy: f64) {
    eprintln!(" {name}:");
    eprintln!("   inherent reduce (own storage) : {:9.0}us/round", mainline);
    eprintln!("   cursor tactic (chunk storage) : {:9.0}us/round", cursor);
    eprintln!("   proxy tactic  (chunk storage) : {:9.0}us/round  ({:.2}x cursor-same, {:.2}x inherent)", proxy, proxy / cursor, proxy / mainline);
}

#[test]
#[ignore]
fn bench_reduce_tactics() {
    eprintln!();
    for (name, run) in [
        ("churn (50k keys, 1 time/flush)", run_churn as fn(Mode) -> f64),
        ("multimoment (10k keys, 4 times/flush)", run_multimoment),
    ] {
        report(name, run(Mode::Mainline), run(Mode::CursorSame), run(Mode::Proxy));
    }
    let (mainline, sum_m) = run_propagate(Mode::Mainline);
    let (cursor, sum_c) = run_propagate(Mode::CursorSame);
    let (proxy, sum_p) = run_propagate(Mode::Proxy);
    assert_eq!(sum_m, sum_c, "cursor-tactic propagate output must match the inherent reduce");
    assert_eq!(sum_m, sum_p, "proxy-tactic propagate output must match the inherent reduce");
    report("propagate (2k nodes, 4k edges, iterate)", mainline, cursor, proxy);
}
