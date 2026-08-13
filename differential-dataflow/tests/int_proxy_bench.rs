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

/// A `String`-valued churn: the same shape as `churn`, but every value clone allocates. The
/// `u64` workloads cannot show what the proxy boundary costs to marshal owned data, because
/// cloning a `u64` is free.
fn wide_keys() -> u64 { sized("WIDE_KEYS", 20_000) }
fn wide_len() -> usize { sized("WIDE_LEN", 48) as usize }

fn wide_value(k: u64, t: u64, len: usize) -> String {
    let mut s = String::with_capacity(len);
    s.push_str(&format!("{k:016x}{t:016x}"));
    while s.len() < len { s.push('x'); }
    s
}

fn run_wide(mode: Mode) -> f64 {
    let keys = wide_keys();
    let len = wide_len();
    timely::execute_directly(move |worker| {
        let mut input: InputSession<u64, (u64, String), isize> = InputSession::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let coll = input.to_collection(scope);
            let mut ph = ProbeHandle::new();
            match mode {
                Mode::Mainline => {
                    coll.reduce(|_k, i: &[(&String, isize)], o: &mut Vec<(String, isize)>| {
                        if let Some(m) = i.iter().filter(|(_, d)| *d > 0).map(|(v, _)| (*v).clone()).max() {
                            o.push((m, 1));
                        }
                    }).inner.probe_with(&mut ph);
                }
                Mode::CursorSame => {
                    let hashed = coll.map(|(k, v)| (k.hashed(), (k, v)));
                    let arr = arrange_core::<Pipeline, Vec<((u64, (u64, String)), u64, isize)>,
                        ContainerChunker<VecChunk<u64, (u64, String), u64, isize>>,
                        VChunkBatcher<u64, (u64, String), u64, isize>,
                        VChunkBuilder<u64, (u64, String), u64, isize>,
                        VChunkSpine<u64, (u64, String), u64, isize>>(hashed.inner, Pipeline, "ArrW");
                    arr.reduce_core::<_, VChunkBuilder<u64, (u64, String), u64, isize>,
                        VChunkSpine<u64, (u64, String), u64, isize>,
                        <VecChunkCursor<u64, (u64, String), u64, isize> as Cursor>::KeyContainer, _>(
                        "CursorWide",
                        |_h, input, current, updates| {
                            if let Some(kv) = input.iter().filter(|(_, d)| *d > 0).map(|(kv, _)| (*kv).clone()).max_by(|a, b| a.1.cmp(&b.1)) {
                                updates.push((kv, 1));
                            }
                            for (w, d) in current.iter() { updates.push((w.clone(), -*d)); }
                        },
                        |chunk, key, list| {
                            use timely::container::PushInto;
                            *chunk = Default::default();
                            for (v, t, d) in list.drain(..) { chunk.push_into(((*key, v), t, d)); }
                        },
                    ).stream.probe_with(&mut ph);
                }
                Mode::Proxy => {
                    let hashed = coll.map(|(k, v)| (k.hashed(), (k, v)));
                    let arr = arrange_core::<Pipeline, Vec<((u64, (u64, String)), u64, isize)>,
                        ContainerChunker<VecChunk<u64, (u64, String), u64, isize>>,
                        VChunkBatcher<u64, (u64, String), u64, isize>,
                        VChunkBuilder<u64, (u64, String), u64, isize>,
                        VChunkSpine<u64, (u64, String), u64, isize>>(hashed.inner, Pipeline, "ArrW");
                    reduce_with_tactic::<_, VChunkSpine<u64, (u64, String), u64, isize>, _>(
                        arr, "ProxyWide",
                        ProxyReduceTactic::new(VecReduceBackend::new(
                            |_k: &u64, input: &[(String, isize)], current: &mut Vec<(String, isize)>, updates: &mut Vec<(String, isize)>| {
                                if let Some(m) = input.iter().filter(|(_, d)| *d > 0).map(|(v, _)| v.clone()).max() {
                                    updates.push((m, 1));
                                }
                                for (w, d) in current.iter() { updates.push((w.clone(), -*d)); }
                            },
                        )),
                    ).stream.probe_with(&mut ph);
                }
            }
            ph
        });
        input.advance_to(0);
        for k in 0..keys { input.insert((k, wide_value(k, 0, len))); }
        input.advance_to(1);
        input.flush();
        while probe.less_than(&1) { worker.step(); }
        let mut times = Vec::new();
        let warm = warmup();
        for r in 0..rounds() {
            let t = 1 + r;
            input.advance_to(t);
            for k in 0..keys {
                if t > 1 { input.remove((k, wide_value(k, t - 1, len))); }
                input.insert((k, wide_value(k, t, len)));
            }
            input.advance_to(t + 1);
            input.flush();
            let start = Instant::now();
            while probe.less_than(&(t + 1)) { worker.step(); }
            if r >= warm { times.push(start.elapsed().as_micros()); }
        }
        times.iter().sum::<u128>() as f64 / times.len() as f64
    })
}

fn max_fold(iter: impl Iterator<Item = (u64, u64)>) -> Option<(u64, u64)> {
    iter.max_by_key(|kv| kv.1)
}

fn min_fold(iter: impl Iterator<Item = (u64, u64)>) -> Option<(u64, u64)> {
    iter.min_by_key(|kv| kv.1)
}

/// Workload sizes, overridable so the suite can be run at a scale where costs that are invisible
/// at a few megabytes show up. `churn_keys()=4000000 ROUNDS=6 WARMUP=2` moves gigabytes per round.
fn sized(var: &str, default: u64) -> u64 {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn churn_keys() -> u64 { sized("CHURN_KEYS", 50_000) }
fn mm_keys() -> u64 { sized("MM_KEYS", 10_000) }
fn mm_sub() -> u64 { sized("MM_SUB", 4) }
fn rounds() -> u64 { sized("ROUNDS", 16) }
fn warmup() -> u64 { sized("WARMUP", 6) }

/// Bounded churn, one distinct time per flush; returns averaged microseconds per round.
fn run_churn(mode: Mode) -> f64 {
    run_flat(mode, churn_keys(), 1)
}

/// Bounded churn, `mm_sub()` distinct times per flush.
fn run_multimoment(mode: Mode) -> f64 {
    run_flat(mode, mm_keys(), mm_sub())
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
        for r in 0..rounds() {
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
            if r >= warmup() {
                times.push(start.elapsed().as_micros());
            }
        }
        times.iter().sum::<u128>() as f64 / times.len() as f64
    })
}

fn prop_nodes() -> u64 { sized("PROP_NODES", 2_000) }
fn prop_edges() -> usize { sized("PROP_EDGES", 4_000) as usize }
fn prop_churn() -> usize { sized("PROP_CHURN", 50) as usize }
fn prop_rounds() -> u64 { sized("PROP_ROUNDS", 40) }
fn prop_warmup() -> u64 { sized("PROP_WARMUP", 5) }

fn edge(i: usize) -> (u64, u64) {
    let n = prop_nodes();
    ((i as u64).wrapping_mul(2654435761) % n, (i as u64).wrapping_mul(40503).wrapping_add(7) % n)
}

/// Label propagation (min over self and in-neighbors) inside `iterate`, with edge churn: the
/// carried-interesting-times shape. Returns averaged microseconds per round and a checksum of the
/// produced label updates, which must agree across modes.
fn run_propagate(mode: Mode) -> (f64, i64) {
    assert!(
        prop_rounds() as usize * prop_churn() <= prop_edges(),
        "the churn retracts edge `r * PROP_CHURN + c`, which must be one of the PROP_EDGES inserted",
    );
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
        for k in 0..prop_nodes() {
            nodes.insert((k, k));
        }
        for i in 0..prop_edges() {
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
        for r in 0..prop_rounds() {
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
            for c in 0..prop_churn() {
                let dead = (r as usize) * prop_churn() + c;
                let (s, d) = edge(dead);
                edges.remove((s, d));
                let (s, d) = edge(prop_edges() + dead);
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
            if r >= prop_warmup() {
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
    // `PROG=churn MODE=proxy` runs one cell of the matrix, which is what a large run wants.
    if let (Some(prog), Some(mode)) = (std::env::var("PROG").ok(), match std::env::var("MODE").ok().as_deref() {
        Some("proxy") => Some(Mode::Proxy),
        Some("cursor") => Some(Mode::CursorSame),
        Some("mainline") => Some(Mode::Mainline),
        _ => None,
    }) {
        let us = match prog.as_str() {
            "churn" => run_churn(mode),
            "multimoment" => run_multimoment(mode),
            "propagate" => run_propagate(mode).0,
            "wide" => run_wide(mode),
            other => panic!("unknown PROG {other:?}"),
        };
        eprintln!("  {prog} {:?}: {us:.0}us/round", mode);
        return;
    }
    for (name, run) in [
        (format!("churn ({} keys, 1 time/flush)", churn_keys()), run_churn as fn(Mode) -> f64),
        (format!("multimoment ({} keys, {} times/flush)", mm_keys(), mm_sub()), run_multimoment),
    ] {
        report(&name, run(Mode::Mainline), run(Mode::CursorSame), run(Mode::Proxy));
    }
    let (mainline, sum_m) = run_propagate(Mode::Mainline);
    let (cursor, sum_c) = run_propagate(Mode::CursorSame);
    let (proxy, sum_p) = run_propagate(Mode::Proxy);
    assert_eq!(sum_m, sum_c, "cursor-tactic propagate output must match the inherent reduce");
    assert_eq!(sum_m, sum_p, "proxy-tactic propagate output must match the inherent reduce");
    report("propagate (2k nodes, 4k edges, iterate)", mainline, cursor, proxy);
}
