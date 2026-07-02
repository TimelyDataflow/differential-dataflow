//! Wall-clock comparison of the default cursor reduce tactic vs the model-derived reference tactic.
//!
//!   cargo run --release --example reduce_bench
//!
//! Two workloads: a reduce-dominated 1-D (total-order) churn, which isolates the base per-key engine
//! overhead (determination is trivial with total orders), and an iterative BFS with Product times,
//! where the synthetic-time determination actually does work.

use std::time::Instant;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::{Data, ExchangeData, Hashable};
use differential_dataflow::input::Input;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::reduce_trace_reference;
use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
use differential_dataflow::lattice::Lattice;

type Node = usize;
type Edge = (Node, Node);

// Stand-alone equivalent of the default `reduce`, driven by the model-derived reference tactic. The
// library exposes only the low-level, doc-hidden `reduce_trace_reference`; this helper supplies the
// same arrange / abelian-negate / as_collection glue `reduce_named` uses, so the benchmark drives the
// reference tactic without it living on the public `Collection`/`Arranged` API.
fn reduce_reference<'scope, Ti, K, V, R, V2, R2, L>(
    collection: VecCollection<'scope, Ti, (K, V), R>,
    mut logic: L,
) -> VecCollection<'scope, Ti, (K, V2), R2>
where
    Ti: timely::progress::Timestamp + Lattice + Ord,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
    V2: Data,
    R2: Ord + Abelian + 'static,
    L: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>) + 'static,
{
    // See tests/reduce_reference.rs for why `Tr1` and the spine/builder types are pinned explicitly:
    // as a free function the output-key bound is checked before the argument would infer `Tr1`, and
    // the closure params are annotated to the concrete `&K`/`&V` types so the user-logic coercion
    // normalizes here, exactly as the default `reduce_named` gets for free.
    let arranged: Arranged<'scope, TraceAgent<ValSpine<K, V, Ti, R>>> =
        collection.arrange_by_key_named("Arrange: ReduceReference");
    reduce_trace_reference::<TraceAgent<ValSpine<K, V, Ti, R>>, ValBuilder<K, V2, Ti, R2>, ValSpine<K, V2, Ti, R2>, _, _>(
        arranged,
        "ReduceReference",
        move |key: &K, input: &[(&V, R)], output: &mut Vec<(V2, R2)>, change: &mut Vec<(V2, R2)>| {
            if !input.is_empty() { logic(key, input, change); }
            change.extend(output.drain(..).map(|(x, mut d)| { d.negate(); (x, d) }));
            differential_dataflow::consolidation::consolidate(change);
        },
        |vec, key: &K, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r))); },
    )
    .as_collection(|k, v| (k.clone(), v.clone()))
}

fn main() {
    for _ in 0 .. 2 {
        let c = reduce_churn(false);
        let r = reduce_churn(true);
        println!("reduce_churn  cursor {:>8.3}s   reference {:>8.3}s   ratio {:.2}x",
            c.as_secs_f64(), r.as_secs_f64(), r.as_secs_f64() / c.as_secs_f64());
    }
    for _ in 0 .. 2 {
        let c = fewkeys(false);
        let r = fewkeys(true);
        println!("fewkeys       cursor {:>8.3}s   reference {:>8.3}s   ratio {:.2}x",
            c.as_secs_f64(), r.as_secs_f64(), r.as_secs_f64() / c.as_secs_f64());
    }
    for _ in 0 .. 2 {
        let c = bfs_bench(false);
        let r = bfs_bench(true);
        println!("bfs           cursor {:>8.3}s   reference {:>8.3}s   ratio {:.2}x",
            c.as_secs_f64(), r.as_secs_f64(), r.as_secs_f64() / c.as_secs_f64());
    }
}

// Reduce-dominated, totally-ordered time: many keys churn values across many rounds; reduce counts
// distinct values per key. Determination is trivial here, so this measures base engine overhead.
fn reduce_churn(reference: bool) -> std::time::Duration {
    let keys = 20000usize;
    let vals = 100usize;
    let rounds = 500usize;
    let per_round = 5000usize;

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    let mut batches: Vec<Vec<(usize, usize, isize)>> = Vec::with_capacity(rounds);
    for _ in 0 .. rounds {
        let mut batch = Vec::with_capacity(per_round);
        for _ in 0 .. per_round {
            let k = rng.gen_range(0, keys);
            let v = rng.gen_range(0, vals);
            let d = if rng.gen::<bool>() { 1 } else { -1 };
            batch.push((k, v, d));
        }
        batches.push(batch);
    }

    let start = Instant::now();
    timely::execute_directly(move |worker| {
        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, coll) = scope.new_collection::<(usize, usize), isize>();
            let reduced = if reference {
                reduce_reference(coll, |_k, s, t| t.push((s.len() as isize, 1)))
            } else {
                coll.reduce(|_k, s, t| t.push((s.len() as isize, 1)))
            };
            reduced.inspect(|_| {});
            input
        });
        let mut round = 0usize;
        for batch in batches {
            for (k, v, d) in batch { input.update((k, v), d); }
            round += 1;
            input.advance_to(round);
        }
    });
    start.elapsed()
}

// Few keys, wide processing intervals (via `update_at` + coarse `advance_to`), so each key's
// per-round history is long — the regime where the phase-2 re-sort would actually cost.
fn fewkeys(reference: bool) -> std::time::Duration {
    let keys = 8usize;
    let vals = 40usize;
    let times = 24000usize;
    let step = 6000usize;

    let seed: &[_] = &[7, 7, 7, 7];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    let mut ups: Vec<(usize, usize, usize, isize)> = Vec::new();
    for t in 1 ..= times {
        for k in 0 .. keys {
            ups.push((k, rng.gen_range(0, vals), t, if rng.gen::<bool>() { 1 } else { -1 }));
        }
    }

    let start = Instant::now();
    timely::execute_directly(move |worker| {
        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, coll) = scope.new_collection::<(usize, usize), isize>();
            let reduced = if reference {
                reduce_reference(coll, |_k, s, t| t.push((s.len() as isize, 1)))
            } else {
                coll.reduce(|_k, s, t| t.push((s.len() as isize, 1)))
            };
            reduced.inspect(|_| {});
            input
        });
        let mut next_flush = step;
        for (k, v, t, d) in ups {
            input.update_at((k, v), t, d);
            if t >= next_flush { input.advance_to(next_flush); next_flush += step; }
        }
        input.advance_to(times + 1);
    });
    start.elapsed()
}

fn bfs_bench(reference: bool) -> std::time::Duration {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let nodes = 4000usize;
    let edges = 32000usize;
    let rounds = 400usize;

    let seed: &[_] = &[9, 8, 7, 6];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);
    let mut rng2: StdRng = SeedableRng::from_seed(seed);
    let mut edge_list: Vec<((usize, usize), usize, isize)> = Vec::new();
    for _ in 0 .. edges { edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1)); }
    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round, -1));
    }
    let root_list = vec![(1usize, 0usize, 1isize)];
    let counter = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let c = counter.clone();
    timely::execute_directly(move |worker| {
        let mut roots_list = root_list.clone();
        let mut edges_list = edge_list.clone();
        let (mut roots, mut edges) = worker.dataflow::<usize, _, _>(|scope| {
            let (root_input, roots) = scope.new_collection();
            let (edge_input, edges) = scope.new_collection();
            let c = c.clone();
            bfs(edges, roots, reference).inspect(move |_| { c.fetch_add(1, Ordering::Relaxed); });
            (root_input, edge_input)
        });
        roots_list.sort_by(|x, y| y.1.cmp(&x.1));
        edges_list.sort_by(|x, y| y.1.cmp(&x.1));
        let mut round = 0;
        while !roots_list.is_empty() || !edges_list.is_empty() {
            while roots_list.last().map(|x| x.1) == Some(round) { let (n, _, d) = roots_list.pop().unwrap(); roots.update(n, d); }
            while edges_list.last().map(|x| x.1) == Some(round) { let ((s, t), _, d) = edges_list.pop().unwrap(); edges.update((s, t), d); }
            round += 1;
            roots.advance_to(round);
            edges.advance_to(round);
        }
    });
    let elapsed = start.elapsed();
    eprintln!("    (bfs {} output updates)", counter.load(std::sync::atomic::Ordering::Relaxed));
    elapsed
}

fn bfs<'scope, T>(edges: VecCollection<'scope, T, Edge>, roots: VecCollection<'scope, T, Node>, reference: bool) -> VecCollection<'scope, T, (Node, usize)>
where
    T: timely::progress::Timestamp + Lattice + Ord,
{
    let nodes = roots.map(|x| (x, 0));
    nodes.clone().iterate(|scope, inner| {
        let edges = edges.enter(scope);
        let nodes = nodes.enter(scope);
        let combined = inner.join_map(edges, |_k, l, d| (*d, l + 1)).concat(nodes);
        if reference {
            reduce_reference(combined, |_, s, t| t.push((*s[0].0, 1)))
        } else {
            combined.reduce(|_, s, t| t.push((*s[0].0, 1)))
        }
    })
}
