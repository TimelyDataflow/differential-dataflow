//! SCC with the proxy join tactic substituted for the cursor tactic.
//!
//! Identical computation to `examples/scc.rs` — same `propagate_at`, same dataflow
//! shape, same filter/map stages — except `trim_edges`' two joins run through
//! `join_with_tactic` with a value-token proxy backend over the same `ValSpine`
//! arrangements. This is the proxy tactics' first exercise inside a real dataflow:
//! nested iterative scopes, `Product` timestamps, fuel-drained interleaved units.
//!
//! Usage: scc_proxy [timely args] [nodes [edges [batch [rounds]]]]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

use timely::dataflow::operators::probe::Handle;
use timely::order::Product;
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

use differential_dataflow::algorithms::graphs::propagate::propagate_at;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::int_proxy::{
    JoinInstance, JoinWindow, ProxyJoinBackend, ProxyJoinTactic,
};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::{join_with_tactic, Fresh};
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use differential_dataflow::collection::AsCollection;
use differential_dataflow::VecCollection;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

type Diff = isize;

/// The batch type of a `ValSpine` arrangement.
type BatchOf<K, V, T> = <ValSpine<K, V, T, Diff> as TraceReader>::Batch;

/// A value-token proxy join backend over two `ValSpine` arrangements sharing key `K`:
/// `Group = K` (exact — no hashing), tokens are the values themselves, and `logic`
/// shapes output exactly as `join_core`'s closure would.
struct SpineJoinBackend<K, V1, V2, T, D, L> {
    window: usize,
    target: usize,
    logic: L,
    phantom: std::marker::PhantomData<(K, V1, V2, T, D)>,
}

impl<K, V1, V2, T, D, L> SpineJoinBackend<K, V1, V2, T, D, L> {
    fn new(logic: L) -> Self {
        SpineJoinBackend { window: 1024, target: 1 << 16, logic, phantom: std::marker::PhantomData }
    }
}

/// Group-ordered scanning state over one side's batch cursors.
struct SideCursors<C> {
    cursors: Vec<C>,
}

impl<C> Default for SideCursors<C> {
    fn default() -> Self {
        SideCursors { cursors: Vec::new() }
    }
}

impl<C> SideCursors<C> {
    fn start<B>(&mut self, batches: &[B])
    where
        B: Navigable<Cursor = C>,
    {
        if self.cursors.len() != batches.len() {
            self.cursors = batches.iter().map(|b| b.cursor()).collect();
        }
    }
}

/// Advance a time to the (singleton, per the tactic) lower frontier.
fn advance<T: Lattice + Clone>(t: &T, lower: AntichainRef<'_, T>) -> T {
    lower.iter().fold(t.clone(), |t, l| t.join(l))
}

/// Per-unit cursor state: a batch-cursor per batch per side.
struct SpineJoinCursor<C0, C1> {
    c0: SideCursors<C0>,
    c1: SideCursors<C1>,
}

impl<C0, C1> Default for SpineJoinCursor<C0, C1> {
    fn default() -> Self {
        SpineJoinCursor { c0: SideCursors::default(), c1: SideCursors::default() }
    }
}

impl<K, V1, V2, T, D, L, I> ProxyJoinBackend<BatchOf<K, V1, T>, BatchOf<K, V2, T>>
    for SpineJoinBackend<K, V1, V2, T, D, L>
where
    K: differential_dataflow::Data + Copy,
    V1: differential_dataflow::Data + Copy,
    V2: differential_dataflow::Data + Copy,
    T: Timestamp + Lattice + Ord + differential_dataflow::Data,
    D: 'static,
    I: IntoIterator<Item = D>,
    L: FnMut(&K, &V1, &V2) -> I + 'static,
{
    type Group = K;
    type Token0 = V1;
    type Token1 = V2;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    type Output = Vec<(D, T, Diff)>;
    type Cursor = SpineJoinCursor<
        <BatchOf<K, V1, T> as Navigable>::Cursor,
        <BatchOf<K, V2, T> as Navigable>::Cursor,
    >;
    type Sink = Vec<(D, T, Diff)>;

    fn next_window(
        &mut self,
        instance: &JoinInstance<'_, BatchOf<K, V1, T>, BatchOf<K, V2, T>>,
        fresh: Fresh,
        cursor: &mut Self::Cursor,
        reuse: Option<JoinWindow<K, V1, V2, T, Diff, Diff>>,
    ) -> Option<JoinWindow<K, V1, V2, T, Diff, Diff>> {
        cursor.c0.start(instance.batches0);
        cursor.c1.start(instance.batches1);
        let (mut run0, mut run1) = match reuse {
            Some(JoinWindow { input0, input1 }) => (input0, input1),
            None => (Vec::new(), Vec::new()),
        };
        loop {
            run0.clear();
            run1.clear();
            let mut groups = 0;
            while groups < self.window {
                // The fresh side's next group; the accumulated side seeks to it.
                let g = match fresh {
                    Fresh::Input0 => cursor
                        .c0
                        .cursors
                        .iter()
                        .zip(instance.batches0)
                        .filter_map(|(c, b)| c.get_key(b).copied())
                        .min(),
                    Fresh::Input1 => cursor
                        .c1
                        .cursors
                        .iter()
                        .zip(instance.batches1)
                        .filter_map(|(c, b)| c.get_key(b).copied())
                        .min(),
                };
                let Some(g) = g else { break };
                groups += 1;
                // Drain group g from side 0 (seeking first when it is the accumulated side).
                for (c, b) in cursor.c0.cursors.iter_mut().zip(instance.batches0) {
                    if matches!(fresh, Fresh::Input1) {
                        c.seek_key(b, &g);
                    }
                    if c.get_key(b) == Some(&g) {
                        while let Some(v) = c.get_val(b) {
                            let v = *v;
                            c.map_times(b, |t, d| run0.push(((g, v), advance(t, instance.lower), *d)));
                            c.step_val(b);
                        }
                        c.step_key(b);
                    }
                }
                for (c, b) in cursor.c1.cursors.iter_mut().zip(instance.batches1) {
                    if matches!(fresh, Fresh::Input0) {
                        c.seek_key(b, &g);
                    }
                    if c.get_key(b) == Some(&g) {
                        while let Some(v) = c.get_val(b) {
                            let v = *v;
                            c.map_times(b, |t, d| run1.push(((g, v), advance(t, instance.lower), *d)));
                            c.step_val(b);
                        }
                        c.step_key(b);
                    }
                }
            }
            if groups == 0 {
                return None;
            }
            consolidate_updates(&mut run0);
            consolidate_updates(&mut run1);
            if run0.is_empty() && run1.is_empty() {
                continue;
            }
            return Some(JoinWindow { input0: run0, input1: run1 });
        }
    }

    fn absorb(
        &mut self,
        _instance: &JoinInstance<'_, BatchOf<K, V1, T>, BatchOf<K, V2, T>>,
        sink: &mut Self::Sink,
        left: (K, V1),
        right: (K, V2),
        time: T,
        diff: Diff,
    ) -> Option<Self::Output> {
        for d in (self.logic)(&left.0, &left.1, &right.1) {
            sink.push((d, time.clone(), diff));
        }
        if sink.len() >= self.target {
            Some(std::mem::take(sink))
        } else {
            None
        }
    }

    fn flush(
        &mut self,
        _instance: &JoinInstance<'_, BatchOf<K, V1, T>, BatchOf<K, V2, T>>,
        sink: &mut Self::Sink,
    ) -> Option<Self::Output> {
        if sink.is_empty() {
            None
        } else {
            Some(std::mem::take(sink))
        }
    }
}

/// `join_core` by way of the proxy tactic: same arrangements, same logic shape.
fn proxy_join<'scope, K, V1, V2, T, D, L, I>(
    arranged1: Arranged<'scope, TraceAgent<ValSpine<K, V1, T, Diff>>>,
    arranged2: Arranged<'scope, TraceAgent<ValSpine<K, V2, T, Diff>>>,
    logic: L,
) -> VecCollection<'scope, T, D, Diff>
where
    K: Copy + Ord + Hash + differential_dataflow::ExchangeData,
    V1: Copy + Ord + differential_dataflow::ExchangeData,
    V2: Copy + Ord + differential_dataflow::ExchangeData,
    T: Timestamp + Lattice + Ord + Clone + 'static,
    D: differential_dataflow::Data,
    I: IntoIterator<Item = D> + 'static,
    L: FnMut(&K, &V1, &V2) -> I + 'static,
{
    join_with_tactic(arranged1, arranged2, ProxyJoinTactic::new(SpineJoinBackend::new(logic)))
        .as_collection()
}

/// `strongly_connected`, with `trim_edges`' joins run through the proxy tactic.
fn strongly_connected_proxy<'scope, T>(
    graph: VecCollection<'scope, T, (usize, usize), Diff>,
) -> VecCollection<'scope, T, (usize, usize), Diff>
where
    T: Timestamp + Lattice + Hash + Ord,
{
    let outer = graph.scope();
    outer.scoped::<Product<_, usize>, _, _>("StronglyConnectedProxy", |scope| {
        let edges = graph.enter(scope);
        let trans = edges.clone().map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        let (variable, inner) = Variable::new_from(edges.clone(), Product::new(Default::default(), 1));
        let result = trim_edges_proxy(trim_edges_proxy(inner, edges), trans);
        variable.set(result.clone());
        result.leave(outer)
    })
}

fn trim_edges_proxy<'scope, T>(
    cycle: VecCollection<'scope, T, (usize, usize), Diff>,
    edges: VecCollection<'scope, T, (usize, usize), Diff>,
) -> VecCollection<'scope, T, (usize, usize), Diff>
where
    T: Timestamp + Lattice + Hash + Ord,
{
    let outer = edges.inner.scope();
    outer.region_named("TrimEdgesProxy", |region| {
        let cycle = cycle.enter_region(region);
        let edges = edges.enter_region(region);

        let nodes = edges
            .clone()
            .map_in_place(|x| x.0 = x.1)
            .consolidate();

        // `|_| 0` (no label staggering), matching stock `strongly_connected` exactly.
        let labels = propagate_at(cycle, nodes, |_| 0).arrange_by_key();

        let step1 = proxy_join(
            edges.arrange_by_key(),
            labels.clone(),
            |e1: &usize, e2: &usize, l1: &usize| [(*e2, (*e1, *l1))],
        );
        proxy_join(
            step1.arrange_by_key(),
            labels,
            |e2: &usize, (e1, l1): &(usize, usize), l2: &usize| [((*e1, *e2), (*l1, *l2))],
        )
        .filter(|(_, (l1, l2))| l1 == l2)
        .map(|((x1, x2), _)| (x2, x1))
        .leave_region(outer)
    })
}

fn hash_to_u64<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn edge_for(index: usize, nodes: usize) -> (usize, usize) {
    let h1 = hash_to_u64(&index);
    let h2 = hash_to_u64(&h1);
    ((h1 as usize) % nodes, (h2 as usize) % nodes)
}

fn main() {
    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let positional: Vec<String> = std::env::args()
            .skip(1)
            .filter(|a| !a.starts_with('-'))
            .collect();
        let nodes: usize = positional.get(0).and_then(|s| s.parse().ok()).unwrap_or(100_000);
        let edges: usize = positional.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
        let batch: usize = positional.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000);
        let rounds: usize = positional.get(3).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

        if worker.index() == 0 {
            println!(
                "proxy scc — nodes: {nodes}, edges: {edges}, batch: {batch}, rounds: {}, workers: {}",
                if rounds == usize::MAX { "∞".to_string() } else { rounds.to_string() },
                worker.peers()
            );
        }

        let validate = std::env::var("SCC_VALIDATE").is_ok();
        let mut probe = Handle::new();
        let mut input = worker.dataflow(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), Diff>();
            let scc = strongly_connected_proxy(graph.clone());
            if validate {
                // Oracle: stock and proxy SCC must agree exactly, at every time.
                use differential_dataflow::algorithms::graphs::scc::strongly_connected;
                strongly_connected(graph)
                    .negate()
                    .concat(scc.clone())
                    .consolidate()
                    .inspect(|x| panic!("proxy scc diverged from stock: {:?}", x))
                    .probe_with(&mut probe);
            }
            scc.probe_with(&mut probe);
            input
        });

        let index = worker.index();
        let peers = worker.peers();

        let timer_load = std::time::Instant::now();
        for i in (0..edges).filter(|i| i % peers == index) {
            input.insert(edge_for(i, nodes));
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        if index == 0 {
            println!("{:?}\t{:?}\tloaded {edges} edges", timer.elapsed(), timer_load.elapsed());
        }

        for round in 0..rounds {
            let timer_round = std::time::Instant::now();
            for i in (0..batch).filter(|i| i % peers == index) {
                input.remove(edge_for(round * batch + i, nodes));
                input.insert(edge_for(edges + round * batch + i, nodes));
            }
            input.advance_to(round + 2);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            if index == 0 {
                println!(
                    "{:?}\t{:?}\tround {round} ({} changes)",
                    timer.elapsed(),
                    timer_round.elapsed(),
                    batch * 2
                );
            }
        }
    })
    .unwrap();

    println!("{:?}\tshut down", timer.elapsed());
}
