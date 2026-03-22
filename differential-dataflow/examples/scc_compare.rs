//! Benchmark DD's built-in strongly_connected vs a version using propagate_at.
//! Same random graph generator as ddir example for fair comparison.

use std::mem;
use std::hash::Hash;
use std::sync::Arc;

use timely::dataflow::*;
use timely::order::Product;

use differential_dataflow::{VecCollection, ExchangeData};
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::difference::{Abelian, Multiply};
use differential_dataflow::algorithms::graphs::scc::strongly_connected;
use differential_dataflow::algorithms::graphs::propagate::propagate_at;

/// SCC using propagate_at for faster label propagation, specialized to u64 nodes.
fn scc_with_propagate_at<G>(graph: VecCollection<G, (u64, u64), isize>) -> VecCollection<G, (u64, u64), isize>
where
    G: Scope<Timestamp: Lattice + Ord + Hash>,
{
    graph.scope().scoped::<Product<_, usize>, _, _>("SCC_at", |scope| {
        let edges = graph.enter(scope);
        let trans = edges.clone().map_in_place(|x| mem::swap(&mut x.0, &mut x.1));

        use differential_dataflow::operators::iterate::Variable;
        let (variable, inner) = Variable::new_from(edges.clone(), Product::new(Default::default(), 1));

        let result = trim_edges_at(trim_edges_at(inner, edges), trans);
        variable.set(result.clone());
        result.leave()
    })
}

fn trim_edges_at<G>(cycle: VecCollection<G, (u64, u64), isize>, edges: VecCollection<G, (u64, u64), isize>)
    -> VecCollection<G, (u64, u64), isize>
where
    G: Scope<Timestamp: Lattice + Ord + Hash>,
{
    edges.inner.scope().region_named("TrimEdgesAt", |region| {
        let cycle = cycle.enter_region(region);
        let edges = edges.enter_region(region);

        let nodes = edges.clone()
                         .map_in_place(|x| x.0 = x.1.clone())
                         .consolidate();

        // The key change: use propagate_at instead of propagate
        let labels = propagate_at(cycle, nodes, |label: &u64| *label).arrange_by_key();

        edges.arrange_by_key()
             .join_core(labels.clone(), |e1,e2,l1| [(e2.clone(),(e1.clone(),l1.clone()))])
             .arrange_by_key()
             .join_core(labels, |e2,(e1,l1),l2| [((e1.clone(),e2.clone()),(l1.clone(),l2.clone()))])
             .filter(|(_,(l1,l2))| l1 == l2)
             .map(|((x1,x2),_)| (x2,x1))
             .leave_region()
    })
}

// ---- SmallVec row version: same algorithm as propagate_at, but with SmallVec<[u64;2]> rows ----

type Row = smallvec::SmallVec<[u64; 2]>;

fn scc_smallvec<G>(graph: VecCollection<G, (Row, Row), isize>) -> VecCollection<G, (Row, Row), isize>
where
    G: Scope<Timestamp: Lattice + Ord + Hash>,
{
    graph.scope().scoped::<Product<_, usize>, _, _>("SCC_sv", |scope| {
        let edges = graph.enter(scope);
        let trans = edges.clone().map(|(src, dst)| (dst, src));

        use differential_dataflow::operators::iterate::Variable;
        let (variable, inner) = Variable::new_from(edges.clone(), Product::new(Default::default(), 1));

        let result = trim_edges_sv(trim_edges_sv(inner, edges), trans);
        variable.set(result.clone());
        result.leave()
    })
}

fn trim_edges_sv<G>(cycle: VecCollection<G, (Row, Row), isize>, edges: VecCollection<G, (Row, Row), isize>)
    -> VecCollection<G, (Row, Row), isize>
where
    G: Scope<Timestamp: Lattice + Ord + Hash>,
{
    edges.inner.scope().region_named("TrimEdgesSV", |region| {
        let cycle = cycle.enter_region(region);
        let edges = edges.enter_region(region);

        let nodes = edges.clone().map(|(_src, dst)| (dst.clone(), dst));

        let labels = propagate_at(cycle, nodes, |label: &Row| label.first().copied().unwrap_or(0)).arrange_by_key();

        edges.arrange_by_key()
             .join_core(labels.clone(), |e1: &Row, e2: &Row, l1: &Row| {
                 let mut val = e1.clone();
                 val.extend_from_slice(l1);
                 Some((e2.clone(), val))
             })
             .arrange_by_key()
             .join_core(labels, |e2: &Row, e1_l1: &Row, l2: &Row| {
                 let mid = e1_l1.len() / 2;
                 let e1 = &e1_l1[..mid];
                 let l1 = &e1_l1[mid..];
                 if l1 == l2.as_slice() {
                     Some((<Row as From<&[u64]>>::from(e2.as_slice()), <Row as From<&[u64]>>::from(e1)))
                 } else {
                     None
                 }
             })
             .leave_region()
    })
}

// ---- SmallVec + Arc<dyn> version: measure dynamic dispatch overhead ----

fn scc_dyn<G>(graph: VecCollection<G, (Row, Row), isize>) -> VecCollection<G, (Row, Row), isize>
where
    G: Scope<Timestamp: Lattice + Ord + Hash>,
{
    let swap: Arc<dyn Fn(&(Row, Row)) -> (Row, Row) + Send + Sync> =
        Arc::new(|(src, dst): &(Row, Row)| (dst.clone(), src.clone()));

    graph.scope().scoped::<Product<_, usize>, _, _>("SCC_dyn", |scope| {
        let edges = graph.enter(scope);
        let swap2 = swap.clone();
        let trans = edges.clone().map(move |x| swap2(&x));

        use differential_dataflow::operators::iterate::Variable;
        let (variable, inner) = Variable::new_from(edges.clone(), Product::new(Default::default(), 1));

        let result = trim_edges_dyn(trim_edges_dyn(inner, edges), trans);
        variable.set(result.clone());
        result.leave()
    })
}

fn trim_edges_dyn<G>(cycle: VecCollection<G, (Row, Row), isize>, edges: VecCollection<G, (Row, Row), isize>)
    -> VecCollection<G, (Row, Row), isize>
where
    G: Scope<Timestamp: Lattice + Ord + Hash>,
{
    let mk_nodes: Arc<dyn Fn(&(Row, Row)) -> (Row, Row) + Send + Sync> =
        Arc::new(|(_src, dst): &(Row, Row)| (dst.clone(), dst.clone()));
    let enter_at_logic: Arc<dyn Fn(&Row) -> u64 + Send + Sync> =
        Arc::new(|label: &Row| label.first().copied().unwrap_or(0));
    let join1: Arc<dyn Fn(&Row, &Row, &Row) -> Option<(Row, Row)> + Send + Sync> =
        Arc::new(|e1: &Row, e2: &Row, l1: &Row| {
            let mut val = e1.clone();
            val.extend_from_slice(l1);
            Some((e2.clone(), val))
        });
    let join2: Arc<dyn Fn(&Row, &Row, &Row) -> Option<(Row, Row)> + Send + Sync> =
        Arc::new(|e2: &Row, e1_l1: &Row, l2: &Row| {
            let mid = e1_l1.len() / 2;
            let e1 = &e1_l1[..mid];
            let l1 = &e1_l1[mid..];
            if l1 == l2.as_slice() {
                Some((<Row as From<&[u64]>>::from(e2.as_slice()), <Row as From<&[u64]>>::from(e1)))
            } else {
                None
            }
        });

    edges.inner.scope().region_named("TrimEdgesDyn", |region| {
        let cycle = cycle.enter_region(region);
        let edges = edges.enter_region(region);

        let mk = mk_nodes.clone();
        let nodes = edges.clone().map(move |x| mk(&x));

        let ea = enter_at_logic.clone();
        let labels = propagate_at(cycle, nodes, move |label: &Row| ea(label)).arrange_by_key();

        let j1 = join1.clone();
        let j2 = join2.clone();
        edges.arrange_by_key()
             .join_core(labels.clone(), move |e1, e2, l1| j1(e1, e2, l1))
             .arrange_by_key()
             .join_core(labels, move |e2, e1_l1, l2| j2(e2, e1_l1, l2))
             .leave_region()
    })
}

fn main() {
    let n: usize = std::env::args().nth(1).unwrap_or("100".into()).parse().unwrap();
    let edges_count = 2 * n;

    let mut rng = n as u64;
    let lcg = |r: &mut u64| { *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *r };
    let mut edge_list = Vec::new();
    for _ in 0..edges_count {
        let src = (lcg(&mut rng) >> 32) % (n as u64);
        let dst = (lcg(&mut rng) >> 32) % (n as u64);
        edge_list.push((src, dst));
    }

    let mode = std::env::args().nth(2).unwrap_or("both".into());

    timely::execute_directly(move |worker| {
        if mode == "plain" || mode == "both" {
            let timer = std::time::Instant::now();
            let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
                let (handle, edges) = scope.new_collection::<(u64, u64), isize>();
                let mut probe = timely::dataflow::ProbeHandle::new();
                strongly_connected(edges).probe_with(&mut probe);
                (handle, probe)
            });
            for &(src, dst) in &edge_list { input.update((src, dst), 1); }
            input.advance_to(1); input.flush();
            while probe.less_than(&1u64) { worker.step(); }
            println!("DD plain      n={}: {:?}", n, timer.elapsed());
        }

        if mode == "at" || mode == "both" {
            let timer = std::time::Instant::now();
            let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
                let (handle, edges) = scope.new_collection::<(u64, u64), isize>();
                let mut probe = timely::dataflow::ProbeHandle::new();
                scc_with_propagate_at(edges).probe_with(&mut probe);
                (handle, probe)
            });
            for &(src, dst) in &edge_list { input.update((src, dst), 1); }
            input.advance_to(1); input.flush();
            while probe.less_than(&1u64) { worker.step(); }
            println!("DD propagate_at n={}: {:?}", n, timer.elapsed());
        }
        if mode == "dyn" || mode == "both" {
            let timer = std::time::Instant::now();
            let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
                let (handle, edges) = scope.new_collection::<(Row, Row), isize>();
                let mut probe = timely::dataflow::ProbeHandle::new();
                scc_dyn(edges).probe_with(&mut probe);
                (handle, probe)
            });
            for &(src, dst) in &edge_list {
                input.update((Row::from_slice(&[src]), Row::from_slice(&[dst])), 1);
            }
            input.advance_to(1); input.flush();
            while probe.less_than(&1u64) { worker.step(); }
            println!("DD dyn        n={}: {:?}", n, timer.elapsed());
        }

        if mode == "sv" || mode == "both" {
            let timer = std::time::Instant::now();
            let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
                let (handle, edges) = scope.new_collection::<(Row, Row), isize>();
                let mut probe = timely::dataflow::ProbeHandle::new();
                scc_smallvec(edges).probe_with(&mut probe);
                (handle, probe)
            });
            for &(src, dst) in &edge_list {
                input.update((Row::from_slice(&[src]), Row::from_slice(&[dst])), 1);
            }
            input.advance_to(1); input.flush();
            while probe.less_than(&1u64) { worker.step(); }
            println!("DD sv         n={}: {:?}", n, timer.elapsed());
        }
    });
}
