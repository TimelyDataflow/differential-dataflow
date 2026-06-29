//! Reports how tight the reduce phase-1 time discovery is on a non-trivial (SCC) computation.
//!
//! Debug-only: the audit counters live behind `cfg(debug_assertions)`. This is verification
//! scaffolding for the discover/evaluate phase split. The phase-split's correctness (no missed times)
//! is asserted inside `reduce::compute` against the retained reference implementation and exercised by
//! `tests/scc.rs`; here we surface the over-enumeration picture: how many discovered times actually led
//! to a logic invocation and to produced output.

#![cfg(debug_assertions)]

use std::hash::Hash;
use std::mem;

use rand::{Rng, SeedableRng, StdRng};
use timely::Config;

use differential_dataflow::input::Input;
use differential_dataflow::VecCollection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::reduce::audit;
use differential_dataflow::lattice::Lattice;

type Node = usize;
type Edge = (Node, Node);

#[test]
fn reduce_audit_scc() {
    audit::reset();

    // A dynamic SCC computation with random edge churn over many rounds, exercising the multi-temporal
    // reduce inside `_reachability` on the incremental path (where the value-dependent `output_produced`
    // synthetic times would matter, if they ever do).
    let (nodes, edges_n, rounds) = (50usize, 100usize, 200usize);
    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);
    let mut rng2: StdRng = SeedableRng::from_seed(seed);
    let mut edge_list = Vec::new();
    for _ in 0 .. edges_n { edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1)); }
    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round, -1));
    }

    timely::execute(Config::thread(), move |worker| {
        let mut edge_list = edge_list.clone();
        let mut edges = worker.dataflow(|scope| {
            let (edge_input, edges) = scope.new_collection();
            _strongly_connected(edges).consolidate();
            edge_input
        });

        edge_list.sort_by(|x,y| y.1.cmp(&x.1));
        if worker.index() == 0 {
            let mut round = 0;
            while !edge_list.is_empty() {
                while edge_list.last().map(|x| x.1) == Some(round) {
                    let ((src, dst), _t, diff) = edge_list.pop().unwrap();
                    edges.update((src, dst), diff);
                }
                round += 1;
                edges.advance_to(round);
            }
        }
    }).unwrap();

    let s = audit::snapshot();
    eprintln!("reduce audit (SCC): {s:?}");
    eprintln!(
        "  discovered={} logic={} produced={}  (idle={}, empty={})",
        s.discovered, s.logic, s.produced,
        s.discovered.saturating_sub(s.logic), s.logic.saturating_sub(s.produced),
    );

    // Sanity: pass 1 found work and pass 2 evaluated it. Output correctness is asserted by tests/scc.rs.
    assert!(s.discovered > 0, "expected pass 1 to discover some times");
    assert!(s.logic > 0, "expected pass 2 to invoke logic");
}

fn _strongly_connected<'scope, T>(graph: VecCollection<'scope, T, Edge>) -> VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + Lattice + Ord + Hash,
{
    graph.clone().iterate(|scope, inner| {
        let edges = graph.enter(scope);
        let trans = edges.clone().map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        _trim_edges(_trim_edges(inner, edges), trans)
    })
}

fn _trim_edges<'scope, T>(cycle: VecCollection<'scope, T, Edge>, edges: VecCollection<'scope, T, Edge>) -> VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + Lattice + Ord + Hash,
{
    let nodes = edges.clone()
                     .map_in_place(|x| x.0 = x.1)
                     .consolidate();

    let labels = _reachability(cycle, nodes);

    edges.consolidate()
         .join_map(labels.clone(), |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_map(labels.clone(), |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .filter(|&(_,(l1,l2))| l1 == l2)
         .map(|((x1,x2),_)| (x2,x1))
}

fn _reachability<'scope, T>(edges: VecCollection<'scope, T, Edge>, nodes: VecCollection<'scope, T, (Node, Node)>) -> VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + Lattice + Ord + Hash,
{
    edges.clone()
         .filter(|_| false)
         .iterate(|scope, inner| {
             let edges = edges.enter(scope);
             let nodes = nodes.enter_at(scope, |r| 256 * (64 - (r.0 as u64).leading_zeros() as u64));

             inner.join_map(edges, |_k,l,d| (*d,*l))
                  .concat(nodes)
                  .reduce(|_, s, t| t.push((*s[0].0, 1)))
         })
}
