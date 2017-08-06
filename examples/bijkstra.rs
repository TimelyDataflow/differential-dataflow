extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Variable;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(4).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(5).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(6), move |worker| {
        
        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut roots, mut graph) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let mut result = bidijkstra(&graph, &roots);

            if !inspect {
                result = result.filter(|_| false);
            }

            result.map(|(_,l)| l)
                  .consolidate()
                  .inspect(|x| println!("\t{:?}", x))
                  .probe_with(&mut probe);

            (root_input, edge_input)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }
        println!("loaded; elapsed: {:?}", timer.elapsed());

        roots.advance_to(1); roots.flush();
        graph.advance_to(1); graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));
        println!("stable; elapsed: {:?}", timer.elapsed());

        roots.insert((0, 1));
        roots.close();
        graph.advance_to(2); graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));
        println!("queried; elapsed: {:?}", timer.elapsed());

        for round in 0 .. rounds {
            for element in 0 .. batch {
                if worker.index() == 0 {
                    graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                }
                graph.advance_to(3 + round * batch + element);                
            }
            graph.flush();

            let timer = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(&graph.time()));

            if worker.index() == 0 {
                let elapsed = timer.elapsed();
                println!("{:?}:\t{}", round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bidijkstra<G: Scope>(edges: &Collection<G, Edge>, goals: &Collection<G, (Node, Node)>) -> Collection<G, ((Node, Node), u32)>
where G::Timestamp: Lattice+Ord {

    edges.scope().scoped(|inner| {

        // Our plan is to start evolving distances from both sources and destinations. 
        // The evolution from a source or destination should continue as long as there
        // is a corresponding destination or source that has not yet been reached.

        // forward and reverse (node, (root, dist))
        let forward = Variable::from(goals.map(|(x,_)| (x,(x,0))).enter(inner));
        let reverse = Variable::from(goals.map(|(_,y)| (y,(y,0))).enter(inner));

        let goals = goals.enter(inner);
        let edges = edges.enter(inner);

        // Let's determine which (src, dst) pairs are ready to return.
        //
        //   done(src, dst) := forward(src, med), reverse(dst, med), goal(src, dst).
        //
        // This is a cyclic join, which should scare us a bunch.
        let reached = 
        forward
            .join_map_u(&reverse, |_, &(src,d1), &(dst,d2)| ((src, dst), d1 + d2))
            .group(|_key, s, t| t.push((*s[0].0, 1)));

        let active =
        reached
            .semijoin(&goals)
            .negate()
            .map(|(srcdst,_)| srcdst)
            .concat(&goals)
            .consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.map(|(x,_y)| x).distinct_u();
        let forward_next = 
        forward
            .map(|(med, (src, dist))| (src, (med, dist)))
            .semijoin_u(&forward_active)
            .map(|(src, (med, dist))| (med, (src, dist)))
            .join_map_u(&edges, |_med, &(src, dist), &next| (next, (src, dist+1)))
            .concat(&forward)
            .map(|(next, (src, dist))| ((next, src), dist))
            .group(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next, src), dist)| (next, (src, dist)));

        forward.set(&forward_next);

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x,y)| y).distinct_u();
        let reverse_next = 
        reverse
            .map(|(med, (rev, dist))| (rev, (med, dist)))
            .semijoin_u(&reverse_active)
            .map(|(rev, (med, dist))| (med, (rev, dist)))
            .join_map_u(&edges.map(|(x,y)| (y,x)), |_med, &(rev, dist), &next| (next, (rev, dist+1)))
            .concat(&reverse)
            .map(|(next, (rev, dist))| ((next, rev), dist))
            .group(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next,rev), dist)| (next, (rev, dist)));

        reverse.set(&reverse_next);

        reached.leave()
    })
}