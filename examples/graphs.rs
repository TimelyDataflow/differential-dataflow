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

type Node = usize;
type Edge = (Node, Node);

fn main() {

    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().any(|x| x == "inspect");

    // Our setting involves four read query types, and two updatable base relations.
    //
    //  Q1: Point lookup: reads "state" associated with a node.
    //  Q2: One-hop lookup: reads "state" associated with neighbors of a node.
    //  Q3: Two-hop lookup: reads "state" associated with n-of-n's of a node.
    //  Q4: Shortest path: reports hop count between two query nodes.
    //
    //  R1: "State": a pair of (node, T) for some type T that I don't currently know.
    //  R2: "Graph": pairs (node, node) indicating linkage between the two nodes.

    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        
        let index = worker.index();
        let peers = worker.peers();
        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();

        let (mut q1, mut q2, mut q3, mut q4, mut state, mut graph) = worker.dataflow(|scope| {

            let (q1_input, q1) = scope.new_collection();
            let (q2_input, q2) = scope.new_collection();
            let (q3_input, q3) = scope.new_collection();
            let (q4_input, q4) = scope.new_collection();

            let (state_input, state) = scope.new_collection();
            let (graph_input, graph) = scope.new_collection();

            // Q1: Point lookups on `state`:
            state
                .semijoin(&q1)
                .filter(move |_| inspect)
                .inspect(|x| println!("Q1: {:?}", x))
                .probe_with(&mut probe);

            // Q2: One-hop lookups on `state`:
            graph
                .semijoin(&q2)
                .map(|(query, friend)| (friend, query))
                .join_map(&state, |_friend, &query, &state| (query, state))
                .filter(move |_| inspect)
                .inspect(|x| println!("Q2: {:?}", x))
                .probe_with(&mut probe);

            // Q3: Two-hop lookups on `state`:
            graph
                .semijoin(&q3)
                .map(|(query, friend)| (friend, query))
                .join_map(&graph, |_friend, &query, &friend2| (friend2, query))
                .join_map(&state, |_friend2, &query, &state| (query, state))
                .filter(move |_| inspect)
                .consolidate()
                .inspect(|x| println!("Q3: {:?}", x))
                .probe_with(&mut probe);

            // Q4: Shortest path queries:
            bidijkstra(&graph, &q4)
                .filter(move |_| inspect)
                .inspect(|x| println!("Q4: {:?}", x))
                .probe_with(&mut probe);

            (q1_input, q2_input, q3_input, q4_input, state_input, graph_input)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        // let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions
        let mut rng3: StdRng = SeedableRng::from_seed(seed);    // rng for queries

        println!("performing workload on random graph with {} nodes, {} edges:", nodes, edges);

        let worker_edges = edges/peers + if index < (edges % peers) { 1 } else { 0 };
        for _ in 0 .. worker_edges {
            graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
        }
        for node in 0 .. nodes {
            if node % peers == index {
                state.insert((node, node));
            }
        }

        q1.advance_to(1);       q1.flush();     // q1 queries start now.
        q2.advance_to(1001);    q2.flush();     // q2 queries start here.
        q3.advance_to(2001);    q3.flush();     // q3 queries start here.
        q4.advance_to(3001);    q4.flush();     // q4 queries start here.
        state.close();                          // no changes to state.
        graph.close();                          // no changes to graph.

        // finish graph loading work.
        while probe.less_than(q1.time()) { worker.step(); }

        println!("{:?}\tgraph loaded", timer.elapsed());

        // Q1 testing:
        let timer_q1 = ::std::time::Instant::now();
        for round in 1 .. 1001 {
            q1.insert(rng3.gen_range(0, nodes));
            q1.advance_to(round);
            q1.flush();
            while probe.less_than(q1.time()) { worker.step(); }
        }
        println!("{:?}\tq1 eval complete; avg: {:?}", timer.elapsed(), timer_q1.elapsed()/1000);
        q1.close();

        // Q2 testing:
        let timer_q2 = ::std::time::Instant::now();
        for round in 1001 .. 2001 {
            q2.insert(rng3.gen_range(0, nodes));
            q2.advance_to(round);
            q2.flush();
            while probe.less_than(q2.time()) { worker.step(); }
        }
        println!("{:?}\tq2 eval complete; avg: {:?}", timer.elapsed(), timer_q2.elapsed()/1000);
        q2.close();

        // Q3 testing:
        let timer_q3 = ::std::time::Instant::now();
        for round in 2001 .. 3001 {
            q3.insert(rng3.gen_range(0, nodes));
            q3.advance_to(round);
            q3.flush();
            while probe.less_than(q3.time()) { worker.step(); }
        }
        println!("{:?}\tq3 eval complete; avg: {:?}", timer.elapsed(), timer_q3.elapsed()/1000);
        q3.close();

        // Q4 testing:
        let timer_q4 = ::std::time::Instant::now();
        for round in 3001 .. 4001 {
            q4.insert((rng3.gen_range(0, nodes), rng3.gen_range(0, nodes)));
            q4.advance_to(round);
            q4.flush();
            while probe.less_than(q4.time()) { worker.step(); }
        }
        println!("{:?}\tq4 eval complete; avg: {:?}", timer.elapsed(), timer_q4.elapsed()/1000);
        q4.close();

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
            .join_map(&reverse, |_, &(src,d1), &(dst,d2)| ((src, dst), d1 + d2))
            .group(|_key, s, t| t.push((*s[0].0, 1)))
            .semijoin(&goals);

        let active =
        reached
            .negate()
            .map(|(srcdst,_)| srcdst)
            .concat(&goals)
            .consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.map(|(x,_y)| x).distinct();
        let forward_next = 
        forward
            .map(|(med, (src, dist))| (src, (med, dist)))
            .semijoin(&forward_active)
            .map(|(src, (med, dist))| (med, (src, dist)))
            .join_map(&edges, |_med, &(src, dist), &next| (next, (src, dist+1)))
            .concat(&forward)
            .map(|(next, (src, dist))| ((next, src), dist))
            .group(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next, src), dist)| (next, (src, dist)));

        forward.set(&forward_next);

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x,y)| y).distinct();
        let reverse_next = 
        reverse
            .map(|(med, (rev, dist))| (rev, (med, dist)))
            .semijoin(&reverse_active)
            .map(|(rev, (med, dist))| (med, (rev, dist)))
            .join_map(&edges.map(|(x,y)| (y,x)), |_med, &(rev, dist), &next| (next, (rev, dist+1)))
            .concat(&reverse)
            .map(|(next, (rev, dist))| ((next, rev), dist))
            .group(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next,rev), dist)| (next, (rev, dist)));

        reverse.set(&reverse_next);

        reached.leave()
    })
}