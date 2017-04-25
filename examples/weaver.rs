extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
extern crate vec_map;
extern crate graph_map;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::{Collection, AsCollection, Data};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    // define a new computational scope, in which to run reach
    timely::execute_from_args(std::env::args(), move |worker| {
        
        // contruct iterative transaction dataflow, attach reachability query to graph.
        let (mut raw, mut trans, mut query, mut roots, query_probe, roots_probe) = worker.dataflow(|scope| {

            // transactional updates to edge collection.
            let (raw_input, raw) = scope.new_input();
            let raw = raw.as_collection();

            // transactional updates to edge collection.
            let (trans_input, trans) = scope.new_input();
            let trans = trans.as_collection();

            // graph contains committed edges, will persist across epochs.
            let (handle, graph) = scope.loop_variable(u64::max_value(), 1);
            let graph = graph.as_collection();

            let aborts = transactional(&trans, &graph);

            // restrict writes to those that commit.
            let writes = trans.filter(|x| !x.1)
                              .map(|x| (x.0, x.2))
                              .antijoin(&aborts)
                              .map(|(_,edge)| edge);

            // define the graph as itself plus any committed changes.
            let graph = raw.concat(&writes);

            graph.inner.connect_loop(handle);

            // read queries against edge collection
            let (query_input, query) = scope.new_input();
            let query = query.as_collection();

            // query the graph, return a probe.
            let query_probe = graph.semijoin(&query)
                                   .probe();


            // issue reachability queries from various roots
            let (roots_input, roots) = scope.new_input();
            let roots = roots.as_collection();

            // do reach on the graph from the roots, report counts at each distance.
            let result = reach(&graph, &roots);
            let roots_probe = result.probe();

            (raw_input, trans_input, query_input, roots_input, query_probe, roots_probe)
        });

        let filename = std::env::args().nth(1).unwrap();
        let graph = GraphMMap::new(&filename);

        if worker.index() == 0 {
            let &time = raw.time();
            for node in 0 .. graph.nodes() {
                for &edge in graph.edges(node) {
                    raw.send(((node as u32, edge as u32), time, 1));
                }
            }
        }
        raw.close();

        let next = trans.epoch() + 2;
        trans.advance_to(next);
        query.advance_to(next);
        roots.advance_to(next);

        // do computation for a bit, until we see outputs from each.
        while query_probe.lt(&trans.time()) || roots_probe.lt(&trans.time()) {
            worker.step();
        }

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        let block: u32 = std::env::args().nth(2).unwrap().parse().unwrap();

        let mut reads = Vec::with_capacity(1000);
        while reads.len() < reads.capacity() {
            let mut buffer = vec![];
            for i in 0 .. block as usize {
                let key = rng.gen_range(0, graph.nodes() as u32);
                if i % worker.peers() == worker.index() {
                    buffer.push(key);
                }
            }
            reads.push(buffer);
        }

        let timer = ::std::time::Instant::now();

        for buffer in &reads {
            let &time = query.time();
            for &entry in buffer {
                query.send((entry, time, 1));
            }

            let next = trans.epoch() + 1;
            trans.advance_to(next);
            query.advance_to(next);
            roots.advance_to(next);

            // do computation for a bit, until we see outputs from each.
            while query_probe.lt(&trans.time()) || roots_probe.lt(&trans.time()) {
                worker.step();
            }
        }

        query.close();
        println!("query elapsed: {:?} for 1,000 x {}", timer.elapsed(), block);


        let mut travs = Vec::with_capacity(1000);
        while travs.len() < travs.capacity() {
            let mut temp = vec![];
            for _ in 0 .. block {
                temp.push(rng.gen_range(0, graph.nodes() as u32));
            }
            travs.push(temp);
        }

        let mut latencies = vec![];

        let timer = ::std::time::Instant::now();

        for buffer in &travs {
            let inner_timer = ::std::time::Instant::now();
            let &time = roots.time();
            for &src in buffer {
                roots.send((src, time, 1));
            }

            let next = trans.epoch() + 1;
            trans.advance_to(next);
            // query.advance_to(next);
            roots.advance_to(next);

            // do computation for a bit, until we see outputs from each.
            while query_probe.lt(&trans.time()) || roots_probe.lt(&trans.time()) {
                worker.step();
            }

            let elapsed = inner_timer.elapsed();
            latencies.push(elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64);
        }

        roots.close();
        println!("travs elapsed: {:?} for 1,000 x {}", timer.elapsed(), block);

        if worker.index() == 0 {
            latencies[500 ..].sort();
            for &x in latencies[500..].iter() {
                println!("{}", (x as f64) / 1000000000.0f64);
            }
        }

        let mut writes = Vec::with_capacity(1000);
        let index = worker.index(); 
        let peers = worker.peers(); 
        let mut t_id = index;

        while writes.len() < writes.capacity() {
            let mut edge_set = vec![];
            for _ in 0 .. block {
                edge_set.push((rng.gen_range(0, graph.nodes() as u32), rng.gen_range(0, graph.nodes() as u32)));
            }
            let mut buffer = vec![];
            for i in 0 .. block as usize {
                let edge1 = edge_set[rng.gen_range(0, edge_set.len())];
                let edge2 = edge_set[rng.gen_range(0, edge_set.len())];
                if i % worker.peers() == worker.index() {
                    buffer.push((t_id, true, edge1));
                    buffer.push((t_id, false, edge2));
                }
                t_id += peers;
            }
            writes.push(buffer);
        }

        let timer = ::std::time::Instant::now();

        for buffer in &writes {
            let &time = trans.time();
            for &entry in buffer {
                trans.send((entry, time, 1));
            }

            let next = trans.epoch() + 1;
            trans.advance_to(next);
            // query.advance_to(next);
            // roots.advance_to(next);

            // do computation for a bit, until we see outputs from each.
            while query_probe.lt(&trans.time()) || roots_probe.lt(&trans.time()) {
                worker.step();
            }
        }

        println!("trans elapsed: {:?} for 1,000 x {}", timer.elapsed(), block);


    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from node s.
fn reach<G>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, Node)>
where G: Scope, G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves
    let nodes = roots.map(|x| (x, x));

    // repeatedly update reachable nodes
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,&s,&d| (d, s))
             .concat(&nodes)
             .distinct()
     })
}

fn transactional<G, D>(trans: &Collection<G, (usize, bool, D)>, state: &Collection<G, D>) -> Collection<G, usize>
where G: Scope, D: Data+Default+::std::hash::Hash, G::Timestamp: Lattice+Ord {

    // we develop the set of transaction ids that must abort, 
    // starting from the optimistic assumption that none must.

    trans.filter(|_| false)
         .map(|_| 0)
         .iterate(|abort| {

            // bring in transactions and initial state.
            let trans = trans.enter(&abort.scope());
            let state = state.enter(&abort.scope())
                             .map(|x| (x, 0));

            // reads -> (location, trans_id)
            let reads = trans.filter(|x| x.1 == true)
                             .map(|x| (x.2, x.0));

            // writes -> (location, trans_id), filtered by aborts
            let writes = trans.filter(|x| x.1 == false)
                              .map(|x| (x.0, x.2))
                              .antijoin(&abort)
                              .map(|x| (x.1, x.0));

            // cogroup may be the right way to do this, 
            // but we can do a join for now.
            let lookup = writes.concat(&state)
                               .join_map(&reads, |key, &tid1, &tid2| (key.clone(), tid1, tid2))
                               .filter(|&(_, tid1, tid2)| tid1 < tid2)
                               .map(|(k, _, tid2)| (k, tid2))
                               .distinct();

            reads.map(|x| (x,()))
                 .antijoin(&lookup)
                 .map(|((_,t),())| t)
                 .distinct()
        })

}
