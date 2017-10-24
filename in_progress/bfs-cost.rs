extern crate rand;
// extern crate timely;
// extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

// use timely::dataflow::*;
// use timely::dataflow::operators::probe::Handle;

// use differential_dataflow::input::Input;
// use differential_dataflow::Collection;
// use differential_dataflow::operators::*;
// use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

fn main() {


    /*
        Our goal is to write an efficient single-threaded implementation of BFS for a
        history of edge changes, input of the form `Vec<(Edge, usize, isize)>`. We are
        simplifying our lives by not starting from any pre-existing changes, nor do we
        require that the result support further computation.

        Our plan is to restructure the computation as three nested iterations, in a 
        non-standard order:

            for round in rounds
                for node in nodes
                    for time in times
                        do stuff
        
        Round by round, we need to track changes that occur to distances, which could
        be both "now reachable" and "no longer reachable". The distance is a function
        of the round, so we need not track that so much as accumulate the number of 
        paths that can reach each node, and determine when that transitions between 
        zero and non-zero.

        The root is chosen to be node `0`, but this could be changed if needed. The
        edges are provided in order of their timestamp, which we will want to change.

     */

    let node_cnt: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edge_cnt: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(4).unwrap().parse().unwrap();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
    let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

    // a flat list of all changes to node reachability, in the form (node, time, diff).
    let node_changes = vec![((0, 0), 1)];

    let mut graph = Vec::new();


    for _ in 0 .. edge_cnt {
        graph.push(((rng1.gen_range(0, node_cnt), rng1.gen_range(0, node_cnt)), 0, 1));
    }

    for round in 0 .. rounds {
        for element in 0 .. batch {
            let time = 2 + round * batch + element - 1;
            graph.push(((rng1.gen_range(0, node_cnt), rng1.gen_range(0, node_cnt)), time, 1));
            graph.push(((rng2.gen_range(0, node_cnt), rng2.gen_range(0, node_cnt)), time, -1));
        }
    }

    // println!("performing BFS on {} nodes, {} edges; {} changes", node_cnt, edge_cnt, graph.len());

    bfs_cost(node_changes, graph, node_cnt as usize);
}

fn bfs_cost<T: Copy+Ord+std::fmt::Debug>(
    mut node_changes: Vec<((Node, T), isize)>, 
    mut edge_changes: Vec<(Edge, T, isize)>, 
    node_cnt: usize) 
{
    // nodes[i] holds a Vec<(T, isize)> indicating round and change of reachability.
    // edges[i] holds a Vec<((Node, T), isize)> indicating changes to edge connectivity.
    let mut nodes = vec![Vec::<(T, isize)>::new(); node_cnt];
    let mut edges = vec![Vec::new(); node_cnt];

    let mut next_changes = vec![];

    let timer = ::std::time::Instant::now();

    // The Vec `graph` now contains the changes to edges at various moments in time. 
    // It should track the input used by the differential dataflow `bfs` example.

    // load up `edges` and sort each history by `time`.
    for ((src, dest), time, diff) in edge_changes.drain(..) {
        edges[src as usize].push(((time, dest), diff));
    }
    for node in 0 .. node_cnt {
        consolidate(&mut edges[node as usize]);
    }

    let mut cur_edges = Vec::new();

    let mut round = 0;

    let mut total_count = 0;

    // start working through each round of reachability
    while !node_changes.is_empty() {

        total_count += node_changes.len();

        // println!("{:?}\tround {:?}", timer.elapsed(), round);
        round += 1;

        // iterate over nodes experiencing reachability changes.
        let mut node_change_cursor = 0;
        while node_change_cursor < node_changes.len() {

            let node = (node_changes[node_change_cursor].0).0;

            // identify all changes for the subject node.
            let mut upper = node_change_cursor;
            while node_changes.get(upper).map(|x| (x.0).0) == Some(node) {
                upper += 1;
            }

            // walk through history, determining if new changes alter the reachability.
            // each time reachability changes, join against the current edges and emit.
            // awkwardly, the set of all times are defined in three places, and we seem
            // to need to do a merge for correct behavior.
            // 
            // this code is the pita code, where we are glad someone else writes it.
            //
            // rather than be especially clever, we should probably just go through all
            // times, maintaining the old and new "reachable" count and current edges.
            //
            // Our plan is to first determine what changes exist in the "reachable" bit,
            // at which point we want to (i) join this change against existing edges, and
            // keep the change live to join against subsequent edge changes.
            {
                let mut old_nodes = &nodes[node as usize][..];
                let mut new_nodes = &node_changes[node_change_cursor .. upper];
                let mut all_edges = &edges[node as usize][..];

                cur_edges.clear();
                let mut cur_edges_len = 0;

                let mut old_sum: isize = 0;
                let mut new_sum: isize = 0;

                let mut accum_diff: isize = 0;

                // until we have drained all changes, keep going.
                while let Some(time) = [old_nodes.first().map(|x| x.0),
                                        new_nodes.first().map(|x| (x.0).1),
                                        all_edges.first().map(|x| (x.0).0),
                                       ].into_iter().filter_map(|&t| t).min() {

                    // println!("node, time: {:?}", (node, time));

                    let old_reach = if old_sum > 0 { 1 } else { 0 };
                    let new_reach = if new_sum > 0 { 1 } else { 0 };

                    // fold in any existing changes to reachability.
                    while old_nodes.first().map(|x| x.0) == Some(time) {
                        old_sum += old_nodes[0].1;
                        new_sum += old_nodes[0].1;
                        old_nodes = &old_nodes[1..];
                    }

                    // fold in any new changes to reachability.
                    while new_nodes.first().map(|x| (x.0).1) == Some(time) {
                        new_sum += new_nodes[0].1;
                        new_nodes = &new_nodes[1..];
                    }

                    let old_diff = if old_sum > 0 { 1 } else { 0 } - old_reach;
                    let new_diff = if new_sum > 0 { 1 } else { 0 } - new_reach;

                    // determine if change occurred, and propagate updates.
                    if old_diff != new_diff {

                        // println!("(({}, {}), (Root, {:?}), {})", node, round - 1, time, new_diff - old_diff);

                        // if we have changed edges since last we looked, consolidate.
                        if cur_edges.len() > cur_edges_len {
                            consolidate(&mut cur_edges);
                            cur_edges_len = cur_edges.len();
                        }

                        // propagate the change in reachability along all edges.
                        for &(dst, diff) in cur_edges.iter() {
                            next_changes.push(((dst, time), diff * (new_diff - old_diff)));
                        }

                        accum_diff += (new_diff - old_diff);
                    }
                    
                    // fold all edge changes into the working set.
                    while all_edges.first().map(|x| (x.0).0) == Some(time) {
                        if accum_diff != 0 { 
                            next_changes.push((((all_edges[0].0).1, time), all_edges[0].1 * accum_diff));
                        }
                        cur_edges.push(((all_edges[0].0).1, all_edges[0].1));
                        all_edges = &all_edges[1..];
                    }
                }
            }

            for &((node, time), diff) in node_changes[node_change_cursor .. upper].iter() {
                nodes[node as usize].push((time, diff));
            }
            consolidate(&mut nodes[node as usize]);

            node_change_cursor = upper;
        }

        // swap in next changes for the node changes.
        // println!("{:?}\tconsolidating {} changes", timer.elapsed(), next_changes.len());
        consolidate(&mut next_changes);
        // println!("{:?}\tconsolidated to {} changes", timer.elapsed(), next_changes.len());
        ::std::mem::swap(&mut node_changes, &mut next_changes);
        next_changes.clear();
    }

    println!("finished; elapsed: {:?}; total: {:?}", timer.elapsed(), total_count);

}


#[inline(never)]
fn consolidate<T: Ord>(list: &mut Vec<(T, isize)>) {
    list.sort_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 {
            list[index].1 = list[index].1 + list[index-1].1;
            list[index-1].1 = 0;
        }
    }
    list.retain(|x| x.1 != 0);
}
