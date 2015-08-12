extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use differential_dataflow::sort::radix_merge::Accumulator;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {
    let graph = GraphMMap::new("/Users/mcsherry/Projects/Datasets/twitter-dedup");
    // let mut merge = RadixMerge::new(Rc::new(|x: &u32| *x as u64));

    let parts = 256;

    let mut merges = vec![];
    for _ in 0..parts {
        merges.push(Accumulator::new());
    }

    let mut node_counter = 0u64;
    let mut edge_counter = 0u64;
    for node in 0..graph.nodes() {
        node_counter += 1;
        let edges = graph.edges(node);
        for &dest in edges {
            edge_counter += 2;
            merges[node % parts].push(node as u32, dest, 1, &|&x| x as u64 >> 8);
            merges[dest as usize % parts].push(dest, node as u32, 1, &|&x| x as u64 >> 8);
        }
    }
    println!("nodes: {}", node_counter);
    println!("edges: {}", edge_counter);
    println!("d_avg: {}", edge_counter as f64 / node_counter as f64);

    for merge in merges.into_iter() {
        merge.done(&|&x| x as u64 >> 8);
    }
}
