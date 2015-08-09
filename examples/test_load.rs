extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use differential_dataflow::sort::radix_merge::{Merge};

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {
    let graph = GraphMMap::new("/Users/mcsherry/Projects/Datasets/twitter-dedup");
    // let mut merge = RadixMerge::new(Rc::new(|x: &u32| *x as u64));

    let mut merges = vec![];
    for _ in 0..256 {
        merges.push(Merge::new());
    }

    let mut node_counter = 0u64;
    let mut edge_counter = 0u64;
    for node in 0..graph.nodes() {
        node_counter += 1;
        let edges = graph.edges(node);
        for &dest in edges {
            edge_counter += 2;
            merges[node % 256].push(node as u32, dest, 1);
            merges[dest as usize % 256].push(dest, node as u32, 1);
        }
    }
    println!("nodes: {}", node_counter);
    println!("edges: {}", edge_counter);
    println!("d_avg: {}", edge_counter as f64 / node_counter as f64);

    for merge in &mut merges {
        merge.prune();
    }
}
