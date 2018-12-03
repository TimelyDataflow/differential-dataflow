//! Breadth-first distance labeling.

use std::hash::Hash;

use timely::dataflow::*;

use ::{Collection, Data};
use ::operators::*;
use ::lattice::Lattice;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs<G, N>(edges: &Collection<G, (N,N)>, roots: &Collection<G, N>) -> Collection<G, (N,u32)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: Data+Hash,
{
    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,l,d| (d.clone(), l+1))
             .concat(&nodes)
             .group(|_, s, t| t.push((s[0].0.clone(), 1)))
     })
}