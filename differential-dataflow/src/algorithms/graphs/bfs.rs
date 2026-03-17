//! Breadth-first distance labeling.

use std::hash::Hash;

use timely::dataflow::*;

use crate::{VecCollection, ExchangeData};
use crate::operators::*;
use crate::lattice::Lattice;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs<G, N>(edges: VecCollection<G, (N,N)>, roots: VecCollection<G, N>) -> VecCollection<G, (N,u32)>
where
    G: Scope<Timestamp: Lattice+Ord+crate::Data>,
    N: ExchangeData+Hash,
{
    let edges = edges.arrange_by_key();
    bfs_arranged(edges, roots)
}

use crate::trace::TraceReader;
use crate::operators::arrange::Arranged;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs_arranged<G, N, Tr>(edges: Arranged<G, Tr>, roots: VecCollection<G, N>) -> VecCollection<G, (N, u32)>
where
    G: Scope<Timestamp=Tr::Time>,
    N: ExchangeData+Hash,
    Tr: TraceReader<Key=N, Val=N, Diff=isize>+Clone+'static,
{
    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.clone().iterate(|scope, inner| {

        let edges = edges.enter(&scope);
        let nodes = nodes.enter(&scope);

        inner.join_core(edges, |_k,l,d| Some((d.clone(), l+1)))
             .concat(nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}
