//! Breadth-first distance labeling.

use std::hash::Hash;

use timely::progress::Timestamp;

use crate::{VecCollection, ExchangeData};
use crate::operators::*;
use crate::lattice::Lattice;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs<T, N>(edges: VecCollection<T, (N,N)>, roots: VecCollection<T, N>) -> VecCollection<T, (N,u32)>
where
    T: Timestamp + Lattice + Ord,
    N: ExchangeData+Hash,
{
    let edges = edges.arrange_by_key();
    bfs_arranged(edges, roots)
}

use crate::trace::TraceReader;
use crate::operators::arrange::Arranged;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs_arranged<N, Tr>(edges: Arranged<Tr>, roots: VecCollection<Tr::Time, N>) -> VecCollection<Tr::Time, (N, u32)>
where
    N: ExchangeData+Hash,
    Tr: for<'a> TraceReader<Key<'a>=&'a N, Val<'a>=&'a N, Diff=isize, Time: Lattice + Ord>+Clone+'static,
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
