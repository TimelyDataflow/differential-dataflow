//! Directed label reachability.

use std::hash::Hash;

use timely::dataflow::*;

use ::{Collection, Data};
use ::operators::*;
use ::lattice::Lattice;

/// Propagates labels forward, retaining the minimum label.
pub fn propagate<G, N, L>(edges: &Collection<G, (N,N)>, nodes: &Collection<G,(N,L)>) -> Collection<G,(N,L)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: Data+Hash,
    L: Data,
{
    nodes.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter(&inner.scope());

             inner.join_map(&edges, |_k,l,d| (d.clone(),l.clone()))
                  .concat(&nodes)
                  .group(|_, s, t| t.push((s[0].0.clone(), 1)))

         })
}

/// Propagates labels forward, retaining the minimum label.
pub fn propagate_at<G, N, L, F>(edges: &Collection<G, (N,N)>, nodes: &Collection<G,(N,L)>, logic: F) -> Collection<G,(N,L)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: Data+Hash,
    L: Data,
    F: Fn(&L)->u64+'static,
{
    nodes.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter_at(&inner.scope(), move |r| 256 * (64 - (logic(&r.1)).leading_zeros() as u64));

             inner.join_map(&edges, |_k,l,d| (d.clone(),l.clone()))
                  .concat(&nodes)
                  .group(|_, s, t| t.push((s[0].0.clone(), 1)))

         })
}
