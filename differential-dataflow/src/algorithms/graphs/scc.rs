//! Strongly connected component structure.

use std::mem;
use std::hash::Hash;

use timely::progress::Timestamp;

use crate::{VecCollection, ExchangeData};
use crate::lattice::Lattice;
use crate::difference::{Abelian, Multiply};

use super::propagate::propagate;

/// Returns the subset of edges in the same strongly connected component.
pub fn strongly_connected<T, N, R>(graph: VecCollection<T, (N,N), R>) -> VecCollection<T, (N,N), R>
where
    T: Timestamp + Lattice + Ord + Hash,
    N: ExchangeData + Hash,
    R: ExchangeData + Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>
{
    use timely::order::Product;
    let outer = graph.scope();
    outer.scoped::<Product<_, usize>,_,_>("StronglyConnected", |scope| {
        // Bring in edges and transposed edges.
        let edges = graph.enter(&scope);
        let trans = edges.clone().map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        // Create a new variable that will be intra-scc edges.
        use crate::operators::iterate::Variable;
        let (variable, inner) = Variable::new_from(edges.clone(), Product::new(Default::default(), 1));

        let result = trim_edges(trim_edges(inner, edges), trans);
        variable.set(result.clone());
        result.leave(&outer)
    })
}

fn trim_edges<T, N, R>(cycle: VecCollection<T, (N,N), R>, edges: VecCollection<T, (N,N), R>)
    -> VecCollection<T, (N,N), R>
where
    T: Timestamp + Lattice + Ord + Hash,
    N: ExchangeData + Hash,
    R: ExchangeData + Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>
{
    let outer = edges.inner.scope();
    outer.region_named("TrimEdges", |region| {
        let cycle = cycle.enter_region(region);
        let edges = edges.enter_region(region);

        let nodes = edges.clone()
                         .map_in_place(|x| x.0 = x.1.clone())
                         .consolidate();

        // NOTE: With a node -> int function, can be improved by:
        // let labels = propagate_at(&cycle, &nodes, |x| *x as u64);
        let labels = propagate(cycle, nodes).arrange_by_key();

        edges.arrange_by_key()
             .join_core(labels.clone(), |e1,e2,l1| [(e2.clone(),(e1.clone(),l1.clone()))])
             .arrange_by_key()
             .join_core(labels, |e2,(e1,l1),l2| [((e1.clone(),e2.clone()),(l1.clone(),l2.clone()))])
             .filter(|(_,(l1,l2))| l1 == l2)
             .map(|((x1,x2),_)| (x2,x1))
             .leave_region(&outer)
    })
}
