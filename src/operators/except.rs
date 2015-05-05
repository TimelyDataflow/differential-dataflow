
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;

pub trait ExceptExt {
    fn except(&self, &Self) -> Self;
}

impl<G: GraphBuilder, D: Data> ExceptExt for Stream<G, (D, i32)> {
    fn except(&self, other: &Stream<G, (D, i32)>) -> Stream<G, (D, i32)> {
        other.map(|(x,d)| (x,-d)).concat(self)
    }
}
