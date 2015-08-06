use timely::Data;
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Concat};

pub trait ExceptExt {
    fn except(&self, &Self) -> Self;
}

impl<G: Scope, D: Data> ExceptExt for Stream<G, (D, i32)> {
    fn except(&self, other: &Stream<G, (D, i32)>) -> Stream<G, (D, i32)> {
        other.map(|(x,d)| (x,-d)).concat(self)
    }
}
