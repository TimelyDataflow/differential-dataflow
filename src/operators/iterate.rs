use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;

use timely::Data;

use timely::progress::nested::product::Product;
use timely::progress::nested::Summary::Local;
use timely::progress::timestamp::Timestamp;
// use timely::serialization::Serializable;

// use columnar::Columnar;

use collection_trace::lookup::UnsignedInt;
use collection_trace::LeastUpperBound;
use operators::ExceptExt;
use operators::ConsolidateExt;

pub trait One {
    fn one() -> Self;
}

impl One for u64 { fn one() -> u64 { 1 } }
impl One for u32 { fn one() -> u32 { 1 } }

pub trait IterateExt<G: Scope, D: Data> {
    fn iterate<P: Fn(&D)->U+'static,
               U: UnsignedInt,
               F: FnOnce(&Stream<Child<G, T>, (D,i32)>)->
                         Stream<Child<G, T>, (D,i32)>,
               T: Timestamp+LeastUpperBound=u64,
               >
        (&self, iterations: T, part: P, logic: F) -> Stream<G, (D,i32)> where G::Timestamp: LeastUpperBound, T::Summary: One;
}

impl<G: Scope, D: Ord+Data> IterateExt<G, D> for Stream<G, (D, i32)> {
    fn iterate<P: Fn(&D)->U+'static,
               U: UnsignedInt,
               F: FnOnce(&Stream<Child<G, T>, (D,i32)>)->
                         Stream<Child<G, T>, (D,i32)>,
               T: Timestamp+LeastUpperBound=u64,
               >
        (&self, iterations: T, part: P, logic: F) -> Stream<G, (D,i32)>
where G::Timestamp: LeastUpperBound, T::Summary: One {

        self.scope().scoped(|subgraph| {

            let (feedback, cycle) = subgraph.loop_variable(Product::new(G::Timestamp::max(), iterations), Local(T::Summary::one()));
            let ingress = subgraph.enter(&self);

            let bottom = logic(&ingress.concat(&cycle));

            bottom.except(&ingress).consolidate(part).connect_loop(feedback);
            bottom.leave()
        })
    }
}
