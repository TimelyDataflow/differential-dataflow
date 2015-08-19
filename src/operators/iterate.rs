//! Iterative application of a differential dataflow fragment.
//!
//! The `iterate` operator takes as an argument a closure from a differential dataflow stream to a
//! stream of the same type. The output are differences which accumulate to the result of applying
//! this closure a specified number of times.
//!
//! The implementation of `iterate` does not directly apply the closure, but rather establishes an
//! iterative timely dataflow subcomputation, in which differences circulate until they dissipate
//! (indicating that the computation has reached fixed point), or until some number of iterations
//! have passed.
//!
//! #Examples
//!
//! The example repeatedly divides even numbers by two, and leaves odd numbers as they are. Although
//! some numbers may take multiple iterations to converge, converged numbers have no overhead in
//! subsequent iterations.
//!
//! ```ignore
//! // repeatedly divide out factors of two.
//! let limits = numbers.iterate(u64::max_value(), |x| x, |values| {
//!     values.map(|(x,w) if x % 2 == 0 { (x / 2, w) } else { (x, w) })
//! });
//! ```
//!
//! If anyone knows of a form of the Collatz conjecture in which the iterates achieve fixed point,
//! rather than cycling, let me know!

use std::fmt::Debug;

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
// use operators::ExceptExt;
use operators::ConsolidateExt;

pub trait One {
    fn one() -> Self;
}

impl One for u64 { fn one() -> u64 { 1 } }
impl One for u32 { fn one() -> u32 { 1 } }

/// An extension trait for the `iterate` method.
pub trait IterateExt<G: Scope, D: Data> {
    /// Iteratively applies a function `logic` to the input data stream, `iterations` many times.
    ///
    /// The actual computation uses differential dataflow logic to avoid re-evaluating stabilized
    /// parts of the computation.
    ///
    /// The `part` parameter is a partitioning function from `D` to an unsigned integer, required
    /// to partition the data for data-parallel equality testing, to detect per-record stabilization.
    fn iterate<P, U, F, T=u64>
        (&self, iterations: T, part: P, logic: F) -> Stream<G, (D,i32)>
        where G::Timestamp: LeastUpperBound,
              T::Summary: One,
              P: Fn(&D)->U+'static,
             U: UnsignedInt,
             F: FnOnce(&Stream<Child<G, T>, (D,i32)>)->
                       Stream<Child<G, T>, (D,i32)>,
             T: Timestamp+LeastUpperBound;
}

impl<G: Scope, D: Ord+Data+Debug> IterateExt<G, D> for Stream<G, (D, i32)> {
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

            bottom.concat(&ingress.map(|(x,w)| (x,-w))).consolidate(part).connect_loop(feedback);
            bottom.leave()
        })
    }
}
