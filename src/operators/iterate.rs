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
//! let limits = numbers.iterate(|values| {
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
use timely::progress::timestamp::Timestamp;

use ::{Data, Collection};
use collection::LeastUpperBound;

/// An extension trait for the `iterate` method.
pub trait IterateExt<G: Scope, D: Data> {
    /// Iteratively apply `logic` to the source collection until convergence.
    fn iterate<F>(&self, logic: F) -> Collection<G, D>
        where G::Timestamp: LeastUpperBound,
              F: FnOnce(&Collection<Child<G, u64>, D>)->Collection<Child<G, u64>, D>;
}

impl<G: Scope, D: Ord+Data+Debug> IterateExt<G, D> for Collection<G, D> {
    fn iterate<F>(&self, logic: F) -> Collection<G, D>
        where G::Timestamp: LeastUpperBound,
              F: FnOnce(&Collection<Child<G, u64>, D>)->Collection<Child<G, u64>, D> {

        Collection::new(self.inner.scope().scoped(|subgraph| {

            let (feedback, cycle) = subgraph.loop_variable(u64::max(), 1);
            let ingress = self.inner.enter(subgraph);

            let bottom = logic(&Collection::new(ingress.concat(&cycle))).inner;

            bottom.concat(&ingress.map_in_place(|x| x.1 = -x.1))
                  .connect_loop(feedback);

            bottom.leave()
        }))
    }
}
