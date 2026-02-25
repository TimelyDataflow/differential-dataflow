//! Iterative application of a differential dataflow fragment.
//!
//! The `iterate` operator takes as an argument a closure from a differential dataflow collection
//! to a collection of the same type. The output collection is the result of applying this closure
//! an unbounded number of times.
//!
//! The implementation of `iterate` does not directly apply the closure, but rather establishes an
//! iterative timely dataflow subcomputation, in which differences circulate until they dissipate
//! (indicating that the computation has reached fixed point), or until some number of iterations
//! have passed.
//!
//! **Note**: The dataflow assembled by `iterate` does not automatically insert `consolidate` for
//! you. This means that either (i) you should insert one yourself, (ii) you should be certain that
//! all paths from the input to the output of the loop involve consolidation, or (iii) you should
//! be worried that logically cancelable differences may circulate indefinitely.
//!
//! # Details
//!
//! The `iterate` method is written using a `Variable`, which lets you define your own iterative
//! computations when `iterate` itself is not sufficient. This can happen when you have two
//! collections that should evolve simultaneously, or when you would like to rotate your loop and
//! return an intermediate result.
//!
//! Using `Variable` requires more explicit arrangement of your computation, but isn't much more
//! complicated. You must define a new variable from an existing stream (its initial value), and
//! then set it to be a function of this variable (and perhaps other collections and variables).
//!
//! A `Variable` dereferences to a `Collection`, the one corresponding to its value in each iteration,
//! and it can be used in most situations where a collection can be used. The act of setting a
//! `Variable` consumes it and returns the corresponding `Collection`, preventing you from setting
//! it multiple times.

use std::fmt::Debug;

use timely::Container;
use timely::progress::Timestamp;
use timely::order::Product;

use timely::dataflow::*;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::feedback::Handle;

use crate::{Data, VecCollection, Collection};
use crate::difference::{Semigroup, Abelian};
use crate::lattice::Lattice;

/// An extension trait for the `iterate` method.
pub trait Iterate<G: Scope<Timestamp: Lattice>, D: Data, R: Semigroup> {
    /// Iteratively apply `logic` to the source collection until convergence.
    ///
    /// Importantly, this method does not automatically consolidate results.
    /// It may be important to conclude the closure you supply with `consolidate()` to ensure that
    /// logically empty collections that contain cancelling records do not result in non-termination.
    /// Operators like `reduce`, `distinct`, and `count` also perform consolidation, and are safe to conclude with.
    ///
    /// The closure is also passed a copy of the inner scope, to facilitate importing external collections.
    /// It can also be acquired by calling `.scope()` on the closure's collection argument, but the code
    /// can be awkward to write fluently.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Iterate;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     scope.new_collection_from(1 .. 10u32).1
    ///          .iterate(|_scope, values| {
    ///              values.map(|x| if x % 2 == 0 { x/2 } else { x })
    ///                    .consolidate()
    ///          });
    /// });
    /// ```
    fn iterate<F>(self, logic: F) -> VecCollection<G, D, R>
    where
        for<'a> F: FnOnce(Iterative<'a, G, u64>, VecCollection<Iterative<'a, G, u64>, D, R>)->VecCollection<Iterative<'a, G, u64>, D, R>;
}

impl<G: Scope<Timestamp: Lattice>, D: Ord+Data+Debug, R: Abelian+'static> Iterate<G, D, R> for VecCollection<G, D, R> {
    fn iterate<F>(self, logic: F) -> VecCollection<G, D, R>
    where
        for<'a> F: FnOnce(Iterative<'a, G, u64>, VecCollection<Iterative<'a, G, u64>, D, R>)->VecCollection<Iterative<'a, G, u64>, D, R>,
    {
        self.inner.scope().scoped("Iterate", |subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let (variable, collection) = Variable::new_from(self.enter(subgraph), Product::new(Default::default(), 1));
            let result = logic(subgraph.clone(), collection);
            variable.set(result.clone());
            result.leave()
        })
    }
}

impl<G: Scope<Timestamp: Lattice>, D: Ord+Data+Debug, R: Semigroup+'static> Iterate<G, D, R> for G {
    fn iterate<F>(mut self, logic: F) -> VecCollection<G, D, R>
    where
        for<'a> F: FnOnce(Iterative<'a, G, u64>, VecCollection<Iterative<'a, G, u64>, D, R>)->VecCollection<Iterative<'a, G, u64>, D, R>,
    {
        self.scoped("Iterate", |subgraph| {
                // create a new variable, apply logic, bind variable, return.
                //
                // this could be much more succinct if we returned the collection
                // wrapped by `variable`, but it also results in substantially more
                // diffs produced; `result` is post-consolidation, and means fewer
                // records are yielded out of the loop.
                let (variable, collection) = Variable::new(subgraph, Product::new(Default::default(), 1));
                let result = logic(subgraph.clone(), collection);
                variable.set(result.clone());
                result.leave()
            }
        )
    }
}

/// A recursively defined collection.
///
/// The `Variable` struct allows differential dataflow programs requiring more sophisticated
/// iterative patterns than singly recursive iteration. For example: in mutual recursion two
/// collections evolve simultaneously.
///
/// # Examples
///
/// The following example is equivalent to the example for the `Iterate` trait.
///
/// ```
/// use timely::order::Product;
/// use timely::dataflow::Scope;
///
/// use differential_dataflow::input::Input;
/// use differential_dataflow::operators::iterate::Variable;
///
/// ::timely::example(|scope| {
///
///     let numbers = scope.new_collection_from(1 .. 10u32).1;
///
///     scope.iterative::<u64,_,_>(|nested| {
///         let summary = Product::new(Default::default(), 1);
///         let (variable, collection) = Variable::new_from(numbers.enter(nested), summary);
///         let result = collection.map(|x| if x % 2 == 0 { x/2 } else { x })
///                                .consolidate();
///         variable.set(result.clone());
///         result.leave()
///     });
/// })
/// ```
pub struct Variable<G, C>
where
    G: Scope<Timestamp: Lattice>,
    C: Container,
{
    feedback: Handle<G, C>,
    source: Option<Collection<G, C>>,
    step: <G::Timestamp as Timestamp>::Summary,
}

/// A `Variable` specialized to a vector container of update triples (data, time, diff).
pub type VecVariable<G, D, R> = Variable<G, Vec<(D, <G as ScopeParent>::Timestamp, R)>>;

impl<G, C: Container> Variable<G, C>
where
    G: Scope<Timestamp: Lattice>,
    C: crate::collection::containers::ResultsIn<<G::Timestamp as Timestamp>::Summary>,
{
    /// Creates a new initially empty `Variable`.
    ///
    /// This method produces a simpler dataflow graph than `new_from`, and should
    /// be used whenever the variable has an empty input.
    pub fn new(scope: &mut G, step: <G::Timestamp as Timestamp>::Summary) -> (Self, Collection<G, C>) {
        let (feedback, updates) = scope.feedback(step.clone());
        let collection = Collection::<G, C>::new(updates);
        (Self { feedback, source: None, step }, collection)
    }

    /// Creates a new `Variable` from a supplied `source` stream.
    pub fn new_from(source: Collection<G, C>, step: <G::Timestamp as Timestamp>::Summary) -> (Self, Collection<G, C>) where C: Clone + crate::collection::containers::Negate {
        let (feedback, updates) = source.inner.scope().feedback(step.clone());
        let collection = Collection::<G, C>::new(updates).concat(source.clone());
        (Variable { feedback, source: Some(source.negate()), step }, collection)
    }

    /// Set the definition of the `Variable` to a collection.
    ///
    /// This method binds the `Variable` to be equal to the supplied collection,
    /// which may be recursively defined in terms of the variable itself.
    pub fn set(mut self, mut result: Collection<G, C>) {
        if let Some(source) = self.source.take() {
            result = result.concat(source);
        }
        result
            .results_in(self.step)
            .inner
            .connect_loop(self.feedback);
    }
}
