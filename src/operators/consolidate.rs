//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use timely::dataflow::*;

use ::{Collection, Data, Diff, Hashable};
use operators::arrange::ArrangeBySelf;

/// An extension method for consolidating weighted streams.
pub trait Consolidate<D: Data+Hashable> {
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data. The data are 
    /// accumulated in place, each held back until their timestamp has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// #
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Consolidate;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///         x.negate()
    ///          .concat(&x)
    ///          .consolidate() // <-- ensures cancellation occurs
    ///          .assert_empty();
    ///     });
    /// }
    /// ```
    fn consolidate(&self) -> Self;
}

impl<G: Scope, D, R> Consolidate<D> for Collection<G, D, R>
where
    D: Data+Hashable,
    R: Diff,
    G::Timestamp: ::lattice::Lattice+Ord,
 {
    fn consolidate(&self) -> Self {
       self.arrange_by_self().as_collection(|d,_| d.item.clone())
    }
}
