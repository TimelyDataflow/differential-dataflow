//! Negate the diffs of collections and streams.

use timely::Data;
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::dataflow::operators::Map;

use crate::{AsCollection, Collection};
use crate::difference::Abelian;

/// Negate the contents of a stream.
pub trait Negate<G, C> {
    /// Creates a new collection whose counts are the negation of those in the input.
    ///
    /// This method is most commonly used with `concat` to get those element in one collection but not another.
    /// However, differential dataflow computations are still defined for all values of the difference type `R`,
    /// including negative counts.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let odds = data.filter(|x| x % 2 == 1);
    ///     let evens = data.filter(|x| x % 2 == 0);
    ///
    ///     odds.negate()
    ///         .concat(&data)
    ///         .assert_eq(&evens);
    /// });
    /// ```
    fn negate(&self) -> Self;
}

impl<G, D, R, C> Negate<G, C> for Collection<G, D, R, C>
where
    G: Scope,
    C: Clone,
    StreamCore<G, C>: Negate<G, C>,
{
    fn negate(&self) -> Self {
        self.inner.negate().as_collection()
    }
}

impl<G: Scope, D: Data, T: Data, R: Data + Abelian> Negate<G, Vec<(D, T, R)>> for Stream<G, (D, T, R)> {
    fn negate(&self) -> Self {
        self.map_in_place(|x| x.2.negate())
    }
}

