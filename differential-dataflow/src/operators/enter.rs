//! Enter a collection into a scope.

use timely::Data;
use timely::dataflow::{Scope, ScopeParent, Stream, StreamCore};
use timely::dataflow::operators::core::{Enter, Map};
use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Refines;

/// Extension trait for streams.
pub trait EnterTime<C, G, TInner>
where
    G: ScopeParent,
    TInner: Refines<G::Timestamp>,
{
    /// The containers in the output stream.
    type Container: Clone;

    /// Brings a stream into a nested scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::Scope;
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let result = scope.region(|child| {
    ///         data.enter(child)
    ///             .leave()
    ///     });
    ///
    ///     data.assert_eq(&result);
    /// });
    /// ```
    fn enter_time<'a>(&self, child: &Child<'a, G, TInner>) -> StreamCore<Child<'a, G, TInner>, Self::Container>;
}

impl<G, D, R, TInner> EnterTime<Vec<(D, G::Timestamp, R)>, G, TInner> for Stream<G, (D, G::Timestamp, R)>
where
    G: Scope,
    D: Data,
    R: Data,
    TInner: Refines<G::Timestamp>,
{
    type Container = Vec<(D, TInner, R)>;

    fn enter_time<'a>(&self, child: &Child<'a, G, TInner>) -> Stream<Child<'a, G, TInner>, (D, TInner, R)> {
        self.enter(child)
            .map(|(data, time, diff)| (data, TInner::to_inner(time), diff))
    }
}
