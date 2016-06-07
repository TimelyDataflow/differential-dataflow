use std::hash::Hash;

use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::*;

use ::Delta;

/// A mutable collection of values of type `D`
#[derive(Clone)]
pub struct Collection<G: Scope, D: Data> {
    /// Underlying timely dataflow stream.
    pub inner: Stream<G, (D, Delta)>
}

impl<G: Scope, D: Data> Collection<G, D> {
    /// Creates a new collection from a timely dataflow stream.
    pub fn new(stream: Stream<G, (D, Delta)>) -> Collection<G, D> {
        Collection { inner: stream }
    }
    /// Applies the supplied function to each element of the collection.
    pub fn map<D2: Data, L: Fn(D) -> D2 + 'static>(&self, logic: L) -> Collection<G, D2> {
        self.inner.map(move |(data, delta)| (logic(data), delta))
                  .as_collection()
    }
    /// Applies the supplied function to each element of the collection, re-using allocated memory.
    pub fn map_in_place<L: Fn(&mut D) + 'static>(&self, logic: L) -> Collection<G, D> {
        self.inner.map_in_place(move |&mut (ref mut data, _)| logic(data))
                  .as_collection()
    }
    /// Applies the supplied function to each element of the collection.
    pub fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D) -> I + 'static>(&self, logic: L) -> Collection<G, D2> {
        self.inner.flat_map(move |(data, delta)| logic(data).map(move |x| (x, delta)))
                  .as_collection()
    }
    /// Negates the counts of each element in the collection.
    pub fn negate(&self) -> Collection<G, D> {
        self.inner.map_in_place(|x| x.1 *= -1)
                  .as_collection()
    }
    /// Retains only the elements of the collection satisifying the supplied predicate.
    pub fn filter<L: Fn(&D) -> bool + 'static>(&self, logic: L) -> Collection<G, D> {
        self.inner.filter(move |&(ref data, _)| logic(data))
                  .as_collection()
    }
    /// Adds the counts of elements from each collection.
    pub fn concat(&self, other: &Collection<G, D>) -> Collection<G, D> {
        self.inner.concat(&other.inner)
                  .as_collection()
    }
    /// Brings a collection into a nested scope.
    pub fn enter<'a, T: Timestamp>(&self, child: &Child<'a, G, T>) -> Collection<Child<'a, G, T>, D> {
        self.inner.enter(child)
                  .as_collection()
    }
    /// Brings a collection into a nested scope, at varying times.
    ///
    /// The `initial` function indicates the time at which each element of the collection should appear.
    pub fn enter_at<'a, T: Timestamp, F>(&self, child: &Child<'a, G, T>, initial: F) -> Collection<Child<'a, G, T>, D> 
    where F: Fn(&(D, Delta)) -> T + 'static,
          G::Timestamp: Hash, T: Hash {
        self.inner.enter_at(child, initial)
                  .as_collection()
    }
    /// Applies a supplied function to each update. Diagnostic.
    pub fn inspect<F: FnMut(&(D, Delta))+'static>(&self, func: F) -> Collection<G, D> {
        self.inner.inspect(func)
                  .as_collection()
    }
    /// Applies a supplied function to each batch of updates. Diagnostic.
    pub fn inspect_batch<F: FnMut(&G::Timestamp, &[(D, Delta)])+'static>(&self, func: F) -> Collection<G, D> {
        self.inner.inspect_batch(func)
                  .as_collection()
    }
    /// Attaches a timely dataflow probe to the output of a collection.
    ///
    /// This probe is used to determine when the state of the collection has stabilized and can
    /// be read out. 
    pub fn probe(&self) -> (probe::Handle<G::Timestamp>, Collection<G, D>) {
        let (handle, stream) = self.inner.probe();
        (handle, stream.as_collection())
    }
    /// The scope containing the underlying timely dataflow stream.
    pub fn scope(&self) -> G {
        self.inner.scope()
    }
}

impl<'a, G: Scope, T: Timestamp, D: Data> Collection<Child<'a, G, T>, D> {
    /// Returns the final value of a collection from a nested scope to its containing scope.
    pub fn leave(&self) -> Collection<G, D> {
        self.inner.leave()
                  .as_collection()
    }
}

/// Conversion to a differential dataflow collection.
pub trait AsCollection<G: Scope, D: Data> {
    /// Conversion to a differential dataflow collection.
    fn as_collection(&self) -> Collection<G, D>;
}

impl<G: Scope, D: Data> AsCollection<G, D> for Stream<G, (D, Delta)> {
    fn as_collection(&self) -> Collection<G, D> {
        Collection::new(self.clone())
    }
}