use std::hash::Hash;

use timely::Data;
use timely::progress::Timestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::*;

use ::Ring;

/// A mutable collection of values of type `D`
#[derive(Clone)]
pub struct Collection<G: Scope, D, R: Ring = isize> {
    /// Underlying timely dataflow stream.
    pub inner: Stream<G, (D, G::Timestamp, R)>
}

impl<G: Scope, D: Data, R: Ring> Collection<G, D, R> where G::Timestamp: Data {
    /// Creates a new Collection from a timely dataflow stream.
    pub fn new(stream: Stream<G, (D, G::Timestamp, R)>) -> Collection<G, D, R> {
        Collection { inner: stream }
    }
    /// Applies the supplied function to each element of the Collection.
    pub fn map<D2: Data, L: Fn(D) -> D2 + 'static>(&self, logic: L) -> Collection<G, D2, R> {
        self.inner.map(move |(data, time, delta)| (logic(data), time, delta))
                  .as_collection()
    }
    /// Applies the supplied function to each element of the Collection, re-using allocated memory.
    pub fn map_in_place<L: Fn(&mut D) + 'static>(&self, logic: L) -> Collection<G, D, R> {
        self.inner.map_in_place(move |&mut (ref mut data, _, _)| logic(data))
                  .as_collection()
    }
    /// Applies the supplied function to each element of the Collection.
    pub fn flat_map<D2: Data, I: Iterator<Item=D2>, L: Fn(D) -> I + 'static>(&self, logic: L) -> Collection<G, D2, R> 
        where G::Timestamp: Clone {
        self.inner.flat_map(move |(data, time, delta)| logic(data).map(move |x| (x, time.clone(), delta)))
                  .as_collection()
    }
    /// Negates the counts of each element in the Collection.
    pub fn negate(&self) -> Collection<G, D, R> {
        self.inner.map_in_place(|x| x.2 = -x.2)
                  .as_collection()
    }
    /// Retains only the elements of the Collection satisifying the supplied predicate.
    pub fn filter<L: Fn(&D) -> bool + 'static>(&self, logic: L) -> Collection<G, D, R> {
        self.inner.filter(move |&(ref data, _, _)| logic(data))
                  .as_collection()
    }
    /// Adds the counts of elements from each Collection.
    pub fn concat(&self, other: &Collection<G, D, R>) -> Collection<G, D, R> {
        self.inner.concat(&other.inner)
                  .as_collection()
    }
    /// Brings a Collection into a nested scope.
    pub fn enter<'a, T: Timestamp>(&self, child: &Child<'a, G, T>) -> Collection<Child<'a, G, T>, D, R> {
        self.inner.enter(child)
                  .map(|(data, time, diff)| (data, Product::new(time, Default::default()), diff))
                  .as_collection()
    }
    /// Brings a Collection into a nested scope, at varying times.
    ///
    /// The `initial` function indicates the time at which each element of the Collection should appear.
    pub fn enter_at<'a, T: Timestamp, F>(&self, child: &Child<'a, G, T>, initial: F) -> Collection<Child<'a, G, T>, D, R> 
    where F: Fn(&D) -> T + 'static,
          G::Timestamp: Hash, T: Hash {

            let initial1 = ::std::rc::Rc::new(initial);
            let initial2 = initial1.clone();

            // TODO: Need to wrap initial in Rc, to share with `enter_at` and `map`.
        self.inner.enter_at(child, move |x| (*initial1)(&x.0))
                  .map(move |(data, time, diff)| {
                      let new_time = Product::new(time, (*initial2)(&data));
                      (data, new_time, diff)
                  })
                  .as_collection()
    }
    /// Applies a supplied function to each update. Diagnostic.
    pub fn inspect<F: FnMut(&(D, G::Timestamp, R))+'static>(&self, func: F) -> Collection<G, D, R> {
        self.inner.inspect(func)
                  .as_collection()
    }
    /// Applies a supplied function to each batch of updates. Diagnostic.
    pub fn inspect_batch<F: FnMut(&G::Timestamp, &[(D, G::Timestamp, R)])+'static>(&self, func: F) -> Collection<G, D, R> {
        self.inner.inspect_batch(func)
                  .as_collection()
    }
    /// Attaches a timely dataflow probe to the output of a Collection.
    ///
    /// This probe is used to determine when the state of the Collection has stabilized and can
    /// be read out. 
    pub fn probe(&self) -> (probe::Handle<G::Timestamp>, Collection<G, D, R>) {
        let (handle, stream) = self.inner.probe();
        (handle, stream.as_collection())
    }
    /// The scope containing the underlying timely dataflow stream.
    pub fn scope(&self) -> G {
        self.inner.scope()
    }
}

impl<'a, G: Scope, T: Timestamp, D: Data, R: Ring> Collection<Child<'a, G, T>, D, R> {
    /// Returns the final value of a Collection from a nested scope to its containing scope.
    pub fn leave(&self) -> Collection<G, D, R> {
        self.inner.leave()
                  .map(|(data, time, diff)| (data, time.outer, diff))
                  .as_collection()
    }
}

/// Conversion to a differential dataflow Collection.
pub trait AsCollection<G: Scope, D: Data, R: Ring> {
    /// Conversion to a differential dataflow Collection.
    fn as_collection(&self) -> Collection<G, D, R>;
}

impl<G: Scope, D: Data, R: Ring> AsCollection<G, D, R> for Stream<G, (D, G::Timestamp, R)> {
    fn as_collection(&self) -> Collection<G, D, R> {
        Collection::new(self.clone())
    }
}