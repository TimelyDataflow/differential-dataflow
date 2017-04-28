//! Types and traits associated with collections of data.

use std::hash::Hash;

use timely::Data;
use timely::progress::Timestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::*;

use ::Diff;

/// A mutable collection of values of type `D`
///
/// The `Collection` type is the core abstraction in differential dataflow programs. As you write your
/// differential dataflow computation, you write as if the collection is a static dataset to which you
/// apply functional transformations, creating new collections. Once your computation is written, you 
/// are able to mutate the collection (by inserting and removing elements); differential dataflow will
/// propagate changes through your functional computation and report the corresponding changes to the 
/// output collections.
///
/// Each collection has three generic parameters. The parameter `G` is for the scope in which the 
/// collection exists; as you write more complicated programs you may wish to introduce nested scopes 
/// (e.g. for iteration) and this parameter tracks the scope (for timely dataflow's benefit). The `D`
/// parameter is the type of data in your collection, for example `String`, or `(u32, Vec<Option<()>>)`.
/// The `R` parameter represents the types of changes that the data undergo, and is most commonly (and
/// defaults to) `isize`, representing changes to the occurrence count of each record.
#[derive(Clone)]
pub struct Collection<G: Scope, D, R: Diff = isize> {
    /// The underlying timely dataflow stream.
    ///
    /// This field is exposed to support direct timely dataflow manipulation when required, but it is 
    /// not intended to be the idiomatic way to work with the collection.
    pub inner: Stream<G, (D, G::Timestamp, R)>
}

impl<G: Scope, D: Data, R: Diff> Collection<G, D, R> where G::Timestamp: Data {
    /// Creates a new Collection from a timely dataflow stream.
    pub fn new(stream: Stream<G, (D, G::Timestamp, R)>) -> Collection<G, D, R> {
        Collection { inner: stream }
    }
    /// Creates a new collection by applying the supplied function to each input element.
    pub fn map<D2: Data, L: Fn(D) -> D2 + 'static>(&self, logic: L) -> Collection<G, D2, R> {
        self.inner.map(move |(data, time, delta)| (logic(data), time, delta))
                  .as_collection()
    }
    /// Creates a new collection by applying the supplied function to each input element.
    ///
    /// Although the name suggests in-place mutation, this function does not change the source collection, 
    /// but rather re-uses the underlying allocations in its implementation. The method is semantically 
    /// equivalent to `map`, but can be more efficient.
    pub fn map_in_place<L: Fn(&mut D) + 'static>(&self, logic: L) -> Collection<G, D, R> {
        self.inner.map_in_place(move |&mut (ref mut data, _, _)| logic(data))
                  .as_collection()
    }
    /// Creates a new collection by applying the supplied function to each input element and accumulating the results.
    ///
    /// This method extracts an iterator from each input element, and extracts the full contents of the iterator. Be 
    /// warned that if the iterators produce substantial amounts of data, they are currently fully drained before attempting
    /// to consolidate the results.
    pub fn flat_map<D2: Data, I: IntoIterator<Item=D2>, L: Fn(D) -> I + 'static>(&self, logic: L) -> Collection<G, D2, R> 
        where G::Timestamp: Clone {
        self.inner.flat_map(move |(data, time, delta)| logic(data).into_iter().map(move |x| (x, time.clone(), delta)))
                  .as_collection()
    }
    /// Creates a new collection whose counts are the negation of those in the input.
    ///
    /// This method is most commonly used with `concat` to get those element in one collection but not another. 
    /// However, differential dataflow computations are still defined for all values of the difference type `R`, 
    /// including negative counts.
    pub fn negate(&self) -> Collection<G, D, R> {
        self.inner.map_in_place(|x| x.2 = -x.2)
                  .as_collection()
    }
    /// Creates a new collection containing those input records satisfying the supplied predicate.
    pub fn filter<L: Fn(&D) -> bool + 'static>(&self, logic: L) -> Collection<G, D, R> {
        self.inner.filter(move |&(ref data, _, _)| logic(data))
                  .as_collection()
    }
    /// Creates a new collection accumulating the contents of the two collections.
    ///
    /// Despite the name, differential dataflow collections are unordered. This method is so named because the 
    /// implementation is the concatenation of the stream of updates, but it corresponds to the addition of the
    /// two collections.
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

        self.inner.enter_at(child, move |x| (*initial1)(&x.0))
                  .map(move |(data, time, diff)| {
                      let new_time = Product::new(time, (*initial2)(&data));
                      (data, new_time, diff)
                  })
                  .as_collection()
    }
    /// Applies a supplied function to each update.
    ///
    /// This method is most commonly used to report information back to the user, often for debugging purposes. 
    /// Any function can be used here, but be warned that the incremental nature of differential dataflow does
    /// not guarantee that it will be called as many times as you might expect.
    ///
    /// The `(data, time, diff)` triples indicate a change `diff` to the frequency of `data` which takes effect
    /// at the logical time `time`. When times are totally ordered (for example, `usize`), these updates reflect
    /// the changes along the sequence of collections. For partially ordered times, the mathematics are more 
    /// interesting and less intuitive, unfortunately. 
    pub fn inspect<F: FnMut(&(D, G::Timestamp, R))+'static>(&self, func: F) -> Collection<G, D, R> {
        self.inner.inspect(func)
                  .as_collection()
    }
    /// Applies a supplied function to each batch of updates.
    ///
    /// This method is analogous to `inspect`, but operates on batches and reveals the timely dataflow capability
    /// associated with the batch of updates.
    pub fn inspect_batch<F: FnMut(&G::Timestamp, &[(D, G::Timestamp, R)])+'static>(&self, func: F) -> Collection<G, D, R> {
        self.inner.inspect_batch(func)
                  .as_collection()
    }
    /// Attaches a timely dataflow probe to the output of a Collection.
    ///
    /// This probe is used to determine when the state of the Collection has stabilized and can
    /// be read out. 
    pub fn probe(&self) -> probe::Handle<G::Timestamp> {
        self.inner.probe()
    }
    /// Attaches a timely dataflow probe to the output of a Collection.
    ///
    /// This probe is used to determine when the state of the Collection has stabilized and all updates observed.
    /// In addition, a probe is also often use to limit the number of rounds of input in flight at any moment; a
    /// computation can wait until the probe has caught up to the input before introducing more rounds of data, to
    /// avoid swamping the system.
    pub fn probe_with(&self, handle: &mut ::timely::dataflow::operators::probe::Handle<G::Timestamp>) -> Collection<G, D, R> {
        self.inner.probe_with(handle).as_collection()
    }
    /// The scope containing the underlying timely dataflow stream.
    pub fn scope(&self) -> G {
        self.inner.scope()
    }
}

impl<'a, G: Scope, T: Timestamp, D: Data, R: Diff> Collection<Child<'a, G, T>, D, R> {
    /// Returns the final value of a Collection from a nested scope to its containing scope.
    pub fn leave(&self) -> Collection<G, D, R> {
        self.inner.leave()
                  .map(|(data, time, diff)| (data, time.outer, diff))
                  .as_collection()
    }
}

/// Conversion to a differential dataflow Collection.
pub trait AsCollection<G: Scope, D: Data, R: Diff> {
    /// Converts the type to a differential dataflow collection.
    fn as_collection(&self) -> Collection<G, D, R>;
}

impl<G: Scope, D: Data, R: Diff> AsCollection<G, D, R> for Stream<G, (D, G::Timestamp, R)> {
    fn as_collection(&self) -> Collection<G, D, R> {
        Collection::new(self.clone())
    }
}