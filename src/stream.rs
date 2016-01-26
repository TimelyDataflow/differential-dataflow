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
    pub inner: Stream<G, (D, Delta)>
}

impl<G: Scope, D: Data> Collection<G, D> {
    pub fn new(inner: Stream<G, (D, Delta)>) -> Collection<G, D> {
        Collection {
            inner: inner
        }
    }

    pub fn map<D2: Data, L: Fn(D) -> D2 + 'static>(&self, logic: L) -> Collection<G, D2> {
        Collection {
            inner: self.inner.map(move |(data, delta)| (logic(data), delta))
        }
    }

    pub fn map_in_place<L: Fn(&mut D) + 'static>(&self, logic: L) -> Collection<G, D> {
        Collection {
            inner: self.inner.map_in_place(move |&mut (ref mut data, _)| logic(data))
        }
    }

    pub fn negate(&self) -> Collection<G, D> {
        Collection {
            inner: self.inner.map_in_place(|x| x.1 *= -1)
        }
    }

    pub fn filter<L: Fn(&D) -> bool + 'static>(&self, logic: L) -> Collection<G, D> {
        Collection {
            inner: self.inner.filter(move |&(ref data, _)| logic(data))
        }
    }

    pub fn concat(&self, other: &Collection<G, D>) -> Collection<G, D> {
        Collection {
            inner: self.inner.concat(&other.inner)
        }
    }

    pub fn enter<T: Timestamp>(&self, child: &Child<G, T>) -> Collection<Child<G, T>, D> {
        Collection {
            inner: self.inner.enter(child)
        }
    }

    pub fn enter_at<T: Timestamp, F: Fn(&(D, Delta)) -> T + 'static>(&self, child: &Child<G, T>, initial: F) -> Collection<Child<G, T>, D> where G::Timestamp: Hash, T: Hash {
        Collection {
            inner: self.inner.enter_at(child, initial)
        }
    }

    pub fn inspect<F: FnMut(&(D, Delta))+'static>(&self, func: F) -> Collection<G, D> {
        Collection {
            inner: self.inner.inspect(func)
        }
    }

    pub fn inspect_batch<F: FnMut(&G::Timestamp, &[(D, Delta)])+'static>(&self, func: F) -> Collection<G, D> {
        Collection {
            inner: self.inner.inspect_batch(func)
        }
    }

    pub fn probe(&self) -> (probe::Handle<G::Timestamp>, Collection<G, D>) {
        let (handle, stream) = self.inner.probe();
        (handle, Collection {
            inner: stream
        })
    }

    pub fn scope(&self) -> G {
        self.inner.scope()
    }
}

impl<G: Scope, T: Timestamp, D: Data> Collection<Child<G, T>, D> {
    pub fn leave(&self) -> Collection<G, D> {
        Collection {
            inner: self.inner.leave()
        }
    }
}
