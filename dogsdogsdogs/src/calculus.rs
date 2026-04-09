//! Traits and implementations for differentiating and integrating collections.
//!
//! The `Differentiate` and `Integrate` traits allow us to move between standard differential
//! dataflow collections, and collections that describe their instantaneous change. The first
//! trait converts a collection to one that contains each change at the moment it occurs, but
//! then immediately retracting it. The second trait takes such a representation are recreates
//! the collection from its instantaneous changes.
//!
//! These two traits together allow us to build dataflows that maintain computates over inputs
//! that are the instantaneous changes, and then to reconstruct collections from them. The most
//! clear use case for this are "delta query" implementations of relational joins, where linearity
//! allows us to write dataflows based on instantaneous changes, whose "accumluated state" is
//! almost everywhere empty (and so has a low memory footprint, if the system works as planned).

use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::dataflow::operators::vec::{Filter, Map};
use differential_dataflow::{AsCollection, VecCollection, Data};
use differential_dataflow::difference::Abelian;

use crate::altneu::AltNeu;

/// Produce a collection containing the changes at the moments they happen.
pub trait Differentiate<G: Timestamp, D: Data, R: Abelian> {
    fn differentiate(self, child: &Scope<AltNeu<G>>) -> VecCollection<AltNeu<G>, D, R>;
}

/// Collect instantaneous changes back in to a collection.
pub trait Integrate<G: Timestamp, D: Data, R: Abelian> {
    fn integrate(self, outer: &Scope<G>) -> VecCollection<G, D, R>;
}

impl<G, D, R> Differentiate<G, D, R> for VecCollection<G, D, R>
where
    G: Timestamp,
    D: Data,
    R: Abelian + 'static,
{
    // For each (data, Alt(time), diff) we add a (data, Neu(time), -diff).
    fn differentiate(self, child: &Scope<AltNeu<G>>) -> VecCollection<AltNeu<G>, D, R> {
        self.enter(child)
            .inner
            .flat_map(|(data, time, diff)| {
                let mut neg_diff = diff.clone();
                neg_diff.negate();
                let neu = (data.clone(), AltNeu::neu(time.time.clone()), neg_diff);
                let alt = (data, time, diff);
                Some(alt).into_iter().chain(Some(neu))
            })
            .as_collection()
    }
}

impl<G, D, R> Integrate<G, D, R> for VecCollection<AltNeu<G>, D, R>
where
    G: Timestamp,
    D: Data,
    R: Abelian + 'static,
{
    // We discard each `neu` variant and strip off the `alt` wrapper.
    fn integrate(self, outer: &Scope<G>) -> VecCollection<G, D, R> {
        self.inner
            .filter(|(_d,t,_r)| !t.neu)
            .as_collection()
            .leave(outer)
    }
}
