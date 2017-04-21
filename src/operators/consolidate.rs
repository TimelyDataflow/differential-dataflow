//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.
//!
//! #Examples
//!
//! This example performs a standard "word count", where each line of text is split into multiple
//! words, each word is converted to a word with count 1, and `consolidate` then accumulates the
//! counts of equivalent words.
//!
//! ```ignore
//! stream.flat_map(|line| line.split_whitespace())
//!       .map(|word| (word, 1))
//!       .consolidate();
//! ```

use std::fmt::Debug;

use timely::dataflow::*;

use ::{Collection, Data, Diff, Hashable};
use operators::arrange::ArrangeBySelf;

/// An extension method for consolidating weighted streams.
pub trait Consolidate<D: Data> {
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data. The data are 
    /// accumulated in place, each held back until their timestamp has completed.
    /// 
    /// #Examples
    ///
    /// In the following fragment, we should see only `(1, 3)`:
    ///
    /// ```ignore
    /// vec![(0,1),(1,1),(0,-1),(1,2)]
    ///     .to_stream(scope)
    ///     .as_collection()
    ///     .consolidate()
    ///     .inspect(|x| println!("{:}", x));
    /// ```
    fn consolidate(&self) -> Self where D: Hashable;
}

impl<G: Scope, D, R> Consolidate<D> for Collection<G, D, R>
where
    D: Data+Debug+Hashable+Default,
    R: Diff,
    G::Timestamp: ::lattice::Lattice+Ord,
 {
    fn consolidate(&self) -> Self where D: Hashable {
       self.arrange_by_self().as_collection(|d,_| d.item.clone())
    }
}
