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
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;
use timely_sort::Unsigned;

use ::{Collection, Data, Ring, Hashable};
use operators::group::Group;

/// An extension method for consolidating weighted streams.
pub trait Consolidate<D: Data> {
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data.
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

    /// Aggregates the weights of equal records into at most one record, partitions the
    /// data using the supplied partition function.
    ///
    /// #Examples
    ///
    /// In the following fragment, `result` contains only `(1, 3)`:
    ///
    /// ```ignore
    /// vec![(0,1),(1,1),(0,-1),(1,2)]
    ///     .to_stream(scope)
    ///     .as_collection()
    ///     .consolidate(|&x| x)
    ///     .inspect(|x| println!("{:}", x));
    /// ```    
    fn consolidate_by<U: Unsigned, F: Fn(&D)->U+'static>(&self, part: F) -> Self;
}

impl<G: Scope, D, R> Consolidate<D> for Collection<G, D, R>
where
    D: Data+Debug+Hashable+Default,
    R: Ring,
    G::Timestamp: ::lattice::Lattice+Ord,
 {
    fn consolidate(&self) -> Self where D: Hashable {
       self.consolidate_by(|x| x.hashed())
    }

    fn consolidate_by<U: Unsigned, F: Fn(&D)->U+'static>(&self, part: F) -> Self {

        // let mut buffer = Vec::new();
        // let mut capabilities = Vec::new();

        self.map(|x| (x,()))
            .group(|_,s,t| t.push(((), s[0].1)))
            .map(|(x,_)| x)

        // let exch = Exchange::new(move |&(ref x,_,_): &(D,G::Timestamp,isize)| part(x).as_u64());
        // Collection::new(self.inner.unary_notify(exch, "Consolidate", vec![], move |_input, _output, _notificator| {

        //     unimplemented!();

        // }))
    }
}


// /// Compacts `(T, Delta)` pairs lazily.
// pub struct BatchCompact<T: Ord> {
//     sorted: usize,
//     buffer: Vec<(T, Delta)>,
// }

// impl<T: Ord> BatchCompact<T> {
//     /// Allocates a new batch compacter.
//     pub fn new() -> BatchCompact<T> {
//         BatchCompact {
//             sorted: 0,
//             buffer: Vec::new(),
//         }
//     }

//     /// Adds an element to the batch compacter.
//     pub fn push(&mut self, element: (T, Delta)) {
//         self.buffer.push(element);
//         if self.buffer.len() > ::std::cmp::max(self.sorted * 2, 1 << 20) {
//             self.buffer.sort();
//             for index in 1 .. self.buffer.len() {
//                 if self.buffer[index].0 == self.buffer[index-1].0 {
//                     self.buffer[index].1 += self.buffer[index-1].1;
//                     self.buffer[index-1].1 = 0;
//                 }
//             }
//             self.buffer.retain(|x| x.1 != 0);
//             self.sorted = self.buffer.len();
//         }
//     }
//     /// Adds several elements to the batch compacted.
//     pub fn extend<I: Iterator<Item=(T, Delta)>>(&mut self, iter: I) {
//         for item in iter {
//             self.push(item);
//         }
//     }
//     /// Finishes compaction, returns results.
//     pub fn done(mut self) -> Vec<(T, Delta)> {
//         if self.buffer.len() > self.sorted {
//             self.buffer.sort();
//             for index in 1 .. self.buffer.len() {
//                 if self.buffer[index].0 == self.buffer[index-1].0 {
//                     self.buffer[index].1 += self.buffer[index-1].1;
//                     self.buffer[index-1].1 = 0;
//                 }
//             }
//             self.buffer.retain(|x| x.1 != 0);
//             self.sorted = self.buffer.len();
//         }
//         self.buffer
//     }
// }

