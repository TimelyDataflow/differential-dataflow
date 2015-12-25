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

use std::rc::Rc;
use std::fmt::Debug;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;
// use timely::dataflow::channels::Content;

use collection::Lookup;
use iterators::coalesce::Coalesce;
use radix_sort::{RadixSorter, Unsigned};

use ::{Collection, Data};

use timely::drain::DrainExt;

/// An extension method for consolidating weighted streams.
pub trait ConsolidateExt<D: Data> {
    /// Aggregates the weights of equal records into at most one record.
    fn consolidate(&self) -> Self;

    /// Aggregates the weights of equal records into at most one record, partitions data using the
    /// supplied partition function.
    fn consolidate_by<U: Unsigned, F: Fn(&D)->U+'static>(&self, part: F) -> Self;
}

impl<G: Scope, D: Ord+Data+Debug> ConsolidateExt<D> for Collection<G, D> {
    fn consolidate(&self) -> Self {
       self.consolidate_by(|x| x.hashed())
    }

    fn consolidate_by<U: Unsigned, F: Fn(&D)->U+'static>(&self, part: F) -> Self {
        let mut inputs = Vec::new();    // Vec<(G::Timestamp, Vec<(D, i32))>
        let part1 = Rc::new(part);
        let part2 = part1.clone();

        let exch = Exchange::new(move |&(ref x,_)| (*part1)(x).as_u64());
        Collection::new(self.inner.unary_notify(exch, "Consolidate", vec![], move |input, output, notificator| {

            // input.for_each(|index: &G::Timestamp, data: &mut Content<(D, i32)>| {
            while let Some((index, data)) = input.next() {
                notificator.notify_at(&index);
                inputs.entry_or_insert(index.clone(), || RadixSorter::new())
                      .extend(data.drain_temp(), &|x| (*part2)(&x.0));
            }
            // });

            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {
            // notificator.for_each(|index, _count| {
                if let Some(mut stash) = inputs.remove_key(&index) {

                    // let start = ::time::precise_time_s();

                    let mut session = output.session(&index);
                    let mut buffer = vec![];
                    let mut current = 0;

                    let source = stash.finish(&|x| (*part2)(&x.0));
                    for (datum, wgt) in source.into_iter().flat_map(|x| x.into_iter()) {
                        let hash = (*part2)(&datum).as_u64();
                        if buffer.len() > 0 && hash != current {
                            buffer.sort_by(|x: &(D,i32),y: &(D,i32)| x.0.cmp(&y.0));
                            session.give_iterator(buffer.drain_temp().coalesce());
                        }
                        buffer.push((datum,wgt));
                        current = hash;
                    }

                    if buffer.len() > 0 {
                        buffer.sort_by(|x: &(D,i32),y: &(D,i32)| x.0.cmp(&y.0));
                        session.give_iterator(buffer.drain_temp().coalesce());
                    }

                    // println!("consolidated {:?} in {:?}s", index, ::time::precise_time_s() - start);
                }
            }
            // });
        }))
    }
}
