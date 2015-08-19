//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use std::rc::Rc;
use std::fmt::Debug;

use timely::Data;
use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::Content;

// use sort::*;
// use sort::radix_merge::{Accumulator, Compact};
use collection_trace::Lookup;
use collection_trace::lookup::UnsignedInt;
use iterators::coalesce::Coalesce;
use radix_sort::RadixSorter;

use timely::drain::DrainExt;

/// An extension method for consolidating weighted streams.
pub trait ConsolidateExt<D> {
    /// Aggregates the weights of equal records into at most one record.
    fn consolidate<U: UnsignedInt, F: Fn(&D)->U+'static>(&self, part: F) -> Self;
}

impl<G: Scope, D: Ord+Data+Debug> ConsolidateExt<D> for Stream<G, (D, i32)> {
    fn consolidate<U: UnsignedInt, F: Fn(&D)->U+'static>(&self, part: F) -> Self {

        let mut inputs = Vec::new();    // Vec<(G::Timestamp, Vec<(D, i32))>
        let part1 = Rc::new(part);
        let part2 = part1.clone();

        let exch = Exchange::new(move |&(ref x,_)| (*part1)(x).as_u64());
        self.unary_notify(exch, "Consolidate", vec![], move |input, output, notificator| {

            // input.for_each(|index: &G::Timestamp, data: &mut Content<(D, i32)>| {
            while let Some((index, data)) = input.next() {
                notificator.notify_at(&index);
                inputs.entry_or_insert(index.clone(), || RadixSorter::new())
                      .extend(data.drain_temp(), &|x| (*part2)(&x.0).as_u64());
            }
            // });

            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {
            // notificator.for_each(|index, _count| {
                if let Some(mut stash) = inputs.remove_key(&index) {

                    let mut session = output.session(&index);
                    let mut buffer = vec![];
                    let mut current = 0;

                    let source = stash.finish(&|x| (*part2)(&x.0).as_u64());
                    for (datum, wgt) in source.into_iter().flat_map(|x| x.into_iter()) {
                        let hash = (*part2)(&datum).as_u64();
                        if buffer.len() > 0 && hash != current {
                            // if hash < current { println!("  radix sort error? {} < {}", hash, current); }
                            buffer.sort_by(|x: &(D,i32),y: &(D,i32)| x.0.cmp(&y.0));
                            session.give_iterator(buffer.drain_temp().coalesce());
                            current = hash;
                        }
                        buffer.push((datum,wgt));
                    }

                    if buffer.len() > 0 {
                        buffer.sort_by(|x: &(D,i32),y: &(D,i32)| x.0.cmp(&y.0));
                        session.give_iterator(buffer.drain_temp().coalesce());
                    }
                }
            }
            // });
        })
    }
}
