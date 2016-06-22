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

use linear_map::LinearMap;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;
use timely_sort::{LSBRadixSorter, Unsigned};

use collection::Lookup;
use iterators::coalesce::Coalesce;

use ::{Collection, Data};

/// An extension method for consolidating weighted streams.
pub trait ConsolidateExt<D: Data> {
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data.
    /// 
    /// #Examples
    ///
    /// In the following fragment, `result` contains only `(1, 3)`:
    ///
    /// ```ignore
    /// let stream = vec![(0,1),(1,1),(0,-1),(1,2)].to_stream(scope);
    /// let collection = Collection::new(stream);
    /// let result = collection.consolidate();
    /// ```
    fn consolidate(&self) -> Self;

    /// Aggregates the weights of equal records into at most one record, partitions the
    /// data using the supplied partition function.
    ///
    /// Note that `consolidate_by` attempts to aggregate weights as it goes, to ensure
    /// that it does not consume more memory than is required of its collection. It does
    /// among blocks of records with the same `part` value, so if you just set all part 
    /// values to the same value, it may not do a great job because you'll have lots of 
    /// blocks with distinct values. Just, bear that in mind if you want to be clever.
    ///
    /// #Examples
    ///
    /// In the following fragment, `result` contains only `(1, 3)`:
    ///
    /// ```ignore
    /// let stream = vec![(0,1),(1,1),(0,-1),(1,2)].to_stream(scope);
    /// let collection = Collection::new(stream);
    /// let result = collection.consolidate_by(|&x| x);
    /// ```    
    fn consolidate_by<U: Unsigned, F: Fn(&D)->U+'static>(&self, part: F) -> Self;
}

impl<G: Scope, D: Ord+Data+Debug> ConsolidateExt<D> for Collection<G, D> {
    fn consolidate(&self) -> Self {
       self.consolidate_by(|x| x.hashed())
    }

    fn consolidate_by<U: Unsigned, F: Fn(&D)->U+'static>(&self, part: F) -> Self {
        let mut inputs = LinearMap::new();    // LinearMap<G::Timestamp, Vec<(D, i32)>>
        let part1 = Rc::new(part);
        let part2 = part1.clone();

        let exch = Exchange::new(move |&(ref x,_)| (*part1)(x).as_u64());
        Collection::new(self.inner.unary_notify(exch, "Consolidate", vec![], move |input, output, notificator| {

            // Consolidation needs to address the issue that the amount of incoming data
            // may be too large to maintain in queues, but small enough once aggregated.
            // Although arbitrarily large queues *should* work at sequential disk io (?)
            // it seems like we should be smarter than that. We could overflow disk when
            // counting crazy 12-cliques or something, right?

            // So, we can't just pop data off and enqueue it, we need to do something like
            // sort and consolidate what we have, or something like that. We know how to 
            // sort and consolidate (see below), but who knows how we should be doing merges
            // and weird stuff like that... Probably power-of-two merges or somesuch...

            // If we thought we were unlikely to have collisions, we could just sort each 
            // of the hunks we get out of the radix sorter and coalesce them. This could be
            // arbitrarily ineffective if we have lots of collisions though (consolidating 
            // by a key of a (key,val) pair, for some reason). Let's try it for now.

            input.for_each(|index, data| {

                // make large to turn off compaction.
                let default_threshold = usize::max_value();
                // let default_threshold = 1 << 20;

                // an entry stores a sorter (the data), a current count, and a compaction threshold.
                let entry = inputs.entry_or_insert(index.time(), || (LSBRadixSorter::new(), 0, default_threshold));
                let (ref mut sorter, ref mut count, ref mut thresh) = *entry;

                *count += data.len();
                sorter.extend(data.drain(..), &|x| (*part2)(&x.0));

                if count > thresh {

                    *count = 0; 
                    *thresh = 0;

                    // pull out blocks sorted by the hash; coalesce each.
                    let finished = sorter.finish(&|x| (*part2)(&x.0));
                    for mut block in finished {
                        let mut finger = 0;
                        for i in 1..block.len() {
                            if block[finger].0 == block[i].0 {
                                block[finger].1 += block[i].1;
                                block[i].1 = 0;
                            }
                            else {
                                finger = i;
                            }
                        }
                        block.retain(|x| x.1 != 0);
                        *thresh += block.len();
                        sorter.push_batch(block, &|x| (*part2)(&x.0));
                    }

                    if *thresh < default_threshold { *thresh = default_threshold; }
                }

                notificator.notify_at(index);
            });

            // 2. go through each time of interest that has reached completion
            notificator.for_each(|index, _count, _notificator| {

                // pull out sorter, ignore count and thresh (irrelevant now).
                if let Some((mut sorter, _, _)) = inputs.remove_key(&index) {

                    let mut session = output.session(&index);
                    let mut buffer = vec![];
                    let mut current = 0;

                    let source = sorter.finish(&|x| (*part2)(&x.0));
                    for (datum, wgt) in source.into_iter().flat_map(|x| x.into_iter()) {
                        let hash = (*part2)(&datum).as_u64();
                        if buffer.len() > 0 && hash != current {
                            buffer.sort_by(|x: &(D,i32),y: &(D,i32)| x.0.cmp(&y.0));
                            session.give_iterator(buffer.drain(..).coalesce());
                        }
                        buffer.push((datum,wgt));
                        current = hash;
                    }

                    if buffer.len() > 0 {
                        buffer.sort_by(|x: &(D,i32),y: &(D,i32)| x.0.cmp(&y.0));
                        session.give_iterator(buffer.drain(..).coalesce());
                    }
                }
            });
        }))
    }
}
