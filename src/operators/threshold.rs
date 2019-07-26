//! Reduce the collection to one occurrence of each distinct element.
//!
//! The `distinct_total` and `distinct_total_u` operators are optimizations of the more general
//! `distinct` and `distinct_u` operators for the case in which time is totally ordered.

use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use lattice::Lattice;
use ::{ExchangeData, Collection};
use ::difference::{Monoid, Abelian};
use hashable::Hashable;
use collection::AsCollection;
use operators::arrange::{Arranged, ArrangeBySelf};
use trace::{BatchReader, Cursor, TraceReader};

/// Extension trait for the `distinct` differential dataflow method.
pub trait ThresholdTotal<G: Scope, K: ExchangeData, R: ExchangeData+Monoid> where G::Timestamp: TotalOrder+Lattice+Ord {
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::ThresholdTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .threshold_total(|_,c| c % 2);
    ///     });
    /// }
    /// ```
    fn threshold_total<R2: Abelian, F: Fn(&K,&R)->R2+'static>(&self, thresh: F) -> Collection<G, K, R2>;
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// This reduction only tests whether the weight associated with a record is non-zero, and otherwise
    /// ignores its specific value. To take more general actions based on the accumulated weight, consider
    /// the `threshold` method.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::ThresholdTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .distinct_total();
    ///     });
    /// }
    /// ```
    fn distinct_total(&self) -> Collection<G, K, isize> {
        self.threshold_total(|_,c| if c.is_zero() { 0isize } else { 1isize })
    }
}

impl<G: Scope, K: ExchangeData+Hashable, R: ExchangeData+Monoid> ThresholdTotal<G, K, R> for Collection<G, K, R>
where G::Timestamp: TotalOrder+Lattice+Ord {
    fn threshold_total<R2: Abelian, F: Fn(&K,&R)->R2+'static>(&self, thresh: F) -> Collection<G, K, R2> {
        self.arrange_by_self()
            .threshold_total(thresh)
    }
}

impl<G: Scope, T1> ThresholdTotal<G, T1::Key, T1::R> for Arranged<G, T1>
where
    G::Timestamp: TotalOrder+Lattice+Ord,
    T1: TraceReader<Val=(), Time=G::Timestamp>+Clone+'static,
    T1::Key: ExchangeData,
    T1::R: ExchangeData+Monoid,
    T1::Batch: BatchReader<T1::Key, (), G::Timestamp, T1::R>,
    T1::Cursor: Cursor<T1::Key, (), G::Timestamp, T1::R>,
{

    fn threshold_total<R2: Abelian, F:Fn(&T1::Key,&T1::R)->R2+'static>(&self, thresh: F) -> Collection<G, T1::Key, R2> {

        let mut trace = self.trace.clone();
        let mut buffer = Vec::new();

        self.stream.unary(Pipeline, "ThresholdTotal", move |_,_| move |input, output| {

            let thresh = &thresh;

            input.for_each(|capability, batches| {
                batches.swap(&mut buffer);
                let mut session = output.session(&capability);
                for batch in buffer.drain(..) {

                    let mut batch_cursor = batch.cursor();
                    let (mut trace_cursor, trace_storage) = trace.cursor_through(batch.lower()).unwrap();

                    while batch_cursor.key_valid(&batch) {
                        let key = batch_cursor.key(&batch);
                        let mut count = None;

                        // Compute the multiplicity of this key before the current batch.
                        trace_cursor.seek_key(&trace_storage, key);
                        if trace_cursor.key_valid(&trace_storage) && trace_cursor.key(&trace_storage) == key {
                            trace_cursor.map_times(&trace_storage, |_, diff| {
                                count.as_mut().map(|c| *c += diff);
                                if count.is_none() { count = Some(diff.clone()); }
                            });
                        }

                        // Apply `thresh` both before and after `diff` is applied to `count`.
                        // If the result is non-zero, send it along.
                        batch_cursor.map_times(&batch, |time, diff| {

                            // Determine old and new weights.
                            // If a count is zero, the weight must be zero.
                            let old_weight = count.as_ref().map(|c| thresh(key, c));
                            count.as_mut().map(|c| *c += diff);
                            if count.is_none() { count = Some(diff.clone()); }
                            let new_weight = count.as_ref().map(|c| thresh(key, c));

                            let difference =
                            match (old_weight, new_weight) {
                                (Some(old), Some(new)) => { let mut diff = -old; diff += &new; Some(diff) },
                                (Some(old), None) => { Some(-old) },
                                (None, Some(new)) => { Some(new) },
                                (None, None) => None,
                            };

                            if let Some(difference) = difference {
                                if !difference.is_zero() {
                                    session.give((key.clone(), time.clone(), difference));
                                }
                            }
                        });

                        batch_cursor.step_key(&batch);
                    }

                    // Tidy up the shared input trace.
                    trace.advance_by(batch.upper());
                    trace.distinguish_since(batch.upper());
                }
            });
        })
        .as_collection()
    }
}