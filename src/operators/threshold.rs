//! Reduce the collection to one occurrence of each distinct element.
//!
//! The `distinct_total` and `distinct_total_u` operators are optimizations of the more general
//! `distinct` and `distinct_u` operators for the case in which time is totally ordered.

use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use lattice::Lattice;
use ::{Data, Collection, Diff};
use hashable::Hashable;
use collection::AsCollection;
use operators::arrange::{Arranged, ArrangeBySelf};
use trace::{BatchReader, Cursor, TraceReader};

/// Extension trait for the `distinct` differential dataflow method.
pub trait ThresholdTotal<G: Scope, K: Data, R: Diff> where G::Timestamp: TotalOrder+Lattice+Ord {
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
    ///              .threshold_total(|c| c % 2);
    ///     });
    /// }
    /// ```
    fn threshold_total<R2: Diff, F: Fn(R)->R2+'static>(&self, thresh: F) -> Collection<G, K, R2>;
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
        self.threshold_total(|c| if c.is_zero() { 0 } else { 1 })
    }
}

impl<G: Scope, K: Data+Hashable, R: Diff> ThresholdTotal<G, K, R> for Collection<G, K, R>
where G::Timestamp: TotalOrder+Lattice+Ord {
    fn threshold_total<R2: Diff, F: Fn(R)->R2+'static>(&self, thresh: F) -> Collection<G, K, R2> {
        self.arrange_by_self()
            .threshold_total(thresh)
    }
}

impl<G: Scope, K: Data, R: Diff, T1> ThresholdTotal<G, K, R> for Arranged<G, K, (), R, T1>
where
    G::Timestamp: TotalOrder+Lattice+Ord,
    T1: TraceReader<K, (), G::Timestamp, R>+Clone+'static,
    T1::Batch: BatchReader<K, (), G::Timestamp, R> {

    fn threshold_total<R2: Diff, F:Fn(R)->R2+'static>(&self, thresh: F) -> Collection<G, K, R2> {

        let mut trace = self.trace.clone();
        let mut buffer = Vec::new();

        self.stream.unary(Pipeline, "ThresholdTotal", move |_,_| move |input, output| {

            let thresh = &thresh;

            input.for_each(|capability, batches| {
                batches.swap(&mut buffer);
                let mut session = output.session(&capability);
                for batch in buffer.drain(..).map(|x| x.item) {

                    let mut batch_cursor = batch.cursor();
                    let (mut trace_cursor, trace_storage) = trace.cursor_through(batch.lower()).unwrap();

                    while batch_cursor.key_valid(&batch) {
                        let key = batch_cursor.key(&batch);
                        let mut count = R::zero();

                        // Compute the multiplicity of this key before the current batch.
                        trace_cursor.seek_key(&trace_storage, key);
                        if trace_cursor.key_valid(&trace_storage) && trace_cursor.key(&trace_storage) == key {
                            trace_cursor.map_times(&trace_storage, |_, diff| count = count + diff);
                        }

                        // Apply `thresh` both before and after `diff` is applied to `count`.
                        // If the result is non-zero, send it along.
                        batch_cursor.map_times(&batch, |time, diff| {
                            let old_weight = if count.is_zero() { R2::zero() } else { thresh(count) };
                            count = count + diff;
                            let new_weight = if count.is_zero() { R2::zero() } else { thresh(count) };
                            let difference = new_weight - old_weight;
                            if !difference.is_zero() {
                                session.give((key.clone(), time.clone(), difference));
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