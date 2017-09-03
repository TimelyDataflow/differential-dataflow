//! Reduce the collection to one occurrence of each distinct element.
//!
//! The `distinct_total` and `distinct_total_u` operators are optimizations of the more general
//! `distinct` and `distinct_u` operators for the case in which time is totally ordered.

use std::default::Default;

use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely_sort::Unsigned;

use lattice::Lattice;
use ::{Data, Collection, Diff};
use hashable::{Hashable, UnsignedWrapper};
use collection::AsCollection;
use operators::arrange::{Arrange, Arranged, ArrangeBySelf};
use trace::{BatchReader, Cursor, Trace, TraceReader};
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;

/// Extension trait for the `distinct` differential dataflow method.
pub trait DistinctTotal<G: Scope, K: Data, R: Diff> where G::Timestamp: TotalOrder+Lattice+Ord {
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::DistinctTotal;
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
    fn distinct_total(&self) -> Collection<G, K, isize>;
    /// Reduces the collection to one occurrence of each distinct element.
    /// 
    /// This method is a specialization for when the key is an unsigned integer fit for distributing
    /// the data.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::DistinctTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10u32).1
    ///              .map(|x| x / 3)
    ///              .distinct_total_u();
    ///     });
    /// }
    /// ```
    fn distinct_total_u(&self) -> Collection<G, K, isize> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, R: Diff> DistinctTotal<G, K, R> for Collection<G, K, R>
where G::Timestamp: TotalOrder+Lattice+Ord {
    fn distinct_total(&self) -> Collection<G, K, isize> {
        self.arrange_by_self()
            .distinct_total_core()
            .map(|k| k.item)
    }
    fn distinct_total_u(&self) -> Collection<G, K, isize> where K: Unsigned+Copy {
        self.map(|k| (UnsignedWrapper::from(k), ()))
            .arrange(DefaultKeyTrace::new())
            .distinct_total_core()
            .map(|k| k.item)
    }
}


/// Extension trait for the `distinct_total_core` differential dataflow method.
pub trait DistinctTotalCore<G: Scope, K: Data, R: Diff> where G::Timestamp: TotalOrder+Lattice+Ord {
    /// Applies `distinct` to arranged data, and returns a collection of output data.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::Arrange;
    /// use differential_dataflow::operators::distinct::DistinctTotalCore;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdKeySpine;
    /// use differential_dataflow::hashable::OrdWrapper;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         // wrap and order input, then group manually.
    ///         scope.new_collection_from(1 .. 10u32).1
    ///              .map(|x| (OrdWrapper { item: x / 3 }, ()))
    ///              .arrange(OrdKeySpine::new())
    ///              .distinct_total_core();
    ///     });
    /// }    
    /// ```
    fn distinct_total_core(&self) -> Collection<G, K, isize>;
}

impl<G: Scope, K: Data, R: Diff, T1> DistinctTotalCore<G, K, R> for Arranged<G, K, (), R, T1>
where 
    G::Timestamp: TotalOrder+Lattice+Ord,
    T1: TraceReader<K, (), G::Timestamp, R>+Clone+'static,
    T1::Batch: BatchReader<K, (), G::Timestamp, R> {

    fn distinct_total_core(&self) -> Collection<G, K, isize> {

        let mut trace = self.trace.clone();

        self.stream.unary_stream(Pipeline, "DistinctTotal", move |input, output| {

            input.for_each(|capability, batches| {

                let mut session = output.session(&capability);
                for batch in batches.drain(..).map(|x| x.item) {

                    let (mut batch_cursor, batch_storage) = batch.cursor();
                    let (mut trace_cursor, trace_storage) = trace.cursor_through(batch.lower()).unwrap();

                    while batch_cursor.key_valid(&batch_storage) {
                        let key = batch_cursor.key(&batch_storage);
                        let mut count = R::zero();

                        // Compute the multiplicity of this key before the current batch.
                        trace_cursor.seek_key(&trace_storage, key);
                        if trace_cursor.key_valid(&trace_storage) && trace_cursor.key(&trace_storage) == key {
                            trace_cursor.map_times(&trace_storage, |_, diff| count = count + diff);
                        }

                        // Take into account the current batch. At each time, check whether the
                        // "presence" of the key changes. If it was previously present (i.e. had
                        // nonzero multiplicity) but now is no more, emit -1. Conversely, if it is
                        // newly present, emit +1. In both remaining cases, the result remains
                        // unchanged (note that this is better than the naive approach which would
                        // eliminate the "previous" record and immediately re-add it).
                        batch_cursor.map_times(&batch_storage, |time, diff| {
                            let old_distinct = !count.is_zero();
                            count = count + diff;
                            let new_distinct = !count.is_zero();
                            if old_distinct != new_distinct {
                                session.give((key.clone(), time.clone(), if old_distinct { -1 } else { 1 }));
                            }
                        });

                        batch_cursor.step_key(&batch_storage);
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