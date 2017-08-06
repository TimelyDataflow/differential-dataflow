//! Group records by a key, and apply a reduction function.
//!
//! The `group` operators act on data that can be viewed as pairs `(key, val)`. They group records
//! with the same key, and apply user supplied functions to the key and a list of values, which are
//! expected to populate a list of output values.
//!
//! Several variants of `group` exist which allow more precise control over how grouping is done.
//! For example, the `_by` suffixed variants take arbitrary data, but require a key-value selector
//! to be applied to each record. The `_u` suffixed variants use unsigned integers as keys, and
//! will use a dense array rather than a `HashMap` to store their keys.
//!
//! The list of values are presented as an iterator which internally merges sorted lists of values.
//! This ordering can be exploited in several cases to avoid computation when only the first few
//! elements are required.

use std::default::Default;

use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely_sort::Unsigned;

use ::{Data, Collection, Diff};
use hashable::{Hashable, UnsignedWrapper};
use collection::AsCollection;
use operators::arrange::{Arrange, Arranged, ArrangeBySelf};
use lattice::TotalOrder;
use trace::{BatchReader, Cursor, Trace, TraceReader};
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;

/// Extension trait for the `count` differential dataflow method.
pub trait CountTotal<G: Scope, K: Data, R: Diff> where G::Timestamp: TotalOrder+Ord {
    /// Counts the number of occurrences of each element.
    ///
    /// # Examples
    ///
    /// ```
    /// #
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::CountTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .count_total();
    ///     });
    /// }
    /// ```
    fn count_total(&self) -> Collection<G, (K, R), isize>;
    /// Counts the number of occurrences of each element.
    /// 
    /// This method is a specialization for when the key is an unsigned integer fit for distributing the data.
    ///
    /// # Examples
    ///
    /// ```
    /// #
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::CountTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10u32).1
    ///              .map(|x| x / 3)
    ///              .count_total_u();
    ///     });
    /// }
    /// ```
    fn count_total_u(&self) -> Collection<G, (K, R), isize> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, R: Diff> CountTotal<G, K, R> for Collection<G, K, R>
where G::Timestamp: TotalOrder+Ord {
    fn count_total(&self) -> Collection<G, (K, R), isize> {
        self.arrange_by_self()
            .count_total_core()
            .map(|(k,c)| (k.item, c))
    }
    fn count_total_u(&self) -> Collection<G, (K, R), isize> where K: Unsigned+Copy {
        self.map(|k| (UnsignedWrapper::from(k), ()))
            .arrange(DefaultKeyTrace::new())
            .count_total_core()
            .map(|(k,c)| (k.item, c))
    }
}


/// Extension trait for the `group_arranged` differential dataflow method.
pub trait CountTotalCore<G: Scope, K: Data, R: Diff> where G::Timestamp: TotalOrder+Ord {
    /// Applies `group` to arranged data, and returns an arrangement of output data.
    ///
    /// This method is used by the more ergonomic `group`, `distinct`, and `count` methods, although
    /// it can be very useful if one needs to manually attach and re-use existing arranged collections.
    ///
    /// # Examples
    ///
    /// ```
    /// #
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::Arrange;
    /// use differential_dataflow::operators::count::CountTotalCore;
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
    ///              .count_total_core();
    ///     });
    /// }    
    /// ```
    fn count_total_core(&self) -> Collection<G, (K, R), isize>;
}

impl<G: Scope, K: Data, R: Diff, T1> CountTotalCore<G, K, R> for Arranged<G, K, (), R, T1>
where 
    G::Timestamp: TotalOrder+Ord,
    T1: TraceReader<K, (), G::Timestamp, R>+Clone+'static,
    T1::Batch: BatchReader<K, (), G::Timestamp, R> {

    fn count_total_core(&self) -> Collection<G, (K, R), isize> {

        let mut trace = self.trace.clone();

        self.stream.unary_stream(Pipeline, "CountTotal", move |input, output| {

            input.for_each(|capability, batches| {

                let mut session = output.session(&capability);
                for batch in batches.drain(..).map(|x| x.item) {

                    let (mut batch_cursor, batch_storage) = batch.cursor();
                    let (mut trace_cursor, trace_storage) = trace.cursor_through(batch.lower()).unwrap();

                    while batch_cursor.key_valid(&batch_storage) {

                        let key: K = batch_cursor.key(&batch_storage).clone();
                        let mut count = R::zero();

                        trace_cursor.seek_key(&trace_storage, batch_cursor.key(&batch_storage));
                        if trace_cursor.key_valid(&trace_storage) && trace_cursor.key(&trace_storage) == batch_cursor.key(&batch_storage) {
                            trace_cursor.map_times(&trace_storage, |_, diff| count = count + diff);
                        }

                        batch_cursor.map_times(&batch_storage, |time, diff| {

                            if !count.is_zero() {
                                session.give(((key.clone(), count), time.clone(), -1));
                            }
                            count = count + diff;
                            if !count.is_zero() {
                                session.give(((key.clone(), count), time.clone(), 1));
                            }

                        });

                        batch_cursor.step_key(&batch_storage);
                    }

                    // tidy up the shared input trace.
                    trace.advance_by(batch.upper());
                    trace.distinguish_since(batch.upper());
                }
            });
        })
        .as_collection()
    }
}