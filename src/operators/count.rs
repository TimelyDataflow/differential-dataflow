//! Count the number of occurrences of each element.

use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use crate::trace::cursor::IntoOwned;

use crate::lattice::Lattice;
use crate::{ExchangeData, Collection};
use crate::difference::{IsZero, Semigroup};
use crate::hashable::Hashable;
use crate::collection::AsCollection;
use crate::operators::arrange::{Arranged, ArrangeBySelf};
use crate::trace::{BatchReader, Cursor, TraceReader};

/// Extension trait for the `count` differential dataflow method.
pub trait CountTotal<G: Scope, K: ExchangeData, R: Semigroup> where G::Timestamp: TotalOrder+Lattice+Ord {
    /// Counts the number of occurrences of each element.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::CountTotal;
    ///
    /// ::timely::example(|scope| {
    ///     // report the number of occurrences of each key
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x / 3)
    ///          .count_total();
    /// });
    /// ```
    fn count_total(&self) -> Collection<G, (K, R), isize> {
        self.count_total_core()
    }

    /// Count for general integer differences.
    ///
    /// This method allows `count_total` to produce collections whose difference
    /// type is something other than an `isize` integer, for example perhaps an
    /// `i32`.
    fn count_total_core<R2: Semigroup + From<i8> + 'static>(&self) -> Collection<G, (K, R), R2>;
}

impl<G: Scope, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> CountTotal<G, K, R> for Collection<G, K, R>
where G::Timestamp: TotalOrder+Lattice+Ord {
    fn count_total_core<R2: Semigroup + From<i8> + 'static>(&self) -> Collection<G, (K, R), R2> {
        self.arrange_by_self_named("Arrange: CountTotal")
            .count_total_core()
    }
}

impl<G, K, T1> CountTotal<G, K, T1::Diff> for Arranged<G, T1>
where
    G: Scope<Timestamp=T1::Time>,
    T1: for<'a> TraceReader<Val<'a>=&'a ()>+Clone+'static,
    for<'a> T1::Key<'a>: IntoOwned<'a, Owned = K>,
    for<'a> T1::Diff : Semigroup<T1::DiffGat<'a>>,
    K: ExchangeData,
    T1::Time: TotalOrder,
    T1::Diff: ExchangeData,
{
    fn count_total_core<R2: Semigroup + From<i8> + 'static>(&self) -> Collection<G, (K, T1::Diff), R2> {

        let mut trace = self.trace.clone();
        let mut buffer = Vec::new();

        self.stream.unary_frontier(Pipeline, "CountTotal", move |_,_| {

            // tracks the lower and upper limits of known-complete timestamps.
            let mut lower_limit = timely::progress::frontier::Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());
            let mut upper_limit = timely::progress::frontier::Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());

            move |input, output| {

                use crate::trace::cursor::IntoOwned;
                input.for_each(|capability, batches| {
                    batches.swap(&mut buffer);
                    let mut session = output.session(&capability);
                    for batch in buffer.drain(..) {
                        let mut batch_cursor = batch.cursor();
                        trace.advance_upper(&mut lower_limit);
                        let (mut trace_cursor, trace_storage) = trace.cursor_through(lower_limit.borrow()).unwrap();
                        upper_limit.clone_from(batch.upper());

                        while let Some(key) = batch_cursor.get_key(&batch) {
                            let mut count: Option<T1::Diff> = None;

                            trace_cursor.seek_key(&trace_storage, key);
                            if trace_cursor.get_key(&trace_storage) == Some(key) {
                                trace_cursor.map_times(&trace_storage, |_, diff| {
                                    count.as_mut().map(|c| c.plus_equals(&diff));
                                    if count.is_none() { count = Some(diff.into_owned()); }
                                });
                            }

                            batch_cursor.map_times(&batch, |time, diff| {

                                if let Some(count) = count.as_ref() {
                                    if !count.is_zero() {
                                        session.give(((key.into_owned(), count.clone()), time.into_owned(), R2::from(-1i8)));
                                    }
                                }
                                count.as_mut().map(|c| c.plus_equals(&diff));
                                if count.is_none() { count = Some(diff.into_owned()); }
                                if let Some(count) = count.as_ref() {
                                    if !count.is_zero() {
                                        session.give(((key.into_owned(), count.clone()), time.into_owned(), R2::from(1i8)));
                                    }
                                }
                            });

                            batch_cursor.step_key(&batch);
                        }
                    }
                });

                // tidy up the shared input trace.
                trace.advance_upper(&mut upper_limit);
                trace.set_logical_compaction(upper_limit.borrow());
                trace.set_physical_compaction(upper_limit.borrow());
            }
        })
        .as_collection()
    }
}

#[cfg(test)]
mod tests {
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Probe;
    use timely::progress::frontier::AntichainRef;

    use crate::input::Input;
    use crate::operators::CountTotal;
    use crate::operators::arrange::ArrangeBySelf;
    use crate::trace::TraceReader;

    #[test]
    fn test_count_total() {
        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();
            let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
                let (handle, input) = scope.new_collection();
                let arrange = input.arrange_by_self();
                arrange.stream.probe_with(&mut probe);
                (handle, arrange.trace)
            });

            // ingest some batches
            for _ in 0..10 {
                input.insert(10);
                input.advance_to(input.time() + 1);
                input.flush();
                worker.step_while(|| probe.less_than(input.time()));
            }

            // advance the trace
            trace.set_physical_compaction(AntichainRef::new(&[2]));
            trace.set_logical_compaction(AntichainRef::new(&[2]));

            worker.dataflow::<u32, _, _>(|scope| {
                let arrange = trace.import(scope);
                arrange.count_total();
            });
        });
    }
}
