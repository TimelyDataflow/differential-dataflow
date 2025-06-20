//! Count the number of occurrences of each element.

use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use crate::lattice::Lattice;
use crate::{IntoOwned, ExchangeData, Collection};
use crate::difference::{IsZero, Semigroup};
use crate::hashable::Hashable;
use crate::collection::AsCollection;
use crate::operators::arrange::{Arranged, ArrangeBySelf};
use crate::trace::{BatchReader, Cursor, TraceReader};

/// Extension trait for the `count` differential dataflow method.
pub trait CountTotal<G: Scope<Timestamp: TotalOrder+Lattice+Ord>, K: ExchangeData, R: Semigroup> {
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

impl<G, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> CountTotal<G, K, R> for Collection<G, K, R>
where
    G: Scope<Timestamp: TotalOrder+Lattice+Ord>,
{
    fn count_total_core<R2: Semigroup + From<i8> + 'static>(&self) -> Collection<G, (K, R), R2> {
        self.arrange_by_self_named("Arrange: CountTotal")
            .count_total_core()
    }
}

impl<G, K, T1> CountTotal<G, K, T1::Diff> for Arranged<G, T1>
where
    G: Scope<Timestamp=T1::Time>,
    T1: for<'a> TraceReader<
        Key<'a>: IntoOwned<'a, Owned = K>,
        Val<'a>=&'a (),
        Time: TotalOrder,
        Diff: ExchangeData+Semigroup<T1::DiffGat<'a>>
    >+Clone+'static,
    K: ExchangeData,
{
    fn count_total_core<R2: Semigroup + From<i8> + 'static>(&self) -> Collection<G, (K, T1::Diff), R2> {

        let mut trace = self.trace.clone();

        self.stream.unary_frontier(Pipeline, "CountTotal", move |_,_| {

            // tracks the lower and upper limit of received batches.
            let mut lower_limit = timely::progress::frontier::Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());
            let mut upper_limit = timely::progress::frontier::Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());

            move |input, output| {

                let mut batch_cursors = Vec::new();
                let mut batch_storage = Vec::new();

                // Downgrade previous upper limit to be current lower limit.
                lower_limit.clear();
                lower_limit.extend(upper_limit.borrow().iter().cloned());

                let mut cap = None;
                input.for_each(|capability, batches| {
                    if cap.is_none() {                          // NB: Assumes batches are in-order
                        cap = Some(capability.retain());
                    }
                    for batch in batches.drain(..) {
                        upper_limit.clone_from(batch.upper());  // NB: Assumes batches are in-order
                        batch_cursors.push(batch.cursor());
                        batch_storage.push(batch);
                    }
                });

                if let Some(capability) = cap {

                    let mut session = output.session(&capability);

                    use crate::trace::cursor::CursorList;
                    let mut batch_cursor = CursorList::new(batch_cursors, &batch_storage);
                    let (mut trace_cursor, trace_storage) = trace.cursor_through(lower_limit.borrow()).unwrap();

                    while let Some(key) = batch_cursor.get_key(&batch_storage) {
                        let mut count: Option<T1::Diff> = None;

                        trace_cursor.seek_key(&trace_storage, key);
                        if trace_cursor.get_key(&trace_storage) == Some(key) {
                            trace_cursor.map_times(&trace_storage, |_, diff| {
                                count.as_mut().map(|c| c.plus_equals(&diff));
                                if count.is_none() { count = Some(diff.into_owned()); }
                            });
                        }

                        batch_cursor.map_times(&batch_storage, |time, diff| {

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

                        batch_cursor.step_key(&batch_storage);
                    }
                }

                // tidy up the shared input trace.
                trace.advance_upper(&mut upper_limit);
                trace.set_logical_compaction(upper_limit.borrow());
                trace.set_physical_compaction(upper_limit.borrow());
            }
        })
        .as_collection()
    }
}
