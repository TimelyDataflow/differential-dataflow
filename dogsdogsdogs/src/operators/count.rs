use std::rc::Rc;
use std::collections::HashMap;
use std::ops::Mul;

use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;

use timely_sort::Unsigned;

use differential_dataflow::{ExchangeData, Collection, AsCollection, Hashable};
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};

/// Reports a number of extensions to a stream of prefixes.
///
/// This method takes as input a stream of `(prefix, count, index)` triples.
/// For each triple, it extracts a key using `key_selector`, and finds the
/// associated count in `arrangement`. If the found count is less than `count`,
/// the `count` and `index` fields are overwritten with their new values.
pub fn count<G, Tr, R, F, P>(
    prefixes: &Collection<G, (P, usize, usize), R>,
    arrangement: Arranged<G, Tr>,
    key_selector: Rc<F>,
    index: usize,
) -> Collection<G, (P, usize, usize), R>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Val=(), Time=G::Timestamp, R=isize>+Clone+'static,
    Tr::Key: Ord+Hashable,
    Tr::Batch: BatchReader<Tr::Key, (), Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, (), Tr::Time, Tr::R>,
    R: Monoid+Mul<Output = R>+ExchangeData,
    F: Fn(&P)->Tr::Key+'static,
    P: ExchangeData,
{
    // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
    // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
    // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
    // that. We *could* organize the input differences by key and save some time, or we could skip that.

    let counts_stream = arrangement.stream;
    let mut counts_trace = Some(arrangement.trace);

    let mut stash = HashMap::new();
    let logic1 = key_selector.clone();
    let logic2 = key_selector.clone();

    let exchange = Exchange::new(move |update: &((P,usize,usize),G::Timestamp,R)| logic1(&(update.0).0).hashed().as_u64());

    let mut buffer1 = Vec::new();
    let mut buffer2 = Vec::new();

    // TODO: This should be a custom operator with no connection from the second input to the output.
    prefixes.inner.binary_frontier(&counts_stream, exchange, Pipeline, "Count", move |_,_| move |input1, input2, output| {

        // drain the first input, stashing requests.
        input1.for_each(|capability, data| {
            data.swap(&mut buffer1);
            stash.entry(capability.retain())
                 .or_insert(Vec::new())
                 .extend(buffer1.drain(..))
        });

        // advance the `distinguish_since` frontier to allow all merges.
        input2.for_each(|_, batches| {
            batches.swap(&mut buffer2);
            for batch in buffer2.drain(..) {
                if let Some(ref mut trace) = counts_trace {
                    trace.distinguish_since(batch.upper());
                }
            }
        });

        if let Some(ref mut trace) = counts_trace {

            for (capability, prefixes) in stash.iter_mut() {

                // defer requests at incomplete times.
                // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                if !input2.frontier.less_equal(capability.time()) {

                    let mut session = output.session(capability);

                    // sort requests for in-order cursor traversal. could consolidate?
                    prefixes.sort_by(|x,y| logic2(&(x.0).0).cmp(&logic2(&(y.0).0)));

                    let (mut cursor, storage) = trace.cursor();

                    for &mut ((ref prefix, old_count, old_index), ref time, ref mut diff) in prefixes.iter_mut() {
                        if !input2.frontier.less_equal(time) {
                            let key = logic2(prefix);
                            cursor.seek_key(&storage, &key);
                            if cursor.get_key(&storage) == Some(&key) {
                                let mut count = 0;
                                cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                // assert!(count >= 0);
                                let count = count as usize;
                                if count > 0 {
                                    if count < old_count {
                                        session.give(((prefix.clone(), count, index), time.clone(), diff.clone()));
                                    }
                                    else {
                                        session.give(((prefix.clone(), old_count, old_index), time.clone(), diff.clone()));
                                    }
                                }
                            }
                            *diff = R::zero();
                        }
                    }

                    prefixes.retain(|ptd| !ptd.2.is_zero());
                }
            }
        }

        // drop fully processed capabilities.
        stash.retain(|_,prefixes| !prefixes.is_empty());

        // advance the consolidation frontier (TODO: wierd lexicographic times!)
        counts_trace.as_mut().map(|trace| trace.advance_by(&input1.frontier().frontier()));

        if input1.frontier().is_empty() && stash.is_empty() {
            counts_trace = None;
        }

    }).as_collection()
}
