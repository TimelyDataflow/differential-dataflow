use std::collections::HashMap;

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

/// Proposes extensions to a stream of prefixes.
///
/// This method takes a stream of prefixes and for each determines a
/// key with `key_selector` and then proposes all pair af the prefix
/// and values associated with the key in `arrangement`.
pub fn lookup_map<G, D, R, Tr, F, DOut, ROut, S>(
    prefixes: &Collection<G, D, R>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
    output_func: S,
) -> Collection<G, DOut, ROut>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+Default,
    Tr::Val: Clone,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::R: Monoid+ExchangeData,
    F: Fn(&D, &mut Tr::Key)+Clone+'static,
    D: ExchangeData,
    R: ExchangeData+Monoid,
    DOut: Clone+'static,
    ROut: Monoid,
    S: Fn(&D, &R, &Tr::Val, &Tr::R)->(DOut, ROut)+'static,
{
    let propose_stream = arrangement.stream;
    let mut propose_trace = Some(arrangement.trace);

    let mut stash = HashMap::new();
    let logic1 = key_selector.clone();
    let logic2 = key_selector.clone();

    let mut buffer1 = Vec::new();
    let mut buffer2 = Vec::new();

    let mut key: Tr::Key = Default::default();
    let exchange = Exchange::new(move |update: &(D,G::Timestamp,R)| {
        logic1(&update.0, &mut key);
        key.hashed().as_u64()
    });

    let mut key1: Tr::Key = Default::default();
    let mut key2: Tr::Key = Default::default();
    prefixes.inner.binary_frontier(&propose_stream, exchange, Pipeline, "Propose", move |_,_| move |input1, input2, output| {

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
                if let Some(ref mut trace) = propose_trace {
                    trace.distinguish_since(batch.upper());
                }
            }
        });

        if let Some(ref mut trace) = propose_trace {

            for (capability, prefixes) in stash.iter_mut() {

                // defer requests at incomplete times.
                // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                if !input2.frontier.less_equal(capability.time()) {

                    let mut session = output.session(capability);

                    // sort requests for in-order cursor traversal. could consolidate?
                    prefixes.sort_by(|x,y| {
                        logic2(&x.0, &mut key1);
                        logic2(&y.0, &mut key2);
                        key1.cmp(&key2)
                    });

                    let (mut cursor, storage) = trace.cursor();

                    for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                        if !input2.frontier.less_equal(time) {
                            logic2(prefix, &mut key1);
                            // let key = logic2(prefix);
                            cursor.seek_key(&storage, &key1);
                            if cursor.get_key(&storage) == Some(&key1) {
                                while let Some(value) = cursor.get_val(&storage) {
                                    let mut count = Tr::R::zero();
                                    cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                    if !count.is_zero() {
                                        let (dout, rout) = output_func(prefix, diff, value, &count);
                                        if !rout.is_zero() {
                                            session.give((dout, time.clone(), rout));
                                        }
                                    }
                                    cursor.step_val(&storage);
                                }
                                cursor.rewind_vals(&storage);
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
        propose_trace.as_mut().map(|trace| trace.advance_by(&input1.frontier().frontier()));

        if input1.frontier().is_empty() && stash.is_empty() {
            propose_trace = None;
        }

    }).as_collection()
}