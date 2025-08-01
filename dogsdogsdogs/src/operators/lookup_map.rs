use std::collections::HashMap;

use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, Collection, AsCollection, Hashable};
use differential_dataflow::difference::{IsZero, Semigroup, Monoid};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

/// Proposes extensions to a stream of prefixes.
///
/// This method takes a stream of prefixes and for each determines a
/// key with `key_selector` and then proposes all pair af the prefix
/// and values associated with the key in `arrangement`.
pub fn lookup_map<G, D, K, R, Tr, F, DOut, ROut, S>(
    prefixes: &Collection<G, D, R>,
    mut arrangement: Arranged<G, Tr>,
    key_selector: F,
    mut output_func: S,
    supplied_key0: K,
    supplied_key1: K,
    supplied_key2: K,
) -> Collection<G, DOut, ROut>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: for<'a> TraceReader<
        KeyOwn = K,
        Diff : Semigroup<Tr::DiffGat<'a>>+Monoid+ExchangeData,
    >+Clone+'static,
    K: Hashable + Ord + 'static,
    F: FnMut(&D, &mut K)+Clone+'static,
    D: ExchangeData,
    R: ExchangeData+Monoid,
    DOut: Clone+'static,
    ROut: Monoid + 'static,
    S: FnMut(&D, &R, Tr::Val<'_>, &Tr::Diff)->(DOut, ROut)+'static,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut propose_trace = Some(arrangement.trace);
    let propose_stream = arrangement.stream;

    let mut stash = HashMap::new();
    let mut logic1 = key_selector.clone();
    let mut logic2 = key_selector.clone();

    let mut key: K = supplied_key0;
    let exchange = Exchange::new(move |update: &(D,G::Timestamp,R)| {
        logic1(&update.0, &mut key);
        key.hashed().into()
    });

    let mut key1: K = supplied_key1;
    let mut key2: K = supplied_key2;

    prefixes.inner.binary_frontier(&propose_stream, exchange, Pipeline, "LookupMap", move |_,_| move |input1, input2, output| {

        // drain the first input, stashing requests.
        input1.for_each(|capability, data| {
            stash.entry(capability.retain())
                 .or_insert(Vec::new())
                 .extend(data.drain(..))
        });

        // Drain input batches; although we do not observe them, we want access to the input
        // to observe the frontier and to drive scheduling.
        input2.for_each(|_, _| { });

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
                    // Key container to stage keys for comparison.
                    let mut key_con = Tr::KeyContainer::with_capacity(1);
                    for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                        if !input2.frontier.less_equal(time) {
                            logic2(prefix, &mut key1);
                            key_con.clear(); key_con.push_own(&key1);
                            cursor.seek_key(&storage, key_con.index(1));
                            if cursor.get_key(&storage) == Some(key_con.index(1)) {
                                while let Some(value) = cursor.get_val(&storage) {
                                    let mut count = Tr::Diff::zero();
                                    cursor.map_times(&storage, |t, d| {
                                        if Tr::owned_time(t).less_equal(time) { count.plus_equals(&d); }
                                    });
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

        // The logical merging frontier depends on both input1 and stash.
        let mut frontier = timely::progress::frontier::Antichain::new();
        for time in input1.frontier().frontier().to_vec() {
            frontier.insert(time);
        }
        for key in stash.keys() {
            frontier.insert(key.time().clone());
        }
        propose_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

        if input1.frontier().is_empty() && stash.is_empty() {
            propose_trace = None;
        }

    }).as_collection()
}
