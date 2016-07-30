//! An operator acting on `(Key, ())` collections.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use std::hash::BuildHasherDefault;

use std::collections::HashMap;
use linear_map::LinearMap;
use vec_map::VecMap;
use fnv::FnvHasher;

use ::{Collection, Data, Delta};
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;

use timely_sort::Unsigned;

use lattice::Lattice;
use collection::Lookup;
use collection::count::Offset;
use collection::count::Count as CountTrace;
use collection::compact::Compact;
use collection::trace::Trace;

use operators::arrange::{Arranged, ArrangeBySelf};

/// Extension trait for the `threshold` differential dataflow method
pub trait Threshold<G: Scope, K: Data>
    where G::Timestamp: Lattice {

    /// Groups records by their first field, and applies reduction logic to the associated values.
    ///
    /// It would be nice for this to be generic over possible arrangements of data, but it seems that Rust
    /// ICEs when I try this. I'm not exactly sure how one would specify the arrangement (the type is used
    /// in `logic`, by way of its associated iterator types, but not clearly enough to drive type inference),
    /// but we don't have that problem yet anyhow.
    ///
    /// In any case, it would be great if the same implementation could handle `(K,V)` pairs and `K` elements
    /// treated as `(K, ())` pairs.
    fn threshold<L>(&self, logic: L) -> Collection<G, K> where L: Fn(&K, Delta)->Delta+'static;

    /// Collects distinct elements.
    fn distinct(&self) -> Collection<G, K> {
        self.threshold(|_,_| 1)
    }

    /// Groups records by their first field, when this field implements `Unsigned`.
    ///
    /// This method uses a `Vec<Option<_>>` as its internal storage, allocating
    /// enough memory to directly index based on the first fields. This can be
    /// very useful when these fields are small integers, but it can be expensive
    /// if they are large and sparse.
    fn threshold_u<L>(&self, logic: L) -> Collection<G, K> where K: Unsigned+Default, L: Fn(&K, Delta)->Delta+'static;

    /// Collects distinct elements, when they implement `Unsigned`.
    fn distinct_u(&self) -> Collection<G, K> where K: Unsigned+Default {
        self.threshold_u(|_,_| 1)
    }
}

impl<G: Scope, K: Data+Default> Threshold<G, K> for Collection<G, K> where G::Timestamp: Lattice {
    fn threshold<L>(&self, logic: L) -> Collection<G, K>
        where L: Fn(&K, Delta)->Delta+'static {
            self.arrange_by_self(|k| k.hashed(), |_| { 
                    let x: HashMap<_,_,BuildHasherDefault<FnvHasher>> = HashMap::default();
                    x } )
                .threshold(|k| k.hashed(), |_| { 
                    let x: HashMap<_,_,BuildHasherDefault<FnvHasher>> = HashMap::default();
                    x }, logic)
                .as_collection()
                .map(|(k,_)| k)
    }

    fn threshold_u<L>(&self, logic: L) -> Collection<G, K>
    where K: Unsigned+Default, L: Fn(&K, Delta)->Delta+'static {
        self.arrange_by_self(|k| k.as_u64(), |x| (VecMap::new(), x))
            .threshold(|k| k.as_u64(), |x| (VecMap::new(), x), logic)
            .as_collection()
            .map(|(k,_)| k)
    }
}

/// Extension trait for the `group` operator on `Arrange<_>` data.
pub trait ThresholdArranged<G: Scope, K: Data> {
    /// Groups arranged data using a key hash function, a lookup generator, and user logic.
    ///
    /// This method is used by `group` and `group_u` as defined on `Collection`, and it can
    /// also be called directly by user code that wants access to the arranged output. This
    /// can be helpful when the resulting data are re-used immediately with the same keys.
    fn threshold<U, KH, Look, LookG, Logic>(&self, key_h: KH, look: LookG, logic: Logic)
        -> Arranged<G,CountTrace<K,G::Timestamp,Look>>
    where
        G::Timestamp: Lattice,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, Delta)->Delta+'static;
}

impl<G, K, L> ThresholdArranged<G, K> for Arranged<G, CountTrace<K, G::Timestamp, L>>
where
    G: Scope,
    K: Data,
    L: Lookup<K, Offset>+'static,
    G::Timestamp: Lattice {

    fn threshold<U, KH, Look, LookG, Logic>(&self, key_h: KH, look: LookG, logic: Logic)
        -> Arranged<G, CountTrace<K,G::Timestamp,Look>>
    where
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, Delta)->Delta+'static {

        let peers = self.stream.scope().peers();
        let mut log_peers = 0;
        while (1 << (log_peers + 1)) <= peers {
            log_peers += 1;
        }

        let source = self.trace.clone();
        let result = Rc::new(RefCell::new(CountTrace::new(look(log_peers))));
        let target = result.clone();

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = LinearMap::new();

        // A map from times to a list of keys that need processing at that time.
        let mut to_do = LinearMap::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = self.stream.unary_notify(Pipeline, "CountArranged", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area.
            // only stash the keys, because vals etc are in self.trace
            input.for_each(|time, data| {
                inputs.entry_or_insert(time.time(), || {
                    notificator.notify_at(time);
                    data.drain(..).next().unwrap().0
                });
            });

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.

            notificator.for_each(|capability, _count, notificator| {

                let time = capability.time();

                // 2a. If we received any keys, determine the interesting times for each.
                //     We then enqueue the keys at the corresponding time, for use later.
                if let Some(queue) = inputs.remove_key(&time) {

                    let source = source.borrow();
                    let mut stash = Vec::new();
                    for key in queue {
                        if source.get_difference(&key, &time).is_some() {

                            // determine times at which updates may occur.
                            stash.push(capability.time());
                            source.interesting_times(&key, &time, &mut stash);

                            for new_time in &stash {
                                to_do.entry_or_insert(new_time.clone(), || {
                                         notificator.notify_at(capability.delayed(new_time));
                                         Vec::new()
                                     })
                                     .push(key.clone());
                            }
                            stash.clear();
                        }
                    }
                }

                // 2b. Process any interesting keys at this time.
                if let Some(mut keys) = to_do.remove_key(&time) {

                    // We would like these keys in a particular order.
                    // We also want to de-duplicate them, in case there are dupes.
                    keys.sort_by(|x,y| (key_h(&x), x).cmp(&(key_h(&y), y)));
                    keys.dedup();

                    // accumulations for installation into result
                    let mut accumulation = Compact::new(0,0);

                    // borrow `source` to avoid constant re-borrowing.
                    let source_borrow = source.borrow_mut();

                    for key in keys {

                        let count = source_borrow.get_count(&key, &time);
                        let output = if count > 0 { logic(&key, count) } else { 0 };
                        let current = target.borrow().get_count(&key, &time);

                        if output != current {
                            let mut compact = accumulation.session();
                            // session.give((key.clone(), output - current));
                            compact.push((), output - current);
                            compact.done(key);
                        }

                    }

                    if accumulation.vals.len() > 0 {
                        output.session(&capability).give((accumulation.keys.clone(), accumulation.cnts.clone(), accumulation.vals.clone()));
                        target.borrow_mut().set_difference(time, accumulation);
                    }
                }
            });
        });

        Arranged { stream: stream, trace: result }
    }
}
