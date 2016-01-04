use std::rc::Rc;
use std::ops::DerefMut;
use std::default::Default;

use itertools::Itertools;

use ::{Collection, Data};
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;

use radix_sort::{RadixSorter, Unsigned};

use collection::{LeastUpperBound, Lookup};
use collection::count::{Count, Offset};
use collection::compact::Compact;
use collection::trace::Trace;

/// Extension trait for the `group` differential dataflow method
pub trait Threshold<G: Scope, D: Data+Default+'static>
    where G::Timestamp: LeastUpperBound {
    fn threshold<
        F: Fn(&D, i32)->i32+'static,
        U: Unsigned+Default+'static,
        KeyH: Fn(&D)->U+'static,
        Look:  Lookup<D, Offset>+'static,
        LookG: Fn(u64)->Look+'static,
        >(&self, key_h: KeyH, look: LookG, function: F) -> Collection<G, D>;
}

impl<G: Scope, D: Data+Default+'static> Threshold<G, D> for Collection<G, D> where G::Timestamp: LeastUpperBound {
    fn threshold<
        F: Fn(&D, i32)->i32+'static,
        U: Unsigned+Default+'static,
        KeyH: Fn(&D)->U+'static,
        Look:  Lookup<D, Offset>+'static,
        LookG: Fn(u64)->Look+'static,
        >(&self, key_h: KeyH, look: LookG, function: F) -> Collection<G, D> {

        let mut source = Count::new(look(0));
        let mut result = Count::new(look(0));

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = Vec::new();

        // A map from times to a list of keys that need processing at that time.
        let mut to_do = Vec::new();

        let mut sorter = RadixSorter::new();

        let key1 = Rc::new(key_h);
        let key2 = key1.clone();

        Collection::new(self.inner.unary_notify(Exchange::new(move |x: &(D, i32)| key1(&x.0).as_u64()), "Count", vec![], move |input, output, notificator| {

            while let Some((time, data)) = input.next() {
                notificator.notify_at(&time);
                inputs.entry_or_insert(time.clone(), || Vec::new())
                      .push(::std::mem::replace(data.deref_mut(), Vec::new()));
            }

            while let Some((index, _count)) = notificator.next() {
                // 2a. fetch any data associated with this time.
                if let Some(mut queue) = inputs.remove_key(&index) {
                    // sort things; radix if many, .sort_by if few.
                    let compact = if queue.len() > 1 {
                        for element in queue.into_iter() {
                            sorter.extend(element.into_iter().map(|(d,w)| ((d,()),w)), &|x| key2(&(x.0).0));
                        }
                        let mut sorted = sorter.finish(&|x| key2(&(x.0).0));
                        let result = Compact::from_radix(&mut sorted, &|k| key2(k));
                        sorted.truncate(256);
                        sorter.recycle(sorted);
                        result
                    }
                    else {
                        let mut vec = queue.pop().unwrap();
                        let mut vec = vec.drain_temp().map(|(d,w)| ((d,()),w)).collect::<Vec<_>>();
                        vec.sort_by(|x,y| key2(&(x.0).0).cmp(&key2((&(y.0).0))));
                        Compact::from_radix(&mut vec![vec], &|k| key2(k))
                    };
                    if let Some(compact) = compact {
                        let mut stash = Vec::new();
                        for key in &compact.keys {
                            stash.push(index.clone());
                            source.interesting_times(key, &index, &mut stash);
                            for time in &stash {
                                let mut queue = to_do.entry_or_insert((*time).clone(), || { notificator.notify_at(time); Vec::new() });
                                queue.push((*key).clone());
                            }
                            stash.clear();
                        }

                        source.set_difference(index.clone(), compact);
                    }
                }

                // we may need to produce output at index
                let mut session = output.session(&index);

                // 2b. We must now determine for each interesting key at this time, how does the
                // currently reported output match up with what we need as output. Should we send
                // more output differences, and what are they?

                // Much of this logic used to hide in `OperatorTrace` and `CollectionTrace`.
                // They are now gone and simpler, respectively.
                if let Some(mut keys) = to_do.remove_key(&index) {

                    // we would like these keys in a particular order.
                    keys.sort_by(|x,y| (key2(x), x).cmp(&(key2(y), y)));
                    keys.dedup();

                    // accumulations for installation into result
                    let mut accumulation = Compact::new(0,0);

                    for key in keys {

                        let count = source.get_count(&key, &index);
                        let output = if count > 0 { function(&key, count) } else { 0 };
                        let current = result.get_count(&key, &index);

                        if output != current {
                            let mut compact = accumulation.session();
                            session.give((key.clone(), output - current));
                            compact.push((), output - current);
                            compact.done(key);
                        }
                    }

                    if accumulation.vals.len() > 0 {
                        result.set_difference(index.clone(), accumulation);
                    }
                }
            }
        }))
    }
}
