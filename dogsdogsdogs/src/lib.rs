#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
#[macro_use]
extern crate serde_derive;
extern crate serde;

use std::rc::Rc;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Mul;

use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;
use timely::dataflow::operators::Partition;
use timely::dataflow::operators::Concatenate;

use timely_sort::Unsigned;

use differential_dataflow::{Data, Collection, AsCollection, Hashable};
use differential_dataflow::operators::Threshold;
use differential_dataflow::difference::{Monoid};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ArrangeByKey};
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::{OrdValBatch, OrdKeyBatch};

pub mod altneu;

/// A type capable of extending a stream of prefixes.
///
/**
    Implementors of `PrefixExtension` provide types and methods for extending a differential dataflow collection,
    via the three methods `count`, `propose`, and `validate`.
**/
pub trait PrefixExtender<G: Scope, R: Monoid+Mul<Output = R>> {
    /// The required type of prefix to extend.
    type Prefix;
    /// The type to be produced as extension.
    type Extension;
    /// Annotates prefixes with the number of extensions the relation would propose.
    fn count(&mut self, &Collection<G, (Self::Prefix, usize, usize), R>, usize) -> Collection<G, (Self::Prefix, usize, usize), R>;
    /// Extends each prefix with corresponding extensions.
    fn propose(&mut self, &Collection<G, Self::Prefix, R>) -> Collection<G, (Self::Prefix, Self::Extension), R>;
    /// Restricts proposed extensions by those the extender would have proposed.
    fn validate(&mut self, &Collection<G, (Self::Prefix, Self::Extension), R>) -> Collection<G, (Self::Prefix, Self::Extension), R>;
}

pub trait ProposeExtensionMethod<G: Scope, P: Data+Ord, R: Monoid+Mul<Output = R>> {
    fn propose_using<PE: PrefixExtender<G, R, Prefix=P>>(&self, extender: &mut PE) -> Collection<G, (P, PE::Extension), R>;
    fn extend<E: Data+Ord>(&self, extenders: &mut [&mut PrefixExtender<G,R,Prefix=P,Extension=E>]) -> Collection<G, (P, E), R>;
}

impl<G: Scope, P: Data+Ord, R: Monoid+Mul<Output = R>> ProposeExtensionMethod<G, P, R> for Collection<G, P, R> {
    fn propose_using<PE: PrefixExtender<G, R, Prefix=P>>(&self, extender: &mut PE) -> Collection<G, (P, PE::Extension), R> {
        extender.propose(self)
    }
    fn extend<E: Data+Ord>(&self, extenders: &mut [&mut PrefixExtender<G,R,Prefix=P,Extension=E>]) -> Collection<G, (P, E), R>
    {

        if extenders.len() == 1 {
            extenders[0].propose(&self.clone())
        }
        else {
            let mut counts = self.map(|p| (p, 1 << 31, 0));
            for (index,extender) in extenders.iter_mut().enumerate() {
                counts = extender.count(&counts, index);
            }

            let parts = counts.inner.partition(extenders.len() as u64, |((p, _, i),t,d)| (i as u64, (p,t,d)));

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(&nominations.as_collection());
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].validate(&extensions);
                }

                results.push(extensions.inner);    // save extensions
            }

            self.scope().concatenate(results).as_collection()
        }
    }
}

pub trait ValidateExtensionMethod<G: Scope, R: Monoid+Mul<Output = R>, P, E> {
    fn validate_using<PE: PrefixExtender<G, R, Prefix=P, Extension=E>>(&self, extender: &mut PE) -> Collection<G, (P, E), R>;
}

impl<G: Scope, R: Monoid+Mul<Output = R>, P, E> ValidateExtensionMethod<G, R, P, E> for Collection<G, (P, E), R> {
    fn validate_using<PE: PrefixExtender<G, R, Prefix=P, Extension=E>>(&self, extender: &mut PE) -> Collection<G, (P, E), R> {
        extender.validate(self)
    }
}

// These are all defined here so that users can be assured a common layout.
type TraceValSpine<K,V,T,R> = Spine<K, V, T, R, Rc<OrdValBatch<K,V,T,R>>>;
type TraceValHandle<K,V,T,R> = TraceAgent<K, V, T, R, TraceValSpine<K,V,T,R>>;
type TraceKeySpine<K,T,R> = Spine<K, (), T, R, Rc<OrdKeyBatch<K,T,R>>>;
type TraceKeyHandle<K,T,R> = TraceAgent<K, (), T, R, TraceKeySpine<K,T,R>>;

pub struct CollectionIndex<K, V, T, R>
where
    K: Data,
    V: Data,
    T: Lattice+Data,
    R: Monoid+Mul<Output = R>,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: TraceKeyHandle<K, T, isize>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: TraceValHandle<K, V, T, R>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: TraceKeyHandle<(K, V), T, R>,
}

impl<K, V, T, R> Clone for CollectionIndex<K, V, T, R>
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
    R: Monoid+Mul<Output = R>,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<K, V, T, R> CollectionIndex<K, V, T, R>
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
    R: Monoid+Mul<Output = R>,
{

    pub fn index<G: Scope<Timestamp = T>>(collection: &Collection<G, (K, V), R>) -> Self {
        // We need to count the number of (k, v) pairs and not rely on the given Monoid R and its binary addition operation.
        // counts and validate can share the base arrangement
        let arranged = collection.arrange_by_self();
        let counts = arranged
            .distinct()
            .map(|(k, _v)| k)
            .arrange_by_self()
            .trace;
        let propose = collection.arrange_by_key().trace;
        let validate = arranged.trace;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
        }
    }
    pub fn extend_using<P, F: Fn(&P)->K>(&self, logic: F) -> CollectionExtender<K, V, T, R, P, F> {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: Rc::new(logic),
        }
    }
}

pub struct CollectionExtender<K, V, T, R, P, F>
where
    K: Data,
    V: Data,
    T: Lattice+Data,
    R: Monoid+Mul<Output = R>,
    F: Fn(&P)->K,
{
    phantom: std::marker::PhantomData<P>,
    indices: CollectionIndex<K, V, T, R>,
    key_selector: Rc<F>,
}

impl<G, K, V, R, P, F> PrefixExtender<G, R> for CollectionExtender<K, V, G::Timestamp, R, P, F>
where
    G: Scope,
    K: Data+Hash,
    V: Data+Hash,
    P: Data,
    G::Timestamp: Lattice+Data,
    R: Monoid+Mul<Output = R>,
    F: Fn(&P)->K+'static,
{

    type Prefix = P;
    type Extension = V;

    fn count(&mut self, prefixes: &Collection<G, (P, usize, usize), R>, index: usize) -> Collection<G, (P, usize, usize), R> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let counts = self.indices.count_trace.import(&prefixes.scope());
        let mut counts_trace = Some(counts.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &((P,usize,usize),G::Timestamp,R)| logic1(&(update.0).0).hashed().as_u64());

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        // TODO: This should be a custom operator with no connection from the second input to the output.
        prefixes.inner.binary_frontier(&counts.stream, exchange, Pipeline, "Count", move |_,_| move |input1, input2, output| {

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

                        prefixes.retain(|ptd| ptd.2 != R::zero());
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

    fn propose(&mut self, prefixes: &Collection<G, P, R>) -> Collection<G, (P, V), R> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let propose = self.indices.propose_trace.import(&prefixes.scope());
        let mut propose_trace = Some(propose.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &(P,G::Timestamp,R)| logic1(&update.0).hashed().as_u64());

        prefixes.inner.binary_frontier(&propose.stream, exchange, Pipeline, "Propose", move |_,_| move |input1, input2, output| {

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
                        prefixes.sort_by(|x,y| logic2(&x.0).cmp(&logic2(&y.0)));

                        let (mut cursor, storage) = trace.cursor();

                        for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                let key = logic2(prefix);
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    while let Some(value) = cursor.get_val(&storage) {
                                        let mut count = R::zero();
                                        cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                        // assert!(count >= 0);
                                        if count > R::zero() {
                                            session.give(((prefix.clone(), value.clone()), time.clone(), diff.clone() * count));
                                        }
                                        cursor.step_val(&storage);
                                    }
                                    cursor.rewind_vals(&storage);
                                }
                                *diff = R::zero();
                            }
                        }

                        prefixes.retain(|ptd| ptd.2 != R::zero());
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

    fn validate(&mut self, extensions: &Collection<G, (P, V), R>) -> Collection<G, (P, V), R> {


        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let validate = self.indices.validate_trace.import(&extensions.scope());
        let mut validate_trace = Some(validate.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &((P,V),G::Timestamp,R)|
            (logic1(&(update.0).0).clone(), ((update.0).1).clone()).hashed().as_u64()
        );

        extensions.inner.binary_frontier(&validate.stream, exchange, Pipeline, "Validate", move |_,_| move |input1, input2, output| {

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
                    if let Some(ref mut trace) = validate_trace {
                        trace.distinguish_since(batch.upper());
                    }
                }
            });

            if let Some(ref mut trace) = validate_trace {

                for (capability, prefixes) in stash.iter_mut() {

                    // defer requests at incomplete times.
                    // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                    if !input2.frontier.less_equal(capability.time()) {

                        let mut session = output.session(capability);

                        // sort requests for in-order cursor traversal. could consolidate?
                        prefixes.sort_by(|x,y| (logic2(&(x.0).0), &((x.0).1)).cmp(&(logic2(&(y.0).0), &((y.0).1))));

                        let (mut cursor, storage) = trace.cursor();

                        for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                let key = (logic2(&prefix.0), (prefix.1).clone());
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    let mut count = R::zero();
                                    cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                    // assert!(count >= 0);
                                    if count > R::zero(){
                                        session.give((prefix.clone(), time.clone(), diff.clone() * count));
                                    }
                                }
                                *diff = R::zero();
                            }
                        }

                        prefixes.retain(|ptd| ptd.2 != R::zero());
                    }
                }
            }

            // drop fully processed capabilities.
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            validate_trace.as_mut().map(|trace| trace.advance_by(&input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                validate_trace = None;
            }

        }).as_collection()

    }

}


