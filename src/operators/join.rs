//! Match pairs of records based on a key.
//!
//! The various `join` implementations require that the units of each collection can be multiplied, and that
//! the multiplication distributes over addition. That is, we will repeatedly evaluate (a + b) * c as (a * c)
//! + (b * c), and if this is not equal to the former term, little is known about the actual output.
use std::fmt::Debug;
use std::ops::Mul;
use std::cmp::Ordering;

use timely::order::PartialOrder;
use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::generic::{Operator, OutputHandle};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::channels::pushers::tee::Tee;

use hashable::Hashable;
use ::{Data, ExchangeData, Collection, AsCollection};
use ::difference::{Semigroup, Abelian};
use lattice::Lattice;
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf};
use trace::{BatchReader, Cursor};
use operators::ValueHistory;

use trace::TraceReader;

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data, V: Data, R: Semigroup> {

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and yields pairs `(key, (val1, val2))`.
    ///
    /// The [`join_map`](Join::join_map) method may be more convenient for non-trivial processing pipelines.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///         let z = scope.new_collection_from(vec![(0, (1, 'a')), (1, (3, 'b'))]).1;
    ///
    ///         x.join(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join<V2, R2>(&self, other: &Collection<G, (K,V2), R2>) -> Collection<G, (K,(V,V2)), <R as Mul<R2>>::Output>
    where
        K: ExchangeData,
        V2: ExchangeData,
        R2: ExchangeData+Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup
    {
        self.join_map(other, |k,v,v2| (k.clone(),(v.clone(),v2.clone())))
    }

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///         x.join_map(&y, |_key, &a, &b| (a,b))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_map<V2, R2, D, L>(&self, other: &Collection<G, (K,V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where K: ExchangeData, V2: ExchangeData, R2: ExchangeData+Semigroup, R: Mul<R2>, <R as Mul<R2>>::Output: Semigroup, D: Data, L: FnMut(&K, &V, &V2)->D+'static;

    /// Matches pairs `(key, val)` and `key` based on `key`, producing the former with frequencies multiplied.
    ///
    /// When the second collection contains frequencies that are either zero or one this is the more traditional
    /// relational semijoin. When the second collection may contain multiplicities, this operation may scale up
    /// the counts of the records in the first input.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![0, 2]).1;
    ///         let z = scope.new_collection_from(vec![(0, 1)]).1;
    ///
    ///         x.semijoin(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn semijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output>
    where K: ExchangeData, R2: ExchangeData+Semigroup, R: Mul<R2>, <R as Mul<R2>>::Output: Semigroup;
    /// Subtracts the semijoin with `other` from `self`.
    ///
    /// In the case that `other` has multiplicities zero or one this results
    /// in a relational antijoin, in which we discard input records whose key
    /// is present in `other`. If the multiplicities could be other than zero
    /// or one, the semantic interpretation of this operator is less clear.
    ///
    /// In almost all cases, you should ensure that `other` has multiplicities
    /// that are zero or one, perhaps by using the `distinct` operator.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![0, 2]).1;
    ///         let z = scope.new_collection_from(vec![(1, 3)]).1;
    ///
    ///         x.antijoin(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn antijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where K: ExchangeData, R2: ExchangeData+Semigroup, R: Mul<R2, Output = R>, R: Abelian;
}

impl<G, K, V, R> Join<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    K: ExchangeData+Hashable,
    V: ExchangeData,
    R: ExchangeData+Semigroup,
    G::Timestamp: Lattice+Ord,
{
    fn join_map<V2: ExchangeData, R2: ExchangeData+Semigroup, D: Data, L>(&self, other: &Collection<G, (K, V2), R2>, mut logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where R: Mul<R2>, <R as Mul<R2>>::Output: Semigroup, L: FnMut(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_key();
        arranged1.join_core(&arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
    }

    fn semijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output>
    where R: Mul<R2>, <R as Mul<R2>>::Output: Semigroup {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_self();
        arranged1.join_core(&arranged2, |k,v,_| Some((k.clone(), v.clone())))
    }

    fn antijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where R: Mul<R2, Output=R>, R: Abelian {
        self.concat(&self.semijoin(other).negate())
    }
}

impl<G, Tr> Join<G, Tr::Key, Tr::Val, Tr::R> for Arranged<G, Tr>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Data+Hashable,
    Tr::Val: Data,
    Tr::R: Semigroup,
    Tr::Batch: BatchReader<Tr::Key,Tr::Val,G::Timestamp,Tr::R>+'static,
    Tr::Cursor: Cursor<Tr::Key,Tr::Val,G::Timestamp,Tr::R>+'static,
{
    fn join_map<V2: ExchangeData, R2: ExchangeData+Semigroup, D: Data, L>(&self, other: &Collection<G, (Tr::Key, V2), R2>, mut logic: L) -> Collection<G, D, <Tr::R as Mul<R2>>::Output>
    where Tr::Key: ExchangeData, Tr::R: Mul<R2>, <Tr::R as Mul<R2>>::Output: Semigroup, L: FnMut(&Tr::Key, &Tr::Val, &V2)->D+'static {
        let arranged2 = other.arrange_by_key();
        self.join_core(&arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
    }

    fn semijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, Tr::Key, R2>) -> Collection<G, (Tr::Key, Tr::Val), <Tr::R as Mul<R2>>::Output>
    where Tr::Key: ExchangeData, Tr::R: Mul<R2>, <Tr::R as Mul<R2>>::Output: Semigroup {
        let arranged2 = other.arrange_by_self();
        self.join_core(&arranged2, |k,v,_| Some((k.clone(), v.clone())))
    }

    fn antijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, Tr::Key, R2>) -> Collection<G, (Tr::Key, Tr::Val), Tr::R>
    where Tr::Key: ExchangeData, Tr::R: Mul<R2, Output=Tr::R>, Tr::R: Abelian {
        self.as_collection(|k,v| (k.clone(), v.clone()))
            .concat(&self.semijoin(other).negate())
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinCore<G: Scope, K: 'static, V: 'static, R: Semigroup> where G::Timestamp: Lattice+Ord {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
    /// which produces something implementing `IntoIterator`, where the output collection will have
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait
    /// contains the implementations for collections.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeByKey;
    /// use differential_dataflow::operators::join::JoinCore;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    /// use differential_dataflow::hashable::OrdWrapper;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0u32, 1), (1, 3)]).1
    ///                      .arrange_by_key();
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1
    ///                      .arrange_by_key();
    ///
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///         x.join_core(&y, |_key, &a, &b| Some((a, b)))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_core<Tr2,I,L> (&self, stream2: &Arranged<G,Tr2>, result: L) -> Collection<G,I::Item,<R as Mul<Tr2::R>>::Output>
    where
        Tr2: TraceReader<Key=K, Time=G::Timestamp>+Clone+'static,
        Tr2::Batch: BatchReader<K, Tr2::Val, G::Timestamp, Tr2::R>+'static,
        Tr2::Cursor: Cursor<K, Tr2::Val, G::Timestamp, Tr2::R>+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        R: Mul<Tr2::R>,
        <R as Mul<Tr2::R>>::Output: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&K,&V,&Tr2::Val)->I+'static,
        ;
}


impl<G, K, V, R> JoinCore<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    K: ExchangeData+Hashable,
    V: ExchangeData,
    R: ExchangeData+Semigroup,
    G::Timestamp: Lattice+Ord,
{
    fn join_core<Tr2,I,L> (&self, stream2: &Arranged<G,Tr2>, result: L) -> Collection<G,I::Item,<R as Mul<Tr2::R>>::Output>
    where
        Tr2: TraceReader<Key=K, Time=G::Timestamp>+Clone+'static,
        Tr2::Batch: BatchReader<K, Tr2::Val, G::Timestamp, Tr2::R>+'static,
        Tr2::Cursor: Cursor<K, Tr2::Val, G::Timestamp, Tr2::R>+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        R: Mul<Tr2::R>,
        <R as Mul<Tr2::R>>::Output: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&K,&V,&Tr2::Val)->I+'static,
    {
        self.arrange_by_key()
            .join_core(stream2, result)
    }
}

impl<G, T1> JoinCore<G, T1::Key, T1::Val, T1::R> for Arranged<G,T1>
    where
        G: Scope,
        G::Timestamp: Lattice+Ord+Debug,
        T1: TraceReader<Time=G::Timestamp>+Clone+'static,
        T1::Key: Ord+Debug+'static,
        T1::Val: Ord+Clone+Debug+'static,
        T1::R: Semigroup,
        T1::Batch: BatchReader<T1::Key,T1::Val,G::Timestamp,T1::R>+'static,
        T1::Cursor: Cursor<T1::Key,T1::Val,G::Timestamp,T1::R>+'static,
{
    fn join_core<Tr2,I,L>(&self, other: &Arranged<G,Tr2>, mut result: L) -> Collection<G,I::Item,<T1::R as Mul<Tr2::R>>::Output>
    where
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2: TraceReader<Key=T1::Key,Time=G::Timestamp>+Clone+'static,
        Tr2::Batch: BatchReader<T1::Key, Tr2::Val, G::Timestamp, Tr2::R>+'static,
        Tr2::Cursor: Cursor<T1::Key, Tr2::Val, G::Timestamp, Tr2::R>+'static,
        Tr2::R: Semigroup,
        T1::R: Mul<Tr2::R>,
        <T1::R as Mul<Tr2::R>>::Output: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&T1::Key,&T1::Val,&Tr2::Val)->I+'static {

        // handles to shared trace data structures.
        let mut trace1 = Some(self.trace.clone());
        let mut trace2 = Some(other.trace.clone());

        // acknowledged frontier for each input.
        use timely::progress::frontier::Antichain;
        let mut acknowledged1: Option<Antichain<G::Timestamp>> = None;
        let mut acknowledged2: Option<Antichain<G::Timestamp>> = None;

        // deferred work of batches from each input.
        let mut todo1 = std::collections::VecDeque::new();
        let mut todo2 = std::collections::VecDeque::new();

        let mut input1_buffer = Vec::new();
        let mut input2_buffer = Vec::new();

        self.stream.binary_frontier(&other.stream, Pipeline, Pipeline, "Join", move |_cap, info| {

            use timely::scheduling::Activator;
            let activations = self.stream.scope().activations().clone();
            let activator = Activator::new(&info.address[..], activations);

            move |input1, input2, output| {

                // The join computation repeatedly accepts batches of updates from each of its inputs.
                //
                // For each accepted batch, it prepares a work-item to join the batch against previously "accepted"
                // updates from its other input. It is important to track which updates have been accepted, through
                // a combination of the input's frontier and the most recently received batch's upper bound, because
                // we use a shared trace and there may be updates present that are in advance of this accepted bound.

                // drain input 1, prepare work.
                input1.for_each(|capability, data| {
                    if let Some(ref mut trace2) = trace2 {
                        let capability = capability.retain();
                        data.swap(&mut input1_buffer);
                        for batch1 in input1_buffer.drain(..) {
                            if !batch1.is_empty() {
                                if let Some(acknowledged2) = &acknowledged2 {
                                    // TODO : cursor_through may be problematic for pre-merged traces.
                                    // A trace should provide the contract that whatever its `distinguish_since` capability,
                                    // it is safe (and reasonable) to await delivery of batches up through that frontier.
                                    // In this case, we should be able to await (not block on) the arrival of these batches.
                                    let (trace2_cursor, trace2_storage) = trace2.cursor_through(acknowledged2.borrow()).unwrap();
                                    let batch1_cursor = batch1.cursor();
                                    todo1.push_back(Deferred::new(trace2_cursor, trace2_storage, batch1_cursor, batch1.clone(), capability.clone(), |r2,r1| (r1.clone()) * (r2.clone())));
                                }
                            }

                            // It would be alarming (incorrect) to receieve a non-empty batch that does not advance the
                            // acknowledged frontier, as each batch must be greater than previous batches, and the input.
                            // Empty batches may be received as information races between frontier forwarding of the trace
                            // and the empty batches themselves (which can be sent as part of trace importing).
                            if acknowledged1.is_none() { acknowledged1 = Some(timely::progress::frontier::Antichain::from_elem(<G::Timestamp>::minimum())); }
                            if let Some(acknowledged1) = &mut acknowledged1 {
                                if !PartialOrder::less_equal(&*acknowledged1, batch1.upper()) {
                                        if !batch1.is_empty() {
                                        panic!("Non-empty batch1 upper not beyond acknowledged frontier: {:?}, {:?}", batch1.upper(), acknowledged1);
                                    }
                                }
                                acknowledged1.clone_from(batch1.upper());
                            }
                        }
                    }
                });

                // drain input 2, prepare work.
                input2.for_each(|capability, data| {
                    if let Some(ref mut trace1) = trace1 {
                        let capability = capability.retain();
                        data.swap(&mut input2_buffer);
                        for batch2 in input2_buffer.drain(..) {
                            if !batch2.is_empty() {
                                if let Some(acknowledged1) = &acknowledged1 {
                                    // TODO : cursor_through may be problematic for pre-merged traces.
                                    // A trace should provide the contract that whatever its `distinguish_since` capability,
                                    // it is safe (and reasonable) to await delivery of batches up through that frontier.
                                    // In this case, we should be able to await (not block on) the arrival of these batches.
                                    let (trace1_cursor, trace1_storage) = trace1.cursor_through(acknowledged1.borrow()).unwrap();
                                    let batch2_cursor = batch2.cursor();
                                    todo2.push_back(Deferred::new(trace1_cursor, trace1_storage, batch2_cursor, batch2.clone(), capability.clone(), |r1,r2| (r1.clone()) * (r2.clone())));
                                }
                            }
                            // It would be alarming (incorrect) to receieve a non-empty batch that does not advance the
                            // acknowledged frontier, as each batch must be greater than previous batches, and the input.
                            // Empty batches may be received as information races between frontier forwarding of the trace
                            // and the empty batches themselves (which can be sent as part of trace importing).
                            if acknowledged2.is_none() { acknowledged2 = Some(timely::progress::frontier::Antichain::from_elem(<G::Timestamp>::minimum())); }
                            if let Some(acknowledged2) = &mut acknowledged2 {
                                if !PartialOrder::less_equal(&*acknowledged2, batch2.upper()) {
                                    if !batch2.is_empty() {
                                        panic!("Non-empty batch2 upper not beyond acknowledged frontier: {:?}, {:?}", batch2.upper(), acknowledged2);
                                    }
                                }
                                acknowledged2.clone_from(batch2.upper());
                            }
                        }
                    }
                });

                // For each of the inputs, we do some amount of work (measured in terms of number
                // of output records produced). This is meant to yield control to allow downstream
                // operators to consume and reduce the output, but it it also means to provide some
                // degree of responsiveness. There is a potential risk here that if we fall behind
                // then the increasing queues hold back physical compaction of the underlying traces
                // which results in unintentionally quadratic processing time (each batch of either
                // input must scan all batches from the other input).

                // perform some amount of outstanding work.
                let mut fuel = 1_000_000;
                while !todo1.is_empty() && fuel > 0 {
                    todo1.front_mut().unwrap().work(output, &mut |k,v2,v1| result(k,v1,v2), &mut fuel);
                    if !todo1.front().unwrap().work_remains() { todo1.pop_front(); }
                }

                // perform some amount of outstanding work.
                let mut fuel = 1_000_000;
                while !todo2.is_empty() && fuel > 0 {
                    todo2.front_mut().unwrap().work(output, &mut |k,v1,v2| result(k,v1,v2), &mut fuel);
                    if !todo2.front().unwrap().work_remains() { todo2.pop_front(); }
                }

                // Re-activate operator if work remains.
                if !todo1.is_empty() || !todo2.is_empty() {
                    activator.activate();
                }

                // shut down or advance trace2.
                if trace2.is_some() && input1.frontier().is_empty() { trace2 = None; }
                if let Some(ref mut trace2) = trace2 {
                    trace2.advance_by(input1.frontier().frontier());
                    // At this point, if we haven't seen any input batches we should establish a frontier anyhow.
                    if acknowledged2.is_none() {
                        acknowledged2 = Some(Antichain::from_elem(<G::Timestamp>::minimum()));
                    }
                    if let Some(acknowledged2) = &mut acknowledged2 {
                        trace2.advance_upper(acknowledged2);
                        trace2.distinguish_since(acknowledged2.borrow());
                    }
                }

                // shut down or advance trace1.
                if trace1.is_some() && input2.frontier().is_empty() { trace1 = None; }
                if let Some(ref mut trace1) = trace1 {
                    trace1.advance_by(input2.frontier().frontier());
                    // At this point, if we haven't seen any input batches we should establish a frontier anyhow.
                    if acknowledged1.is_none() {
                        acknowledged1 = Some(Antichain::from_elem(<G::Timestamp>::minimum()));
                    }
                    if let Some(acknowledged1) = &mut acknowledged1 {
                        trace1.advance_upper(acknowledged1);
                        trace1.distinguish_since(acknowledged1.borrow());
                    }
                }
            }
        })
        .as_collection()
    }
}

/// Deferred join computation.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
/// dataflow system a chance to run operators that can consume and aggregate the data.
struct Deferred<K, V1, V2, T, R1, R2, R3, C1, C2, M, D>
where
    V1: Ord+Clone,
    V2: Ord+Clone,
    T: Timestamp+Lattice+Ord+Debug,
    R1: Semigroup,
    R2: Semigroup,
    C1: Cursor<K, V1, T, R1>,
    C2: Cursor<K, V2, T, R2>,
    M: FnMut(&R1,&R2)->R3,
    D: Ord+Clone+Data,
{
    phant: ::std::marker::PhantomData<(K, V1, V2, R1, R2)>,
    trace: C1,
    trace_storage: C1::Storage,
    batch: C2,
    batch_storage: C2::Storage,
    capability: Capability<T>,
    mult: M,
    done: bool,
    temp: Vec<((D, T), R3)>,
    // thinker: JoinThinker<V1, V2, T, R1, R2>,
}

impl<K, V1, V2, T, R1, R2, R3, C1, C2, M, D> Deferred<K, V1, V2, T, R1, R2, R3, C1, C2, M, D>
where
    K: Ord+Debug+Eq,
    V1: Ord+Clone+Debug,
    V2: Ord+Clone+Debug,
    T: Timestamp+Lattice+Ord+Debug,
    R1: Semigroup,
    R2: Semigroup,
    R3: Semigroup,
    C1: Cursor<K, V1, T, R1>,
    C2: Cursor<K, V2, T, R2>,
    M: FnMut(&R1,&R2)->R3,
    D: Ord+Clone+Data,
{
    fn new(trace: C1, trace_storage: C1::Storage, batch: C2, batch_storage: C2::Storage, capability: Capability<T>, mult: M) -> Self {
        Deferred {
            phant: ::std::marker::PhantomData,
            trace,
            trace_storage,
            batch,
            batch_storage,
            capability,
            mult,
            done: false,
            temp: Vec::new(),
            // thinker: JoinThinker::new(),
        }
    }

    fn work_remains(&self) -> bool {
        !self.done
    }

    /// Process keys until at least `limit` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn work<L, I>(&mut self, output: &mut OutputHandle<T, (D, T, R3), Tee<T, (D, T, R3)>>, logic: &mut L, fuel: &mut usize)
    where I: IntoIterator<Item=D>, L: FnMut(&K, &V1, &V2)->I {

        let meet = self.capability.time();

        let mut effort = 0;
        let mut session = output.session(&self.capability);

        let trace_storage = &self.trace_storage;
        let batch_storage = &self.batch_storage;

        let trace = &mut self.trace;
        let batch = &mut self.batch;
        let mult = &mut self.mult;

        let temp = &mut self.temp;
        // let thinker = &mut self.thinker;
        let mut thinker = JoinThinker::new();

        while batch.key_valid(batch_storage) && trace.key_valid(trace_storage) && effort < *fuel {

            match trace.key(trace_storage).cmp(batch.key(batch_storage)) {
                Ordering::Less => trace.seek_key(trace_storage, batch.key(batch_storage)),
                Ordering::Greater => batch.seek_key(batch_storage, trace.key(trace_storage)),
                Ordering::Equal => {

                    thinker.history1.edits.load(trace, trace_storage, |time| time.join(&meet));
                    thinker.history2.edits.load(batch, batch_storage, |time| time.clone());

                    assert_eq!(temp.len(), 0);

                    // populate `temp` with the results in the best way we know how.
                    thinker.think(|v1,v2,t,r1,r2|
                        for result in logic(batch.key(batch_storage), v1, v2) {
                            temp.push(((result, t.clone()), mult(r1, r2)));
                        }
                    );

                    // TODO: This consolidation is optional, and it may not be very
                    //       helpful. We might try harder to understand whether we
                    //       should do this work here, or downstream at consumers.
                    // TODO: Perhaps `thinker` should have the buffer, do smarter
                    //       consolidation, and then deposit results in `session`.
                    crate::consolidation::consolidate(temp);

                    effort += temp.len();
                    for ((d, t), r) in temp.drain(..) {
                        session.give((d, t, r));
                    }

                    batch.step_key(batch_storage);
                    trace.step_key(trace_storage);

                    thinker.history1.clear();
                    thinker.history2.clear();
                }
            }
        }

        self.done = !batch.key_valid(batch_storage) || !trace.key_valid(trace_storage);

        if effort > *fuel { *fuel = 0; }
        else              { *fuel -= effort; }
    }
}

struct JoinThinker<'a, V1: Ord+Clone+'a, V2: Ord+Clone+'a, T: Lattice+Ord+Clone, R1: Semigroup, R2: Semigroup> {
    pub history1: ValueHistory<'a, V1, T, R1>,
    pub history2: ValueHistory<'a, V2, T, R2>,
}

impl<'a, V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R1: Semigroup, R2: Semigroup> JoinThinker<'a, V1, V2, T, R1, R2>
where V1: Debug, V2: Debug, T: Debug
{
    fn new() -> Self {
        JoinThinker {
            history1: ValueHistory::new(),
            history2: ValueHistory::new(),
        }
    }

    fn think<F: FnMut(&V1,&V2,T,&R1,&R2)>(&mut self, mut results: F) {

        // for reasonably sized edits, do the dead-simple thing.
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            self.history1.edits.map(|v1, t1, d1| {
                self.history2.edits.map(|v2, t2, d2| {
                    results(v1, v2, t1.join(t2), &d1, &d2);
                })
            })
        }
        else {

            let mut replay1 = self.history1.replay();
            let mut replay2 = self.history2.replay();

            // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
            //       in here. If a time is ever repeated, for example, the call will be identical
            //       and accomplish nothing. If only a single record has been added, it may not
            //       be worth the time to collapse (advance, re-sort) the data when a linear scan
            //       is sufficient.

            while !replay1.is_done() && !replay2.is_done() {

                if replay1.time().unwrap().cmp(&replay2.time().unwrap()) == ::std::cmp::Ordering::Less {
                    replay2.advance_buffer_by(replay1.meet().unwrap());
                    for &((ref val2, ref time2), ref diff2) in replay2.buffer().iter() {
                        let (val1, time1, ref diff1) = replay1.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay1.step();
                }
                else {
                    replay1.advance_buffer_by(replay2.meet().unwrap());
                    for &((ref val1, ref time1), ref diff1) in replay1.buffer().iter() {
                        let (val2, time2, ref diff2) = replay2.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay2.step();
                }
            }

            while !replay1.is_done() {
                replay2.advance_buffer_by(replay1.meet().unwrap());
                for &((ref val2, ref time2), ref diff2) in replay2.buffer().iter() {
                    let (val1, time1, ref diff1) = replay1.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                replay1.step();
            }
            while !replay2.is_done() {
                replay1.advance_buffer_by(replay2.meet().unwrap());
                for &((ref val1, ref time1), ref diff1) in replay1.buffer().iter() {
                    let (val2, time2, ref diff2) = replay2.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                replay2.step();
            }
        }
    }
}
