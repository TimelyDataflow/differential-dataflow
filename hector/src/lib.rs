extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;

use std::rc::Rc;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::DerefMut;

use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;

use timely_sort::Unsigned;

use differential_dataflow::{Data, Collection, AsCollection, Hashable};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::difference::Diff;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ArrangeByKey};
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};
use differential_dataflow::trace::implementations::spine::Spine;
use differential_dataflow::trace::implementations::ord::{OrdValBatch, OrdKeyBatch};

/// A type capable of extending a stream of prefixes.
///
/** 
    Implementors of `PrefixExtension` provide types and methods for extending a differential dataflow collection,
    via the three methods `count`, `propose`, and `validate`.
**/ 
pub trait PrefixExtender<G: Scope> {

    /// The required type of prefix to extend.
    type Prefix;

    /// The type to be produced as extension.
    type Extension;

    /// Annotates prefixes with the number of extensions the relation would propose.
    fn count(&mut self, &Collection<G, (Self::Prefix, usize, usize)>, usize) -> Collection<G, (Self::Prefix, usize, usize)>;

    /// Extends each prefix with corresponding extensions.
    fn propose(&mut self, &Collection<G, Self::Prefix>) -> Collection<G, (Self::Prefix, Self::Extension)>;

    /// Restricts proposed extensions by those the extender would have proposed.
    fn validate(&mut self, &Collection<G, (Self::Prefix, Self::Extension)>) -> Collection<G, (Self::Prefix, Self::Extension)>;
}

// These are all defined here so that users can be assured a common layout.
type TraceValBatch<K,V,T,R> = OrdValBatch<K, V, T, R>;
type TraceValSpine<K,V,T,R> = Spine<K, V, T, R, Rc<TraceValBatch<K,V,T,R>>>;
type TraceValHandle<K,V,T,R> = TraceAgent<K, V, T, R, TraceValSpine<K,V,T,R>>;

type TraceKeyBatch<K,T,R> = OrdKeyBatch<K, T, R>;
type TraceKeySpine<K,T,R> = Spine<K, (), T, R, Rc<TraceKeyBatch<K,T,R>>>;
type TraceKeyHandle<K,T,R> = TraceAgent<K, (), T, R, TraceKeySpine<K,T,R>>;


pub struct CollectionIndex<K, V, T> 
where
    K: Data,
    V: Data,
    T: Lattice+Data,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: TraceKeyHandle<K, T, isize>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: TraceValHandle<K, V, T, isize>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: TraceKeyHandle<(K, V), T, isize>,
}

impl<K, V, T> Clone for CollectionIndex<K, V, T> 
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<K, V, T> CollectionIndex<K, V, T> 
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
{
    pub fn from<G: Scope<Timestamp=T>>(collection: &Collection<G, (K, V), isize>) -> Self {
        let counts = collection.map(|(k,_v)| k).arrange_by_self().trace;
        let propose = collection.arrange_by_key().trace;
        let validate = collection.arrange_by_self().trace;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
        }
    }

    pub fn extend_using<P, F: Fn(&P)->&K>(&self, logic: F) -> CollectionExtender<K, V, T, P, F> {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: Rc::new(logic),
        }
    }
}

pub struct CollectionExtender<K, V, T, P, F> 
where
    K: Data,
    V: Data,
    T: Lattice+Data,
    F: Fn(&P)->&K,
{
    phantom: std::marker::PhantomData<P>,
    indices: CollectionIndex<K, V, T>,
    key_selector: Rc<F>,
}

impl<G, K, V, P, F> PrefixExtender<G> for CollectionExtender<K, V, G::Timestamp, P, F> 
where
    G: Scope,
    K: Data+Hash,
    V: Data+Hash,
    P: Data,
    G::Timestamp: Lattice+Data,
    F: Fn(&P)->&K+'static,
{

    type Prefix = P;
    type Extension = V;

    fn count(&mut self, prefixes: &Collection<G, (P, usize, usize)>, index: usize) -> Collection<G, (P, usize, usize)> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let counts = self.indices.count_trace.import(&prefixes.scope());
        let mut counts_trace = Some(counts.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &((P,usize,usize),G::Timestamp,isize)| logic1(&(update.0).0).hashed().as_u64());

        prefixes.inner.binary_frontier(&counts.stream, exchange, Pipeline, "Count", move |_| move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data|
                stash.entry(capability)
                     .or_insert(MergeSorter::new(self.key_selector.clone()))
                     .push(data.deref_mut())
             );

            // advance the `distinguish_since` frontier to allow all merges.
            input2.for_each(|_, batches| 
                for batch in batches.drain(..) {
                    if let Some(ref mut trace) = counts_trace {
                        trace.distinguish_since(batch.item.upper());
                    }
                }
            );

            if let Some(ref mut trace) = counts_trace {

                for (capability, prefixes) in stash.iter_mut() {

                    // defer requests at incomplete times.
                    // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                    if !input2.frontier.less_equal(capability.time()) {

                        let mut session = output.session(capability);

                        // sort requests for in-order cursor traversal. could consolidate?
                        // prefixes.sort_by(|x,y| logic2(&(x.0).0).cmp(logic2(&(y.0).0)));

                        let mut sorted = Vec::new();
                        prefixes.finish_into(&mut sorted);

                        let (mut cursor, storage) = trace.cursor();

                        for &mut ((ref prefix, old_count, old_index), ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                cursor.seek_key(&storage, logic2(prefix));
                                if cursor.get_key(&storage) == Some(logic2(prefix)) {
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
                                *diff = 0;
                            }
                        }

                        prefixes.retain(|ptd| ptd.2 != 0);
                    }
                }
            }

            // drop fully processed capabilities.
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            counts_trace.as_mut().map(|trace| trace.advance_by(input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                counts_trace = None;
            }

        }).as_collection()
    }

    fn propose(&mut self, prefixes: &Collection<G, P>) -> Collection<G, (P, V)> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let propose = self.indices.propose_trace.import(&prefixes.scope());
        let mut propose_trace = Some(propose.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &(P,G::Timestamp,isize)| logic1(&update.0).hashed().as_u64());

        prefixes.inner.binary_frontier(&propose.stream, exchange, Pipeline, "Propose", move |_| move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| 
                stash.entry(capability)
                     .or_insert(Vec::new())
                     .extend(data.drain(..))
             );

            // advance the `distinguish_since` frontier to allow all merges.
            input2.for_each(|_, batches| 
                for batch in batches.drain(..) {
                    if let Some(ref mut trace) = propose_trace {
                        trace.distinguish_since(batch.item.upper());
                    }
                }
            );

            if let Some(ref mut trace) = propose_trace {

                for (capability, prefixes) in stash.iter_mut() {

                    // defer requests at incomplete times.
                    // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                    if !input2.frontier.less_equal(capability.time()) {

                        let mut session = output.session(capability);

                        // sort requests for in-order cursor traversal. could consolidate?
                        prefixes.sort_by(|x,y| logic2(&x.0).cmp(logic2(&y.0)));

                        let (mut cursor, storage) = trace.cursor();

                        for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                cursor.seek_key(&storage, logic2(prefix));
                                if cursor.get_key(&storage) == Some(logic2(prefix)) {
                                    while let Some(value) = cursor.get_val(&storage) {
                                        let mut count = 0;
                                        cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                        // assert!(count >= 0);
                                        if count > 0 {
                                            session.give(((prefix.clone(), value.clone()), time.clone(), diff.clone()));
                                        }
                                        cursor.step_val(&storage);
                                    }
                                    cursor.rewind_vals(&storage);
                                }
                                *diff = 0;
                            }
                        }

                        prefixes.retain(|ptd| ptd.2 != 0);
                    }
                }
            }

            // drop fully processed capabilities.
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            propose_trace.as_mut().map(|trace| trace.advance_by(input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                propose_trace = None;
            }

        }).as_collection()
    }

    fn validate(&mut self, extensions: &Collection<G, (P, V)>) -> Collection<G, (P, V)> {


        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let validate = self.indices.validate_trace.import(&extensions.scope());
        let mut validate_trace = Some(validate.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &((P,V),G::Timestamp,isize)| 
            (logic1(&(update.0).0).clone(), ((update.0).1).clone()).hashed().as_u64()
        );

        extensions.inner.binary_frontier(&validate.stream, exchange, Pipeline, "Validate", move |_| move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| 
                stash.entry(capability)
                     .or_insert(Vec::new())
                     .extend(data.drain(..))
             );

            // advance the `distinguish_since` frontier to allow all merges.
            input2.for_each(|_, batches| 
                for batch in batches.drain(..) {
                    if let Some(ref mut trace) = validate_trace {
                        trace.distinguish_since(batch.item.upper());
                    }
                }
            );

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
                                let key = (logic2(&prefix.0).clone(), (prefix.1).clone());
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    let mut count = 0;
                                    cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                    // assert!(count >= 0);
                                    if count > 0 {
                                        session.give((prefix.clone(), time.clone(), diff.clone()));
                                    }
                                }
                                *diff = 0;
                            }
                        }

                        prefixes.retain(|ptd| ptd.2 != 0);
                    }
                }
            }

            // drop fully processed capabilities.
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            validate_trace.as_mut().map(|trace| trace.advance_by(input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                validate_trace = None;
            }

        }).as_collection()

    }

} 



use std::slice::{from_raw_parts};

pub struct VecQueue<T> {
    list: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> VecQueue<T> {
    #[inline(always)]
    pub fn new() -> Self { VecQueue::from(Vec::new()) }
    #[inline(always)]
    pub fn pop(&mut self) -> T {
        debug_assert!(self.head < self.tail);
        self.head += 1;
        unsafe { ::std::ptr::read(self.list.as_mut_ptr().offset(((self.head as isize) - 1) )) }
    }
    #[inline(always)]
    pub fn peek(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.head) }
    }
    #[inline(always)]
    pub fn _peek_tail(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.tail-1) }
    }
    #[inline(always)]
    pub fn _slice(&self) -> &[T] {
        debug_assert!(self.head < self.tail);
        unsafe { from_raw_parts(self.list.get_unchecked(self.head), self.tail - self.head) }
    }
    #[inline(always)]
    pub fn from(mut list: Vec<T>) -> Self {
        let tail = list.len();
        unsafe { list.set_len(0); }
        VecQueue {
            list: list,
            head: 0,
            tail: tail,
        }
    }
    // could leak, if self.head != self.tail.
    #[inline(always)]
    pub fn done(self) -> Vec<T> {
        debug_assert!(self.head == self.tail);
        self.list
    }
    #[inline(always)]
    pub fn len(&self) -> usize { self.tail - self.head }
    #[inline(always)]
    pub fn is_empty(&self) -> bool { self.head == self.tail }
}

#[inline(always)]
unsafe fn push_unchecked<T>(vec: &mut Vec<T>, element: T) {
    debug_assert!(vec.len() < vec.capacity());
    let len = vec.len();
    ::std::ptr::write(vec.get_unchecked_mut(len), element);
    vec.set_len(len + 1);
}

pub struct MergeSorter<D, T: Ord, R: Diff, K: Ord, F: Fn(&D)->&K+'static> {
    queue: Vec<Vec<Vec<(D, T, R)>>>,    // each power-of-two length list of allocations.
    stash: Vec<Vec<(D, T, R)>>,
    logic: Rc<F>,
}

impl<D, T: Ord, R: Diff, K: Ord, F: Fn(&D)->&K+'static> MergeSorter<D, T, R, K, F> {

    #[inline]
    pub fn new(logic: Rc<F>) -> Self { 
        MergeSorter { 
            queue: Vec::new(), 
            stash: Vec::new(),
            logic: logic,
        }
    }

    #[inline]
    pub fn empty(&mut self) -> Vec<(D, T, R)> {
        self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024))
    }

    #[inline(never)]
    pub fn _sort(&mut self, list: &mut Vec<Vec<(D, T, R)>>) {
        for mut batch in list.drain(..) {
            self.push(&mut batch);
        }
        self.finish_into(list);
    }

    #[inline]
    pub fn push(&mut self, batch: &mut Vec<(D, T, R)>) {

        let mut batch = if self.stash.len() > 2 {
            ::std::mem::replace(batch, self.stash.pop().unwrap())
        }
        else {
            ::std::mem::replace(batch, Vec::new())
        };
        
        if batch.len() > 0 {
            batch.sort_unstable_by(|x,y| ((self.logic)(&x.0), &x.1).cmp(&((self.logic)(&y.0), &y.1)));
            // for index in 1 .. batch.len() {
            //     if batch[index].0 == batch[index - 1].0 && batch[index].1 == batch[index - 1].1 {
            //         batch[index].2 = batch[index].2 + batch[index - 1].2;
            //         batch[index - 1].2 = R::zero();
            //     }
            // }
            // batch.retain(|x| !x.2.is_zero());

            self.queue.push(vec![batch]);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want 
    // to break it down to be so.
    pub fn push_list(&mut self, list: Vec<Vec<(D, T, R)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);            
        }
        self.queue.push(list);
    }
    
    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<Vec<(D, T, R)>>) {
        while self.queue.len() > 1 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }

        if let Some(mut last) = self.queue.pop() {
            ::std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<Vec<(D, T, R)>>, list2: Vec<Vec<(D, T, R)>>) -> Vec<Vec<(D, T, R)>> {
        
        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = Vec::with_capacity(1024);

        let mut list1 = VecQueue::from(list1);
        let mut list2 = VecQueue::from(list2);

        let mut head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() }; 
        let mut head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() }; 

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && head1.len() > 0 && head2.len() > 0 {
                
                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    ((self.logic)(&x.0), &x.1) < ((self.logic)(&y.0), &y.1)
                };

                if cmp {
                    unsafe { push_unchecked(&mut result, head1.pop()); } 
                }
                else {
                    unsafe { push_unchecked(&mut result, head2.pop()); } 
                }
            }
            
            if result.capacity() == result.len() {
                output.push(result);
                result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024)); 
            }

            if head1.is_empty() { 
                let done1 = head1.done(); 
                if done1.capacity() == 1024 { self.stash.push(done1); }
                head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() }; 
            }
            if head2.is_empty() { 
                let done2 = head2.done(); 
                if done2.capacity() == 1024 { self.stash.push(done2); }
                head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() }; 
            }
        }

        if result.len() > 0 { output.push(result); }
        else if result.capacity() > 0 { self.stash.push(result); }

        if !head1.is_empty() {
            let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            for _ in 0 .. head1.len() { result.push(head1.pop()); }
            output.push(result);
        }
        while !list1.is_empty() { 
            output.push(list1.pop()); 
        }

        if !head2.is_empty() {
            let mut result = self.stash.pop().unwrap_or(Vec::with_capacity(1024));
            for _ in 0 .. head2.len() { result.push(head2.pop()); }
            output.push(result);
        }
        while !list2.is_empty() { 
            output.push(list2.pop()); 
        }

        output
    }
}