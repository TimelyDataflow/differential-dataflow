//! Worst-case optimal joins as differential dataflows.
//!
//! This crate implements the BiGJoin / Delta-GJ algorithms of Ammar, McSherry, Salihoglu and
//! Joglekar, "Distributed Evaluation of Subgraph Queries Using Worst-case Optimal and Low-Memory
//! Dataflows" (VLDB 2018). Prefixes are extended one attribute at a time: each relation binding
//! the next attribute reports how many extensions it would propose, the smallest proposes them,
//! and the others intersect against their own extensions.
//!
//! # Set semantics
//!
//! The algorithm is stated over *sets*. Its extension indices are set-valued, its intersection
//! step is an existence test, and its count minimization ranks by set cardinality. Accordingly,
//! the differences here are `isize` and the relations are **expected to be sets**: each record
//! present with multiplicity one.
//!
//! This expectation is documented rather than enforced, because a caller whose data are already
//! distinct should not pay for a `distinct()` that does nothing. A caller who is unsure should
//! apply `distinct()` to the collection before handing it to [`CollectionIndex::index`].
//!
//! Feeding a multiset in does not produce the multiset join. `propose` and `validate` multiply
//! the matched record's multiplicity into the output, so multiplicities scale rather than filter,
//! and `count` reports set cardinalities that no longer describe the proposals. Nor is this
//! repairable by adjusting the operators: with relations of arity above two, an extension index
//! is a *projection* of its relation, and a projection's multiplicity is a count of completions
//! rather than the record's own annotation. Carrying annotations correctly through a worst-case
//! optimal join requires indicator projections and a rule that each relation contributes its
//! annotation exactly once, when its last attribute binds — that is InsideOut (Abo Khamis, Ngo,
//! Rudra, "FAQ: Questions Asked Frequently", PODS 2016), a different algorithm with different
//! indices, not a tuning of this one.

use std::hash::Hash;
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};
use timely::dataflow::operators::vec::Partition;
use timely::dataflow::operators::Concatenate;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceAgent;

pub mod operators;

/// Holds back logical compaction so that total-order time comparisons stay meaningful.
///
/// Conventional compaction collapses unequal times to the frontier, which would lose the
/// distinction between "strictly before" and "at the same time" that the delta discipline
/// rests on. See [`crate::operators::half_join`].
pub type FrontierFunc<T> = Rc<dyn Fn(&T, &mut Antichain<T>)>;

/// A type capable of extending a stream of prefixes.
///
/**
    Implementors of `PrefixExtension` provide types and methods for extending a differential dataflow collection,
    via the three methods `count`, `propose`, and `validate`.

    Each prefix travels with a payload time alongside it. The payload starts as the prefix's own
    time, accumulates the times of the records it matches, and is delayed to once the delta region
    is left. The update itself stays at the time it entered on, which is what lets the total-order
    comparison decide exactly once which stage produces each output.
**/
pub trait PrefixExtender<'scope, T: Timestamp> {
    /// The required type of prefix to extend.
    type Prefix;
    /// The type to be produced as extension.
    type Extension;
    /// Annotates prefixes with the number of extensions the relation would propose.
    fn count(&mut self, prefixes: VecCollection<'scope, T, ((Self::Prefix, usize, usize), T), isize>, index: usize) -> VecCollection<'scope, T, ((Self::Prefix, usize, usize), T), isize>;
    /// Extends each prefix with corresponding extensions.
    fn propose(&mut self, prefixes: VecCollection<'scope, T, (Self::Prefix, T), isize>) -> VecCollection<'scope, T, ((Self::Prefix, Self::Extension), T), isize>;
    /// Restricts proposed extensions by those the extender would have proposed.
    fn validate(&mut self, extensions: VecCollection<'scope, T, ((Self::Prefix, Self::Extension), T), isize>) -> VecCollection<'scope, T, ((Self::Prefix, Self::Extension), T), isize>;
}

pub trait ProposeExtensionMethod<'scope, T: Timestamp, P: ExchangeData+Ord> {
    fn propose_using<PE: PrefixExtender<'scope, T, Prefix=P>>(self, extender: &mut PE) -> VecCollection<'scope, T, ((P, PE::Extension), T), isize>;
    fn extend<E: ExchangeData+Ord>(self, extenders: &mut [&mut dyn PrefixExtender<'scope, T, Prefix=P, Extension=E>]) -> VecCollection<'scope, T, ((P, E), T), isize>;
}

impl<'scope, T, P> ProposeExtensionMethod<'scope, T, P> for VecCollection<'scope, T, (P, T), isize>
where
    T: Timestamp,
    P: ExchangeData+Ord,
{
    fn propose_using<PE>(self, extender: &mut PE) -> VecCollection<'scope, T, ((P, PE::Extension), T), isize>
    where
        PE: PrefixExtender<'scope, T, Prefix=P>
    {
        extender.propose(self)
    }
    fn extend<E>(self, extenders: &mut [&mut dyn PrefixExtender<'scope, T, Prefix=P, Extension=E>]) -> VecCollection<'scope, T, ((P, E), T), isize>
    where
        E: ExchangeData+Ord
    {

        if extenders.len() == 1 {
            extenders[0].propose(self)
        }
        else {
            let mut counts = self.clone().map(|(p, payload)| ((p, 1 << 31, 0), payload));
            for (index,extender) in extenders.iter_mut().enumerate() {
                counts = extender.count(counts, index);
            }

            let parts = counts.inner.partition(extenders.len() as u64, |(((p, _, i), payload),t,d)| (i as u64, ((p, payload),t,d)));

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(nominations.as_collection());
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].validate(extensions);
                }

                results.push(extensions.inner);    // save extensions
            }

            self.scope().concatenate(results).as_collection()
        }
    }
}

pub trait ValidateExtensionMethod<'scope, T: Timestamp, P, E> {
    fn validate_using<PE: PrefixExtender<'scope, T, Prefix=P, Extension=E>>(self, extender: &mut PE) -> VecCollection<'scope, T, ((P, E), T), isize>;
}

impl<'scope, T: Timestamp, P, E> ValidateExtensionMethod<'scope, T, P, E> for VecCollection<'scope, T, ((P, E), T), isize> {
    fn validate_using<PE: PrefixExtender<'scope, T, Prefix=P, Extension=E>>(self, extender: &mut PE) -> VecCollection<'scope, T, ((P, E), T), isize> {
        extender.validate(self)
    }
}

// These are all defined here so that users can be assured a common layout.
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::implementations::{KeySpine, ValSpine};
type TraceValHandle<K,V,T> = TraceAgent<ValSpine<K,V,T,isize>>;
type TraceKeyHandle<K,T> = TraceAgent<KeySpine<K,T,isize>>;

/// The three arrangements a relation must present to extend prefixes.
///
/// The arrangements are scope-bound rather than exported traces, so that the operators
/// reading them observe timely's own progress tracking. An imported trace instead reports
/// its frontier in-band, and in a cycle those statements circulate without ever settling.
///
/// The indexed collection is expected to be a set; see the note on set semantics in [`crate`].
pub struct CollectionIndex<'scope, K, V, T>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: Arranged<'scope, TraceKeyHandle<K, T>>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: Arranged<'scope, TraceValHandle<K, V, T>>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: Arranged<'scope, TraceKeyHandle<(K, V), T>>,

    /// Holds back compaction; see [`FrontierFunc`].
    frontier_func: FrontierFunc<T>,
}

impl<'scope, K, V, T> Clone for CollectionIndex<'scope, K, V, T>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
            frontier_func: Rc::clone(&self.frontier_func),
        }
    }
}

impl<'scope, K, V, T> CollectionIndex<'scope, K, V, T>
where
    K: ExchangeData+Hash,
    V: ExchangeData+Hash,
    T: Lattice+ExchangeData+Timestamp,
{

    pub fn index<FF>(collection: VecCollection<'scope, T, (K, V), isize>, frontier_func: FF) -> Self
    where
        FF: Fn(&T, &mut Antichain<T>) + 'static,
    {
        // We need to count the number of (k, v) pairs and not rely on the given Monoid R and its binary addition operation.
        // counts and validate can share the base arrangement
        let arranged = collection.clone().arrange_by_self();
        // TODO: This could/should be arrangement to arrangement, via `reduce_abelian`, but the types are a mouthful at the moment.
        let counts = arranged
            .clone()
            .as_collection(|k,_v| k.clone())
            .distinct()
            .map(|(k, _v)| k)
            .arrange_by_self();
        let propose = collection.arrange_by_key();
        let validate = arranged;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
            frontier_func: Rc::new(frontier_func),
        }
    }
    /// Prepares to extend prefixes by this relation, using `logic` to find the key.
    ///
    /// For a delta query, `strict` follows from the positions of the two relations: a relation
    /// looking up in a *later* one is strict, and cannot see updates concurrent with the delta
    /// it is responding to; looking up in an *earlier* one is non-strict, and can. A relation
    /// never looks up in itself.
    pub fn extend_using<P, F: Fn(&P)->K+Clone>(&self, logic: F, strict: bool) -> CollectionExtender<'scope, K, V, T, P, F> {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: logic,
            strict,
        }
    }
}

pub struct CollectionExtender<'scope, K, V, T, P, F>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
    F: Fn(&P)->K+Clone,
{
    phantom: std::marker::PhantomData<P>,
    indices: CollectionIndex<'scope, K, V, T>,
    key_selector: F,
    strict: bool,
}

impl<'scope, K, V, T, P, F> CollectionExtender<'scope, K, V, T, P, F>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
    F: Fn(&P)->K+Clone,
{
    /// The index's compaction closure, as a plain callable the operators can accept.
    fn frontier_func(&self) -> impl Fn(&T, &mut Antichain<T>) + 'static {
        let frontier_func = Rc::clone(&self.indices.frontier_func);
        move |t: &T, a: &mut Antichain<T>| frontier_func(t, a)
    }
}

impl<'scope, T, K, V, P, F> PrefixExtender<'scope, T> for CollectionExtender<'scope, K, V, T, P, F>
where
    T: Timestamp + Lattice + ExchangeData + Hash,
    K: ExchangeData+Hash,
    V: ExchangeData+Hash,
    P: ExchangeData,
    F: Fn(&P)->K+Clone+'static,
{
    type Prefix = P;
    type Extension = V;

    fn count(&mut self, prefixes: VecCollection<'scope, T, ((P, usize, usize), T), isize>, index: usize) -> VecCollection<'scope, T, ((P, usize, usize), T), isize> {
        operators::count::count(prefixes, self.indices.count_trace.clone(), self.key_selector.clone(), index, self.frontier_func(), self.strict)
    }

    fn propose(&mut self, prefixes: VecCollection<'scope, T, (P, T), isize>) -> VecCollection<'scope, T, ((P, V), T), isize> {
        operators::propose::propose(prefixes, self.indices.propose_trace.clone(), self.key_selector.clone(), self.frontier_func(), self.strict)
    }

    fn validate(&mut self, extensions: VecCollection<'scope, T, ((P, V), T), isize>) -> VecCollection<'scope, T, ((P, V), T), isize> {
        operators::validate::validate(extensions, self.indices.validate_trace.clone(), self.key_selector.clone(), self.frontier_func(), self.strict)
    }
}
