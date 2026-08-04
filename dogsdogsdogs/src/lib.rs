use std::hash::Hash;
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};
use timely::dataflow::operators::vec::{Map, Partition};
use timely::dataflow::operators::Concatenate;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection};
use differential_dataflow::difference::{Monoid, Multiply};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceAgent;

pub mod altneu;
pub mod calculus;
pub mod operators;

/// A type capable of extending a stream of prefixes.
///
/**
    Implementors of `PrefixExtension` provide types and methods for extending a differential dataflow collection,
    via the three methods `count`, `propose`, and `validate`.
**/
pub trait PrefixExtender<'scope, T: Timestamp, R: Monoid+Multiply<Output = R>> {
    /// The required type of prefix to extend.
    type Prefix;
    /// The type to be produced as extension.
    type Extension;
    /// Annotates prefixes with the number of extensions the relation would propose.
    ///
    /// Prefixes carry a *join* time alongside the payload; `count` passes it through untouched,
    /// as a routing decision contributes no record to the output tuple.
    fn count(&mut self, prefixes: VecCollection<'scope, T, ((Self::Prefix, usize, usize), T), R>, index: usize) -> VecCollection<'scope, T, ((Self::Prefix, usize, usize), T), R>;
    /// Extends each prefix with corresponding extensions, joining the matched times in.
    fn propose(&mut self, prefixes: VecCollection<'scope, T, (Self::Prefix, T), R>) -> VecCollection<'scope, T, ((Self::Prefix, Self::Extension), T), R>;
    /// Restricts proposed extensions by those the extender would have proposed, joining the
    /// matched times in — a validating atom contributes to the output time exactly as a
    /// proposing one does.
    fn validate(&mut self, extensions: VecCollection<'scope, T, ((Self::Prefix, Self::Extension), T), R>) -> VecCollection<'scope, T, ((Self::Prefix, Self::Extension), T), R>;
}

pub trait ProposeExtensionMethod<'scope, T: Timestamp, P: ExchangeData+Ord, R: Monoid+Multiply<Output = R>> {
    fn propose_using<PE: PrefixExtender<'scope, T, R, Prefix=P>>(self, extender: &mut PE) -> VecCollection<'scope, T, (P, PE::Extension), R>;
    fn extend<E: ExchangeData+Ord>(self, extenders: &mut [&mut dyn PrefixExtender<'scope, T,R,Prefix=P,Extension=E>]) -> VecCollection<'scope, T, (P, E), R>;
}

impl<'scope, T, P, R> ProposeExtensionMethod<'scope, T, P, R> for VecCollection<'scope, T, P, R>
where
    T: Timestamp,
    P: ExchangeData+Ord,
    R: Monoid+Multiply<Output = R>+'static,
{
    fn propose_using<PE>(self, extender: &mut PE) -> VecCollection<'scope, T, (P, PE::Extension), R>
    where
        PE: PrefixExtender<'scope, T, R, Prefix=P>
    {
        let seeded = self.inner.map(|(p, t, r)| ((p, t.clone()), t, r)).as_collection();
        extender.propose(seeded)
            .inner.map(|((data, carried), _order, diff)| (data, carried, diff)).as_collection()
    }
    fn extend<E>(self, extenders: &mut [&mut dyn PrefixExtender<'scope, T,R,Prefix=P,Extension=E>]) -> VecCollection<'scope, T, (P, E), R>
    where
        E: ExchangeData+Ord
    {

        // Entering the delta region: each update carries its own time as the initial join
        // time, while its dataflow timestamp stays the order time every cut compares against.
        let seeded = self.inner.map(|(p, t, r)| ((p, t.clone()), t, r)).as_collection();

        let extended = if extenders.len() == 1 {
            extenders[0].propose(seeded)
        }
        else {
            let seeded_scope = seeded.scope();
            let mut counts = seeded.map(|(p, carried)| ((p, 1 << 31, 0), carried));
            for (index,extender) in extenders.iter_mut().enumerate() {
                counts = extender.count(counts, index);
            }

            let parts = counts.inner.partition(extenders.len() as u64, |(((p, _, i), carried),t,d)| (i as u64, ((p, carried),t,d)));

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(nominations.as_collection());
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].validate(extensions);
                }

                results.push(extensions.inner);    // save extensions
            }

            seeded_scope.concatenate(results).as_collection()
        };

        // Leaving the delta region: the carried join time becomes the update's own time, and
        // the order time — scaffolding for the cuts — is discarded.
        extended.inner.map(|((data, carried), _order, diff)| (data, carried, diff)).as_collection()
    }
}

pub trait ValidateExtensionMethod<'scope, T: Timestamp, R: Monoid+Multiply<Output = R>, P, E> {
    fn validate_using<PE: PrefixExtender<'scope, T, R, Prefix=P, Extension=E>>(self, extender: &mut PE) -> VecCollection<'scope, T, (P, E), R>;
}

impl<'scope, T: Timestamp, R: Monoid+Multiply<Output = R>, P, E> ValidateExtensionMethod<'scope, T, R, P, E> for VecCollection<'scope, T, (P, E), R> {
    fn validate_using<PE: PrefixExtender<'scope, T, R, Prefix=P, Extension=E>>(self, extender: &mut PE) -> VecCollection<'scope, T, (P, E), R> {
        let seeded = self.inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();
        extender.validate(seeded)
            .inner.map(|((data, carried), _order, diff)| (data, carried, diff)).as_collection()
    }
}

// These are all defined here so that users can be assured a common layout.
use differential_dataflow::trace::implementations::{KeySpine, ValSpine};
use differential_dataflow::operators::arrange::Arranged;
type ArrangedVal<'scope, K,V,T,R> = Arranged<'scope, TraceAgent<ValSpine<K,V,T,R>>>;
type ArrangedKey<'scope, K,T,R> = Arranged<'scope, TraceAgent<KeySpine<K,T,R>>>;

/// The three arrangements an atom is read through, held as *local* arrangements.
///
/// # Why arrangements and not trace handles
///
/// The obvious alternative is to store `TraceAgent` handles, which carry no scope lifetime and
/// so make an index portable anywhere. Getting an operator input back out of a handle means
/// [`TraceAgent::import`], and re-importing an arrangement produced by the *same* dataflow is
/// an antipattern: the imported stream carries in-line progress statements rather than
/// participating in the scope's progress tracking. Outside a loop that costs only a redundant
/// replay operator per use. Inside a recursive scope it is fatal — progress has no whole-scope
/// view, so it advances capabilities by repeated frontier advancement and simply counts the
/// timestamp upward, never concluding the loop is done. A three-atom join built this way spins
/// at full CPU rather than converging.
///
/// Holding `Arranged` instead means every extender shares one stream and one trace by an
/// ordinary dataflow edge. `Arranged` is `Clone`, so sharing is free, and the redundant replay
/// operators disappear along with the hazard. The cost is the `'scope` lifetime: an index may
/// only be used in the scope that built it, which is what every caller already does.
///
/// Genuinely external arrangements — captured and replayed from another dataflow — would want
/// a second variant here, as `import`'s legitimate use. Crossing into a nested scope is
/// `enter`'s job, not `import`'s. Neither is modelled: this is local-only.
pub struct CollectionIndex<'scope, K, V, T, R>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
{
    /// An arrangement of `(K, ())`, used to count extensions for each prefix.
    count: ArrangedKey<'scope, K, T, isize>,

    /// An arrangement of `(K, V)`, used to propose extensions for each prefix.
    propose: ArrangedVal<'scope, K, V, T, R>,

    /// An arrangement of `((K, V), ())`, used to validate proposed extensions.
    validate: ArrangedKey<'scope, (K, V), T, R>,
}

impl<'scope, K, V, T, R> Clone for CollectionIndex<'scope, K, V, T, R>
where
    K: ExchangeData+Hash,
    V: ExchangeData+Hash,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count: self.count.clone(),
            propose: self.propose.clone(),
            validate: self.validate.clone(),
        }
    }
}

impl<'scope, K, V, T, R> CollectionIndex<'scope, K, V, T, R>
where
    K: ExchangeData+Hash,
    V: ExchangeData+Hash,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
{

    pub fn index(collection: VecCollection<'scope, T, (K, V), R>) -> Self {
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

        CollectionIndex { count: counts, propose, validate: arranged }
    }
    /// An extender reading this index at `cut`.
    ///
    /// The cut and its compaction bound are fixed here, on the extender, rather than at each
    /// of `count` / `propose` / `validate`. That is deliberate: all three must read the *same*
    /// cut relation. If `count` sizes an atom over a different cut than `propose` enumerates,
    /// a prefix can be routed to the atom that offers the fewest extensions and then find
    /// none — and since every other atom only validates, the extension is lost with nothing to
    /// recover it. Binding the cut to the atom makes that unrepresentable.
    pub fn extend_using<P, F, FF>(&self, logic: F, cut: operators::lookup::Cut, frontier_func: FF) -> CollectionExtender<'scope, K, V, T, R, P, F>
    where
        F: Fn(&P)->K+Clone,
        FF: Fn(&T, &mut Antichain<T>) + 'static,
    {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: logic,
            cut,
            frontier_func: Rc::new(frontier_func),
        }
    }
}

pub struct CollectionExtender<'scope, K, V, T, R, P, F>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
    F: Fn(&P)->K+Clone,
{
    phantom: std::marker::PhantomData<P>,
    indices: CollectionIndex<'scope, K, V, T, R>,
    key_selector: F,
    /// The cut this atom is read at, shared by `count`, `propose`, and `validate`.
    cut: operators::lookup::Cut,
    /// The compaction bound the cut requires; see [`operators::lookup::identity_frontier`].
    frontier_func: Rc<dyn Fn(&T, &mut Antichain<T>)>,
}

impl<'scope, T, K, V, R, P, F> PrefixExtender<'scope, T, R> for CollectionExtender<'scope, K, V, T, R, P, F>
where
    T: Timestamp + Lattice + ExchangeData + Hash,
    K: ExchangeData+Hash+Default,
    V: ExchangeData+Hash+Default,
    P: ExchangeData,
    R: Monoid+Multiply<Output = R>+ExchangeData,
    F: Fn(&P)->K+Clone+'static,
{
    type Prefix = P;
    type Extension = V;

    fn count(&mut self, prefixes: VecCollection<'scope, T, ((P, usize, usize), T), R>, index: usize) -> VecCollection<'scope, T, ((P, usize, usize), T), R> {
        let counts = self.indices.count.clone();
        let ff = Rc::clone(&self.frontier_func);
        operators::count::count(prefixes, counts, self.cut, move |t, a| ff(t, a), self.key_selector.clone(), index)
    }

    fn propose(&mut self, prefixes: VecCollection<'scope, T, (P, T), R>) -> VecCollection<'scope, T, ((P, V), T), R> {
        let propose = self.indices.propose.clone();
        let ff = Rc::clone(&self.frontier_func);
        operators::propose::propose(prefixes, propose, self.cut, move |t, a| ff(t, a), self.key_selector.clone())
    }

    fn validate(&mut self, extensions: VecCollection<'scope, T, ((P, V), T), R>) -> VecCollection<'scope, T, ((P, V), T), R> {
        let validate = self.indices.validate.clone();
        let ff = Rc::clone(&self.frontier_func);
        operators::validate::validate(extensions, validate, self.cut, move |t, a| ff(t, a), self.key_selector.clone())
    }
}
