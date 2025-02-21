//! Additional containers supported in Differential Dataflow.

use std::rc::Rc;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::{BatchContainer, Layout, OffsetList, PreferredContainer, Update};
use differential_dataflow::trace::implementations::containers::BatchIndex;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

use crate::columnation::{ColMerger, ColumnationChunker, TimelyStack};

pub mod columnation;
pub mod huffman_container;
pub mod rhh;

/// A trace implementation backed by columnar storage.
pub type PreferredSpine<K, V, T, R> = Spine<Rc<OrdValBatch<Preferred<K,V,T,R>>>>;
/// A batcher for columnar storage.
pub type PreferredBatcher<K, V, T, R> = MergeBatcher<Vec<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>, ColumnationChunker<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>, ColMerger<(<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R>>;
/// A builder for columnar storage.
pub type PreferredBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<Preferred<K,V,T,R>, TimelyStack<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>>>;

/// An update and layout description based on preferred containers.
pub struct Preferred<K: ?Sized, V: ?Sized, T, D> {
    phantom: std::marker::PhantomData<(Box<K>, Box<V>, T, D)>,
}

impl<K,V,T,R> Update for Preferred<K, V, T, R>
where
    K: ToOwned + ?Sized,
    K::Owned: Ord+Clone+'static,
    V: ToOwned + ?Sized,
    V::Owned: Ord+Clone+'static,
    T: Ord+Clone+Lattice+timely::progress::Timestamp,
    R: Ord+Clone+Semigroup+'static,
{
    type Key = K::Owned;
    type Val = V::Owned;
    type Time = T;
    type Diff = R;
}

impl<K, V, T, D> Layout for Preferred<K, V, T, D>
where
    K: Ord+ToOwned+PreferredContainer + ?Sized,
    K::Owned: Ord+Clone+'static,
    for <'a> <K::Container as BatchContainer>::Borrowed<'a>: BatchIndex<Owned = K::Owned>,
    V: Ord+ToOwned+PreferredContainer + ?Sized,
    V::Owned: Ord+Clone+'static,
    for <'a> <V::Container as BatchContainer>::Borrowed<'a>: BatchIndex<Owned = V::Owned>,
    T: Ord+Clone+Lattice+timely::progress::Timestamp,
    D: Ord+Clone+Semigroup+'static,
{
    type Target = Preferred<K, V, T, D>;
    type KeyContainer = K::Container;
    type ValContainer = V::Container;
    type TimeContainer = Vec<T>;
    type DiffContainer = Vec<D>;
    type OffsetContainer = OffsetList;
}
