use std::iter::Peekable;

use ::Data;
use collection::{close_under_lub, LeastUpperBound};
use collection::compact::Compact;

use iterators::merge::{Merge, MergeIterator};
use iterators::coalesce::{Coalesce, CoalesceIterator};

pub trait Trace where for<'a> &'a Self: TraceRef<'a, Self::Key, Self::Index, Self::Value> {

    type Key: Data;
    type Index: LeastUpperBound;
    type Value: Data;

    // type PartKey: Unsigned;                              // the keys are partitioned and likely ordered by this unsigned int
    // fn part(&self, key: &Self::Key) -> Self::PartKey;    // indicates the part for a key


    // TODO : Should probably allow the trace to determine how it receives data. 
    // TODO : Radix sorting and such might live in the trace, rather than in `Arrange`.
    /// Introduces differences in `accumulation` at `time`.
    fn set_difference(&mut self, time: Self::Index, accumulation: Compact<Self::Key, Self::Value>);

    /// Method whose only role is to provide the lifetime to the `Trace` trait.
    fn trace<'a>(&'a self, key: &Self::Key) -> <&'a Self as TraceRef<'a,Self::Key,Self::Index,Self::Value>>::TIterator {
        TraceRef::<'a,Self::Key,Self::Index,Self::Value>::trace(self, key)
    }

    /// Enumerates updates for a specified key and time.
    fn get_difference<'a>(&'a self, key: &Self::Key, time: &Self::Index) 
        -> Option<<&'a Self as TraceRef<'a,Self::Key,Self::Index,Self::Value>>::VIterator> {
        self.trace(key)
            .filter(|x| x.0 == time)
            .map(|x| x.1)
            .next()
    }

    /// Accumulates differences for `key` at times less than or equal to `time`.
    ///
    /// The `&mut self` argument allows the trace to use stashed storage for a merge.
    fn get_collection<'a>(&'a mut self, key: &Self::Key, time: &Self::Index) 
        -> CollectionIterator<<&'a Self as TraceRef<'a,Self::Key,Self::Index,Self::Value>>::VIterator> {
        self.trace(key)
            .into_iter()
            .filter(|x| x.0 <= time)
            .map(|x| x.1)
            .merge()
            .coalesce()
            .peekable()
    }

    // TODO : Make sure the right assumptions are made about contents of stash.
    fn interesting_times<'a>(&'a self, key: &Self::Key, time: &Self::Index, stash: &mut Vec<Self::Index>) {
        // add all times, but filter a bit if possible
        for iter in self.trace(key) {
            let lub = iter.0.least_upper_bound(time);
            if !stash.contains(&lub) {
                stash.push(lub);
            }
        }
        close_under_lub(stash);
    }
}

pub trait TraceRef<'a,K,T:'a,V:'a> {
    type VIterator: Iterator<Item=(&'a V, i32)>+'a;
    type TIterator: Iterator<Item=(&'a T, Self::VIterator)>+'a;
    fn trace(self, key: &K) -> Self::TIterator;
}

pub type CollectionIterator<VIterator> = Peekable<CoalesceIterator<MergeIterator<VIterator>>>;


