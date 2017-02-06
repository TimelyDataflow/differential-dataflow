//! A generic storage interface for differential collection traces.

use std::iter::Peekable;

use ::Data;
use lattice::{close_under_join, Lattice};
use collection::compact::Compact;

use iterators::merge::{Merge, MergeIterator};
use iterators::coalesce::{Coalesce, CoalesceIterator};

/// Types that do not vary with lifetime '
pub trait Trace {

    /// The data-parallel key.
    type Key: Data;
    /// Timestamp for changes to the collection.
    type Index: Lattice;
    /// Values associated with each key.
    type Value: Data;

    // TODO : Should probably allow the trace to determine how it receives data. 
    // TODO : Radix sorting and such might live in the trace, rather than in `Arrange`.
    /// Introduces differences in `accumulation` at `time`.
    fn set_difference(&mut self, time: Self::Index, accumulation: Compact<Self::Key, Self::Value>);

    /// Iterates over differences for a specified key and time.
    fn get_difference<'a>(&'a self, key: &Self::Key, time: &Self::Index) -> Option<Self::VIterator> 
        where Self: TraceReference<'a> {
        self.trace(key)
            .filter(|x| x.0 == time)
            .map(|x| x.1)
            .next()
    }

    /// Accumulates differences for `key` at times less than or equal to `time`.
    ///
    /// The `&mut self` argument allows the trace to use stashed storage for a merge.
    /// This is currently not done, because it appears to require unsafe (the storage
    /// type involves a `&'a`, which cannot outlive this method).
    fn get_collection<'a>(&'a mut self, key: &Self::Key, time: &Self::Index) -> CollectionIterator<Self::VIterator> 
        where Self: TraceReference<'a> {
        self.trace(key)
            .into_iter()
            .filter(|x| x.0 <= time)
            .map(|x| x.1)
            .merge()
            .coalesce()
            .peekable()
    }

    /// Populates `stash` with times for key, closes under least upper bound.
    fn interesting_times<'a>(&'a self, key: &Self::Key, time: &Self::Index, stash: &mut Vec<Self::Index>)
        where Self: TraceReference<'a> {
        // add all times, but filter a bit if possible
        for iter in self.trace(key) {
            if !iter.0.le(time) {
                let lub = iter.0.join(time);
                if !stash.contains(&lub) {
                    stash.push(lub);
                }
            }
        }
        close_under_join(stash);
    }
}

/// A collection trace, corresponding to quadruples `(Key, Index, Value, Delta)`.
pub trait TraceReference<'a> : Trace where 
    <Self as Trace>::Key: 'a,
    <Self as Trace>::Index: 'a,
    <Self as Trace>::Value: 'a,
    {

    /// Iterator over references to values.
    type VIterator: Iterator<Item=(&'a Self::Value, i32)>+Clone+'a;
    /// Iterator over times and `VIterator`s.
    type TIterator: Iterator<Item=(&'a Self::Index, Self::VIterator)>+Clone+'a;
    /// Iterates over differences associated with the key.
    fn trace(&'a self, key: &Self::Key) -> Self::TIterator;

    // type PartKey: Unsigned;                              // the keys are partitioned and likely ordered by this unsigned int
    // fn part(&self, key: &Self::Key) -> Self::PartKey;    // indicates the part for a key

    // /// Iterates over differences for a specified key and time.
    // fn get_difference(&'a self, key: &Self::Key, time: &Self::Index) 
    //     -> Option<Self::VIterator> {
    //     self.trace(key)
    //         .filter(|x| x.0 == time)
    //         .map(|x| x.1)
    //         .next()
    // }

    // /// Accumulates differences for `key` at times less than or equal to `time`.
    // ///
    // /// The `&mut self` argument allows the trace to use stashed storage for a merge.
    // /// This is currently not done, because it appears to require unsafe (the storage
    // /// type involves a `&'a`, which cannot outlive this method).
    // fn get_collection(&'a mut self, key: &Self::Key, time: &Self::Index) 
    //     -> CollectionIterator<Self::VIterator> {
    //     self.trace(key)
    //         .into_iter()
    //         .filter(|x| x.0 <= time)
    //         .map(|x| x.1)
    //         .merge()
    //         .coalesce()
    //         .peekable()
    // }

    // /// Populates `stash` with times for key, closes under least upper bound.
    // fn interesting_times(&'a self, key: &Self::Key, time: &Self::Index, stash: &mut Vec<Self::Index>) {
    //     // add all times, but filter a bit if possible
    //     for iter in self.trace(key) {
    //         if !iter.0.le(time) {
    //             let lub = iter.0.join(time);
    //             if !stash.contains(&lub) {
    //                 stash.push(lub);
    //             }
    //         }
    //     }
    //     close_under_join(stash);
    // }
}

/// An iterator over weighted values.
pub type CollectionIterator<VIterator> = Peekable<CoalesceIterator<MergeIterator<VIterator>>>;


