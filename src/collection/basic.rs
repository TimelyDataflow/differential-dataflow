use ::Data;
use collection::{Trace, TraceRef};
use collection::{LeastUpperBound, Lookup};
use collection::compact::Compact;

/// A collection of values indexed by `key` and `time`.
///
/// A `BasicTrace` maintains a mapping from keys of type `K` to offsets into a vector of
/// `ListEntry` structs, which are themselves linked-list entries of pairs of a time `T` 
/// and an offset into a `TimeEntry` struct, which wraps a `Vec<(V, i32)>`.
///
/// This trie structure is easy to update as new times arrive: the new data form a new 
/// `TimeEntry`, and any involved keys have elements added to their linked lists.
///
/// At the same time, its performance can degrade after large numbers of updates as the 
/// data associated with a given key becomes more and more diffuse. The trace also has 
/// no support for compaction.
pub struct BasicTrace<K, T, V, L> {
    phantom: ::std::marker::PhantomData<K>,
    links: Vec<ListEntry>,
    times: Vec<TimeEntry<T, V>>,
    keys: L,
}

impl<K,V,L,T> Trace for BasicTrace<K, T, V, L> 
    where 
        K: Data, 
        V: Data, 
        L: Lookup<K, Offset>+'static, 
        T: LeastUpperBound+'static {
    type Key = K;
    type Index = T;
    type Value = V;

    #[inline(never)]
    fn set_difference(&mut self, time: T, accumulation: Compact<K, V>) {

        // extract the relevant fields
        let keys = accumulation.keys;
        let cnts = accumulation.cnts;
        let vals = accumulation.vals;

        // index of the self.times entry we are about to insert
        let time_index = self.times.len();

        // counters for offsets in vals and wgts
        let mut vals_offset = 0;

        self.links.reserve(keys.len());

        // for each key and count ...
        for (key, cnt) in keys.into_iter().zip(cnts.into_iter()) {

            // prepare a new head cursor, and recover whatever is currently there.
            let next_position = Offset::new(self.links.len());
            let prev_position = self.keys.entry_or_insert(key, || next_position);

            // if we inserted a previously absent key
            if &prev_position.val() == &next_position.val() {
                // add the appropriate entry with no next pointer
                self.links.push(ListEntry {
                    time: time_index as u32,
                    vals: vals_offset,
                    next: None
                });
            }
            // we haven't yet installed next_position, so do that too
            else {
                // add the appropriate entry
                self.links.push(ListEntry {
                    time: time_index as u32,
                    vals: vals_offset,
                    next: Some(*prev_position)
                });
                *prev_position = next_position;
            }

            // advance offsets.
            vals_offset += cnt;
        }

        // add the values and weights to the list of timed differences.
        self.times.push(TimeEntry { time: time, vals: vals });
    }
}

impl<'a,K,V,L,T> TraceRef<'a,K,T,V> for &'a BasicTrace<K,T,V,L> where K: Data+'a, V: Data+'a, L: Lookup<K, Offset>+'a, T: LeastUpperBound+'a {
    type VIterator = DifferenceIterator<'a, V>;
    type TIterator = TraceIterator<'a,K,T,V,L>;
    fn trace(self, key: &K) -> Self::TIterator {
        TraceIterator {
            trace: self,
            next0: self.keys.get_ref(key).map(|&x|x),
        }
    }   
}

/// Enumerates the elements of a collection for a given key at a given time.
///
/// A collection iterator is only provided for non-empty sets, so one can call `peek.unwrap()` on
/// the iterator without worrying about panicing.
// pub type CollectionIterator<'a, V> = Peekable<CoalesceIterator<MergeUsingIterator<'a, DifferenceIterator<'a, V>>>>;

#[derive(Copy, Clone, Debug)]
pub struct Offset {
    dataz: u32,
}

impl Offset {
    #[inline(always)]
    fn new(offset: usize) -> Offset {
        assert!(offset < ((!0u32) as usize)); // note strict inequality
        Offset { dataz: (!0u32) - offset as u32 }
    }
    #[inline(always)]
    fn val(&self) -> usize { ((!0u32) - self.dataz) as usize }
}

/// A map from keys to time-indexed collection differences.
///
/// A `Trace` is morally equivalent to a `Map<K, Vec<(T, Vec<(V,i32)>)>`.
/// It uses an implementor `L` of the `Lookup<K, Offset>` trait to map keys to an `Offset`, a
/// position in member `self.links` of the head of the linked list for the key.
///
/// The entries in `self.links` form a linked list, where each element contains an index into
/// `self.times` indicating a time, and an offset in the associated vector in `self.times[index]`.
/// Finally, the `self.links` entry contains an optional `Offset` to the next element in the list.
/// Entries are added to `self.links` sequentially, so that one can determine not only where some
/// differences begin, but also where they end, by looking at the next entry in `self.lists`.
///
/// Elements of `self.times` correspond to distinct logical times, and the full set of differences
/// received at each.

struct ListEntry {
    time: u32,
    vals: u32,
    next: Option<Offset>,
}

struct TimeEntry<T, V> {
    time: T,
    vals: Vec<(V, i32)>,
}


impl<K, V, L, T> BasicTrace<K, T, V, L> where K: Ord, V: Ord, L: Lookup<K, Offset>, T: LeastUpperBound {
    #[inline]
    fn get_range<'a>(&'a self, position: Offset) -> DifferenceIterator<'a, V> {

        let time = self.links[position.val()].time as usize;
        let vals_lower = self.links[position.val()].vals as usize;

        // upper limit can be read if next link exists and of the same index. else, is last elt.
        let vals_upper = if (position.val() + 1) < self.links.len()
                                        && time == self.links[position.val() + 1].time as usize {

            self.links[position.val() + 1].vals as usize
        }
        else {
            self.times[time].vals.len()
        };

        DifferenceIterator::new(&self.times[time].vals[vals_lower..vals_upper])
    }
}

impl<K: Eq, L: Lookup<K, Offset>, T, V> BasicTrace<K, T, V, L> {
    pub fn new(l: L) -> BasicTrace<K, T, V, L> {
        // println!("allocating trace");
        BasicTrace {
            phantom: ::std::marker::PhantomData,
            links:   Vec::new(),
            times:   Vec::new(),
            keys:    l,
        }
    }
}


/// Enumerates pairs of time `&T` and `DifferenceIterator<V>` of `(&V, i32)` elements.
#[derive(Clone)]
pub struct TraceIterator<'a, K: Eq+'a, T: 'a, V: 'a, L: Lookup<K, Offset>+'a> {
    trace: &'a BasicTrace<K, T, V, L>,
    next0: Option<Offset>,
}

impl<'a, K, T, V, L> Iterator for TraceIterator<'a, K, T, V, L>
where K: Data,
      T: LeastUpperBound+'a,
      V: Data,
      L: Lookup<K, Offset>+'a {
    type Item = (&'a T, DifferenceIterator<'a, V>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.next0.map(|position| {
            let time_index = self.trace.links[position.val()].time as usize;
            let result = (&self.trace.times[time_index].time, self.trace.get_range(position));
            self.next0 = self.trace.links[position.val()].next;
            result
        })
    }
}

/// Enumerates `(&V,i32)` elements of a difference.
///
/// Morally equivalent to a `&[(V,i32)]` slice iterator, except it returns a `(&V,i32)` rather than a `&(V,i32)`.
/// This is important for consolidate and merge.
pub struct DifferenceIterator<'a, V: 'a> {
    vals: &'a [(V,i32)],
    next: usize,            // index of next entry in vals,
}

impl<'a, V: 'a> DifferenceIterator<'a, V> {
    fn new(vals: &'a [(V, i32)]) -> DifferenceIterator<'a, V> {
        DifferenceIterator {
            vals: vals,
            next: 0,
        }
    }
}

impl<'a, V: 'a> Clone for DifferenceIterator<'a, V> {
    fn clone(&self) -> Self {
        DifferenceIterator {
            vals: self.vals,
            next: self.next,
        }
    }
}

impl<'a, V: 'a> Iterator for DifferenceIterator<'a, V> {
    type Item = (&'a V, i32);

    #[inline]
    fn next(&mut self) -> Option<(&'a V, i32)> {
        if self.next < self.vals.len() {
            self.next += 1;
            Some((&self.vals[self.next - 1].0, self.vals[self.next - 1].1))
        }
        else {
            None
        }
    }
}