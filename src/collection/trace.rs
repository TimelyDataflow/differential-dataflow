use std::iter::Peekable;
use std::fmt::Debug;

use collection::{close_under_lub, LeastUpperBound, Lookup};

use iterators::merge::{MergeUsing, MergeUsingIterator};
use iterators::coalesce::{Coalesce, CoalesceIterator};
use collection::compact::Compact;

/// Enumerates the elements of a collection for a given key at a given time.
///
/// A collection iterator is only provided for non-empty sets, so one can call `peek.unwrap()` on
/// the iterator without worrying about panicing.
pub type CollectionIterator<'a, V> = Peekable<CoalesceIterator<MergeUsingIterator<'a, DifferenceIterator<'a, V>>>>;

#[derive(Copy, Clone)]
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
    wgts: u32,
    next: Option<Offset>,
}

struct TimeEntry<T, V> {
    time: T,
    vals: Vec<V>,
    wgts: Vec<(i32, u32)>,
}

/// A collection of values indexed by `key` and `time`.
///
/// #Safety
/// For reasons of borrow checking, it is difficult to merge references using already-allocated
/// memory. The method `get_collection_using` uses its `heap` argument to store references that should
/// remain valid for as long as the `Trace` is valid, but I cannot convince Rust of this fact because
/// only I know that once installed, differences are immutable. Please do not call `get_collection_using`
/// with a `heap` argument that may out-live the `Trace` itself.
pub struct Trace<K, T, V, L: Lookup<K, Offset>> {
    phantom:    ::std::marker::PhantomData<K>,
    links:      Vec<ListEntry>,
    times:      Vec<TimeEntry<T, V>>,
    keys:       L,
    temp:       Vec<T>,
}

impl<K, V, L, T> Trace<K, T, V, L> where K: Ord, V: Ord, L: Lookup<K, Offset>, T: LeastUpperBound+Debug {

    /// Installs a supplied set of keys and values as the differences for `time`.
    pub fn set_difference(&mut self, time: T, accumulation: Compact<K, V>) {

        // extract the relevant fields
        let keys = accumulation.keys;
        let cnts = accumulation.cnts;
        let vals = accumulation.vals;
        let wgts = accumulation.wgts;

        // index of the self.times entry we are about to insert
        let time_index = self.times.len();

        // counters for offsets in vals and wgts
        let mut vals_offset = 0;
        let mut wgts_offset = 0;

        let links_len = self.links.len();

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
                    wgts: wgts_offset,
                    next: None
                });
            }
            // we haven't yet installed next_position, so do that too
            else {
                // add the appropriate entry
                self.links.push(ListEntry {
                    time: time_index as u32,
                    vals: vals_offset,
                    wgts: wgts_offset,
                    next: Some(*prev_position)
                });
                *prev_position = next_position;
            }

            // advance offsets.
            vals_offset += cnt;
            let mut counter = 0;
            while counter < cnt {
                counter += wgts[wgts_offset as usize].1;
                wgts_offset += 1;
            }
            assert_eq!(counter, cnt);
        }

        // println!("set_difference sizes for {:?}:", time);
        // println!("\tkeys: {}", (self.links.len() - links_len) * ::std::mem::size_of::<ListEntry>());
        // println!("\tvals: {}", vals.len() * ::std::mem::size_of::<V>());
        // println!("\twgts: {}", wgts.len() * ::std::mem::size_of::<(i32, u32)>());

        // add the values and weights to the list of timed differences.
        self.times.push(TimeEntry { time: time, vals: vals, wgts: wgts });
    }

    fn get_range<'a>(&'a self, position: Offset) -> DifferenceIterator<'a, V> {

        let time = self.links[position.val()].time as usize;
        let vals_lower = self.links[position.val()].vals as usize;
        let wgts_lower = self.links[position.val()].wgts as usize;

        // upper limit can be read if next link exists and of the same index. else, is last elt.
        let (vals_upper, wgts_upper) = if (position.val() + 1) < self.links.len()
                                        && time == self.links[position.val() + 1].time as usize {

            (self.links[position.val() + 1].vals as usize,
             self.links[position.val() + 1].wgts as usize)
        }
        else {
            (self.times[time].vals.len(),
             self.times[time].wgts.len())
        };

        DifferenceIterator::new(&self.times[time].vals[vals_lower..vals_upper],
                                &self.times[time].wgts[wgts_lower..wgts_upper])
    }

    /// Enumerates the differences for `key` at `time`.
    pub fn get_difference<'a>(&'a self, key: &K, time: &T) -> DifferenceIterator<'a, V> {
        self.trace(key)
            .filter(|x| x.0 == time)
            .map(|x| x.1)
            .next()
            .unwrap_or(DifferenceIterator::new(&[], &[]))
    }

    // /// Accumulates differences for `key` at times less than or equal to `time`.
    // pub fn get_collection<'a>(&'a self, key: &K, time: &T) -> CollectionIterator<'a, V> {
    //     self.trace(key)
    //         .filter(|x| x.0 <= time)
    //         .map(|x| x.1)
    //         .merge()
    //         .coalesce()
    //         .peekable()
    // }

    /// Enumerates the collection for `key` at `time`.
    ///
    /// A collection is defined as the accumulation of all differences at times less or equal to
    /// `time`.
    pub unsafe fn get_collection_using<'a>(&'a self, key: &K, time: &T, heap: &mut Vec<((&(), i32), DifferenceIterator<'static,()>)>) -> CollectionIterator<'a, V> {
        self.trace(key)
            .filter(|x| x.0 <= time)
            .map(|x| x.1)
            .merge_using(::std::mem::transmute(heap))
            .coalesce()
            .peekable()
    }

    // TODO : this could do a better job of returning newly interesting times: those times that are
    // TODO : now in the least upper bound, but were not previously so. The main risk is that the
    // TODO : easy way to do this computes the LUB before and after, but this can be expensive:
    // TODO : the LUB with `index` is often likely to be smaller than the LUB without it.
    /// Lists times that are the least upper bound of `time` and any subset of existing times.
    pub fn interesting_times<'a>(&'a mut self, key: &K, index: T) -> &'a [T] {
        let mut temp = ::std::mem::replace(&mut self.temp, Vec::new());
        temp.clear();
        temp.push(index);
        for (time, _) in self.trace(key) {
            let lub = time.least_upper_bound(&temp[0]);
            if !temp.contains(&lub) {
                temp.push(lub);
            }
        }
        close_under_lub(&mut temp);
        ::std::mem::replace(&mut self.temp, temp);
        &self.temp[..]
    }

    /// Enumerates pairs of time `&T` and differences `DifferenceIterator<V>` for `key`.
    pub fn trace<'a>(&'a self, key: &K) -> TraceIterator<'a, K, T, V, L> {
        TraceIterator {
            trace: self,
            next0: self.keys.get_ref(key).map(|&x|x),
        }
    }
}

impl<K, L: Lookup<K, Offset>, T, V> Trace<K, T, V, L> {
    pub fn new(l: L) -> Trace<K, T, V, L> {
        Trace {
            phantom: ::std::marker::PhantomData,
            links:   Vec::new(),
            times:   Vec::new(),
            keys:    l,
            temp:    Vec::new(),
        }
    }
}


/// Enumerates pairs of time `&T` and `DifferenceIterator<V>` of `(&V, i32)` elements.
#[derive(Clone)]
pub struct TraceIterator<'a, K: 'a, T: 'a, V: 'a, L: Lookup<K, Offset>+'a> {
    trace: &'a Trace<K, T, V, L>,
    next0: Option<Offset>,
}

impl<'a, K, T, V, L> Iterator for TraceIterator<'a, K, T, V, L>
where K:  Ord+'a,
      T: LeastUpperBound+Debug+'a,
      V: Ord+'a,
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
pub struct DifferenceIterator<'a, V: 'a> {
    vals: &'a [V],
    wgts: &'a [(i32,u32)],
    next: usize,            // index of next entry in vals,
    wgt_curr: usize,
    wgt_left: usize,
}

impl<'a, V: 'a> DifferenceIterator<'a, V> {
    fn new(vals: &'a [V], wgts: &'a [(i32, u32)]) -> DifferenceIterator<'a, V> {
        DifferenceIterator {
            vals: vals,
            wgts: wgts,
            next: 0,
            wgt_curr: 0,
            wgt_left: wgts[0].1 as usize,
        }
    }
}

impl<'a, V: 'a> Clone for DifferenceIterator<'a, V> {
    fn clone(&self) -> Self {
        DifferenceIterator {
            vals: self.vals,
            wgts: self.wgts,
            next: self.next,
            wgt_curr: self.wgt_curr,
            wgt_left: self.wgt_left,
        }
    }
}

impl<'a, V: 'a> Iterator for DifferenceIterator<'a, V> {
    type Item = (&'a V, i32);

    #[inline]
    fn next(&mut self) -> Option<(&'a V, i32)> {
        if self.next < self.vals.len() {
            if self.wgt_left == 0 {
                self.wgt_curr += 1;
                self.wgt_left = self.wgts[self.wgt_curr].1 as usize;
            }
            self.wgt_left -= 1;
            self.next += 1;
            Some((&self.vals[self.next - 1], self.wgts[self.wgt_curr].0))
        }
        else {
            None
        }
    }
}
