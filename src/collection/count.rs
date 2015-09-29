//! Like `Count` but with the value type specialized to `()`.

use std::fmt::Debug;

use collection::{close_under_lub, LeastUpperBound, Lookup};
use collection::compact::Compact;

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

struct ListEntry {
    time: u32,
    wgts: i32,
    next: Option<Offset>,
}

pub struct Count<K, T, L> {
    phantom:    ::std::marker::PhantomData<K>,
    links:      Vec<ListEntry>,
    times:      Vec<T>,
    pub keys:   L,
    temp:       Vec<T>,
}

impl<K, L, T> Count<K, T, L> where K: Ord, L: Lookup<K, Offset>, T: LeastUpperBound+Debug {

    /// Installs a supplied set of keys and values as the differences for `time`.
    pub fn set_difference(&mut self, time: T, accumulation: Compact<K, ()>) {

        // extract the relevant fields
        let keys = accumulation.keys;
        let vals = accumulation.vals;

        // index of the self.times entry we are about to insert
        let time_index = self.times.len();

        self.links.reserve(keys.len());

        // for each key and count ...
        for (key, wgt) in keys.into_iter().zip(vals.into_iter().map(|(_v,w)| w)) {

            // prepare a new head cursor, and recover whatever is currently there.
            let next_position = Offset::new(self.links.len());
            let prev_position = self.keys.entry_or_insert(key, || next_position);

            // if we inserted a previously absent key
            if &prev_position.val() == &next_position.val() {
                // add the appropriate entry with no next pointer
                self.links.push(ListEntry {
                    time: time_index as u32,
                    wgts: wgt,
                    next: None
                });
            }
            // we haven't yet installed next_position, so do that too
            else {
                // add the appropriate entry
                self.links.push(ListEntry {
                    time: time_index as u32,
                    wgts: wgt,
                    next: Some(*prev_position)
                });
                *prev_position = next_position;
            }
        }

        self.times.push(time);
    }

    /// Enumerates the differences for `key` at `time`.
    pub fn get_diff(&self, key: &K, time: &T) -> i32 {
        self.trace(key)
            .filter(|x| x.0 == time)
            .map(|x| x.1)
            .next()
            .unwrap_or(0)
    }

    pub fn get_count(&self, key: &K, time: &T) -> i32 {
        let mut sum = 0;
        for wgt in self.trace(key).filter(|x| x.0 <= time).map(|x| x.1) {
            sum += wgt;
        }
        sum
    }

    // TODO : this could do a better job of returning newly interesting times: those times that are
    // TODO : now in the least upper bound, but were not previously so. The main risk is that the
    // TODO : easy way to do this computes the LUB before and after, but this can be expensive:
    // TODO : the LUB with `index` is often likely to be smaller than the LUB without it.
    /// Lists times that are the least upper bound of `time` and any subset of existing times.
    pub fn interesting_times<'a>(&'a mut self, key: &K, index: T) -> &'a [T] {
        // panic!();
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
    pub fn trace<'a>(&'a self, key: &K) -> CountIterator<'a, K, T, L> {
        CountIterator {
            trace: self,
            next0: self.keys.get_ref(key).map(|&x|x),
        }
    }
}

impl<K: Eq, L: Lookup<K, Offset>, T> Count<K, T, L> {
    pub fn new(l: L) -> Count<K, T, L> {
        Count {
            phantom: ::std::marker::PhantomData,
            links:   Vec::new(),
            times:   Vec::new(),
            keys:    l,
            temp:    Vec::new(),
        }
    }
}

/// Enumerates pairs of time `&T` and `i32`.
#[derive(Clone)]
pub struct CountIterator<'a, K: Eq+'a, T: 'a, L: Lookup<K, Offset>+'a> {
    trace: &'a Count<K, T, L>,
    next0: Option<Offset>,
}

impl<'a, K: Eq, T, L> Iterator for CountIterator<'a, K, T, L>
where K:  Ord+'a,
      T: LeastUpperBound+Debug+'a,
      L: Lookup<K, Offset>+'a {
    type Item = (&'a T, i32);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.next0.map(|position| {
            let time_index = self.trace.links[position.val()].time as usize;
            let result = (&self.trace.times[time_index], self.trace.links[position.val()].wgts);
            self.next0 = self.trace.links[position.val()].next;
            result
        })
    }
}
