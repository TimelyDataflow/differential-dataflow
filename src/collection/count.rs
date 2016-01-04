//! Like `Count` but with the value type specialized to `()`.

use ::Data;
use collection::{LeastUpperBound, Lookup};
use collection::compact::Compact;
use collection::trace::{Trace, TraceRef};

impl<K, L, T> Trace for Count<K, T, L> where K: Data+Ord+'static, L: Lookup<K, Offset>+'static, T: LeastUpperBound+'static {
    type Key = K;
    type Index = T;
    type Value = ();
    
    /// Installs a supplied set of keys and values as the differences for `time`.
    fn set_difference(&mut self, time: T, accumulation: Compact<K, ()>) {

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
}

impl<'a,K,L,T> TraceRef<'a,K,T,()> for &'a Count<K,T,L> where K: Ord+'a, L: Lookup<K, Offset>+'a, T: LeastUpperBound+'a {
    type VIterator = WeightIterator<'a>;
    type TIterator = CountIterator<'a,K,T,L>;
    fn trace(self, key: &K) -> Self::TIterator {
        CountIterator {
            trace: self,
            next0: self.keys.get_ref(key).map(|&x|x),
            // silly: (),
        }
    }   
}

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
    // temp:       Vec<T>,
    silly: (),
}

impl<K, L, T> Count<K, T, L> where K: Data+Ord+'static, L: Lookup<K, Offset>+'static, T: LeastUpperBound+'static {

    pub fn get_count(&self, key: &K, time: &T) -> i32 {
        let mut sum = 0;
        for wgt in Trace::trace(self, key).filter(|x| x.0 <= time).map(|mut x| x.1.next().unwrap().1) {
            sum += wgt;
        }
        sum
    }
}

impl<K: Eq, L: Lookup<K, Offset>, T> Count<K, T, L> {
    pub fn new(l: L) -> Count<K, T, L> {
        Count {
            phantom: ::std::marker::PhantomData,
            links:   Vec::new(),
            times:   Vec::new(),
            keys:    l,
            // temp:    Vec::new(),
            silly: (),
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
where K: Ord+'a,
      T: LeastUpperBound+'a,
      L: Lookup<K, Offset>+'a {
    type Item = (&'a T, WeightIterator<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.next0.map(|position| {
            let time_index = self.trace.links[position.val()].time as usize;
            let result = (&self.trace.times[time_index], WeightIterator { weight: self.trace.links[position.val()].wgts, silly: &self.trace.silly });
            self.next0 = self.trace.links[position.val()].next;
            result
        })
    }
}

pub struct WeightIterator<'a> {
    weight: i32,
    silly: &'a (),
}

impl<'a> Iterator for WeightIterator<'a> {
    type Item = (&'a (), i32);
    fn next(&mut self) -> Option<(&'a (), i32)> {
        if self.weight == 0 { None }
        else {
            let result = self.weight;
            self.weight = 0;
            Some((self.silly, result))
        }
    }
}
