use std::mem;
use std::marker::PhantomData;
use std::iter::Peekable;
use std::fmt::Debug;

use sort::coalesce;
use collection_trace::{close_under_lub, LeastUpperBound, Lookup};
use iterators::merge::{Merge, MergeIterator};
use iterators::coalesce::{Coalesce, CoalesceIterator};

pub type CollectionIterator<'a, V> = Peekable<CoalesceIterator<&'a V, MergeIterator<SliceIterator<'a, V>>>>;

#[derive(Copy, Clone)]
pub struct Offset {
    dataz: u32,
}

impl Offset {
    #[inline(always)]
    pub fn new(offset: usize) -> Offset {
        assert!(offset < u32::max_value() as usize); // note strict inequality
        Offset { dataz: u32::max_value() - offset as u32 }
    }
    #[inline(always)]
    pub fn val(&self) -> usize { (u32::max_value() - self.dataz) as usize }
}

pub struct CollectionTrace<K, T, V, L: Lookup<K, Offset>> {
    phantom:    PhantomData<K>,
    links:      Vec<(u32, u32, Option<Offset>)>,    // (time, offset, next_link)
    times:      Vec<(T, Vec<(V, i32)>)>,            // (time, updates)
    keys:       L,

    temp:       Vec<(V, i32)>,
}

// TODO : Doing a fairly primitive merge here; re-reading every element every time;
// TODO : a heap could improve asymptotics, but would complicate the implementation.
// TODO : This could very easily be an iterator, rather than materializing everything.
// TODO : It isn't clear this makes it easier to interact with user logic, but still...
fn merge<V: Ord+Clone>(mut slices: Vec<&[(V, i32)]>, target: &mut Vec<(V, i32)>) {
    slices.retain(|x| x.len() > 0);
    while slices.len() > 1 {
        let mut value = &slices[0][0].0;    // start with the first value
        for slice in &slices[1..] {         // for each other value
            if &slice[0].0 < value {        //   if it comes before the current value
                value = &slice[0].0;        //     capture a reference to it
            }
        }

        let mut count = 0;                  // start with an empty accumulation
        for slice in &mut slices[..] {      // for each non-empty slice
            if &slice[0].0 == value {       //   if the first diff is for value
                count += slice[0].1;        //     accumulate the delta
                *slice = &slice[1..];       //     advance the slice by one
            }
        }

        // TODO : would be interesting to return references to values,
        // TODO : would prevent string copies and stuff like that.
        if count != 0 { target.push((value.clone(), count)); }

        slices.retain(|x| x.len() > 0);
    }

    if let Some(slice) = slices.pop() {
        target.extend(slice.iter().cloned());
    }
}


impl<K, L, T, V> CollectionTrace<K, T, V, L>
where K: Eq+Clone,
      L: Lookup<K, Offset>,
      T: LeastUpperBound+Clone,
      V: Ord+Clone+Debug {

    // this assumes that someone has gone and sorted things for us.
    pub fn install_differences(&mut self, time: T, keys: &mut Vec<K>, vals: Vec<(V, i32)>) {

        // TODO : build an iterator over (key, lower, slice) or something like that.
        let mut lower = 0;  // the lower limit of the range of vals for the current key.
        while lower < keys.len() {

            // find the upper limit of this key range
            let mut upper = lower + 1;
            while upper < keys.len() && keys[lower] == keys[upper] {
                upper += 1;
            }

            // adjust the linked list for keys[lower]
            let next_position = Offset::new(self.links.len());
            let prev_position = self.keys.entry_or_insert(keys[lower].clone(), || next_position);
            if &prev_position.val() == &next_position.val() {
                self.links.push((self.times.len() as u32, lower as u32, None));
            }
            else {
                self.links.push((self.times.len() as u32, lower as u32, Some(*prev_position)));
                *prev_position = next_position;
            }

            lower = upper;
        }

        // TODO : logic is probably out-dated; should unconditionally pass this
        if self.times.len() == 0 || self.times[self.times.len() - 1].0 != time {
            if let Some(last) = self.times.last_mut() {
                last.1.shrink_to_fit();
            }
            self.times.push((time, vals));
        }
    }

    pub fn set_collection(&mut self, key: K, time: T, collection: &mut Vec<(V, i32)>) {
        coalesce(collection);

        if self.times.len() == 0 || self.times[self.times.len() - 1].0 != time {
            if let Some(last) = self.times.last_mut() {
                last.1.shrink_to_fit();
            }
            self.times.push((time, Vec::new()));
        }

        let mut temp = mem::replace(&mut self.temp, Vec::new());

        self.get_collection(&key, &self.times.last().unwrap().0, &mut temp);
        for index in (0..temp.len()) { temp[index].1 *= -1; }

        let index = self.times.len() - 1;
        let updates = &mut self.times[index].1;

        let offset = updates.len();

        // TODO : Make this an iterator and use set_difference
        merge(vec![&temp[..], collection], updates);
        if updates.len() > offset {
            // we just made a mess in updates, and need to explain ourselves...

            let next_position = Offset::new(self.links.len());
            let prev_position = self.keys.entry_or_insert(key, || next_position);
            if &prev_position.val() == &next_position.val() {
                self.links.push((index as u32, offset as u32, None));
            }
            else {
                self.links.push((index as u32, offset as u32, Some(*prev_position)));
                *prev_position = next_position;
            }
        }

        mem::replace(&mut self.temp, temp);
        self.temp.clear();
    }

    pub fn get_range(&self, position: Offset) -> &[(V, i32)] {

        let index = self.links[position.val()].0 as usize;
        let lower = self.links[position.val()].1 as usize;

        // upper limit can be read if next link exists and of the same index. else, is last elt.
        let upper = if (position.val() + 1) < self.links.len()
                    && index == self.links[position.val() + 1].0 as usize {
            self.links[position.val() + 1].1 as usize
        }
        else {
            self.times[index].1.len()
        };

        &self.times[index].1[lower..upper]
    }

    pub fn get_difference(&self, key: &K, time: &T) -> &[(V, i32)] {
        self.trace(key).filter(|x| x.0 == time).map(|x| x.1).next().unwrap_or(&[])
    }

    pub fn get_collection(&self, key: &K, time: &T, target: &mut Vec<(V, i32)>) {
        assert!(target.len() == 0, "get_collection should be called with an empty target.");
        let slices = self.trace(key).filter(|x| x.0 <= time).map(|x| x.1).collect();
        merge(slices, target);
    }

    pub fn get_collection_iterator(&self, key: &K, time: &T) -> CollectionIterator<V> {
        self.trace(key)
            .filter(|x| x.0 <= time)
            .map(|x| SliceIterator::new(x.1))
            .merge()
            .coalesce()
            .peekable()
    }

    pub fn interesting_times(&mut self, key: &K, index: &T, result: &mut Vec<T>) {
        for (time, _) in self.trace(key) {
            let lub = time.least_upper_bound(index);
            if !result.contains(&lub) {
                result.push(lub);
            }
        }
        close_under_lub(result);
    }

    pub fn trace<'a, 'b>(&'a self, key: &'b K) -> TraceIterator<'a, K, T, V, L> {
        TraceIterator {
            trace: self,
            next0: self.keys.get_ref(key).map(|&x|x),
        }
    }
}


pub struct TraceIterator<'a, K: 'a, T: 'a, V: 'a, L: Lookup<K, Offset>+'a> {
    trace: &'a CollectionTrace<K, T, V, L>,
    next0: Option<Offset>,
}

impl<'a, K, T, V: Debug, L> Iterator for TraceIterator<'a, K, T, V, L>
where K: Eq+Clone+'a,
      T: LeastUpperBound+Clone+'a,
      V: Ord+Clone+'a,
      L: Lookup<K, Offset>+'a {
    type Item = (&'a T, &'a [(V,i32)]);
    fn next(&mut self) -> Option<Self::Item> {
        self.next0.map(|position| {
            let time_index = self.trace.links[position.val()].0 as usize;
            let result = (&self.trace.times[time_index].0, self.trace.get_range(position));
            self.next0 = self.trace.links[position.val()].2;
            result
        })
    }
}

impl<K, L: Lookup<K, Offset>, T, V> CollectionTrace<K, T, V, L> {
    pub fn new(l: L) -> CollectionTrace<K, T, V, L> {
        CollectionTrace {
            phantom: PhantomData,
            links:   Vec::new(),
            times:   Vec::new(),
            keys:    l,
            temp:    Vec::new(),
        }
    }
}

// // special-cased for set_collection.
// fn _sum<V: Ord+Clone>(mut a: &[(V, i32)], mut b: &[(V, i32)], target: &mut Vec<(V, i32)>) {
//     while a.len() > 0 && b.len() > 0 {
//         match a[0].0.cmp(&b[0].0) {
//             Ordering::Less    => { target.push(a[0].clone()); a = &a[1..]; },
//             Ordering::Greater => { target.push(b[0].clone()); b = &b[1..]; },
//             Ordering::Equal   => { target.push((a[0].0.clone(), a[0].1 + b[0].1));
//                                    a = &a[1..]; b = &b[1..]; },
//         }
//     }
//
//     if a.len() > 0 { target.extend(a.iter().map(|x| x.clone())); }
//     if b.len() > 0 { target.extend(b.iter().map(|x| x.clone())); }
// }
//


pub struct SliceIterator<'a, V: 'a> {
    index: usize,
    slice: &'a [(V, i32)],
}

impl<'a, V: 'a> SliceIterator<'a, V> {
    fn new(slice: &'a [(V, i32)]) -> SliceIterator<'a, V> {
        SliceIterator {
            index: 0,
            slice: slice,
        }
    }
}

impl<'a, V: 'a> Iterator for SliceIterator<'a, V> {
    type Item = (&'a V, i32);
    fn next(&mut self) -> Option<(&'a V, i32)> {
        if self.index < self.slice.len() {
            self.index += 1;
            Some((&self.slice[self.index-1].0, self.slice[self.index-1].1))
        }
        else { None }
    }
}
