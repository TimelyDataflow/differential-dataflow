use std::mem;
use std::cmp::Ordering;
use std::marker::PhantomData;

use sort::{coalesce, is_sorted};
use collection_trace::{BatchCollectionTrace, close_under_lub, LeastUpperBound, Lookup};

type IndexEntry<T> = IndexEntry32<T>;

pub struct IndexEntry32<T> {
    index:  T,
    offset: u32,
    length: u32,
    next:   u32,    // Option<NonZero<u32>> for the decrement would be cooler. unsafe, and needs more thinking.
}

impl<T> IndexEntry32<T> {
    pub fn index(&self) -> &T { &self.index }
    pub fn offset(&self) -> usize { self.offset as usize }
    pub fn length(&self) -> usize { self.length as usize }
    pub fn next(&self, _position: usize) -> Option<usize> {
        if self.next != u32::max_value() {
            Some(self.next as usize)
        } else { None }
    }
    pub fn new(index: T, offset: usize, length: usize, next: usize) -> IndexEntry32<T> {

        assert!(offset < u32::max_value() as usize);
        assert!(length < u32::max_value() as usize);
        // assert!(next   < u32::max_value() as usize);

        IndexEntry32 {
            index: index,
            offset: offset as u32,
            length: length as u32,
            next: if next == usize::max_value() { u32::max_value() } else { next as u32 },
        }
    }
}

pub struct IndexEntry64<T> {
    index:  T,
    offset: u64,
    length: u64,
    next:   u64,    // Option<NonZero<u64>> for the decrement would be cooler. unsafe, and needs more thinking.
}

impl<T> IndexEntry64<T> {
    pub fn index(&self) -> &T { &self.index }
    pub fn offset(&self) -> usize { self.offset as usize }
    pub fn length(&self) -> usize { self.length as usize }
    pub fn next(&self, _position: usize) -> Option<usize> {
        if self.next != u64::max_value()  {
            Some(self.next as usize)
        } else { None } }
    pub fn new(index: T, offset: usize, length: usize, next: usize) -> IndexEntry64<T> {
        IndexEntry64 {
            index: index,
            offset: offset as u64,
            length: length as u64,
            next: if next == usize::max_value() { u64::max_value() } else { next as u64 },
        }
    }
}

pub struct BatchVectorCollectionTrace<K, T, V, L: Lookup<K, usize>> {
    phantom:    PhantomData<K>,
    updates:    Vec<(V, i32)>,
    times:      Vec<IndexEntry<T>>,
    keys:       L,
}

impl<K: Eq, T, V, L: Lookup<K, usize>> Default for BatchVectorCollectionTrace<K, T, V, L> {
    fn default() -> BatchVectorCollectionTrace<K, T, V, L> {
        BatchVectorCollectionTrace {
            phantom: PhantomData,
            updates: Vec::new(),
            times:   Vec::new(),
            keys:    L::new(),
        }
    }
}

// TODO : Doing a fairly primitive merge here; re-reading every element every time;
// TODO : a heap could improve asymptotics, but would complicate the implementation.
// TODO : This could very easily be an iterator, rather than materializing everything.
// TODO : It isn't clear this makes it easier to interact with user logic, but still...
fn merge<V: Ord+Clone>(mut slices: Vec<&[(V, i32)]>, target: &mut Vec<(V, i32)>) {
    slices.retain(|x| x.len() > 0);
    while slices.len() > 0 {
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
}

fn _sum<V: Ord+Clone>(mut a: &[(V, i32)], mut b: &[(V, i32)], target: &mut Vec<(V, i32)>) {
    while a.len() > 0 && b.len() > 0 {
        match a[0].0.cmp(&b[0].0) {
            Ordering::Less    => { target.push(a[0].clone()); a = &a[1..]; },
            Ordering::Greater => { target.push(b[0].clone()); b = &b[1..]; },
            Ordering::Equal   => { target.push((a[0].0.clone(), a[0].1 + b[0].1));
                                   a = &a[1..]; b = &b[1..]; },
        }
    }

    if a.len() > 0 { target.extend(a.iter().map(|x| x.clone())); }
    if b.len() > 0 { target.extend(b.iter().map(|x| x.clone())); }
}

// a version of merge which returns a vector of references to values, avoiding the use of Clone
fn _ref_merge<'a, V: Ord>(mut slices: Vec<&'a[(V, i32)]>, target: &mut Vec<(&'a V, i32)>) {
    while slices.len() > 0 {
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

        if count != 0 { target.push((value, count)); }
        slices.retain(|x| x.len() > 0);
    }
}

impl<K: Eq, L: Lookup<K, usize>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> BatchCollectionTrace<K> for BatchVectorCollectionTrace<K, T, V, L> {
    type Index = T;
    type Value = V;
    fn set_difference<I: Iterator<Item=(V, i32)>>(&mut self, key: K, time: T, difference: I) {
        let offset = self.updates.len();
        self.updates.extend(difference);
        assert!(is_sorted(&self.updates[offset..]), "all current uses of set_difference provide sorted data");
        // coalesce_from(&mut self.updates, offset);
        if self.updates.len() > offset {
            let position = self.keys.entry_or_insert(key, || usize::max_value());
            self.times.push(IndexEntry::new(time, offset, self.updates.len() - offset, *position));
            *position = self.times.len() - 1;
        }
    }
    // TODO : It would be nice to exploit the append-only immutability of self.updates and self.times
    // TODO : to allow us to merge directly in to self.updates, but no is permitted without unsafe.
    // TODO : This is good, because if we tripped a re-allocation, our slices would be invalid, but
    // TODO : we could easily reserve a big hunk of space as appropriate.
    fn set_collection(&mut self, key: K, time: T, collection: &mut Vec<(V, i32)>) {
        coalesce(collection);
        let mut temp = Vec::new();
        self.get_collection(&key, &time, &mut temp);
        for index in (0..temp.len()) { temp[index].1 *= -1; }

        let slices = vec![&temp[..], collection];   // TODO : Allocates!
        let count = self.updates.len();
        merge(slices, &mut self.updates);
        if self.updates.len() - count > 0 {
            // we just made a mess in updates, and need to explain ourselves...
            let position = self.keys.entry_or_insert(key, || usize::max_value());
            self.times.push(IndexEntry::new(time, count, self.updates.len() - count, *position));
            *position = self.times.len() - 1;
        }
    }

    fn get_difference(&self, key: &K, time: &T) -> &[(V, i32)] {
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            if self.times[position].index() == time {
                return &self.updates[self.times[position].offset()..][..self.times[position].length()];
            }
            next = self.times[position].next(position);
        }
        return &[]; // didn't find anything
    }
    fn get_collection(&self, key: &K, time: &T, target: &mut Vec<(V, i32)>) {
        let mut slices = Vec::new();

        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            if self.times[position].index() <= time {
                slices.push(&self.updates[self.times[position].offset() ..][..self.times[position].length()]);
            }
            next = self.times[position].next(position);
        }

        // target.clear();
        assert!(target.len() == 0);
        merge(slices, target);
    }

    fn interesting_times(&mut self, key: &K, index: &T, result: &mut Vec<T>) {
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            let lub = index.least_upper_bound(self.times[position].index());
            if !result.contains(&lub) {
                result.push(lub);
            }
            next = self.times[position].next(position);
        }
        close_under_lub(result);
    }
    fn map_over_times<F:FnMut(&Self::Index, &[(Self::Value, i32)])>(&self, key: &K, mut func: F) {
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            func(self.times[position].index(), &self.updates[self.times[position].offset()..][..self.times[position].length()]);
            next = self.times[position].next(position);
        }
    }
}

impl<K: Eq, L: Lookup<K, usize>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> BatchVectorCollectionTrace<K, T, V, L> {
    pub fn new(l: L) -> BatchVectorCollectionTrace<K, T, V, L> {
        BatchVectorCollectionTrace {
            phantom: PhantomData,
            updates: Vec::new(),
            times:   Vec::new(),
            keys:    l,
        }
    }
    pub fn size(&self) -> (usize, usize) {
        (self.updates.len() * mem::size_of::<(V, i32)>(), self.times.len() * mem::size_of::<IndexEntry<T>>())
    }
}

pub struct BatchDifferentialShard<K, T, S: BatchCollectionTrace<K, Index=T>, R: BatchCollectionTrace<K, Index=T>> {
    phantom:        PhantomData<K>,
    src:            Vec<(S::Value,i32)>,
    dst:            Vec<(R::Value,i32)>,
    pub source:     S,
    pub result:     R,
}

impl<K: Eq+Clone, T:Clone, S: BatchCollectionTrace<K, Index=T>, R: BatchCollectionTrace<K, Index=T>> BatchDifferentialShard<K, T, S, R> {
    pub fn new(s: S, r: R) -> BatchDifferentialShard<K, T, S, R> {
        BatchDifferentialShard {
            phantom: PhantomData,
            source: s,
            result: r,
            src:    Vec::new(),
            dst:    Vec::new(),
        }
    }
    pub fn set_collection_with<F: Fn(&K, &[(S::Value, i32)], &mut Vec<(R::Value, i32)>)>(&mut self, key: &K, index: &T, logic: F) {
        self.source.get_collection(key, index, &mut self.src);
        if self.src.len() > 0 { logic(key, &mut self.src, &mut self.dst); }
        self.result.set_collection(key.clone(), index.clone(), &mut self.dst);
        self.src.clear();
        self.dst.clear();
    }
}
