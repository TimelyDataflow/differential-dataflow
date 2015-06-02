use std::mem;
use std::marker::PhantomData;

use std::ops::Deref;

use sort::{coalesce, is_sorted};
use collection_trace::{close_under_lub, LeastUpperBound, Lookup};

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct OffsetOption {
    value: u32,             // zero : None
}

impl OffsetOption {
    pub fn new(val: u32) -> OffsetOption {
        if val == 0 { panic!("cannot construct an OffsetOption with value 0"); }

        OffsetOption { value: val }
    }
}

impl Deref for OffsetOption {
    type Target = u32;
    fn deref(&self) -> &u32 { &self.value }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Offset {
    dataz: OffsetOption,
}

impl Offset {
    pub fn new(offset: usize) -> Offset {
        assert!(offset < u32::max_value() as usize); // note strict inequality
        Offset { dataz: OffsetOption::new(u32::max_value() - offset as u32) }
    }
    #[inline(always)] pub fn val(&self) -> usize { (u32::max_value() - *self.dataz) as usize }
}

type IndexEntry<T> = IndexEntry32<T>;

pub struct IndexEntry32<T> {
    index:  T,
    offset: u32,
    length: u32,
    next:   Option<Offset>,
}

impl<T> IndexEntry32<T> {
    pub fn index(&self) -> &T { &self.index }
    pub fn offset(&self) -> usize { self.offset as usize }
    pub fn length(&self) -> usize { self.length as usize }
    pub fn next(&self, _position: usize) -> Option<Offset> { self.next }
    pub fn new(index: T, offset: usize, length: usize, next: Option<Offset>) -> IndexEntry32<T> {

        assert!(offset < u32::max_value() as usize);
        assert!(length < u32::max_value() as usize);

        IndexEntry32 {
            index:  index,
            offset: offset as u32,
            length: length as u32,
            next:   next,
        }
    }
}

// pub struct IndexEntry64<T> {
//     index:  T,
//     offset: u64,
//     length: u64,
//     next:   u64,    // Option<OffsetOption<u64>> for the decrement would be cooler. unsafe, and needs more thinking.
// }
//
// impl<T> IndexEntry64<T> {
//     pub fn index(&self) -> &T { &self.index }
//     pub fn offset(&self) -> usize { self.offset as usize }
//     pub fn length(&self) -> usize { self.length as usize }
//     pub fn next(&self, _position: usize) -> Option<usize> {
//         if self.next != u64::max_value()  {
//             Some(self.next as usize)
//         } else { None } }
//     pub fn new(index: T, offset: usize, length: usize, next: usize) -> IndexEntry64<T> {
//         IndexEntry64 {
//             index: index,
//             offset: offset as u64,
//             length: length as u64,
//             next: if next == usize::max_value() { u64::max_value() } else { next as u64 },
//         }
//     }
// }

// In at attempt to reduce the amount of space required by collection trace, we are going to exploit
// its append-only immutability, and "everything arrives for an index at once" properties.
//  *  Updates will stay a nice flat vector (though, it could be split if needed).
//  *  Each time will cover one contiguous span of updates: a position in updates determines a time.
//  *  Regions will be indicated by an offset, with the length determined by the next offset in seq
//  *  We'll also need some sort of next pointer to chain these together.

// I reason the per-key cost is one Offset, the per-index cost is two Offsets, and no additional
// per-update cost. The main pain point will be moving through the times sequentially to find which
// one corresponds to a supplied offset, but this could become binary search as well, if needed.
// This also limits us more seriously to 32 bits while we use that many, though I think this was
// already the case with whatever we have at the moment.

pub struct CollectionTrace<K, T, V, L: Lookup<K, Offset>> {
    phantom:    PhantomData<K>,
    updates:    Vec<(V, i32)>,
    times:      Vec<IndexEntry<T>>,
    keys:       L,                      // stores u32::max_value() - target, so that OffsetOption works

    temp:       Vec<(V, i32)>,
}

impl<K: Eq, T, V, L: Lookup<K, Offset>> Default for CollectionTrace<K, T, V, L> {
    fn default() -> CollectionTrace<K, T, V, L> {
        CollectionTrace {
            phantom: PhantomData,
            updates: Vec::new(),
            times:   Vec::new(),
            keys:    L::new(),
            temp:    Vec::new(),
        }
    }
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


impl<K: Eq, L: Lookup<K, Offset>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> CollectionTrace<K, T, V, L> {

    pub fn set_difference<I: Iterator<Item=(V, i32)>>(&mut self, key: K, time: T, difference: I) {

        let offset = self.updates.len();
        self.updates.extend(difference);
        assert!(is_sorted(&self.updates[offset..]), "all current uses of set_difference provide sorted data.");
        // coalesce_from(&mut self.updates, offset);

        if self.updates.len() > offset {
            let next_position = Offset::new(self.times.len());
            let position = self.keys.entry_or_insert(key, || next_position);
            if position == &next_position {
                self.times.push(IndexEntry::new(time, offset, self.updates.len() - offset, None));
            }
            else {
                let pos = mem::replace(position, next_position);
                self.times.push(IndexEntry::new(time, offset, self.updates.len() - offset, Some(pos)));
            }
        }
    }

    pub fn set_collection(&mut self, key: K, time: T, collection: &mut Vec<(V, i32)>) {
        coalesce(collection);

        let mut temp = mem::replace(&mut self.temp, Vec::new());

        self.get_collection(&key, &time, &mut temp);
        for index in (0..temp.len()) { temp[index].1 *= -1; }

        let count = self.updates.len();
        merge(vec![&temp[..], collection], &mut self.updates);
        if self.updates.len() - count > 0 {
            // we just made a mess in updates, and need to explain ourselves...
            let next_position = Offset::new(self.times.len());
            let position = self.keys.entry_or_insert(key, || next_position);
            if position == &next_position {
                self.times.push(IndexEntry::new(time, count, self.updates.len() - count, None));
            }
            else {
                let pos = mem::replace(position, next_position);
                self.times.push(IndexEntry::new(time, count, self.updates.len() - count, Some(pos)));
            }

        }

        mem::replace(&mut self.temp, temp);
        self.temp.clear();
    }

    pub fn get_difference(&self, key: &K, time: &T) -> &[(V, i32)] {
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            let diff = &self.times[position.val()];
            if diff.index() == time {
                return &self.updates[diff.offset()..][..diff.length()];
            }
            next = diff.next(position.val());
        }
        return &[]; // didn't find anything
    }
    pub fn get_collection(&self, key: &K, time: &T, target: &mut Vec<(V, i32)>) {
        let mut slices = Vec::new();
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            let diff = &self.times[position.val()];
            if diff.index() <= time {
                slices.push(&self.updates[diff.offset()..][..diff.length()]);
            }
            next = diff.next(position.val());
        }

        // target.clear();
        assert!(target.len() == 0, "get_collection is expected to be called with an empty target.");
        merge(slices, target);
    }

    pub fn interesting_times(&mut self, key: &K, index: &T, result: &mut Vec<T>) {
        self.map_over_times(key, |time, _| {
            let lub = time.least_upper_bound(index);
            if !result.contains(&lub) {
                result.push(lub);
            }
        });
        close_under_lub(result);
    }
    pub fn map_over_times<F:FnMut(&T, &[(V, i32)])>(&self, key: &K, mut func: F) {
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            let diff = &self.times[position.val()];
            func(diff.index(), &self.updates[diff.offset()..][..diff.length()]);
            next = diff.next(position.val());
        }
    }
}

impl<K: Eq, L: Lookup<K, Offset>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> CollectionTrace<K, T, V, L> {
    pub fn new(l: L) -> CollectionTrace<K, T, V, L> {
        CollectionTrace {
            phantom: PhantomData,
            updates: Vec::new(),
            times:   Vec::new(),
            keys:    l,
            temp:    Vec::new(),
        }
    }
    pub fn size(&self) -> usize {
        self.updates.len() * mem::size_of::<(V, i32)>() +
         self.times.len() * mem::size_of::<IndexEntry<T>>()
    }
}


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
// // a version of merge which returns a vector of references to values, avoiding the use of Clone
// fn _ref_merge<'a, V: Ord>(mut slices: Vec<&'a[(V, i32)]>, target: &mut Vec<(&'a V, i32)>) {
//     while slices.len() > 0 {
//         let mut value = &slices[0][0].0;    // start with the first value
//         for slice in &slices[1..] {         // for each other value
//             if &slice[0].0 < value {        //   if it comes before the current value
//                 value = &slice[0].0;        //     capture a reference to it
//             }
//         }
//
//         let mut count = 0;                  // start with an empty accumulation
//         for slice in &mut slices[..] {      // for each non-empty slice
//             if &slice[0].0 == value {       //   if the first diff is for value
//                 count += slice[0].1;        //     accumulate the delta
//                 *slice = &slice[1..];       //     advance the slice by one
//             }
//         }
//
//         if count != 0 { target.push((value, count)); }
//         slices.retain(|x| x.len() > 0);
//     }
// }
