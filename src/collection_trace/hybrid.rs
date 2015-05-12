use std::mem;
use std::marker::PhantomData;

use sort::{coalesce, is_sorted};
use collection_trace::{close_under_lub, LeastUpperBound, Lookup, Offset};

// Our plan with Hybrid is to make more explicit but indirected reference to times, by adding
// a time index to entries of links, and segregating the updates into per-time updates.
// each link : (u32, u32, Option<Offset>) would indicate a time index, and an offset within
// the updates for that time index. Keeping offsets out of a common space should make compaction
// easier when we finally get around to that. Also, each array of updates can be shrunk when sealed
// to release un-used memory back to the process / os.

pub struct Hybrid<K, T, V, L: Lookup<K, Offset>> {
    phantom:    PhantomData<K>,
    links:      Vec<(u32, u32, Option<Offset>)>,    // (time, offset, next)
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
        target.push_all(slice);
    }
}


impl<K: Eq, L: Lookup<K, Offset>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> Hybrid<K, T, V, L> {

    pub fn set_difference<I: Iterator<Item=(V, i32)>>(&mut self, key: K, time: T, difference: I) {

        if self.times.len() == 0 || self.times[self.times.len() - 1].0 != time {
            if let Some(last) = self.times.last_mut() {
                last.1.shrink_to_fit();
            }
            self.times.push((time, Vec::new()));
        }

        let index = self.times.len() - 1;
        let updates = &mut self.times[index].1;
        let offset = updates.len();
        updates.extend(difference);
        assert!(is_sorted(&updates[offset..]), "all current uses of set_difference provide sorted data.");
        // coalesce_from(&mut self.updates, offset);

        if updates.len() > offset {

            let next_position = Offset::new(self.links.len());
            let prev_position = self.keys.entry_or_insert(key, || next_position);
            if prev_position == &next_position {
                self.links.push((index as u32, offset as u32, None));
            }
            else {
                self.links.push((index as u32, offset as u32, Some(*prev_position)));
                *prev_position = next_position;
            }
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
            if prev_position == &next_position {
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

    fn get_range(&self, position: Offset) -> &[(V, i32)] {

        let index = self.links[position.val()].0 as usize;
        let lower = self.links[position.val()].1 as usize;

        // upper limit can be read if next link exists and corresponds to the same index. else, is last elt.
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

        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            let time_index = self.links[position.val()].0 as usize;
            if &self.times[time_index].0 == time {
                return self.get_range(position);
            }
            next = self.links[position.val()].2;
        }
        return &[]; // didn't find anything
    }
    pub fn get_collection(&self, key: &K, time: &T, target: &mut Vec<(V, i32)>) {
        let mut slices = Vec::new();
        let mut next = self.keys.get_ref(key).map(|&x|x);
        while let Some(position) = next {
            let time_index = self.links[position.val()].0 as usize;
            if &self.times[time_index].0 <= time {
                slices.push(self.get_range(position));
            }
            next = self.links[position.val()].2;
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
            let time_index = self.links[position.val()].0 as usize;
            func(&self.times[time_index].0, self.get_range(position));
            next = self.links[position.val()].2;
        }
    }
}

impl<K: Eq, L: Lookup<K, Offset>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> Hybrid<K, T, V, L> {
    pub fn new(l: L) -> Hybrid<K, T, V, L> {
        Hybrid {
            phantom: PhantomData,
            links:   Vec::new(),
            times:   Vec::new(),
            keys:    l,
            temp:    Vec::new(),
        }
    }
    // pub fn size(&self) -> usize {
    //     self.updates.len() * mem::size_of::<(V, i32)>() +
    //     self.links.len() * mem::size_of::<(u32, Option<Offset>)>() +
    //     self.times.len() * mem::size_of::<(T, usize)>()
    // }
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
