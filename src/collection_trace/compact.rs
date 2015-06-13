// use std::mem;
// use std::marker::PhantomData;
//
// use sort::{coalesce, is_sorted};
// use collection_trace::{close_under_lub, LeastUpperBound, Lookup, Offset};
//
// pub struct Compact<K, T, V, L: Lookup<K, Offset>> {
//     phantom:    PhantomData<K>,
//     updates:    Vec<(V, i32)>,
//     links:      Vec<(u32, Option<Offset>)>,
//     times:      Vec<(T, usize)>,
//     keys:       L,
//
//     temp:       Vec<(V, i32)>,
// }
//
// // TODO : Doing a fairly primitive merge here; re-reading every element every time;
// // TODO : a heap could improve asymptotics, but would complicate the implementation.
// // TODO : This could very easily be an iterator, rather than materializing everything.
// // TODO : It isn't clear this makes it easier to interact with user logic, but still...
// fn merge<V: Ord+Clone>(mut slices: Vec<&[(V, i32)]>, target: &mut Vec<(V, i32)>) {
//     slices.retain(|x| x.len() > 0);
//     while slices.len() > 1 {
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
//         // TODO : would be interesting to return references to values,
//         // TODO : would prevent string copies and stuff like that.
//         if count != 0 { target.push((value.clone(), count)); }
//
//         slices.retain(|x| x.len() > 0);
//     }
//
//     if let Some(slice) = slices.pop() {
//         target.push_all(slice);
//     }
// }
//
//
// impl<K: Eq, L: Lookup<K, Offset>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> Compact<K, T, V, L> {
//
//     pub fn set_difference<I: Iterator<Item=(V, i32)>>(&mut self, key: K, time: T, difference: I) {
//
//         if self.times.len() == 0 || self.times[self.times.len() - 1].0 != time {
//             self.times.push((time, self.links.len()));
//         }
//
//         let offset = self.updates.len();
//         self.updates.extend(difference);
//         assert!(is_sorted(&self.updates[offset..]), "all current uses of set_difference provide sorted data.");
//         // coalesce_from(&mut self.updates, offset);
//
//         if self.updates.len() > offset {
//
//             let next_position = Offset::new(self.links.len());
//             let prev_position = self.keys.entry_or_insert(key, || next_position);
//             if prev_position == &next_position {
//                 self.links.push((offset as u32, None));
//             }
//             else {
//                 self.links.push((offset as u32, Some(*prev_position)));
//                 *prev_position = next_position;
//             }
//         }
//     }
//
//     pub fn set_collection(&mut self, key: K, time: T, collection: &mut Vec<(V, i32)>) {
//         coalesce(collection);
//
//         if self.times.len() == 0 || self.times[self.times.len() - 1].0 != time {
//             self.times.push((time.clone(), self.links.len()));
//         }
//
//         let mut temp = mem::replace(&mut self.temp, Vec::new());
//
//         self.get_collection(&key, &time, &mut temp);
//         for index in (0..temp.len()) { temp[index].1 *= -1; }
//
//         let offset = self.updates.len();
//
//         // TODO : Make this an iterator and use set_difference
//         merge(vec![&temp[..], collection], &mut self.updates);
//         if self.updates.len() > offset {
//             // we just made a mess in updates, and need to explain ourselves...
//
//             let next_position = Offset::new(self.links.len());
//             let prev_position = self.keys.entry_or_insert(key, || next_position);
//             if prev_position == &next_position {
//                 self.links.push((offset as u32, None));
//             }
//             else {
//                 self.links.push((offset as u32, Some(*prev_position)));
//                 *prev_position = next_position;
//             }
//         }
//
//         mem::replace(&mut self.temp, temp);
//         self.temp.clear();
//     }
//
//     // this is the relatively expensive part of Compact.
//     // because we store indices implicitly, we have to look
//     // up the index for a position in self.links. This can
//     // be just a binary search, but that is more expensive
//     // than just looking at the data stashed if it were stashed
//     // in self.links.
//     fn get_index(&self, position: usize) -> usize {
//
//         let mut finger = 0;
//         assert!(self.times[finger].1 <= position);
//
//         let mut step = 1;
//         while finger + step < self.times.len() && self.times[finger + step].1 <= position {
//             finger += step;
//             step = step << 1;
//         }
//
//         step = step >> 1;
//         while step > 0 {
//             if finger + step < self.times.len() && self.times[finger + step].1 <= position {
//                 finger += step;
//             }
//
//             step = step >> 1;
//         }
//
//         // finger should point at the last entry whose offset is less than position.
//         assert!(self.times[finger].1 <= position);
//         assert!(finger + 1 >= self.times.len() || self.times[finger + 1].1 > position);
//
//         finger
//     }
//
//     fn get_range(&self, position: Offset) -> &[(V, i32)] {
//         let lower = self.links[position.val()].0 as usize;
//         let upper = if (position.val() + 1) < self.links.len() {
//             self.links[position.val() + 1].0 as usize
//         } else { self.updates.len() };
//         &self.updates[lower..upper]
//     }
//
//     pub fn get_difference(&self, key: &K, time: &T) -> &[(V, i32)] {
//
//         let mut next = self.keys.get_ref(key).map(|&x|x);
//         while let Some(position) = next {
//             let time_index = self.get_index(position.val());
//             if &self.times[time_index].0 == time {
//                 return self.get_range(position);
//             }
//             next = self.links[position.val()].1;
//         }
//         return &[]; // didn't find anything
//     }
//     pub fn get_collection(&self, key: &K, time: &T, target: &mut Vec<(V, i32)>) {
//         let mut slices = Vec::new();
//         let mut next = self.keys.get_ref(key).map(|&x|x);
//         while let Some(position) = next {
//             let time_index = self.get_index(position.val());
//             if &self.times[time_index].0 <= time {
//                 slices.push(self.get_range(position));
//             }
//             next = self.links[position.val()].1;
//         }
//
//         // target.clear();
//         assert!(target.len() == 0, "get_collection is expected to be called with an empty target.");
//         merge(slices, target);
//     }
//
//     pub fn interesting_times(&mut self, key: &K, index: &T, result: &mut Vec<T>) {
//         self.map_over_times(key, |time, _| {
//             let lub = time.least_upper_bound(index);
//             if !result.contains(&lub) {
//                 result.push(lub);
//             }
//         });
//         close_under_lub(result);
//     }
//     pub fn map_over_times<F:FnMut(&T, &[(V, i32)])>(&self, key: &K, mut func: F) {
//         let mut next = self.keys.get_ref(key).map(|&x|x);
//         while let Some(position) = next {
//             let time_index = self.get_index(position.val());
//             func(&self.times[time_index].0, self.get_range(position));
//             next = self.links[position.val()].1;
//         }
//     }
// }
//
// impl<K: Eq, L: Lookup<K, Offset>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> Compact<K, T, V, L> {
//     pub fn new(l: L) -> Compact<K, T, V, L> {
//         Compact {
//             phantom: PhantomData,
//             updates: Vec::new(),
//             links:   Vec::new(),
//             times:   Vec::new(),
//             keys:    l,
//             temp:    Vec::new(),
//         }
//     }
//     pub fn size(&self) -> usize {
//         self.updates.len() * mem::size_of::<(V, i32)>() +
//         self.links.len() * mem::size_of::<(u32, Option<Offset>)>() +
//         self.times.len() * mem::size_of::<(T, usize)>()
//     }
// }
//
//
// // fn _sum<V: Ord+Clone>(mut a: &[(V, i32)], mut b: &[(V, i32)], target: &mut Vec<(V, i32)>) {
// //     while a.len() > 0 && b.len() > 0 {
// //         match a[0].0.cmp(&b[0].0) {
// //             Ordering::Less    => { target.push(a[0].clone()); a = &a[1..]; },
// //             Ordering::Greater => { target.push(b[0].clone()); b = &b[1..]; },
// //             Ordering::Equal   => { target.push((a[0].0.clone(), a[0].1 + b[0].1));
// //                                    a = &a[1..]; b = &b[1..]; },
// //         }
// //     }
// //
// //     if a.len() > 0 { target.extend(a.iter().map(|x| x.clone())); }
// //     if b.len() > 0 { target.extend(b.iter().map(|x| x.clone())); }
// // }
// //
// // // a version of merge which returns a vector of references to values, avoiding the use of Clone
// // fn _ref_merge<'a, V: Ord>(mut slices: Vec<&'a[(V, i32)]>, target: &mut Vec<(&'a V, i32)>) {
// //     while slices.len() > 0 {
// //         let mut value = &slices[0][0].0;    // start with the first value
// //         for slice in &slices[1..] {         // for each other value
// //             if &slice[0].0 < value {        //   if it comes before the current value
// //                 value = &slice[0].0;        //     capture a reference to it
// //             }
// //         }
// //
// //         let mut count = 0;                  // start with an empty accumulation
// //         for slice in &mut slices[..] {      // for each non-empty slice
// //             if &slice[0].0 == value {       //   if the first diff is for value
// //                 count += slice[0].1;        //     accumulate the delta
// //                 *slice = &slice[1..];       //     advance the slice by one
// //             }
// //         }
// //
// //         if count != 0 { target.push((value, count)); }
// //         slices.retain(|x| x.len() > 0);
// //     }
// // }
