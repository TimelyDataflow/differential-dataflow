//! A trie representation of `(key, time, value, weight)` tuples, and routines to merge them.

use std::rc::Rc;
use std::collections::HashMap;
use std::cmp::Ordering;
use std::hash::Hash;

use iterators::merge::Merge as Whatever;
use iterators::coalesce::Coalesce;

/// Changes to frequencies.
pub type W = i32;

// // This is the type of stuff we may need if we want to support a `Trace` trait, which could allow
// // associated iterator types. Currently a bit confusing because the iterator types need to be
// // generic over lifetimes, which is a type of HKT. There is some information about how to fake this
// // out in the associated items RFC, but let's hold off for now.
//
// pub trait TraceIter<'a, K, T, V> {
//     type I: Iterator<(&'a V, W)>;
//     fn iter(self, key: &K, time: &T) -> I;
// }
//
// pub trait Trace<K, T, V> {
//
//     fn set_difference(&mut self, time: T, accumulation: Compact<K, V>);
//     fn get_collection<'a>(&'a mut self, time: T) -> <&'a Self>::I where &'a Self: TraceIter<'a, K, T, V>;
//
// }

// /// A `Trace` represents a collection of `(K, T, V, W)` tuples, which can be efficiently accessed
// /// first by `K`, then by `T`, and finally as an ordered sequence of pairs `(V, W)`.
// pub struct Trace<K, T, V> {
//     /// Map from key to head of its linked list.
//     ///
//     /// This map is soft-state, redundant with the information in `self.tiers`, but indexed in a
//     /// more appealing manner, from the point of view of accessing keys without looking at each
//     /// `Tier`.
//     ///
//     /// TODO : The implementation should switch from a Rust `HashMap` to something more performant.
//     /// Possibilities include a custom RHH implementation, or an implementor `L: Lookup<K, usize>`.
//     index: HashMap<K, usize>,
//     /// Linked list of cached entries into `self.tier`.
//     ///
//     /// Each represents a tier index, a position in that tier's `keys`
//     /// list, and a pointer to the next entry in the linked list. A next entry of `max_value()`
//     /// indicates that there are no more entries.
//     links: Vec<(usize, usize, usize)>,
//     /// A list of tiers.
//     ///
//     /// The position of each tier is important for our cached representation, and we should not
//     /// change them except under duress, or in the process of finishing a merge, where each of the
//     /// keys associated with the tier will need to have their links updated anyhow.
//     tiers: Vec<Tier<K, T, V>>,
//     /// A list of merges-in-progress.
//     ///
//     /// The two indices indicate which entries of `self.tiers` are being merged, and the third
//     /// element is the merge itself.
//     merges: Vec<(usize, usize, Merge<K, T, V>)>,
// }
//
// impl<K, T, V> Trace<K, T, V> {
//     /// Adds the differences in `accumulation` at `time` to the trace.
//     pub fn set_difference(&mut self, time: T, accumulation: Compact<K, V>) {
//
//         // extract the relevant fields
//         let keys = accumulation.keys;
//         let cnts = accumulation.cnts;
//         let vals = accumulation.vals;
//
//         let mut difference = Tier::with_capacities(keys.len(), keys.len(), 0);
//
//         difference.vals = vals;
//         difference.times.push((time, keys.len()));
//
//         // counters for offsets in vals and wgts
//         let mut idxs_offset = 0;
//         let mut vals_offset = 0;
//         for (key, cnt) in keys.into_iter().zip(cnts.into_iter()) {
//
//             idxs_offset += 1;
//             vals_offset += cnt;
//
//             difference.keys.push((key, idxs_offset));
//             difference.idxs.push((0, vals_offset));
//
//         }
//
//         for &(ref key, off) in &difference.keys {
//
//             // prepare a new head cursor, and recover whatever is currently there.
//             let next_position = Offset::new(self.links.len());
//             let prev_position = self.keys.entry_or_insert(key, || next_position);
//
//             // if we inserted a previously absent key
//             if &prev_position.val() == &next_position.val() {
//                 // add the appropriate entry with no next pointer
//                 self.links.push(ListEntry {
//                     time: self.tiers.len() as u32,
//                     vals: off,
//                     next: None
//                 });
//             }
//             // we haven't yet installed next_position, so do that too
//             else {
//                 // add the appropriate entry
//                 self.links.push(ListEntry {
//                     time: self.tiers.len() as u32,
//                     vals: off,
//                     next: Some(*prev_position)
//                 });
//                 *prev_position = next_position;
//             }
//
//         }
//
//         // add the values and weights to the list of timed differences.
//         self.tiers.push(difference);
//
//     }
// }

/// A collection of `(K, T, V, W)` tuples, grouped by `K` then `T` then `V`.
///
/// A `Tier` is a trie-representation of `(K, T, V, W)` tuples, meaning its representation is as a
/// sorted list of these tuples, where each of the fields are then stored separately and run-length
/// coded. The `keys` field is a sorted list of pairs `(K, usize)`, indicating a key and an offset
/// in the next-level array, `idxs`. Likewise, the `idxs` field contains a flat list of pairs
/// `(usize, usize)`, where each interval described by one entry of `keys` is sorted. Each entry
/// `(usize, usize)` indicates a time (element of the `times` field), and an offset in the `vals`
/// field. Finally, the `vals` field has each interval sorted by `V`.
#[derive(Debug, Eq, PartialEq)]
pub struct Tier<K, T, V> {
    /// Pairs of key and offset into `self.idxs`.
    pub keys: Vec<(K, usize)>,
    /// Pairs of idx and offset into `self.vals`.
    pub idxs: Vec<(usize, usize)>,
    /// Pairs of val and weight.
    pub vals: Vec<(V, W)>,
    /// Pairs of timestamp and the number of references to the timestamp.
    pub times: Vec<(T,usize)>,
}


impl<K: Ord, T: Eq, V: Ord+Clone> Tier<K, T, V> {
    /// Constructs a new `Tier` containing no data.
    pub fn new() -> Tier<K, T, V> {
        Tier::with_capacities(0, 0, 0)
    }

    /// Allocates a new `Tier` with initial capacities for `keys`, `idxs`, and `vals`.
    pub fn with_capacities(k: usize, i: usize, v: usize) -> Tier<K, T, V> {
        Tier {
            keys: Vec::with_capacity(k),
            idxs: Vec::with_capacity(i),
            vals: Vec::with_capacity(v),
            times: Vec::new(),
        }
    }

    /// Returns the number of tuples represented by `Self`.
    pub fn len(&self) -> usize {
        self.vals.len()
    }

    /// A helper method used to merge slices `&[(V,W)]` with the same index and push to the result.
    fn merge_and_push(&mut self, idxs: &mut [(usize, &[(V,W)])]) {

        // track the length of `vals` to know if our merge resulted in any `(V,W)` output.
        let vals_len = self.vals.len();

        if idxs.len() == 1 {
            self.vals.extend(idxs[0].1.iter().cloned());
        }
        else {
            // TODO : merge_using may be important here
            self.vals.extend(idxs.iter()
                                         .map(|&(_, ref slice)| slice.iter().cloned())
                                         .merge()
                                         .coalesce());
        }

        // if we produced `(val,wgt)` data, push the new length and the indicated `idx` in result.
        // also increment the reference count for `idx`, so that we know it is important.
        if self.vals.len() > vals_len {
            self.idxs.push((idxs[0].0, self.vals.len()));
            self.times[idxs[0].0].1 += 1;
        }
    }
}


/// Per-tier information used as part of merging tiers.
struct MergePart<K, T, V> {
    /// Source tier to merge from.
    tier: Rc<Tier<K, T, V>>,
    /// Mapping from timestamp indices used in `self.tier` to indices used in the merge result.
    remap: Vec<usize>,
    /// Current key under consideration.
    key: usize,
}

impl<K, T, V> MergePart<K, T, V> {
    /// Constructs a new `MergePart` from a source `Rc<Tier>`.
    fn new(tier: &Rc<Tier<K, T, V>>) -> MergePart<K, T, V> {
        MergePart {
            tier: tier.clone(),
            remap: Vec::with_capacity(tier.times.len()),
            key: 0,
        }
    }
    /// Returns a reference to the part's next key, or `None` if all keys have been exhausted.
    fn key(&self) -> Option<&K> {
        if self.key < self.tier.keys.len() {
            Some(&self.tier.keys[self.key].0)
        }
        else {
            None
        }
    }
}


/// A merge-in-progress of two instances of `Tier<K, T, V>`.
///
/// A `Merge` represents a partial merge of two instances of `Tier<K, T, V>` into one instance,
/// where times are advanced according to a function `advance` supplied to the `new` constructor,
/// and like `(K, T, V, _)` tuples are consolidated into at most on result tuple.
///
/// A `Merge` can execute progressively, allowing a large amount of work to be amortized over the
/// large number of tuples involved. The `MergePart` structs contained in a `Merge` use `Rc<Tier>`
/// fields to capture their tiers, as the `Merge` does not mutate the source `Tier` instances.
pub struct Merge<K, T, V> {
    /// The first tier and associated information.
    part1: MergePart<K, T, V>,
    /// The second tier and associated information.
    part2: MergePart<K, T, V>,
    /// The result tier, in progress.
    result: Tier<K, T, V>,
}


// The `Merge` struct merges two `Tier`s, progressively. Ideally, it does this relatively quickly,
// without lots of sorting and shuffling and such. The common case is likely to be many regions
// left un-adjusted, and it would be good to optimize for this case.
impl<K: Ord+Clone, T: Eq+Clone+Hash, V: Ord+Clone> Merge<K, T, V> {
    /// Constructs a new `Merge` from two instances of `Teir` and a function advancing timestamps.
    ///
    /// This method initiates a merge of two tiers, consolidating their representation which can
    /// potentially reduce the complexity and amount of memory required to describe a trace. The
    /// required inputs are two `Rc<Tier<K, T, V>>` instances to merge, and a function `advance`
    /// from `&T` to `T` indicating how timestamps should be advanced.
    ///
    /// As part of initiating the merge, `new` will scan through the timestamps used by each source
    /// `Tier`, advancing each and creating a mapping from timestamp indices in each source to new
    /// advanced and unified timestamp indices.
    pub fn new<F: Fn(&T)->T>(tier1: &Rc<Tier<K, T, V>>, tier2: &Rc<Tier<K, T, V>>, advance: &F) -> Merge<K, T, V> {

        // construct wrappers for each tier.
        let part1 = MergePart::<K, T, V>::new(tier1);
        let part2 = MergePart::<K, T, V>::new(tier2);

        // prepare the result, which we will adjust further before returning.
        let mut result = Merge { part1: part1, part2: part2, result: Tier::<K, T, V>::new() };

        // advance times in tier1, update `times_map` and `result.part1.remap`,
        let mut times_map = HashMap::new();
        for &(ref time, count) in tier1.times.iter() {
            if count > 0 {
                let time = advance(time);
                if !times_map.contains_key(&time) {
                    let len = times_map.len();
                    times_map.insert(time.clone(), len);
                    result.result.times.push((time.clone(), 0));
                }

                result.part1.remap.push(times_map[&time]);
            }
        }

        // advance times in tier2, update `times_map` and `result.part1.remap`,
        for &(ref time, count) in tier2.times.iter() {
            if count > 0 {
                let time = advance(time);
                if !times_map.contains_key(&time) {
                    let len = times_map.len();
                    times_map.insert(time.clone(), len);
                    result.result.times.push((time.clone(), 0));
                }

                result.part2.remap.push(times_map[&time]);
            }
        }

        result
    }

    /// Advances the `Merge` by one step, returning the merged tiers if it is now complete.
    ///
    /// The `step` method considers the next key proposed by `tier1` and `tier2`, and populates
    /// `self.result` as appropriate. If both `tier1` and `tier2` have the same key and the merged
    /// results cancel one-another, `self.result` may not actually change (although the merge has
    /// performed useful work).
    ///
    /// The `step` method returns `Some(result)` if the merge is now complete, and `None` if it is
    /// not yet complete, and should be called more.
    ///
    /// Once `step` returns a result, the `Merge` contains an empty `result` field. It is then a
    /// logic error to do anything other than discard the `Merge`, though the usage patterns don't
    /// currently enforce this.
    pub fn step(&mut self) -> Option<Tier<K, T, V>> {

        // the intended logic here is that we must first determine which keys in each of the input
        // tiers we are going to merge. having done this, we populate a vector `to_merge` of pairs
        // `(idx, &[(val, wgt)])`, which is then sorted by `idx` and subranges of the sorted vector
        // and then passed to `merge_and_push`, which performs a merge for several `&[(val, wgt)]`
        // which correspond to the same `idx`.

        // note that even if only a single key is selected, because timestamps have been advanced
        // there may be indices that now occur multiple times. we could consider adding a fast-path
        // for the case where the entries of `to_merge` are already sorted, which could be reduced
        // to a memcpy from the `vals` fields of the corresponding tiers. be careful that just
        // because `to_merge` is sorted does not mean that it derives from only one tier.

        // determine the minimum key, and which of part1 and part2 are involved.
        let (min1, min2) = {
            let (min_key, min1, min2) = match (self.part1.key(), self.part2.key()) {
                (None, None)             => { return Some(::std::mem::replace(&mut self.result, Tier::new())); },
                (Some(key), None)        => (key, true, false),
                (None, Some(key))        => (key, false, true),
                (Some(key1), Some(key2)) => {
                    match key1.cmp(key2) {
                        Ordering::Less    => (key1, true, false),
                        Ordering::Equal   => (key1, true, true),
                        Ordering::Greater => (key2, false, true),
                    }
                },
            };

            // create a vector to populate with &[(V,W)] slices to merge.
            // TODO : can we stash this; maybe with an unsafe transmute?
            let mut to_merge = Vec::new();

            if min1 {
                // the lower bound is either the previous offset or zero.
                let lower = if self.part1.key > 0 { self.part1.tier.keys[self.part1.key-1].1 } else { 0 };
                for i in lower .. self.part1.tier.keys[self.part1.key].1 {
                    let (idx, off) = self.part1.tier.idxs[i];
                    let lower = if i > 0 { self.part1.tier.idxs[i-1].1 } else { 0 };
                    to_merge.push((self.part1.remap[idx], &self.part1.tier.vals[lower .. off]));
                }
            }

            if min2 {
                // the lower bound is either the previous offset or zero.
                let lower = if self.part2.key > 0 { self.part2.tier.keys[self.part2.key-1].1 } else { 0 };
                for i in lower .. self.part2.tier.keys[self.part2.key].1 {
                    let (idx, off) = self.part2.tier.idxs[i];
                    let lower = if i > 0 { self.part2.tier.idxs[i-1].1 } else { 0 };
                    to_merge.push((self.part2.remap[idx], &self.part2.tier.vals[lower .. off]));
                }
            }

            // `to_merge` now has everything we need to merge.
            // we now sort it by index, to co-locate indices.
            to_merge.sort_by(|x,y| (x.0).cmp(&(y.0)));

            // capture the current list of idxs so that we know if our additions have resulted in
            // any data. we should not push the key if we pushed no idxs due to consolidation.
            let idxs_len = self.result.idxs.len();

            let mut old_idx = 0;
            let mut idx_cnt = 0;
            for i in 0..to_merge.len() {

                // if the idx changes we should merge.
                if i > 0 && old_idx != to_merge[i].0 {
                    self.result.merge_and_push(&mut to_merge[i - idx_cnt .. i]);
                    old_idx = to_merge[i].0;
                    idx_cnt = 0;
                }

                idx_cnt += 1;
            }

            let len = to_merge.len();
            self.result.merge_and_push(&mut to_merge[len - idx_cnt .. len]);

            // if the merge resulted in data, push a clone of the key and the current offset.
            if self.result.idxs.len() > idxs_len {
                self.result.keys.push((min_key.clone(), self.result.idxs.len()));
            }

            (min1, min2)
        };

        // we can only advance keys after we release the borrow on the key.
        if min1 { self.part1.key += 1; }
        if min2 { self.part2.key += 1; }

        None
    }
}

#[cfg(test)]
mod tests {

    use std::rc::Rc;
    use super::{Tier, Merge};

    #[test] fn merge_none() {

        let tier1: Rc<Tier<u64, u64, u64>> = Rc::new(Tier::new());
        let tier2: Rc<Tier<u64, u64, u64>> = Rc::new(Tier::new());

        let mut merge = Merge::new(&tier1, &tier2, &|_x| 0);

        loop {
            println!("step");
            if let Some(result) = merge.step() {
                println!("yay");
                println!("{:?}", result);

                assert_eq!(result, Tier::new());

                break;
            }
        }
    }
    #[test] fn merge_one() {

        let tier1 = Rc::new(Tier {
            keys: vec![("a", 2), ("b", 5), ("c", 6)],
            idxs: vec![(0, 2), (1, 3), (0, 5), (1, 6), (2, 7), (0, 9)],
            vals: vec![(0, 1), (1, 1), (0, -1), (0,1), (1,1), (1,-1), (0,-1), (2, 1), (3, 1)],
            times: vec![(0, 3), (1, 2), (2, 1)],
        });

        let tier2 = Rc::new(Tier::new());

        let mut merge = Merge::new(&tier1, &tier2, &|_x| 0);

        loop {
            println!("step");
            if let Some(result) = merge.step() {
                println!("yay");
                println!("{:?}", result);

                assert_eq!(result, Tier {
                    keys: vec![("a", 1), ("c", 2)],
                    idxs: vec![(0, 1), (0, 3)],
                    vals: vec![(1, 1), (2, 1), (3, 1)],
                    times: vec![(0, 2)],
                });

                break;
            }
        }
    }
}
