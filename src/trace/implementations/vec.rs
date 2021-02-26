//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with either
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key` is ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is ordered.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

use std::cmp::Ordering;

use ::Diff;
use lattice::Lattice;

use trace::{Batch, BatchReader, Builder, Merger, Cursor};
use trace::description::Description;

// use trace::layers::MergeBuilder;

// use super::spine_fueled::Spine;
use super::merge_batcher::MergeBatcher;

#[derive(Debug, Abomonation)]
pub struct VecBatch<K: Ord, V: Ord, T: Lattice, R> {
    pub list: Vec<(K, V, T, R)>,
    pub desc: Description<T>,
}

impl<K, V, T, R> BatchReader<K, V, T, R> for VecBatch<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+'static, R: Diff {
    type Cursor = VecCursor;
    fn cursor(&self) -> Self::Cursor { VecCursor { key_pos: 0, val_pos: 0 } }
    fn len(&self) -> usize { self.list.len() }
    fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R> Batch<K, V, T, R> for VecBatch<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
    type Batcher = MergeBatcher<K, V, T, R, Self>;
    type Builder = VecBuilder<K, V, T, R>;
    type Merger = VecMerger<K, V, T, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        VecMerger::new(self, other)
    }
}

impl<K, V, T, R> VecBatch<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
    fn advance_builder_from(list: &mut Vec<(K,V,T,R)>, frontier: &[T], key_pos: usize) {

        for index in key_pos .. list.len() {
            list[index].2 = list[index].2.advance_by(frontier);
        }

        // TODO: Already sorted by key, val; could sort restricted ranges.
        list[key_pos ..].sort();

        for index in key_pos .. (list.len() - 1) {
            if list[index].0 == list[index+1].0 && list[index].1 == list[index+1].1 && list[index].2 == list[index+1].2 {
                list[index+1].3 = list[index+1].3 + list[index].3;
                list[index].3 = R::zero();
            }
        }

        let mut write_position = key_pos;
        for index in key_pos .. list.len() {
            if !list[index].3.is_zero() {
                list.swap(write_position, index);
                write_position += 1;
            }
        }

        list.truncate(write_position);
    }
}

/// State for an in-progress merge.
pub struct VecMerger<K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff> {
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: Vec<(K,V,T,R)>,
    description: Description<T>,
}

impl<K, V, T, R> Merger<K, V, T, R, VecBatch<K, V, T, R>> for VecMerger<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {
    fn new(batch1: &VecBatch<K, V, T, R>, batch2: &VecBatch<K, V, T, R>) -> Self {

        assert!(batch1.upper() == batch2.lower());

        let since = if batch1.description().since().iter().all(|t1| batch2.description().since().iter().any(|t2| t2.less_equal(t1))) {
            batch2.description().since()
        }
        else {
            batch1.description().since()
        };

        let description = Description::new(batch1.lower(), batch2.upper(), since);

        VecMerger {
            lower1: 0,
            upper1: batch1.list.len(),
            lower2: 0,
            upper2: batch2.list.len(),
            result: Vec::with_capacity(batch1.len() + batch2.len()),
            description: description,
        }
    }
    fn done(self) -> VecBatch<K, V, T, R> {

        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        VecBatch {
            list: self.result,
            desc: self.description,
        }
    }
    fn work(&mut self, source1: &VecBatch<K,V,T,R>, source2: &VecBatch<K,V,T,R>, frontier: &Option<Vec<T>>, fuel: &mut usize) {

        let mut effort = 0;

        let initial_pos = self.result.len();

        // while both mergees are still active
        while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {

            let tuple1 = (&source1.list[self.lower1].0,&source1.list[self.lower1].1,&source1.list[self.lower1].2);
            let tuple2 = (&source2.list[self.lower2].0,&source2.list[self.lower2].1,&source1.list[self.lower2].2);

            match tuple1.cmp(&tuple2) {
                Ordering::Less => {
                    self.result.push(source1.list[self.lower1].clone());
                    self.lower1 += 1;
                    effort += 1;
                },
                Ordering::Equal => {
                    let mut new_element = source1.list[self.lower1].clone();
                    new_element.3 = new_element.3 + source2.list[self.lower2].3;
                    self.result.push(new_element);
                    self.lower1 += 1;
                    self.lower2 += 1;
                    effort += 1;
                }
                Ordering::Greater => {
                    self.result.push(source2.list[self.lower2].clone());
                    self.lower2 += 1;
                    effort += 1;
                }
            }
        }

        if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
            // these are just copies, so let's bite the bullet and just do them.
            if self.lower1 < self.upper1 {
                self.result.extend(source1.list[self.lower1 .. self.upper1].iter().cloned());
                self.lower1 = self.upper1;
            }
            if self.lower2 < self.upper2 {
                self.result.extend(source2.list[self.lower2 .. self.upper2].iter().cloned());
                self.lower2 = self.upper2;
            }
        }

        // if we are supplied a frontier, we should compact.
        if let Some(frontier) = frontier.as_ref() {
            VecBatch::advance_builder_from(&mut self.result, frontier, initial_pos)
        }

        if effort >= *fuel { *fuel = 0; }
        else                { *fuel -= effort; }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct VecCursor {
    key_pos: usize,
    val_pos: usize,
}

impl<K, V, T, R> Cursor<K, V, T, R> for VecCursor
where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {

    type Storage = VecBatch<K, V, T, R>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &storage.list[self.key_pos].0 }
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { &storage.list[self.val_pos].1 }
    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key_val = (self.key(storage), self.val(storage));
        let mut temp_index = self.val_pos;
        while temp_index < storage.list.len() && key_val == (&storage.list[temp_index].0,&storage.list[temp_index].1) {
            logic(&storage.list[temp_index].2, storage.list[temp_index].3);
            temp_index += 1;
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.key_pos < storage.list.len() }
    fn val_valid(&self, storage: &Self::Storage) -> bool {
        self.val_pos < storage.list.len() && storage.list[self.key_pos].0 == storage.list[self.val_pos].0
    }
    fn step_key(&mut self, storage: &Self::Storage){
        if let Some(key) = self.get_key(storage) {
            let step = advance(&storage.list[self.key_pos..], |tuple| &tuple.0 == key);
            self.key_pos += step;
        }
    }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) {
        if self.key_valid(storage) {
            let step = advance(&storage.list[self.key_pos..], |tuple| &tuple.0 < key);
            self.key_pos += step;
        }
    }
    fn step_val(&mut self, storage: &Self::Storage) {
        if let Some(val) = self.get_val(storage) {
            let step = advance(&storage.list[self.val_pos..], |tuple| &tuple.0 == self.key(storage) && &tuple.1 == val);
            self.val_pos += step;
        }
    }
    fn seek_val(&mut self, storage: &Self::Storage, val: &V) {
        if self.val_valid(storage) {
            let step = advance(&storage.list[self.val_pos..], |tuple| &tuple.0 == self.key(storage) && &tuple.1 < val);
            self.val_pos += step;
        }
    }
    fn rewind_keys(&mut self, _storage: &Self::Storage) { self.key_pos = 0; }
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        if let Some(key) = self.get_key(storage) {
            self.rewind_keys(storage);
            self.seek_key(storage, key);
        }
    }
}


/// A builder for creating layers from unsorted update tuples.
pub struct VecBuilder<K: Ord, V: Ord, T: Ord+Lattice, R: Diff> {
    list: Vec<(K,V,T,R)>,
}

impl<K, V, T, R> Builder<K, V, T, R, VecBatch<K, V, T, R>> for VecBuilder<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, R: Diff {

    fn new() -> Self { VecBuilder { list: Vec::new() } }
    fn with_capacity(cap: usize) -> Self { VecBuilder { list: Vec::with_capacity(cap) } }

    #[inline]
    fn push(&mut self, (key, val, time, diff): (K, V, T, R)) { self.list.push((key,val,time,diff)); }

    #[inline(never)]
    fn done(mut self, lower: &[T], upper: &[T], since: &[T]) -> VecBatch<K, V, T, R> {
        self.list.sort();
        VecBatch {
            list: self.list,
            desc: Description::new(lower, upper, since)
        }
    }
}


/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
#[inline(never)]
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {

        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step = step << 1;
        }

        // advance in exponentially shrinking steps.
        step = step >> 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step = step >> 1;
        }

        index += 1;
    }

    index
}
