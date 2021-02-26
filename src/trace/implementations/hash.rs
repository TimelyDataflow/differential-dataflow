//! Trace and batch implementations based on Robin Hood hashing.
//!
//! The types and type aliases in this module start with either 
//! 
//! * `HashVal`: Collections whose data have the form `(key, val)` where `key` is hash-ordered.
//! * `HashKey`: Collections whose data have the form `key` where `key` is hash-ordered.
//!
//! Although `HashVal` is more general than `HashKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

use std::rc::Rc;

use ::Diff;
use hashable::HashOrdered;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::hashed::{HashedLayer, HashedBuilder, HashedCursor};
use trace::layers::ordered::{OrderedLayer, OrderedBuilder};
use trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder};

use lattice::Lattice;
use trace::{Batch, BatchReader, Builder, Cursor};
use trace::description::Description;

use super::spine::Spine;
use super::radix_batcher::RadixBatcher;

/// A trace implementation using a spine of hash-map batches.
pub type HashValSpine<K, V, T, R> = Spine<K, V, T, R, Rc<HashValBatch<K, V, T, R>>>;
/// A trace implementation for empty values using a spine of hash-map batches.
pub type HashKeySpine<K, T, R> = Spine<K, (), T, R, Rc<HashKeyBatch<K, T, R>>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct HashValBatch<K: HashOrdered, V: Ord, T: Lattice, R: Diff> {
    /// Where all the dataz is.
    pub layer: HashedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>>,
    /// Description of the update times this layer represents.
    pub desc: Description<T>,
}

impl<K, V, T, R> BatchReader<K, V, T, R> for Rc<HashValBatch<K, V, T, R>>
where K: Clone+Default+HashOrdered+'static, V: Clone+Ord+'static, T: Lattice+Ord+Clone+Default+'static, R: Diff {
    type Cursor = HashValCursor<V, T, R>;
    fn cursor(&self) -> (Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage) { 
        let cursor = HashValCursor {
            cursor: self.layer.cursor()
        };

        (cursor, self.clone())
    }
    fn len(&self) -> usize { <HashedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::tuples(&self.layer) }
    fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R> Batch<K, V, T, R> for Rc<HashValBatch<K, V, T, R>>
where K: Clone+Default+HashOrdered+'static, V: Clone+Ord+'static, T: Lattice+Ord+Clone+Default+'static, R: Diff {
    type Batcher = RadixBatcher<K, V, T, R, Self>;
    type Builder = HashValBuilder<K, V, T, R>;
    fn merge(&self, other: &Self) -> Self {

        // Things are horribly wrong if this is not true.
        assert!(self.desc.upper() == other.desc.lower());

        // one of self.desc.since or other.desc.since needs to be not behind the other...
        let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.less_equal(t1))) {
            other.desc.since()
        }
        else {
            self.desc.since()
        };
        
        Rc::new(HashValBatch {
            layer: <HashedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>> as Trie>::merge(&self.layer, &other.layer),
            desc: Description::new(self.desc.lower(), other.desc.upper(), since),
        })
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct HashValCursor<V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff> {
    cursor: HashedCursor<OrderedLayer<V, OrderedLeaf<T, R>>>,
}

impl<K, V, T, R> Cursor<K, V, T, R> for HashValCursor<V, T, R> 
where K: Clone+HashOrdered, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {

    type Storage = Rc<HashValBatch<K, V, T, R>>;//OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>>>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { &self.cursor.child.key(&storage.layer.vals) }
    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.child.child.rewind(&storage.layer.vals.vals);
        while self.cursor.child.child.valid(&storage.layer.vals.vals) {
            logic(&self.cursor.child.child.key(&storage.layer.vals.vals).0, self.cursor.child.child.key(&storage.layer.vals.vals).1);
            self.cursor.child.child.step(&storage.layer.vals.vals);
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
    fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.child.valid(&storage.layer.vals) }
    fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek(&storage.layer, key); }
    fn step_val(&mut self, storage: &Self::Storage) { self.cursor.child.step(&storage.layer.vals); }
    fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.child.seek(&storage.layer.vals, val); }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); }
    fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.child.rewind(&storage.layer.vals); }
}

/// A builder for creating layers from unsorted update tuples.
pub struct HashValBuilder<K: HashOrdered, V: Ord, T: Ord+Lattice, R: Diff> {
    builder: HashedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>,
}

impl<K, V, T, R> Builder<K, V, T, R, Rc<HashValBatch<K, V, T, R>>> for HashValBuilder<K, V, T, R> 
where K: Clone+Default+HashOrdered+'static, V: Ord+Clone+'static, T: Lattice+Ord+Clone+Default+'static, R: Diff {

    fn new() -> Self { 
        HashValBuilder { 
            builder: HashedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>::new() 
        } 
    }
    fn with_capacity(cap: usize) -> Self { 
        HashValBuilder { 
            builder: HashedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>>>::with_capacity(cap) 
        } 
    }

    #[inline]
    fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
        self.builder.push_tuple((key, (val, (time, diff))));
    }

    #[inline(never)]
    fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<HashValBatch<K, V, T, R>> {
        Rc::new(HashValBatch {
            layer: self.builder.done(),
            desc: Description::new(lower, upper, since)
        })
    }
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug)]
pub struct HashKeyBatch<K: HashOrdered, T: Lattice, R> {
    /// Where all the dataz is.
    pub layer: HashedLayer<K, OrderedLeaf<T, R>>,
    /// Description of the update times this layer represents.
    pub desc: Description<T>,
}

impl<K, T, R> BatchReader<K, (), T, R> for Rc<HashKeyBatch<K, T, R>>
where K: Clone+Default+HashOrdered+'static, T: Lattice+Ord+Clone+Default+'static, R: Diff {
    type Cursor = HashKeyCursor<T, R>;
    // fn cursor(&self) -> (Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage) { 

    fn cursor(&self) -> (Self::Cursor, <Self::Cursor as Cursor<K, (), T, R>>::Storage) { 
        let cursor = HashKeyCursor {
            empty: (),
            valid: true,
            cursor: self.layer.cursor(),
        };
        (cursor, self.clone())

        // HashKeyCursor {
        //     empty: (),
        //     valid: true,
        //     cursor: self.layer.cursor(OwningRef::new(self.clone()).map(|x| &x.layer).erase_owner()),
        // } 
    }
    fn len(&self) -> usize { <HashedLayer<K, OrderedLeaf<T, R>> as Trie>::tuples(&self.layer) }
    fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, T, R> Batch<K, (), T, R> for Rc<HashKeyBatch<K, T, R>>
where K: Clone+Default+HashOrdered+'static, T: Lattice+Ord+Clone+Default+'static, R: Diff {
    type Batcher = RadixBatcher<K, (), T, R, Self>;
    type Builder = HashKeyBuilder<K, T, R>;
    fn merge(&self, other: &Self) -> Self {

        // Things are horribly wrong if this is not true.
        assert!(self.desc.upper() == other.desc.lower());

        // one of self.desc.since or other.desc.since needs to be not behind the other...
        let since = if self.desc.since().iter().all(|t1| other.desc.since().iter().any(|t2| t2.less_equal(t1))) {
            other.desc.since()
        }
        else {
            self.desc.since()
        };
        
        Rc::new(HashKeyBatch {
            layer: <HashedLayer<K, OrderedLeaf<T, R>> as Trie>::merge(&self.layer, &other.layer),
            desc: Description::new(self.desc.lower(), other.desc.upper(), since),
        })
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct HashKeyCursor<T: Lattice+Ord+Clone, R: Diff> {
    valid: bool,
    empty: (),
    cursor: HashedCursor<OrderedLeaf<T, R>>,
}

impl<K: Clone+HashOrdered, T: Lattice+Ord+Clone, R: Diff> Cursor<K, (), T, R> for HashKeyCursor<T, R> {

    type Storage = Rc<HashKeyBatch<K, T, R>>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
    fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () { unsafe { ::std::mem::transmute(&self.empty) } }
    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.child.rewind(&storage.layer.vals);
        while self.cursor.child.valid(&storage.layer.vals) {
            logic(&self.cursor.child.key(&storage.layer.vals).0, self.cursor.child.key(&storage.layer.vals).1);
            self.cursor.child.step(&storage.layer.vals);
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
    fn val_valid(&self, _storage: &Self::Storage) -> bool { self.valid }
    fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); self.valid = true; }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek(&storage.layer, key); self.valid = true; }
    fn step_val(&mut self, _storage: &Self::Storage) { self.valid = false; }
    fn seek_val(&mut self, _storage: &Self::Storage, _val: &()) { }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); self.valid = true; }
    fn rewind_vals(&mut self, _storage: &Self::Storage) { self.valid = true; }
}

/// A builder for creating layers from unsorted update tuples.
pub struct HashKeyBuilder<K: HashOrdered, T: Ord+Lattice, R: Diff> {
    builder: HashedBuilder<K, OrderedLeafBuilder<T, R>>,
}

impl<K, T, R> Builder<K, (), T, R, Rc<HashKeyBatch<K, T, R>>> for HashKeyBuilder<K, T, R> 
where K: Clone+Default+HashOrdered+'static, T: Lattice+Ord+Clone+Default+'static, R: Diff {

    fn new() -> Self { 
        HashKeyBuilder { 
            builder: HashedBuilder::<K, OrderedLeafBuilder<T, R>>::new() 
        } 
    }
    fn with_capacity(cap: usize) -> Self { 
        HashKeyBuilder { 
            builder: HashedBuilder::<K, OrderedLeafBuilder<T, R>>::with_capacity(cap) 
        } 
    }

    #[inline]
    fn push(&mut self, (key, _, time, diff): (K, (), T, R)) {
        self.builder.push_tuple((key, (time, diff)));
    }

    #[inline(never)]
    fn done(self, lower: &[T], upper: &[T], since: &[T]) -> Rc<HashKeyBatch<K, T, R>> {
        Rc::new(HashKeyBatch {
            layer: self.builder.done(),
            desc: Description::new(lower, upper, since)
        })
    }
}

