//! A trait for maps from `K` to `V` and implementations. 
//!
//! The `Lookup<K,V>` trait corresponds to a map from `K` to `V`. This can be
//! implemented many different ways based on different requirements. A `HashMap<K,V>`
//! is a general solution for `K: Hash`, whereas a `Vec<(K,V)>` is a primitive but
//! often effective solution for `K: Eq`, the minimal requirement.
//!
//! Other implementations include a `Vec<Option<V>>` for `usize` keys, and a `RHHMap` 
//! implementation based on a Robin Hood hashing implementation.
use std::hash::Hash;
use std::collections::HashMap;

use timely_sort::Unsigned;

use collection::robin_hood::RHHMap;
use linear_map::LinearMap;
use vec_map::VecMap;

/// A map from `K` to `V`. 
pub trait Lookup<K: Eq, V> {
    /// Recovers a reference to the value associated with a key.
    fn get_ref<'a>(&'a self, &K)->Option<&'a V>;
    /// Recovers a mutable referecne to the value associated with a key.
    fn get_mut<'a>(&'a mut self, &K)->Option<&'a mut V>;
    /// Recovers a mutable reference, inserting a new value if absent.
    fn entry_or_insert<F: FnOnce()->V>(&mut self, K, F) -> &mut V;
    /// Removes the key and returns its value if it exists.
    fn remove_key(&mut self, &K) -> Option<V>;
}

impl<K: Eq+Clone, V, F: Fn(&K)->usize> Lookup<K, V> for RHHMap<K, V, F> {
    #[inline]
    fn get_ref<'a>(&'a self, key: &K) -> Option<&'a V> { self.get_ref(key) }
    #[inline]
    fn get_mut<'a>(&'a mut self, key: &K) -> Option<&'a mut V> { self.get_mut(key) }
    #[inline]
    fn entry_or_insert<G: FnOnce()->V>(&mut self, key: K, func: G) -> &mut V {
        let cloned = key.clone();
        if self.get_ref(&key).is_none() {
            let val = func();
            self.insert(key, val);
        }
        self.get_mut(&cloned).unwrap()
    }
    #[inline]
    fn remove_key(&mut self, key: &K) -> Option<V> {
        self.remove(key).map(|x| x.1)
    }
}

impl<K: Hash+Eq+'static, V: 'static> Lookup<K,V> for HashMap<K,V> {
    #[inline]
    fn get_ref<'a>(&'a self, key: &K) -> Option<&'a V> { self.get(key) }
    #[inline]
    fn get_mut<'a>(&'a mut self, key: &K) -> Option<&'a mut V> { self.get_mut(key) }
    #[inline]
    fn entry_or_insert<F: FnOnce()->V>(&mut self, key: K, func: F) -> &mut V {
        self.entry(key).or_insert_with(func)
    }
    #[inline]
    fn remove_key(&mut self, key: &K) -> Option<V> { self.remove(key) }
}

impl<K: Eq, V> Lookup<K, V> for LinearMap<K, V> {
    #[inline]
    fn get_ref<'a>(&'a self, key: &K) -> Option<&'a V> { self.get(key) }
    #[inline]
    fn get_mut<'a>(&'a mut self, key: &K) -> Option<&'a mut V> { self.get_mut(key) }
    #[inline]
    fn entry_or_insert<F: FnOnce()->V>(&mut self, key: K, func: F) -> &mut V {
        self.entry(key).or_insert_with(func)
    }
    #[inline]
    fn remove_key(&mut self, key: &K) -> Option<V> { self.remove(key) }
}

impl<V: 'static, U: Unsigned> Lookup<U,V> for (VecMap<V>, u64) {
    #[inline]
    fn get_ref<'a>(&'a self, key: &U) -> Option<&'a V> {
        let key = (key.as_u64() >> self.1) as usize;
        self.0.get(key)
    }
    #[inline]
    fn get_mut<'a>(&'a mut self, key: &U) -> Option<&'a mut V> {
        let key = (key.as_u64() >> self.1) as usize;
        self.0.get_mut(key)
    }
    #[inline]
    fn entry_or_insert<F: FnOnce()->V>(&mut self, key: U, func: F) -> &mut V {
        let key = (key.as_u64() >> self.1) as usize;
        self.0.entry(key).or_insert_with(func)
    }
    #[inline]
    fn remove_key(&mut self, key: &U) -> Option<V> {
        let key = (key.as_u64() >> self.1) as usize;
        self.0.remove(key)
    }
}
