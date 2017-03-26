//! Traits and types related to the hashing of data.
//!
//! These traits and types are in support of a flexible approach to data distribution and organization,
//! in which we might like to more explicitly manage how certain types are handled.

use std::hash::Hasher;
use std::ops::Deref;

use abomonation::Abomonation;

use timely_sort::Unsigned;

/// Types with a `hashed` method, producing a hash value of some unsigned output type.
pub trait Hashable {
    /// The type of the output hash.
    type Output: Unsigned+Copy;
    /// A hash of the associated value.
    fn hashed(&self) -> Self::Output;
}

impl<T: ::std::hash::Hash> Hashable for T {
    type Output = u64;
    fn hashed(&self) -> u64 {
        let mut h: ::fnv::FnvHasher = Default::default();
        self.hash(&mut h);
        h.finish()
    }
}

/// A marker trait for types whose `Ord` implementation orders first by `hashed()`.
pub trait HashOrdered : Ord+Hashable { }
impl<T: Ord+Hashable> HashOrdered for OrdWrapper<T> { }
impl<T: Ord+Hashable> HashOrdered for HashableWrapper<T> { }
impl<T: Unsigned+Copy> HashOrdered for UnsignedWrapper<T> { }



// It would be great to use the macros for these, but I couldn't figure out how to get it
// to work with constraints (i.e. `Hashable`) on the generic parameters.
impl<T: Ord+Hashable+Abomonation> Abomonation for OrdWrapper<T> {

    #[inline] unsafe fn entomb(&self, _writer: &mut Vec<u8>) {
        self.item.entomb(_writer);
    }
    #[inline] unsafe fn embalm(&mut self) {
        self.item.embalm();
    }
    #[inline] unsafe fn exhume<'a,'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let temp = bytes; 
        bytes = if let Some(bytes) = self.item.exhume(temp) { bytes } else { return None };
        Some(bytes)
    }
}

// It would be great to use the macros for these, but I couldn't figure out how to get it
// to work with constraints (i.e. `Hashable`) on the generic parameters.
impl<T: Hashable+Abomonation> Abomonation for HashableWrapper<T> {

    #[inline] unsafe fn entomb(&self, _writer: &mut Vec<u8>) {
        self.item.entomb(_writer);
    }
    #[inline] unsafe fn embalm(&mut self) {
        self.item.embalm();
    }
    #[inline] unsafe fn exhume<'a,'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let temp = bytes; 
        bytes = if let Some(bytes) = self.item.exhume(temp) { bytes } else { return None };
        Some(bytes)
    }
}

impl<T: Unsigned+Copy+Hashable+Abomonation> Abomonation for UnsignedWrapper<T> {

    #[inline] unsafe fn entomb(&self, _writer: &mut Vec<u8>) {
        self.item.entomb(_writer);
    }
    #[inline] unsafe fn embalm(&mut self) {
        self.item.embalm();
    }
    #[inline] unsafe fn exhume<'a,'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let temp = bytes; 
        bytes = if let Some(bytes) = self.item.exhume(temp) { bytes } else { return None };
        Some(bytes)
    }
}


/// A wrapper around hashable types that ensures an implementation of `Ord` that compares 
/// hash values first.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct OrdWrapper<T: Ord+Hashable> {
    /// The item, so you can grab it.
    pub item: T
}

impl<T: Ord+Hashable> PartialOrd for OrdWrapper<T> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        (self.item.hashed(), &self.item).partial_cmp(&(other.item.hashed(), &other.item))
    }
}
impl<T: Ord+Hashable> Ord for OrdWrapper<T> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        (self.item.hashed(), &self.item).cmp(&(other.item.hashed(), &other.item))        
    }
}

impl<T: Ord+Hashable> Hashable for OrdWrapper<T> {
    type Output = T::Output;
    fn hashed(&self) -> T::Output { self.item.hashed() }
}

impl<T: Ord+Hashable> Deref for OrdWrapper<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.item }
}


/// Wrapper to stash hash value along actual value.
#[derive(Clone, Default, Ord, PartialOrd, Eq, PartialEq, Debug, Copy)]
pub struct HashableWrapper<T: Hashable> {
    hash: T::Output,
    /// The item, for reference.
    pub item: T,
}

impl<T: Hashable> Hashable for HashableWrapper<T> {
    type Output = T::Output;
    #[inline(always)]
    fn hashed(&self) -> T::Output { self.hash }
}

impl<T: Hashable> Deref for HashableWrapper<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T { &self.item }
}

impl<T: Hashable> From<T> for HashableWrapper<T> {
    #[inline(always)]
    fn from(item: T) -> HashableWrapper<T> { 
        HashableWrapper {
            hash: item.hashed(),
            item: item,
        }
    }
}

/// A wrapper around an unsigned integer, providing `hashed` as the value itself.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Default, Debug, Copy)]
pub struct UnsignedWrapper<T: Unsigned+Copy> {
    /// The item.
    pub item: T,
}

impl<T: Unsigned+Copy> Hashable for UnsignedWrapper<T> {
    type Output = T;
    #[inline(always)]
    fn hashed(&self) -> Self::Output { self.item }
}

impl<T: Unsigned+Copy> Deref for UnsignedWrapper<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T { &self.item }
}

impl<T: Unsigned+Copy> From<T> for UnsignedWrapper<T> {
    #[inline(always)]
    fn from(item: T) -> Self { UnsignedWrapper { item: item } }
}