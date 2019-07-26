//! Common logic for the consolidation of vectors of monoids.
//!
//! Often we find ourselves with collections of records with associated weights (often
//! integers) where we want to reduce the collection to the point that each record occurs
//! at most once, with the accumulated weights. These methods supply that functionality.

use crate::difference::Monoid;

/// Sorts and consolidates `vec`.
pub fn consolidate<T: Ord, R: Monoid>(vec: &mut Vec<(T, R)>) {
    consolidate_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs with the same first
/// element of a pair by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_from<T: Ord, R: Monoid>(vec: &mut Vec<(T, R)>, offset: usize) {
    let length = consolidate_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_slice<T: Ord, R: Monoid>(vec: &mut [(T, R)]) -> usize {

    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    if vec.len() > 1 {
        let mut offset = 0;
        vec.sort_by(|x,y| x.0.cmp(&y.0));
        for index in 1 .. vec.len() {
            if vec[offset].0 == vec[index].0 {
                // TODO: LLVM does not optimize this to the point that it avoids a variety of compares.
                //       We could consider an unsafe implementation, if the performance here registers.
                let (lo, hi) = vec.split_at_mut(index);
                lo[offset].1 += &hi[0].1;
            }
            else {
                if !vec[offset].1.is_zero() {
                    offset += 1;
                }
                vec.swap(offset, index);
            }
        }
        if !vec[offset].1.is_zero() {
            offset += 1;
        }
        
        offset
    }
    else {
        vec.len()
    }
}

/// Sorts and consolidates `vec`.
pub fn consolidate_updates<D: Ord, T: Ord, R: Monoid>(vec: &mut Vec<(D, T, R)>) {
    consolidate_updates_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs with the same first
/// element of a pair by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_updates_from<D: Ord, T: Ord, R: Monoid>(vec: &mut Vec<(D, T, R)>, offset: usize) {
    let length = consolidate_updates_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Monoid>(vec: &mut [(D, T, R)]) -> usize {

    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    if vec.len() > 1 {
        let mut offset = 0;
        vec.sort_by(|x,y| (&x.0, &x.1).cmp(&(&y.0, &y.1)));
        for index in 1 .. vec.len() {
            if vec[offset].0 == vec[index].0 && vec[offset].1 == vec[index].1 {
                // TODO: LLVM does not optimize this to the point that it avoids a variety of compares.
                //       We could consider an unsafe implementation, if the performance here registers.
                let (lo, hi) = vec.split_at_mut(index);
                lo[offset].2 += &hi[0].2;
            }
            else {
                if !vec[offset].2.is_zero() {
                    offset += 1;
                }
                vec.swap(offset, index);
            }
        }
        if !vec[offset].2.is_zero() {
            offset += 1;
        }

        offset
    }
    else {
        vec.len()
    }
}
