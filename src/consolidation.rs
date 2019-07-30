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
pub fn consolidate_slice<T: Ord, R: Monoid>(slice: &mut [(T, R)]) -> usize {

    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    if slice.len() > 1 {

        slice.sort_by(|x,y| x.0.cmp(&y.0));

        let mut offset = 0;
        for index in 1 .. slice.len() {

            // The following unsafe block elides various bounds checks, using the reasoning that `offset`
            // is always strictly less than `index` at the beginning of each iteration. This is initially
            // true, and in each iteration `offset` can increase by at most one (whereas `index` always
            // increases by one). As `index` is always in bounds, and `offset` starts at zero, it too is
            // always in bounds.
            //
            // LLVM appears to struggle to optimize out Rust's split_at_mut, which would prove disjointness
            // using run-time tests.
            unsafe {

                assert!(offset < index);

                // LOOP INVARIANT: offset < index
                let ptr1 = slice.as_mut_ptr().offset(offset as isize);
                let ptr2 = slice.as_mut_ptr().offset(index as isize);

                if (*ptr1).0 == (*ptr2).0 {
                    (*ptr1).1 += &(*ptr2).1;
                }
                else {
                    if !(*ptr1).1.is_zero() {
                        offset += 1;
                    }
                    let ptr1 = slice.as_mut_ptr().offset(offset as isize);
                    std::mem::swap(&mut *ptr1, &mut *ptr2);
                }
            }
        }
        if !slice[offset].1.is_zero() {
            offset += 1;
        }

        offset
    }
    else {
        slice.len()
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
pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Monoid>(slice: &mut [(D, T, R)]) -> usize {

    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    if slice.len() > 1 {

        slice.sort_unstable_by(|x,y| (&x.0, &x.1).cmp(&(&y.0, &y.1)));

        let mut offset = 0;
        for index in 1 .. slice.len() {

            // The following unsafe block elides various bounds checks, using the reasoning that `offset`
            // is always strictly less than `index` at the beginning of each iteration. This is initially
            // true, and in each iteration `offset` can increase by at most one (whereas `index` always
            // increases by one). As `index` is always in bounds, and `offset` starts at zero, it too is
            // always in bounds.
            //
            // LLVM appears to struggle to optimize out Rust's split_at_mut, which would prove disjointness
            // using run-time tests.
            unsafe {

                // LOOP INVARIANT: offset < index
                let ptr1 = slice.as_mut_ptr().offset(offset as isize);
                let ptr2 = slice.as_mut_ptr().offset(index as isize);

                if (*ptr1).0 == (*ptr2).0 && (*ptr1).1 == (*ptr2).1 {
                    (*ptr1).2 += &(*ptr2).2;
                }
                else {
                    if !(*ptr1).2.is_zero() {
                        offset += 1;
                    }
                    let ptr1 = slice.as_mut_ptr().offset(offset as isize);
                    std::mem::swap(&mut *ptr1, &mut *ptr2);
                }

            }
        }
        if !slice[offset].2.is_zero() {
            offset += 1;
        }

        offset
    }
    else {
        slice.len()
    }
}
