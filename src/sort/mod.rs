use std::mem;
use std::ptr;
use std::intrinsics;

pub fn coalesce<T: Ord>(vec: &mut Vec<(T, i32)>) { coalesce_from(vec, 0); }
pub fn coalesce_from<T: Ord>(vec: &mut Vec<(T, i32)>, offset: usize) {
    if !is_sorted(&vec[offset..]) {
        if vec.len() < 32 { isort_by(&mut vec[offset..], &|&(ref x,_)| x); }
        else              { (&mut vec[offset..]).sort(); }

        // else              { qsort_by(&mut vec[offset..], &|&(ref x,_)| x); }
    }
    merge_weights_from(vec, offset);

    // assert!(is_sorted(&vec[offset..]));
    // assert!(vec[offset..].iter().all(|x| x.1 != 0));
}

pub fn is_sorted<T: Ord>(xs: &[T]) -> bool { !(1..xs.len()).any(|i| xs[i-1] > xs[i]) }

pub fn merge_weights_from<T: Ord>(vec: &mut Vec<(T, i32)>, offset: usize) {
    let mut cursor = offset;
    for index in offset+1..vec.len() {
        if vec[cursor].0 == vec[index].0 {
            vec[cursor].1 += vec[index].1;
            vec[index].1 = 0;
        }
        else { cursor = index; }
    }

    let mut cursor = offset;
    for index in offset..vec.len() {
        if vec[index].1 != 0 {
            vec.swap(cursor, index);
            cursor += 1;
        }
    }

    vec.truncate(cursor);
}


// qsort initially stolen from BurntSushi, who uses an unlicense.
// somewhat different now, but some structure remains!
pub fn qsort<T: Ord>(xs: &mut [T]) { qsort_by(xs, &|x| x); }
pub fn qsort_by<V, T: Ord, F: Fn(&V)->&T>(xs: &mut [V], f: &F) {
    let mut work = vec![xs];
    while let Some(slice) = work.pop() {
        if slice.len() < 32 { isort_by(slice, f) }
        else {
            let p = partition1(slice, f);
            let next = slice.split_at_mut(p);
            work.push(&mut next.1[1..]);
            work.push(next.0);
        }
    }
}

pub fn qsort2<T: Ord>(xs: &mut [T]) { qsort2_by(xs, &|x| x); }
pub fn qsort2_by<V, T: Ord, F: Fn(&V)->&T>(xs: &mut [V], f: &F) {
    let mut work = vec![xs];
    while let Some(slice) = work.pop() {
        if slice.len() < 32 { isort_by(slice, f) }
        else {
            let p = partition2(slice, f);
            let next = slice.split_at_mut(p);
            work.push(&mut next.1[1..]);
            work.push(next.0);
        }
    }
}


#[inline(always)]
// selection sort
pub fn _ssort<T: Ord>(xs: &mut [T]) {
    for i in 0..xs.len() {
        for j in (i + 1)..xs.len() {
            if xs[j] < xs[i] {
                xs.swap(i, j);
            }
        }
    }
}

// insertion sort
pub fn isort<T: Ord>(xs: &mut [T]) { isort_by(xs, &|x| x); }
pub fn isort_by<V, T: Ord, F:Fn(&V)->&T>(xs: &mut [V], f: &F) {
    for i in 1..xs.len() {
        let mut j = i;
        unsafe {
            // while j > 0 && f(xs[j-1]) > f(xs[j]) {
            //     xs.swap(j-1, j);
            //     j -= 1;
            // }
            while j > 0 && f(xs.get_unchecked(j-1)) > f(xs.get_unchecked(i)) { j -= 1; }

            let mut tmp: V = mem::uninitialized();
            ptr::swap(&mut tmp, xs.get_unchecked_mut(i));
            intrinsics::copy(xs.get_unchecked_mut(j), xs.get_unchecked_mut(j+1), i-j);
            ptr::swap(&mut tmp, xs.get_unchecked_mut(j));
            mem::forget(tmp);
        }
    }
}

pub fn partition1<V, T: Ord, F: Fn(&V)->&T>(xs: &mut [V], f: &F) -> usize {

    let pivot = xs.len() / 2;

    let mut lower = 0;
    let mut upper = xs.len() - 1;

    unsafe {
        while lower < upper {
            while lower < upper && f(&xs.get_unchecked(lower)) <= f(&xs.get_unchecked(pivot)) { lower += 1; }
            while lower < upper && f(&xs.get_unchecked(pivot)) <= f(&xs.get_unchecked(upper)) { upper -= 1; }
            ptr::swap(xs.get_unchecked_mut(lower), xs.get_unchecked_mut(upper));
        }
    }

    // we want to end up with xs[p] near lower.
    if f(&xs[lower]) < f(&xs[pivot]) && lower < pivot { lower += 1; }
    if f(&xs[lower]) > f(&xs[pivot]) && lower > pivot { lower -= 1; }
    xs.swap(lower, pivot);
    lower
}

pub fn partition2<V, T: Ord, F: Fn(&V)->&T>(xs: &mut [V], f: &F) -> usize {

    let p = xs.len() / 2;
    let lasti = xs.len() - 1;
    let (mut i, mut nextp) = (0, 0);
    xs.swap(lasti, p);
    while i < lasti {
        unsafe {
            if f(xs.get_unchecked(i)) <= f(xs.get_unchecked(lasti)) {
                // xs.swap(i, nextp);
                let pa: *mut V = xs.get_unchecked_mut(i);
                let pb: *mut V = xs.get_unchecked_mut(nextp);
                ptr::swap(pa, pb);

                nextp = nextp + 1;
            }
        }
        i = i + 1;
    }
    xs.swap(nextp, lasti);
    nextp
}

pub fn rshuf<T: Ord, F: Fn(&T)->u64>(slice: &mut [T], func: F) {
    _rstep8(slice, &func, |slice| if slice.len() < 256 { isort(slice) }
                                 else { _rstep8(slice, &|x| func(x) >> 8, |slice| if slice.len() < 256 { isort(slice) } else { slice.sort() }) })
}

pub fn _rstep8<T, F: Fn(&T)->u64, A: Fn(&mut [T])>(slice: &mut [T], func: &F, andthen: A) {
    let mut ranges = [(0, 0); 256];
    for elem in slice.iter() { ranges[(func(elem) & 0xFF) as usize].1 += 1; }
    for i in 1..ranges.len() { ranges[i].0 = ranges[i-1].1;
                               ranges[i].1 += ranges[i].0; }

    let mut cursor = 0;
    for i in 0..256 {
        while ranges[i].0 < ranges[i].1 {
            let dst = (func(&slice[ranges[i].0]) & 0xFF) as usize;
            if dst != i { slice.swap(ranges[i].0, ranges[dst].0); }
            ranges[dst].0 += 1;
        }

        andthen(&mut slice[cursor..ranges[i].0]);
        cursor = ranges[i].0;
    }
}
