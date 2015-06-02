use std::mem;
use std::ptr;
// use std::intrinsics;

pub mod radix;

// we need a new class of sorting algorithms taking a vector of values and a vector of keys.
// the reason is because we want to own the values rather than copy them; otherwise its silly.

#[test]
fn test_coalesce_kv() {
    let mut keys = vec![1, 4, 5, 1, 1, 2, 2, 4];
    let mut vals = vec![(3, 1), (4, 2), (2, 1), (3, -1), (2, 1), (3, 1), (0, 1), (4, -1)];

    coalesce_kv(&mut keys, &mut vals);
    assert!(keys == vec![1, 2, 2, 4, 5]);
    assert!(vals == vec![(2, 1), (0, 1), (3, 1), (4, 1), (2, 1)]);
}

// we need V: Ord to actually coalesce. Vecs rather than slices to merge weights.
pub fn coalesce_kv8<K: Ord, V: Ord, F: Fn(&K)->u64>(keys: &mut Vec<K>, vals: &mut Vec<(V, i32)>, f: &F) {
    assert!(keys.len() == vals.len());

    if !is_sorted(keys) {
        rsort_kv(keys, vals, f, &|k,v| qsort_kv(k, v))
    }
        // qsort_kv(keys, vals); }

    merge_weights_kv(keys, vals);

    keys.shrink_to_fit();
    vals.shrink_to_fit();
}

// we need V: Ord to actually coalesce. Vecs rather than slices to merge weights.
pub fn coalesce_kv<K: Ord, V: Ord>(keys: &mut Vec<K>, vals: &mut Vec<(V, i32)>) {
    assert!(keys.len() == vals.len());

    if !is_sorted(keys) { qsort_kv(keys, vals); }

    merge_weights_kv(keys, vals);

    keys.shrink_to_fit();
    vals.shrink_to_fit();
}

pub fn qsort_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) {
    let mut work = vec![(keys, vals)];
    while let Some((ks, vs)) = work.pop() {
        if ks.len() < 16 { isort_kv(ks, vs); }
        else {
            let p = partition_kv(ks, vs);
            let (ks1, ks2) = ks.split_at_mut(p);
            let (vs1, vs2) = vs.split_at_mut(p);
            work.push((&mut ks2[1..], &mut vs2[1..]));
            work.push((ks1, vs1));
        }
    }
}

#[inline(always)]
pub fn partition_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) -> usize {

    let pivot = keys.len() / 2;

    // if keys[pivot - 1] > keys[pivot + 1] { keys.swap(pivot - 1, pivot + 1); vals.swap(pivot - 1, pivot + 1);}
    // if keys[pivot - 1] > keys[pivot] { keys.swap(pivot - 1, pivot); vals.swap(pivot - 1, pivot); }
    // if keys[pivot] > keys[pivot + 1] { keys.swap(pivot, pivot + 1); vals.swap(pivot, pivot + 1); }

    let mut lower = 0;
    let mut upper = keys.len() - 1;

    unsafe {
        while lower < upper {
            // NOTE : Pairs are here to insulate against "same key" balance issues
            while lower < upper && (keys.get_unchecked(lower),lower) <= (keys.get_unchecked(pivot),pivot) { lower += 1; }
            while lower < upper && (keys.get_unchecked(pivot),pivot) <= (keys.get_unchecked(upper),upper) { upper -= 1; }
            ptr::swap(keys.get_unchecked_mut(lower), keys.get_unchecked_mut(upper));
            ptr::swap(vals.get_unchecked_mut(lower), vals.get_unchecked_mut(upper));
        }
    }

    // we want to end up with xs[p] near lower.
    if keys[lower] < keys[pivot] && lower < pivot { lower += 1; }
    if keys[lower] > keys[pivot] && lower > pivot { lower -= 1; }
    keys.swap(lower, pivot);
    vals.swap(lower, pivot);
    lower
}


// insertion sort
pub fn isort_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) {
    for i in 1..keys.len() {
        let mut j = i;
        unsafe {
            while j > 0 && keys.get_unchecked(j-1) > keys.get_unchecked(i) { j -= 1; }

            // bulk shift the stuff we skipped over
            let mut tmp_k: K = mem::uninitialized();
            ptr::swap(&mut tmp_k, keys.get_unchecked_mut(i));
            ptr::copy(keys.get_unchecked_mut(j), keys.get_unchecked_mut(j+1), i-j);
            ptr::swap(&mut tmp_k, keys.get_unchecked_mut(j));
            mem::forget(tmp_k);

            let mut tmp_v: V = mem::uninitialized();
            ptr::swap(&mut tmp_v, vals.get_unchecked_mut(i));
            ptr::copy(vals.get_unchecked_mut(j), vals.get_unchecked_mut(j+1), i-j);
            ptr::swap(&mut tmp_v, vals.get_unchecked_mut(j));
            mem::forget(tmp_v);
        }
    }
}

// we need to sort within each key group, consolidate, and tighten up both keys and vals.
pub fn merge_weights_kv<K: Ord, V: Ord>(keys: &mut Vec<K>, vals: &mut Vec<(V, i32)>) {

    let mut write = 0;  // where we are writing back to in keys and vals
    let mut lower = 0;  // the lower limit of the range of vals for the current key.

    while lower < keys.len() {

        // find the upper limit of this key range
        let mut upper = lower + 1;
        while upper < keys.len() && keys[lower] == keys[upper] {
            upper += 1;
        }

        // sort within the range associated with the key
        if !is_sorted_by(&vals[lower..upper], &|x| &x.0) {
            qsort_by(&mut vals[lower..upper], &|x| &x.0);
        }

        // accumulate weight forward.
        for index in (lower..upper - 1) {
            if vals[index].0 == vals[index + 1].0 {
                vals[index + 1].1 += vals[index].1;
                vals[index].1 = 0;
            }
        }

        // move non-zero weights forward
        for index in (lower..upper) {
            if vals[index].1 != 0 {
                keys.swap(write, index);    // TODO : blind writes should be faster than swapping
                vals.swap(write, index);    // TODO : blind writes should be faster than swapping
                write += 1;
            }
        }

        lower = upper;
    }

    // truncate zero weights
    keys.truncate(write);
    vals.truncate(write);
}

pub fn rsort_kv<K:Ord, V:Ord, F: Fn(&K)->u64, G: Fn(&mut [K], &mut [V])>(keys: &mut [K], vals: &mut [V], func: &F, and_then: &G) {

    let mut upper = [0; 256];
    let mut lower = [0; 256];

    let mut work = vec![(keys, vals, 0)];

    while let Some((mut ks, mut vs, shift)) = work.pop() {

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; }
        for elem in ks.iter() { unsafe { *upper.get_unchecked_mut(((func(elem) >> shift) & 0xFF) as usize) += 1; } }

        lower[0] = 0;
        for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }
        for i in 0..256 {
            unsafe {
                while *lower.get_unchecked(i) < *upper.get_unchecked(i) {
                    let dst = ((func(ks.get_unchecked_mut(*lower.get_unchecked(i))) >> shift) & 0xFF) as usize;

                    let pa: *mut K = ks.get_unchecked_mut(*lower.get_unchecked(i));
                    let pb: *mut K = ks.get_unchecked_mut(*lower.get_unchecked(dst));
                    ptr::swap(pa, pb);
                    let pa: *mut V = vs.get_unchecked_mut(*lower.get_unchecked(i));
                    let pb: *mut V = vs.get_unchecked_mut(*lower.get_unchecked(dst));
                    ptr::swap(pa, pb);

                    *lower.get_unchecked_mut(dst) += 1;
                }
            }
        }

        let mut cursor = 0;
        for i in 0..256 {
            let tk = ks; let (todo_k, rest_k) = tk.split_at_mut(lower[i] - cursor);
            let tv = vs; let (todo_v, rest_v) = tv.split_at_mut(lower[i] - cursor);

            if todo_k.len() > 256 && todo_k.len() < lower[255] / 2 { work.push((todo_k, todo_v, shift + 8)); }
            else                                                   { and_then(todo_k, todo_v); }

            ks = rest_k;
            vs = rest_v;
            cursor = lower[i];
        }
    }
}



pub fn coalesce8<T: Ord, F: Fn(&T)->u64>(vec: &mut Vec<(T, i32)>, f: &F) {
    rsort(vec, &|y| f(&y.0), &|x| qsort_by(x, &|y| &y.0));
    merge_weights_from(vec, 0);
}

pub fn coalesce<T: Ord>(vec: &mut Vec<(T, i32)>) { coalesce_from(vec, 0); }
pub fn coalesce_from<T: Ord>(vec: &mut Vec<(T, i32)>, offset: usize) {
    if !is_sorted_by(&vec[offset..], &|x| &x.0) {
        qsort_by(&mut vec[offset..], &|x| &x.0);
        // (&mut vec[offset..]).sort();
    }

    merge_weights_from(vec, offset);
}

pub fn is_sorted<T: Ord>(xs: &[T]) -> bool { is_sorted_by(xs, &|x| x) }
pub fn is_sorted_by<V, T: Ord, F: Fn(&V)->&T>(xs: &[V], f: &F) -> bool {
    !(1..xs.len()).any(|i| f(&xs[i-1]) > f(&xs[i]))
}

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
// really quite different now, but some structure remains!
pub fn qsort<T: Ord>(xs: &mut [T]) { qsort_by(xs, &|x| x); }
pub fn qsort_by<V, T: Ord, F: Fn(&V)->&T>(xs: &mut [V], f: &F) {
    let mut work = vec![xs];
    while let Some(slice) = work.pop() {
        if slice.len() < 16 { isort_by(slice, f) }
        else {
            let p = partition1(slice, f);
            let next = slice.split_at_mut(p);
            work.push(&mut next.1[1..]);
            work.push(next.0);
        }
    }
}

// insertion sort
pub fn isort<T: Ord>(xs: &mut [T]) { isort_by(xs, &|x| x); }
pub fn isort_by<V, T: Ord, F:Fn(&V)->&T>(xs: &mut [V], f: &F) {
    for i in 1..xs.len() {
        let mut j = i;
        unsafe {
            while j > 0 && f(xs.get_unchecked(j-1)) > f(xs.get_unchecked(i)) { j -= 1; }

            // bulk shift the stuff we skipped over
            let mut tmp: V = mem::uninitialized();
            ptr::swap(&mut tmp, xs.get_unchecked_mut(i));
            ptr::copy(xs.get_unchecked_mut(j), xs.get_unchecked_mut(j+1), i-j);
            ptr::swap(&mut tmp, xs.get_unchecked_mut(j));
            mem::forget(tmp);
        }
    }
}

#[inline(always)]
pub fn partition1<V, T: Ord, F: Fn(&V)->&T>(xs: &mut [V], f: &F) -> usize {

    let pivot = xs.len() / 2;

    // if f(&xs[pivot - 1]) > f(&xs[pivot + 1]) { xs.swap(pivot - 1, pivot + 1); }
    // if f(&xs[pivot - 1]) > f(&xs[pivot]) { xs.swap(pivot - 1, pivot); }
    // if f(&xs[pivot]) > f(&xs[pivot + 1]) { xs.swap(pivot, pivot + 1); }

    let mut lower = 0;
    let mut upper = xs.len() - 1;

    unsafe {
        while lower < upper {
            // NOTE : Pairs are here to insulate against "same key" balance issues
            while lower < upper && (f(&xs.get_unchecked(lower)),lower as u32) <= (f(&xs.get_unchecked(pivot)),pivot as u32) { lower += 1; }
            while lower < upper && (f(&xs.get_unchecked(pivot)),pivot as u32) <= (f(&xs.get_unchecked(upper)),upper as u32) { upper -= 1; }
            ptr::swap(xs.get_unchecked_mut(lower), xs.get_unchecked_mut(upper));
        }
    }

    // we want to end up with xs[p] near lower.
    if f(&xs[lower]) < f(&xs[pivot]) && lower < pivot { lower += 1; }
    if f(&xs[lower]) > f(&xs[pivot]) && lower > pivot { lower -= 1; }
    xs.swap(lower, pivot);
    lower
}

pub fn rsort<T:Ord, F: Fn(&T)->u64, G: Fn(&mut [T])>(slice: &mut [T], func: &F, and_then: &G) {

    let mut upper = [0; 256];
    let mut lower = [0; 256];

    let mut work = vec![(slice, 0)];

    while let Some((mut slice, shift)) = work.pop() {

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; }
        for elem in slice.iter() { unsafe { *upper.get_unchecked_mut(((func(elem) >> shift) & 0xFF) as usize) += 1; } }

        lower[0] = 0;
        for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }
        for i in 0..256 {
            unsafe {
                while *lower.get_unchecked(i) < *upper.get_unchecked(i) {
                    let dst = ((func(slice.get_unchecked_mut(*lower.get_unchecked(i))) >> shift) & 0xFF) as usize;

                    let pa: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(i));
                    let pb: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(dst));
                    ptr::swap(pa, pb);

                    *lower.get_unchecked_mut(dst) += 1;
                }
            }
        }

        let mut cursor = 0;
        for i in 0..256 {
            let temp = slice;
            let (todo, rest) = temp.split_at_mut(lower[i] - cursor);

            if todo.len() > 256  && todo.len() < lower[255] / 2 { work.push((todo, shift + 8)); }
            else                                                { and_then(todo); }

            slice = rest;
            cursor = lower[i];
        }
    }
}
