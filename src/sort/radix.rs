use std::ptr;
use std::mem;
use std::fmt::Debug;
use std::cmp;

use timely::drain::DrainExt;




pub fn rsort_experimental8<T:Ord+Copy, F: Fn(&T)->u64>(src: &mut Vec<Vec<T>>, dst: &mut Vec<Vec<T>>, func: &F) {

    let mut counts = [0u32; 256];

    for s in src {
        for elem in s.drain_temp() {
            unsafe {
                let byte = (func(&elem) & 0xFF) as usize;
                let p = dst.get_unchecked_mut(byte).as_mut_ptr();
                ptr::write(p.offset(*counts.get_unchecked_mut(byte) as isize), elem);
                *counts.get_unchecked_mut(byte) += 1;
            }
        }
    }

    for i in 0..256 {
        unsafe { dst[i].set_len(counts[i] as usize); }
    }
}


pub fn rsort_experimental8_buf<T:Ord+Copy, F: Fn(&T)->u64>(src: &mut Vec<Vec<T>>, dst: &mut Vec<Vec<T>>, func: &F) {

    let buflen = 8;
    let buffer = Vec::<T>::with_capacity(buflen * 256).as_mut_ptr();

    let mut counts = [0u32; 256];   // TODO : This is limiting ...

    for s in src {
        for elem in s.drain_temp() {
            unsafe {

                let byte = (func(&elem) & 0xFF) as usize;
                let count = *counts.get_unchecked(byte);
                ptr::write(buffer.offset((buflen * byte + (count as usize % buflen)) as isize), elem);
                *counts.get_unchecked_mut(byte) += 1;

                if *counts.get_unchecked(byte) % buflen as u32 == 0 {
                    let p = dst.get_unchecked_mut(byte).as_mut_ptr();
                    ptr::copy_nonoverlapping(buffer.offset((buflen * byte) as isize),
                                             p.offset((*counts.get_unchecked(byte) - buflen as u32) as isize), buflen);
                }
            }
        }
    }

    for byte in 0..256 {
        unsafe { dst[byte].set_len(counts[byte] as usize); }
        for i in 0 .. counts[byte] as usize % buflen {
            dst[byte].push(unsafe { ptr::read(buffer.offset((8 * byte + i) as isize)) });
        }
    }
}

pub fn rsort_msb<T:Ord, F: Fn(&T)->u64, G: Fn(&mut [T])>(slice: &mut [T], func: &F, and_then: &G) {

    let mut upper = [0u32; 256];
    let mut lower = [0u32; 256];

    let mut work = vec![(slice, 0)];

    while let Some((mut slice, shift)) = work.pop() {

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; }
        for elem in slice.iter() { unsafe { *upper.get_unchecked_mut(((func(elem) >> shift) & 0xFF) as usize) += 1; } }
        lower[0] = 0; for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        for i in 0..256 {
            while lower[i] < upper[i] {
                let dst = ((func(unsafe { slice.get_unchecked_mut(*lower.get_unchecked(i) as usize) }) >> shift) & 0xFF) as usize;
                unsafe {
                    let pa: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(i) as usize);
                    let pb: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(dst) as usize);
                    ptr::swap(pa, pb);
                    *lower.get_unchecked_mut(dst) += 1;
                }
            }
        }

        if shift/8 + 1 < <U as Unsigned>::bytes() {
            let mut cursor = 0;
            for i in 0..256 {
                let temp = slice;
                let (todo, rest) = temp.split_at_mut(lower[i] as usize - cursor);

                if todo.len() > 256  && todo.len() < lower[255] as usize / 2 { work.push((todo, shift + 8)); }
                else                                                { and_then(todo); }

                slice = rest;
                cursor = lower[i] as usize;
            }
        }
    }
}

pub fn rsort_msb_safe<T:Ord, F: Fn(&T)->u64, G: Fn(&mut [T])>(slice: &mut [T], func: &F, and_then: &G) {

    let mut upper = [0u32; 256];
    let mut lower = [0u32; 256];

    let mut work = vec![(slice, 0)];

    while let Some((mut slice, shift)) = work.pop() {

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; }
        for elem in slice.iter() { upper[((func(elem) >> shift) & 0xFF) as usize] += 1; }
        lower[0] = 0; for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        for i in 0..256 {
            while lower[i] < upper[i] {
                let dst = ((func(&slice[lower[i] as usize]) >> shift) & 0xFF) as usize;
                slice.swap(lower[i] as usize, lower[dst] as usize);
                lower[dst] += 1;
            }
        }

        let mut cursor = 0;
        for i in 0..256 {
            let temp = slice;
            let (todo, rest) = temp.split_at_mut(lower[i] as usize - cursor);

            if todo.len() > 256  && todo.len() < lower[255] as usize / 2 { work.push((todo, shift + 8)); }
            else                                                { and_then(todo); }

            slice = rest;
            cursor = lower[i] as usize;
        }
    }
}

pub fn rsort_msb_buf<T:Ord+Debug, F: Fn(&T)->u64, G: Fn(&mut [T])>(slice: &mut [T], buffer: &mut [T], func: &F, and_then: &G) {

    let mut upper = [0u32; 256];
    let mut lower = [0u32; 256];

    let mut base = [0u8; 256];      // indicates first valid entry for each digit; eventually zero

    let buflen = 8;

    assert!(buffer.len() == buflen * 256);

    let mut work = vec![(slice, 0)];

    while let Some((mut slice, shift)) = work.pop() {

        // println!("starting");

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; lower[i] = 0; base[i] = 0; }

        // for elem in slice.iter() { upper[((func(elem) >> shift) & 0xFF) as usize] += 1; }
        for elem in slice.iter() { unsafe { *upper.get_unchecked_mut(((func(elem) >> shift) & 0xFF) as usize) += 1; } }
        lower[0] = 0; for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        // load up stream heads into buffer
        for i in 0..256 {
            base[i] = (lower[i] as usize % buflen) as u8;

            // want to stop either at upper[i] or if we would wrap around
            let limit = cmp::min(upper[i], lower[i] - (lower[i] % buflen as u32) + buflen as u32);
            for j in (lower[i]..limit) {
                mem::swap(&mut buffer[buflen * i + (j as usize % buflen)], &mut slice[j as usize]);
            }
        }

        unsafe {
            for i in 0..256 {
                while *lower.get_unchecked(i) < *upper.get_unchecked(i) {

                    let l_index = *lower.get_unchecked(i) as usize % buflen;

                    // stop either at the valid data, or if we would overrun upper
                    let limit = cmp::min(buflen, *upper.get_unchecked(i) as usize - (*lower.get_unchecked(i) as usize - l_index));

                    for index in l_index .. limit {

                        let dst = ((func(buffer.get_unchecked_mut((buflen * i) + index)) >> shift) & 0xFF) as usize;

                        assert!(i <= dst);

                        let pa: *mut T = buffer.get_unchecked_mut((buflen * i) + index);
                        let pb: *mut T = buffer.get_unchecked_mut((buflen * dst) + (*lower.get_unchecked(dst) as usize % buflen));
                        ptr::swap(pa, pb);
                        *lower.get_unchecked_mut(dst) += 1;

                        let ldst = *lower.get_unchecked(dst);
                        if ldst as usize % buflen == 0 {

                            ptr::copy_nonoverlapping(buffer.as_mut_ptr().offset(base[dst] as isize + (buflen * dst) as isize),
                                                     slice.as_mut_ptr().offset(base[dst]  as isize + ldst as isize - buflen as isize),
                                                     buflen - base[dst] as usize);

                            base[dst] = 0;

                            ptr::copy_nonoverlapping(slice.as_mut_ptr().offset(ldst as isize),
                                                     buffer.as_mut_ptr().offset((buflen * dst) as isize),
                                                     cmp::min(upper[dst] as usize - ldst as usize, buflen));



                        }
                    }
                }

                // clean out any incomplete writes from buffer
                // write should start at base[dst] and go up to
                let l = lower[i] as usize;
                for j in base[i] as usize ..(lower[i] as usize % buflen) {
                    mem::swap(&mut buffer[(buflen * i) + j], &mut slice[l - (l % buflen) + j]);
                }
            }
        }

        let mut cursor = 0;
        for i in 0..256 {
            assert!(lower[i] == upper[i]);
            let temp = slice;
            let (todo, rest) = temp.split_at_mut(lower[i] as usize - cursor);

            if todo.len() > 64 && todo.len() < lower[255] as usize / 2 { work.push((todo, shift + 8)); }
            else                                                { and_then(todo); }

            slice = rest;
            cursor = lower[i] as usize;
        }
    }
}



// "clever" version that does all cycles one step at a time, exposing more parallelism to the MMU
// possibly not a great idea, in that it grinds over memory more than it should rather than working
// only on data that fits in the cache. Keeping buffers of stream heads should remove the benefits.
pub fn rsort_msb_clv<T:Ord, F: Fn(&T)->u64, G: Fn(&mut [T])>(slice: &mut [T], func: &F, and_then: &G) {

    let mut upper = [0u32; 256];
    let mut lower = [0u32; 256];

    let mut work = vec![(slice, 0)];

    while let Some((mut slice, shift)) = work.pop() {

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; }
        for elem in slice.iter() { unsafe { *upper.get_unchecked_mut(((func(elem) >> shift) & 0xFF) as usize) += 1; } }
        lower[0] = 0; for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        for i in 0..256 {
            while lower[i] < upper[i] {
                let limit = cmp::min(upper[i], lower[i] + 8);
                for j in lower[i]..limit {
                    let dst = ((func(unsafe { slice.get_unchecked_mut(j as usize) }) >> shift) & 0xFF) as usize;
                    unsafe {
                        let pa: *mut T = slice.get_unchecked_mut(j as usize);
                        let pb: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(dst) as usize);
                        ptr::swap(pa, pb);
                        *lower.get_unchecked_mut(dst) += 1;
                    }
                }
            }
        }

        let mut cursor = 0;
        for i in 0..256 {
            let temp = slice;
            let (todo, rest) = temp.split_at_mut(lower[i] as usize - cursor);

            if todo.len() > 64  && todo.len() < lower[255] as usize / 2 { work.push((todo, shift + 8)); }
            else                                                { and_then(todo); }

            slice = rest;
            cursor = lower[i] as usize;
        }
    }
}
