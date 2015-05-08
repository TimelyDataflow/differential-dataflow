#![feature(test)]
#![feature(core)]
#![feature(collections)]
#![feature(collections_drain)]

extern crate differential_dataflow;
extern crate rand;
extern crate test;

use rand::{Rng, SeedableRng, StdRng, Rand};
use test::Bencher;
use std::ptr;
use std::mem;

use differential_dataflow::sort::*;

fn random_vec<T: Rand>(size: usize) -> Vec<T> {
    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    // println!("{}", rng.gen::<f64>());

    let mut result = Vec::with_capacity(size);
    for _ in 0..size {
        result.push(rng.gen());
    }

    result
}

#[bench] fn sort_sort(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    bencher.iter(|| data.clone().sort());
}

#[bench] fn sort_qsort1(bencher: &mut Bencher) {
    let mut data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    // for i in 0..data.len()  { data[i].0 = i as u64; }

    bencher.iter(|| qsort_by(&mut data.clone(), &|x|&x.0));
}
#[bench] fn sort_qsort2(bencher: &mut Bencher) {
    let mut data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    // for i in 0..data.len()  { data[i].0 = i as u64; }

    bencher.iter(|| qsort2_by(&mut data.clone(), &|x|&x.0));
}
// //
//
// #[bench] fn rsort8_bench(bencher: &mut Bencher) {
//     let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 16);
//     bencher.iter(|| rsort8(&mut data.clone(), &|&x| x.0));
// }


// #[bench] fn rsort88_bench(bencher: &mut Bencher) {
//     let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 16);
//     bencher.iter(|| rsort88(&mut data.clone(), &|&x| x.0));
// }


#[bench] fn sort_rsort888(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    bencher.iter(|| rsort888(&mut data.clone(), &|&x| x.0));
}

#[bench] fn sort_rsort8888(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    bencher.iter(|| rsort8888(&mut data.clone(), &|&x| x.0));
}

// #[bench] fn push_bench(bencher: &mut Bencher) {
//     let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 14);
//     let mut buff = Vec::new();
//
//     bencher.bytes = 5 * 8 * data.len() as u64;
//     bencher.iter(|| {
//         buff.clear();
//         buff.push_all(&data);
//     });
// }
//
// #[bench] fn clone_bench(bencher: &mut Bencher) {
//     let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 14);
//
//     bencher.bytes = 5 * 8 * data.len() as u64;
//     bencher.iter(|| {
//         let mut buff = data.clone();
//     });
// }

#[bench] fn endian_noop(bencher: &mut Bencher) {
    bencher.iter(|| { (0..1024u64).sum::<u64>() })
}

#[bench] fn endian_to_le(bencher: &mut Bencher) {
    bencher.iter(|| { (0..1024u64).map(|x| x.to_le()).sum::<u64>() })
}

#[bench] fn endian_to_be(bencher: &mut Bencher) {
    bencher.iter(|| { (0..1024u64).map(|x| x.to_be()).sum::<u64>() })
}

#[bench] fn shuffle_r_bench(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    let mut buff = Vec::new();
    let mut vecs = vec![Vec::new(); 16];

    bencher.bytes = 5 * 8 * data.len() as u64;
    bencher.iter(|| {
        buff.clear();
        buff.push_all(&data);
        let mut upper = [0; 16];
        rstep(&mut buff, &|&x| x.0, &mut upper);

        let mut cursor = 0;
        for i in 0..vecs.len() {
            vecs[i].clear();
            vecs[i].push_all(&buff[cursor as usize ..upper[i] as usize]);
            cursor = upper[i];
        }
    });
}

#[bench] fn shuffle_s_bench(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    let mut buff = Vec::new();
    let mut buff2 = data.clone();
    let mut vecs = vec![Vec::new(); 16];

    bencher.bytes = 5 * 8 * data.len() as u64;
    bencher.iter(|| {
        buff.clear();
        buff.push_all(&data);
        let mut upper = [0; 16];
        rstep_s(&mut buff, &mut buff2, &|&x| x.0, &mut upper);

        let mut cursor = 0;
        for i in 0..vecs.len() {
            vecs[i].clear();
            vecs[i].push_all(&buff2[cursor as usize ..upper[i] as usize]);
            cursor = upper[i];
        }
    });
}

#[bench] fn radix_r_bench(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    let mut buff = Vec::new();

    bencher.bytes = 5 * 8 * data.len() as u64;
    bencher.iter(|| {
        buff.clear();
        buff.push_all(&data);
        let mut ranges = [0; 16];
        rstep(&mut buff, &|&x| x.0, &mut ranges);
    });
}

#[bench] fn shuffle_bench(bencher: &mut Bencher) {
    let data = random_vec::<(u64, (u64, u64, u64, u64))>(1 << 12);
    let mut buff = Vec::new();
    let mut vecs = vec![Vec::new(); 16];

    bencher.bytes = 5 * 8 * data.len() as u64;
    bencher.iter(|| {
        buff.clear();
        buff.push_all(&data);
        for i in 0..vecs.len() { vecs[i].clear(); }
        for (i, d) in buff.drain(..) {
            unsafe { vecs.get_unchecked_mut((i & 0x0F) as usize) }.push(d);
        }
    });
}

pub fn rsort8<T:Ord+Copy, F: Fn(&T)->u64>(slice: &mut [T], func: &F) {
    rstep8(slice, func, |slice| slice.sort());
}


pub fn rsort88<T:Ord+Copy, F: Fn(&T)->u64>(slice: &mut [T], func: &F) {
    rstep8(slice, func, |slice| rstep8(slice, &|x| func(x) >> 8, |x| x.sort()));
}

pub fn rsort888<T:Ord+Copy, F: Fn(&T)->u64>(slice: &mut [T], func: &F) {
    rstep8(slice, func, |slice|
        rstep8(slice, &|x| func(x) >> 8, |slice|
            rstep8(slice, &|x| func(x) >> 16, |x| qsort(x))));
}

pub fn rsort8888<T:Ord+Copy, F: Fn(&T)->u64>(slice: &mut [T], func: &F) {
    rstep8(slice, func, |slice|
        rstep8(slice, &|x| func(x) >> 8, |slice|
            rstep8(slice, &|x| func(x) >> 16, |slice|
                rstep8(slice, &|x| func(x) >> 24, |x| qsort(x)))));
}


pub fn rstep8<T:Ord+Copy, F: Fn(&T)->u64, A: Fn(&mut [T])>(slice: &mut [T], func: &F, andthen: A) {
    let mut ranges = [(0, 0); 256];
    // for elem in slice.iter() { ranges[(func(elem) & 0xFF) as usize].1 += 1; }
    for elem in slice.iter() { unsafe { ranges.get_unchecked_mut((func(elem) & 0xFF) as usize).1 += 1; } }
    for i in 1..ranges.len() { ranges[i].0 = ranges[i-1].1;
                               ranges[i].1 += ranges[i].0; }

    for i in 0..256 {
        while ranges[i].0 < ranges[i].1 {
            let dst = (func(&slice[ranges[i].0]) & 0xFF) as usize;
            if dst != i {
                slice.swap(ranges[i].0, ranges[dst].0);
            }
            ranges[dst].0 += 1;
        }
    }

    let mut cursor = 0;
    for i in 0..256 {
        if cursor + 16 < ranges[i].0  { andthen(&mut slice[cursor..ranges[i].0]); }
        else                          { isort(&mut slice[cursor..ranges[i].0]) }
        cursor = ranges[i].0;
    }
}

pub fn rstep<T:Ord+Copy, F: Fn(&T)->u64>(slice: &mut [T], func: &F, upper: &mut [u32; 16]) {
    let mut lower = [0; 16];
    unsafe {
        for elem in slice.iter() { *upper.get_unchecked_mut((func(elem) & 0x0F) as usize) += 1; }
        for i in 1..upper.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        for i in 0..16 {
            while *lower.get_unchecked_mut(i) < *upper.get_unchecked_mut(i) {
                let dst = (func(&slice.get_unchecked_mut(*lower.get_unchecked_mut(i) as usize)) & 0x0F) as usize;
                if dst != i {
                    // slice.swap(lower[i] as usize, lower[dst] as usize);
                    let pa: *mut T = slice.get_unchecked_mut(*lower.get_unchecked_mut(i) as usize);
                    let pb: *mut T = slice.get_unchecked_mut(*lower.get_unchecked_mut(dst) as usize);
                    ptr::swap(pa, pb);
                }
                *lower.get_unchecked_mut(dst) += 1;
            }
        }
    }
}

pub fn rstep_s<T:Ord+Copy, F: Fn(&T)->u64>(slice: &mut [T], other: &mut [T], func: &F, upper: &mut [u32; 16]) {
    let mut lower = [0; 16];
    unsafe {
        for elem in slice.iter() { *upper.get_unchecked_mut((func(elem) & 0x0F) as usize) += 1; }
        for i in 1..upper.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        for x in slice {
            let dst = (func(x) & 0x0F) as usize;
            *other.get_unchecked_mut(lower[dst] as usize) = *x;
            lower[dst] += 1;
        }
    }
}
