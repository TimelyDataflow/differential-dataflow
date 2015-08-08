#![feature(test)]

extern crate rand;
extern crate time;
extern crate test;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};
use std::hash::{Hash, SipHasher, Hasher};
use std::collections::HashMap;
use test::Bencher;

use differential_dataflow::collection_trace::index::{Index, OrdIndex};

#[bench] fn index_2_10e7_10e0(bencher: &mut Bencher) { index_2(bencher, 10000000, 1); }
#[bench] fn index_2_10e7_10e1(bencher: &mut Bencher) { index_2(bencher, 10000000, 10); }
#[bench] fn index_2_10e7_10e2(bencher: &mut Bencher) { index_2(bencher, 10000000, 100); }
#[bench] fn index_2_10e7_10e3(bencher: &mut Bencher) { index_2(bencher, 10000000, 1000); }
#[bench] fn index_2_10e7_10e4(bencher: &mut Bencher) { index_2(bencher, 10000000, 10000); }
#[bench] fn index_2_10e7_10e5(bencher: &mut Bencher) { index_2(bencher, 10000000, 100000); }
#[bench] fn index_2_10e7_10e6(bencher: &mut Bencher) { index_2(bencher, 10000000, 1000000); }

fn index_2(bencher: &mut Bencher, size: usize, queries: usize) {
    let mut index = OrdIndex::new();
    let mut inserted = 0;
    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vec = vec![];
    while inserted < size {
        for _ in 0..queries {
            vec.push((rng.next_u64(), 0));
        }
        vec.sort();
        index.for_each(&mut vec, |k,v| { *v = *k; });
        inserted += queries;
        vec.clear();
    }

    for _ in 0..queries {
        vec.push((rng.next_u64(), 0));
    }
    vec.sort();
    bencher.iter(|| {
        let mut v = vec.clone();
        index.find_each(&mut v, |k,v| {})
    });
}

#[bench] fn hash_eval_2(bencher: &mut Bencher) {
    bencher.iter(|| {
        let mut h = SipHasher::new();
        (0u64,0u64).hash(&mut h);
        h.finish()
    });
}

fn hash_2(bencher: &mut Bencher, size: usize) {
    let mut index = HashMap::new();
    for x in 0..size {
        index.entry((x,0u64)).or_insert(x);
    }

    let mut xy = (0,0);
    bencher.iter(|| {
        xy.0 += 1;
        index.get(&xy)
    });
}

// #[bench] fn hash_2_10e2(bencher: &mut Bencher) { hash_2(bencher, 100); }
// #[bench] fn hash_2_10e3(bencher: &mut Bencher) { hash_2(bencher, 1000); }
// #[bench] fn hash_2_10e4(bencher: &mut Bencher) { hash_2(bencher, 10_000); }
// #[bench] fn hash_2_10e5(bencher: &mut Bencher) { hash_2(bencher, 100_000); }
// #[bench] fn hash_2_10e6(bencher: &mut Bencher) { hash_2(bencher, 1_000_000); }
// #[bench] fn hash_2_10e7(bencher: &mut Bencher) { hash_2(bencher, 10_000_000); }
// #[bench] fn hash_2_10e8(bencher: &mut Bencher) { hash_2(bencher, 100_000_000); }


// #[bench]
// fn hash(bencher: &mut Bencher) {
//     bencher.iter(|| {
//         let mut index = HashMap::new();
//
//         for x in 0..10u64 {
//             for y in 0..100u64 {
//                 index.entry((x,y)).or_insert(x + y);
//             }
//         }
//
//         for x in 0..1001u64 {
//             for y in 0..1001u64 {
//                 index.entry((x,y)).or_insert(x + y);
//             }
//         }
//
//         for x in 0..1001u64 {
//             for y in 0..1001u64 {
//                 assert!(*index.get(&(x,y)).unwrap() == x + y);
//             }
//         }
//     });
// }
