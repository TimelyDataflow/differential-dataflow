#![feature(test)]

extern crate rand;
extern crate time;
extern crate test;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::collections::HashMap;
use test::Bencher;

use differential_dataflow::collection_trace::index::{Index, OrdIndex};

#[bench]
fn index(bencher: &mut Bencher) {
    bencher.iter(|| {
        let mut index = OrdIndex::new();

        for x in 0..10u64 {
            for y in 0..100u64 {
                *index.seek(&(x,y)) = x + y;
            }
        }

        index.reset();

        for x in 0..1001u64 {
            for y in 0..1001u64 {
                *index.seek(&(x,y)) = x + y;
            }
        }

        index.reset();

        for x in 0..1001u64 {
            for y in 0..1001u64 {
                assert!(*index.seek(&(x,y)) == x + y);
            }
        }
    });
}

#[bench]
fn index_light(bencher: &mut Bencher) {
    let mut index = OrdIndex::new();
    for x in 0..1001u64 {
        for y in 0..1001u64 {
            *index.seek(&(x,y)) = x + y;
        }
    }

    index.reset();
    bencher.iter(|| {
        for x in 0..100u64 {
            for y in 0..10u64 {
                assert!(*index.seek(&(x,y)) == x + y);
            }
        }
        index.reset();
    });
}

#[bench]
fn hash_light(bencher: &mut Bencher) {
    let mut index = HashMap::new();
    for x in 0..10u64 {
        for y in 0..100u64 {
            index.entry((x,y)).or_insert(x + y);
        }
    }

    for x in 0..1001u64 {
        for y in 0..1001u64 {
            index.entry((x,y)).or_insert(x + y);
        }
    }

    bencher.iter(|| {
        for x in 0..100u64 {
            for y in 0..10u64 {
                assert!(*index.get(&(x,y)).unwrap() == x + y);
            }
        }
    });
}

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
