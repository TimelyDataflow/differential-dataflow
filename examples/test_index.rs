#![feature(test)]

extern crate rand;
extern crate time;
extern crate test;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::collections::HashMap;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::collection_trace::index::{Index, OrdIndex};

fn main() {
    test_index();
    test_hash();
}

fn test_index() {
    let start = time::precise_time_ns();
    let mut index = OrdIndex::new();
    // let mut hash = HashMap::new();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vec = vec![];

    for i in 0..1000 {

        let mut key = (0u64, 0u64);

        for _ in 0..1000 {
            key.0 += rng.gen_range(0, 1000);
            key.1 += rng.gen_range(0, 1000);
            vec.push(key.clone());
        }

        index.for_each(&mut vec, |k,v| { *v = k.clone(); });
        vec.clear();
    }

    println!("loaded in {}ns avg", (time::precise_time_ns() - start) / 1_000_000);
    let mut start = time::precise_time_ns();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vec = vec![];
    for i in 0..10 {
        let mut key = (0u64, 0u64);
        for _ in 0..1000 {
            key.0 += rng.gen_range(0, 1000);
            key.1 += rng.gen_range(0, 1000);
            vec.push(key.clone());
        }
    }
    vec.sort();

    let len = vec.len();
    start = time::precise_time_ns();
    index.for_each(&mut vec, |k,v| {});
    println!("read in {}ns avg", (time::precise_time_ns() - start) / len as u64);
    start = time::precise_time_ns();

}

fn test_hash() {
    let start = time::precise_time_ns();
    // let mut index = OrdIndex::new();
    let mut hash = HashMap::new();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    for i in 0..1_000 {

        let mut key = (0u64, 0u64);

        for _ in 0..1_000 {

            key.0 += rng.gen_range(0, 1000);
            key.1 += rng.gen_range(0, 1000);
            // key.2 += rng.gen_range(0, 1000);

            hash.entry(key.clone()).or_insert(key.clone());
        }
    }

    println!("loaded in {}ns avg", (time::precise_time_ns() - start) / 1_000_000);
    let start = time::precise_time_ns();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vec = vec![];
    for i in 0..1000 {
        let mut key = (0u64, 0u64);
        for _ in 0..1000 {
            key.0 += rng.gen_range(0, 1000);
            key.1 += rng.gen_range(0, 1000);
            vec.push(key.clone());
        }
    }
    vec.sort();

    let start = time::precise_time_ns();
    for k in &vec { assert!(hash.get(k).unwrap() == k); }

    println!("read in {}ns avg", (time::precise_time_ns() - start) / 1_000_000);
}
