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

#[test]
fn c() {
    let mut index = OrdIndex::new();
    // let mut hash = HashMap::new();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    for _ in 0..100 {

        let mut key = (0u64, 0u64, 0u64);

        for _ in 0..100 {

            key.0 += rng.gen_range(0, 1000);
            key.1 += rng.gen_range(0, 1000);
            key.2 += rng.gen_range(0, 1000);

            *index.seek(&key) = key;
            // hash.entry(key.clone()).or_insert(key.clone());

        }
        
        index.reset();
    }

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    for _ in 0..100 {

        let mut key = (0u64, 0u64, 0u64);

        for _ in 0..100 {

            key.0 += rng.gen_range(0, 1000);
            key.1 += rng.gen_range(0, 1000);
            key.2 += rng.gen_range(0, 1000);

            assert!(index.seek(&key) == &key);
            // hash.entry(key.clone()).or_insert(key.clone());

            index.reset();
        }
    }
}
