#![feature(hashmap_hasher)]

extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::collections::HashMap;
use std::collections::hash_state::DefaultState;
use std::hash::{Hash, SipHasher, Hasher};

fn hash_me<T: Hash>(x: &T) -> u64 {
    let mut h = SipHasher::new();
    x.hash(&mut h);
    h.finish()
}

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::collection_trace::hash_index::HashIndex;

fn main() {
    for i in 25..26 {
        test_index(i);
    }
}

fn test_index(log_size: usize) {

    let default_state: DefaultState<SipHasher> = Default::default();
    let mut hash = HashMap::with_capacity_and_hash_state(1 << log_size, default_state);
    let mut hash_index = HashIndex::<(u64, (u64, u64)), (u64, u64), _>::new(|x| x.0);
    let mut hash_index2 = HashIndex::<(u64, u64), (u64, u64), _>::new(|x| x.0 ^ x.1);

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let start = time::precise_time_ns();
    for i in 0u64..(1 << log_size) {
        hash.insert((i,i),(i,i));
    }

    println!("loaded hash_map 2^{}:\t{:.2e} ns; capacity: {}", log_size, (time::precise_time_ns() - start) as f64, hash.capacity());

    let start = time::precise_time_ns();
    for _ in 0u64..(1 << log_size) {
        let key = (rng.next_u64(), rng.next_u64());
        hash_index.entry_or_insert((key.0 ^ key.1, key), || key);
    }

    println!("loaded hash_index 2^{}:\t{:.2e} ns; capacity: {}", log_size, (time::precise_time_ns() - start) as f64, hash_index.capacity());

    let start = time::precise_time_ns();
    for _ in 0u64..(1 << log_size) {
        let key = (rng.next_u64(), rng.next_u64());
        hash_index2.entry_or_insert(key, || key);
    }

    println!("loaded hash_index 3^{}:\t{:.2e} ns; capacity: {}", log_size, (time::precise_time_ns() - start) as f64, hash_index.capacity());

    for query in 0..log_size {

        let mut hash1_tally = 0;
        let mut hash2_tally = 0;
        let mut hash3_tally = 0;

        for _ in 0..10 {
            let mut vec = vec![];

            for _ in 0..(1 << query) {
                let key = (rng.next_u64(), rng.next_u64());
                vec.push((hash_me(&key), key));
            }

            // vec.sort_by(|x,y| (x.0 & ((1 << (log_size + 1)) - 1)).cmp(&(y.0 & ((1 << (log_size + 1)) - 1))));
            let start = time::precise_time_ns();
            for i in vec.iter() {
                assert!(hash.get(&i.1).is_none());
            }
            hash1_tally += time::precise_time_ns() - start;


            // vec.sort_by(|x,y| ((x.1).0 ^ (x.1).1).cmp(&((y.1).0 ^ (y.1).1)));
            let start = time::precise_time_ns();
            for i in vec.iter() {
                assert!(hash_index.get_ref(&((i.1).0 ^ (i.1).1, i.1)).is_none());
            }
            hash2_tally += time::precise_time_ns() - start;

            let start = time::precise_time_ns();
            for i in vec.iter() {
                assert!(hash_index2.get_ref(&i.1).is_none());
            }
            hash3_tally += time::precise_time_ns() - start;
            vec.clear();
        }

        println!("\t2^{}:\t{}\t{}\t{}", query, (hash1_tally / 10) >> query, (hash2_tally / 10) >> query, (hash3_tally / 10) >> query);

    }
}
