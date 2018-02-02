extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/8

use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = "jzgqcdpd";

    timely::execute_from_args(std::env::args(), move |worker| {

                let index = worker.index();
        let peers = worker.peers();

        let worker_input =
        (0 .. 128)
            .filter(move |&pos| pos % peers == index);

        worker.dataflow::<(),_,_>(|scope| {

            let data = scope.new_collection_from(worker_input).1;

            let hashes =
            data.flat_map(move |row| {
                    let string = format!("{}-{}", input, row);
                    let hash = knot_hash(&string);
                    // println!("{:?}: \t{:?}", row, hash);

                    (0 .. 128)
                        .map(move |col| 
                            if hash[col/8] & (1 << (7-(col % 8))) != 0 { 
                                (row, col, '#')
                            }
                            else {
                                (row, col, '.')
                            }
                        )
                    }
                );

            hashes
                .filter(|&(_,_,symbol)| symbol == '#')
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("part1: {:?}", x.2));

            // part two wants the number of connected components of hash regions.

        });

    }).unwrap();
}

fn knot_hash(string: &str) -> Vec<u8> {
    let mut bytes = string.as_bytes().iter().cloned().collect::<Vec<_>>();
    bytes.push(17);
    bytes.push(31);
    bytes.push(73);
    bytes.push(47);
    bytes.push(23);
    let hash = knot_step(bytes.iter().cloned(), 64);

    let mut result = vec![0u8; 16];

    for byte in 0 .. 16 {
        for step in 0 .. 16 {
            result[byte] ^= hash[16 * byte + step];
        }
    }
    result
}

fn knot_step<I: Iterator<Item=u8>+Clone>(iter: I, rounds: usize) -> Vec<u8> {

    let mut state = (0 .. 256u16).map(|x| x as u8).collect::<Vec<_>>();
    let mut cursor = 0;
    let mut step = 0;

    for _ in 0 .. rounds {
        for width in iter.clone() {
            let width = width as usize;
            for swap in 0 .. width / 2 {
                state.swap((cursor + swap) % 256, (cursor + width - swap - 1) % 256);
            }
            cursor = (cursor + width + step) % 256;
            step += 1;
        }
    }

    state
}