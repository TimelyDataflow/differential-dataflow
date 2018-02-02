extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/8

use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = "212,254,178,237,2,0,1,54,167,92,117,125,255,61,159,164";
    // let input = "225,171,131,2,35,5,0,13,1,246,54,97,255,98,254,110";
    // let input = "";
    // let input = "AoC 2017";
    // let input = "1,2,3";
    // let input = "1,2,4";

    timely::execute_from_args(std::env::args(), move |worker| {

        // let parsed = input.split(',').map(|x| x.parse::<u8>().unwrap()).collect::<Vec<_>>();
        // let part1 = knot_step(parsed.iter().cloned(), 1);
        // println!("part1: {:?}", (part1[0] as usize) * (part1[1] as usize));

        let mut bytes = input.as_bytes().iter().cloned().collect::<Vec<_>>();
        bytes.push(17);
        bytes.push(31);
        bytes.push(73);
        bytes.push(47);
        bytes.push(23);
        let part2 = knot_step(bytes.iter().cloned(), 64);

        print!("part2: ");
        for byte in 0 .. 16 {
            let mut result = 0;
            for step in 0 .. 16 {
                result ^= part2[16 * byte + step];
            }
            print!("{:02x}", result);
        }
        println!();

    }).unwrap();
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