extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/8

use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let mut a = 873u64;
    let mut b = 583u64;
    let mut equal = 0;

    let a_iter = (0 ..).map(move |_| { a = (a * 16807) % 2147483647; a });
    let b_iter = (0 ..).map(move |_| { b = (b * 48271) % 2147483647; b });

    for (a,b) in a_iter.zip(b_iter).take(40_000_000) {

        if (a % 65536) == (b % 65536) {
            equal += 1;
        }
    }

    println!("part1: {:?}", equal);



    let mut a = 873u64;
    let mut b = 583u64;
    let mut equal = 0;

    let a_iter = (0 ..).map(move |_| { a = (a * 16807) % 2147483647; a }).filter(|&x| x % 4 == 0);
    let b_iter = (0 ..).map(move |_| { b = (b * 48271) % 2147483647; b }).filter(|&x| x % 8 == 0);

    for (a,b) in a_iter.zip(b_iter).take(5_000_000) {

        if (a % 65536) == (b % 65536) {
            equal += 1;
        }
    }

    println!("part2: {:?}", equal);

}