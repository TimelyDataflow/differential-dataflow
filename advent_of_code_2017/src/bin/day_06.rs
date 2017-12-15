extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/6

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = "10 3   15  10  5   15  5   15  9   2   5   8   5   2   3   6";

    timely::execute_from_args(std::env::args(), move |worker| {

        let worker_input = 
        input
            .split_whitespace()
            .map(|phrase| phrase.parse::<u8>().unwrap())
            .collect::<Vec<_>>();

        worker.dataflow::<(),_,_>(|scope| {

            let banks = scope.new_collection_from(Some(worker_input)).1;

            let stable = banks.iterate(|iter|
                iter.map_in_place(|banks| recycle(banks))
                    .concat(&banks.enter(&iter.scope()))
                    .distinct()
            );

            stable
                .map(|_| ((),()))
                .count()
                .inspect(|x| println!("part 1: {:?}", (x.0).1));

            // determine the repeated state by stepping all states and subtracting.
            let loop_point = stable
                .map_in_place(|banks| recycle(banks))
                .concat(&stable.negate())
                .concat(&banks);

            // restart iteration from known repeated element.
            loop_point                
                .iterate(|iter|
                    iter.map_in_place(|banks| recycle(banks))
                        .concat(&loop_point.enter(&iter.scope()))
                        .distinct()
                )
                .map(|_| ((),()))
                .count()
                .inspect(|x| println!("part 2: {:?}", (x.0).1));
        });

    }).unwrap();
}

fn recycle(banks: &mut [u8]) {
    let mut max_idx = 0;
    for i in 0 .. banks.len() {
        if banks[i] > banks[max_idx] {
            max_idx = i;
        }
    }

    let redistribute = banks[max_idx] as usize;
    banks[max_idx] = 0;
    let banks_len = banks.len();
    for i in 1 .. (redistribute + 1) {
        banks[(max_idx + i) % banks_len] += 1;
    }    
}