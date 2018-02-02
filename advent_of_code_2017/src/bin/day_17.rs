extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/17

fn main() {

    let step = 382;

    // pointer[i] is the element after i in the sequence.
    let mut pointer = vec![0];

    for index in 1 .. 2018 {
        let mut pos = index - 1;
        for _ in 0 .. step {
            pos = pointer[pos];
        }

        let next = pointer[pos];
        pointer.push(next);
        pointer[pos] = index;
    }

    println!("part1: {:?}", pointer[2017]);

    // Solution nicked from the internet. generalization of above works, just is slow.
    // Because we now just look for a fixed location, we don't need to save any state;
    // we can use the fact that "the value after 0" is always position one, because
    // the zero value doesn't move (unlike all the other values).
    let mut result = None;
    let mut position = 0;
    for index in 1 .. 50_000_000 {
        position = (position + step) % index + 1;
        if position == 1 {
            result = Some(index);
        }
    }

    if let Some(value) = result {
        println!("part2: {:?}", value);
    }

}