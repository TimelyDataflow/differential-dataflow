extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/3

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = 347991;

    // The sequence of numbers going down and to the right are the squares of odd numbers.
    //
    //    1, 9, 25, ...
    //
    // The next odd square is arrived at by following four edges, each of which has length
    // equal to the even number between the odd numbers. To determine the position of any
    // given number, we can subtract the largest odd square from it and then subtract the 
    // multiples of the even length, at which point we have a distance along an edge, from
    // which we can determine coordinates.
    // 
    // Similarly, we can determine the sequence number from the coordinates, by determining
    // which layer the point is in, adding the appropriate squared odd number, and then some
    // cases to figure out where the point is in the sequence along that layer.
    //
    // We are going to skip doing part one, because we can just solve for the answer (480),
    // and instead write up part two.

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        worker.dataflow::<(),_,_>(move |scope| {

            let worker_input = (1 .. 2).filter(move |&pos| pos % peers == index);

            let values = scope.new_collection_from(worker_input).1;

            values
                .map(|_val| ((0, 0), (1, 1)))
                .iterate(|inner| {

                    inner
                        .filter(move |&(_, (_, val))| val < input)  // stop working when we've gotten enough
                        .flat_map(|((x,y), (idx, value))|           // announce your value to all neighbors
                            (x-1 .. x+2).flat_map(move |x| (y-1 .. y+2).map(move |y| ((x,y), (idx,value))))
                        )
                        .group(|coords, idx_vals, output| {

                            let seq = sequence(*coords);
                            if seq == 1 {   // output for one is always one.
                                output.push(((1, 1), 1));
                            }
                            // otherwise, if we have data from the immediate predecessor...
                            else if idx_vals.iter().find(|x| (x.0).0 == seq - 1).is_some() {
                                let mut sum = 0;
                                for &(idx_val, wgt) in idx_vals.iter() {
                                    if idx_val.0 < seq {
                                        sum += idx_val.1 * wgt;
                                    }
                                }
                                output.push(((seq, sum), 1));
                            }
                        })
                })
                .map(|(_,(_,val))| ((),val))    // discard keys, retain and group vals.
                .group(|&(), input, output| output.push((*input[input.len()-1].0, 1)))
                .consolidate()
                .inspect(|x| println!("{:?}", (x.0).1))
                ;
        });

    }).unwrap();

}

fn sequence(input: (isize, isize)) -> isize {

    if input == (0, 0) { return 1; }
    else {

        let mut layer = 0;

        // extract the L0 norm (max absolute value).
        if input.0 > layer { layer = input.0; }
        if input.1 > layer { layer = input.1; }
        if -input.0 > layer { layer = -input.0; }
        if -input.1 > layer { layer = -input.1; }

        // input exists in layer from ((2 * layer - 1)^2, (2 * layer + 1)^2].
        // note: excludes first number, includes last number. 

        let base = (2 * layer - 1) * (2 * layer - 1);

             if input.1 == -layer   { base + 3 * (2 * layer) + input.0 + layer }
        else if input.0 == -layer   { base + 2 * (2 * layer) + layer - input.1 }
        else if input.1 == layer    { base + 1 * (2 * layer) + layer - input.0 }
        else if input.0 == layer    { base + 0 * (2 * layer) + input.1 + layer }
        else {
            panic!("should be unreachable");
        }
    }
}