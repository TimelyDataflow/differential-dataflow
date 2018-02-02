extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/13

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = "0: 4
1: 2
2: 3
4: 4
6: 6
8: 5
10: 6
12: 6
14: 6
16: 12
18: 8
20: 9
22: 8
24: 8
26: 8
28: 8
30: 12
32: 10
34: 8
36: 12
38: 10
40: 12
42: 12
44: 12
46: 12
48: 12
50: 14
52: 14
54: 12
56: 12
58: 14
60: 14
62: 14
66: 14
68: 14
70: 14
72: 14
74: 14
78: 18
80: 14
82: 14
88: 18
92: 17";

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let worker_input =
        input
            .split('\n')
            .map(|line| {
                let mut words = line.split_whitespace();
                let start = words.next().unwrap().trim_matches(':').parse::<usize>().unwrap();
                let width = words.next().unwrap().parse::<usize>().unwrap();
                (start, width)
            })
            .filter(move |&(pos,_)| pos % peers == index);

        worker.dataflow::<(),_,_>(|scope| {

            let data = scope.new_collection_from(worker_input).1;

            data.filter(|&(pos, wid)| pos % (2 * (wid-1)) == 0)
                .explode(|(pos, wid)| Some(((), (pos * wid) as isize)))
                .consolidate()
                .inspect(|x| println!("part1: {:?}", x.2));

            // remove redundant constraints
            let data = data.map(|(pos, wid)| (pos % (2 * (wid-1)), wid)).distinct();

            let limit = 10000000;
            data.flat_map(move |(pos, wid)| (0..limit).filter(move |x| (x + pos) % (2 * (wid-1)) == 0))
                .distinct()
                .negate()
                .concat(&scope.new_collection_from(0 .. limit).1)
                .consolidate()
                .inspect(|x| println!("part2: {:?}", x.0));
        });

    }).unwrap();
}
