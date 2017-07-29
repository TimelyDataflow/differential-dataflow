extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::count::CountTotal;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    let inspect: bool = std::env::args().find(|x| x == "inspect").is_some();

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        // create a a degree counting differential dataflow
        let (mut input, probe) = worker.dataflow(|scope| {

            // create edge input, count a few ways.
            let (input, edges) = scope.new_collection();

            let degrs = edges.map(|(src, _dst)| src)
                             .count_total_u();

            // pull of count, and count.
            let distr = degrs.map(|(_src, cnt)| cnt as usize)
                             .count_total_u();

            // show us something about the collection, notice when done.
            let probe = if inspect { 
                distr.inspect(|x| println!("observed: {:?}", x))
                     .probe()
            }
            else { distr.probe() };

            (input, probe)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        // load up graph dataz
        for _ in 0 .. (edges / peers) + if index < (edges % peers) { 1 } else { 0 } {
            input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)))
        }

        input.advance_to(1);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        // println!("round 0 finished after {:?} (loading)", timer.elapsed());

        if batch > 0 {
            let timer = ::std::time::Instant::now();
            let mut wave = 1;
            while timer.elapsed().as_secs() < 10 {
                for round in 0 .. batch {
                    input.advance_to(((wave * batch) + round) * peers + index);
                    input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));                    
                }

                wave += 1;
                input.advance_to(wave * batch * peers);
                input.flush();
                worker.step_while(|| probe.less_than(input.time()));
            }

            let elapsed = timer.elapsed();
            let seconds = elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64) / 1000000000.0;
            println!("{:?}, {:?}", seconds / (wave - 1) as f64, ((wave - 1) * batch * peers) as f64 / seconds);
        }
    }).unwrap();
}