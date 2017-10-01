extern crate rand;
extern crate timely;
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

        let timer = ::std::time::Instant::now();

        let index = worker.index();
        let peers = worker.peers();

        // create a a degree counting differential dataflow
        let (mut input, probe) = worker.dataflow(|scope| {

            // create edge input, count a few ways.
            let (input, edges) = scope.new_collection();

            let degrs = edges.map(|(src, _dst)| src)
                             .count_total();

            // pull of count, and count.
            let distr = degrs.map(|(_src, cnt)| cnt as usize)
                             .count_total();

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

        input.advance_to(1u64);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        println!("round 0 finished after {:?} (loading)", timer.elapsed());

        if batch > 0 {

            if !::std::env::args().any(|x| x == "open-loop") {

                // closed-loop latency-throughput test, parameterized by batch size.
                let timer = ::std::time::Instant::now();
                let mut wave = 1;
                while timer.elapsed().as_secs() < 10 {
                    for round in 0 .. batch {
                        input.advance_to((((wave * batch) + round) * peers + index) as u64);
                        input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                        input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                    }

                    wave += 1;
                    input.advance_to((wave * batch * peers) as u64);
                    input.flush();
                    worker.step_while(|| probe.less_than(input.time()));
                }

                let elapsed = timer.elapsed();
                let seconds = elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64) / 1000000000.0;
                println!("{:?}, {:?}", seconds / (wave - 1) as f64, ((wave - 1) * batch * peers) as f64 / seconds);

            }
            else {

                let requests_per_sec = batch / peers;
                let ns_per_request = 1_000_000_000 / requests_per_sec;
                let mut request_counter = 1;
                let mut measurements = Vec::with_capacity(20 * requests_per_sec);

                let timer = ::std::time::Instant::now();

                while measurements.len() < requests_per_sec * 21 {

                    // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                    let elapsed = timer.elapsed();
                    let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

                    // Introduce any requests that have "arrived" since last we were here.
                    // Request i "arrives" at `index + ns_per_request * (i + 1)`. 
                    while ((index + ns_per_request * request_counter) as u64) < elapsed_ns {
                        input.advance_to((index + ns_per_request * request_counter) as u64);
                        input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                        input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                        input.advance_to((1 + index + ns_per_request * request_counter) as u64);
                        request_counter += 1;
                    }
                    input.flush();

                    while probe.less_than(input.time()) {
                        worker.step();
                    }

                    // Determine completed ns.
                    let acknowledged_ns: u64 = probe.with_frontier(|frontier| frontier[0].inner);

                    let elapsed = timer.elapsed();
                    let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

                    // any un-recorded measurements that are complete should be recorded.
                    while ((index + (measurements.len() + 1) * ns_per_request) as u64) < acknowledged_ns {
                        let requested_at = (index + (measurements.len() + 1) * ns_per_request) as u64;
                        measurements.push(elapsed_ns - requested_at);
                    }
                }

                measurements.truncate(requests_per_sec * 20);
                measurements.drain(0 .. (requests_per_sec * 10));
                measurements.sort();

                let med = measurements[measurements.len() / 2];
                let p99 = measurements[99 * measurements.len() / 100];
                let max = measurements[measurements.len() - 1];

                println!("{}\t{}\t{}\t(of {} measurements)", med, p99, max, measurements.len());
            }
        }
    }).unwrap();
}