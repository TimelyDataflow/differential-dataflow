extern crate rand;
extern crate timely;
extern crate differential_dataflow;
extern crate core_affinity;

use rand::{Rng, SeedableRng, StdRng};

// use timely::dataflow::operators::{Exchange, Probe};
// use timely::progress::nested::product::Product;
// use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::input::Input;
// use differential_dataflow::operators::arrange::Arrange;
// use differential_dataflow::operators::count::CountTotalCore;
use differential_dataflow::operators::Join;

// use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::operators::arrange::ArrangeByKey;

fn main() {

    let mut args = std::env::args();
    args.next();

    let keys: usize = args.next().unwrap().parse().unwrap();
    let recs: usize = args.next().unwrap().parse().unwrap();
    // let rate: usize = args.next().unwrap().parse().unwrap();
    // let work: usize = args.next().unwrap().parse().unwrap_or(usize::max_value());

    timely::execute_from_args(args, move |worker| {

        let index = worker.index();
        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index]);

        // create a a degree counting differential dataflow
        let (mut input, mut trace) = worker.dataflow::<(),_,_>(|scope| {
            let (handle, data) = scope.new_collection();
            let trace_handle = data.arrange_by_key().trace;
            (handle, trace_handle)
        });

        let index = worker.index();
        let peers = worker.peers();

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for additions
        // let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for deletions

        let timer = ::std::time::Instant::now();

        for _ in 0 .. ((recs as usize) / peers) + if index < ((recs as usize) % peers) { 1 } else { 0 } {
            input.insert((rng1.gen_range(0, keys),()));
        }

        input.close();
        while worker.step() { }

        let elapsed_load = timer.elapsed();
        if index == 0 {
            eprintln!("{:?}", elapsed_load);
            let elapsed1_ns = elapsed_load.as_secs() * 1_000_000_000 + (elapsed_load.subsec_nanos() as u64);
            println!("INSTALL\tLOADING\t{}\t{}\t{}", peers, elapsed1_ns, 1000000000.0 * (recs as f64) / (elapsed1_ns as f64));
        }

        for i in 0 .. 21 {
            let mut size = 1 << i;
            let worker_size = size / peers + if (size % peers) < index { 1 } else { 0 };

            let mut counts = vec![[0u64; 16]; 64];

            for _ in 0 .. 10_000 {
                let timer = ::std::time::Instant::now();
                worker.dataflow(|scope| {
                    let data = scope.new_collection_from((0 .. worker_size).map(move |x| (x * peers) + index)).1;
                    trace.import(scope).semijoin(&data).probe();
                });
                while worker.step() { }

                let elapsed = timer.elapsed();
                let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);
                let count_index = elapsed_ns.next_power_of_two().trailing_zeros() as usize;
                let low_bits = (elapsed_ns >> (count_index - 5)) & 0xF;
                counts[count_index][low_bits as usize] += 1;
            }

            if index == 0 {
                let mut results = Vec::new();
                let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
                let mut sum = 0;
                for index in (10 .. counts.len()).rev() {
                    for sub in (0 .. 16).rev() {
                        if sum > 0 && sum < total {
                            let latency = (1 << (index-1)) + (sub << (index-5));
                            let fraction = (sum as f64) / (total as f64);
                            results.push((latency, fraction));
                        }
                        sum += counts[index][sub];
                    }
                }
                for (latency, fraction) in results.drain(..).rev() {
                    println!("INSTALL\tLATENCY\t{}\t{}\t{}", size, latency, fraction);
                }
            }
        }
    }).unwrap();
}
