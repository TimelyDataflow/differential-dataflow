extern crate rand;
extern crate timely;
extern crate differential_dataflow;
extern crate core_affinity;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::{Exchange, Probe};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::count::CountTotalCore;
use differential_dataflow::operators::{Join, JoinCore};

use differential_dataflow::trace::implementations::ord::OrdKeySpine;

fn main() {

    let mut args = std::env::args();
    args.next();

    let keys: usize = args.next().unwrap().parse().unwrap();
    let recs: usize = args.next().unwrap().parse().unwrap();
    // let rate: usize = args.next().unwrap().parse().unwrap();
    let work: usize = args.next().unwrap().parse().unwrap_or(usize::max_value());

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let index = worker.index();
        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index]);

        // create a a degree counting differential dataflow
        let (mut input, mut trace_handle, probe) = worker.dataflow::<u64,_,_>(|scope| {

            let (handle, data) = scope.new_collection();

            let arranged = data.arrange(OrdKeySpine::<usize, Product<RootTimestamp,u64>,isize>::with_effort(work));
            let trace_handle = arranged.trace;
            let probe = arranged.stream.probe();

            (handle, trace_handle, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for deletions

        let timer = ::std::time::Instant::now();

        // eprintln!("{} -> {}", index, ((recs as usize) / peers) + if index < ((recs as usize) % peers) { 1 } else { 0 });
        for _ in 0 .. ((recs as usize) / peers) + if index < ((recs as usize) % peers) { 1 } else { 0 } {
            input.insert((rng1.gen_range(0, keys),()));
        }

        input.advance_to(1u64);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }

        let elapsed_load = timer.elapsed();
        if index == 0 {
            eprintln!("{:?}", elapsed_load);
            let elapsed1_ns = elapsed_load.as_secs() * 1_000_000_000 + (elapsed_load.subsec_nanos() as u64);
            println!("ARRANGE\tLOADING\t{}\t{}\t{}", peers, elapsed1_ns, 1000000000.0 * (recs as f64) / (elapsed1_ns as f64));
        }

        let (mut installed_input, installed_probe) = worker.dataflow::<u64,_,_>(|scope| {
            let (handle, data) = scope.new_collection::<(usize, ()), isize>();
            let probe = trace_handle.import(scope).join(&data).probe();
            (handle, probe)
        });

        ::std::mem::drop(trace_handle);

        // eprintln!("{} -> {}", index, ((recs as usize) / peers) + if index < ((recs as usize) % peers) { 1 } else { 0 });
        // for _ in 0 .. ((recs as usize) / peers) + if index < ((recs as usize) % peers) { 1 } else { 0 } {
        //     input.insert((rng1.gen_range(0, keys),()));
        //     input.remove((rng2.gen_range(0, keys),()));
        // }

        input.advance_to(2u64);
        input.flush();
        installed_input.advance_to(2u64);
        installed_input.flush();
        while installed_probe.less_than(input.time()) { worker.step(); }

        if index == 0 {
            let elapsed1 = timer.elapsed() - elapsed_load;
            eprintln!("{:?}", elapsed1);
            let elapsed1_ns = elapsed1.as_secs() * 1_000_000_000 + (elapsed1.subsec_nanos() as u64);
            println!("ARRANGE\tINSTALLING\t{}\t{}\t{}", peers, elapsed1_ns, 1000000000.0 * (recs as f64) / (elapsed1_ns as f64));
        }

    }).unwrap();
}
