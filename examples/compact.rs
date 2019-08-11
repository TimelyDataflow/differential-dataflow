extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Threshold;

fn main() {

    let records: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args().skip(3), move |worker| {

        let timer = ::std::time::Instant::now();

        let mut probe = timely::dataflow::operators::probe::Handle::new();

        // create a dataflow managing an ever-changing edge collection.
    	let mut handle = worker.dataflow(|scope| {
            let (handle, input) = scope.new_collection();
            input.distinct().probe_with(&mut probe);
            handle
        });

        println!("{:?}:\tloading edges", timer.elapsed());

        let mut next = batch;
        let mut value = worker.index();
        while value < records {
            if value >= next {
                handle.advance_to(next);
                handle.flush();
                next += batch;
                while probe.less_than(handle.time()) { worker.step(); }
                // println!("{:?}\tround {} loaded", timer.elapsed(), next);
            }
            handle.advance_to(value);
            handle.insert(value);
            value += worker.peers();
        }

        handle.advance_to(records);
        handle.flush();
        while probe.less_than(handle.time()) { worker.step(); }

        println!("{:?}\tdata loaded", timer.elapsed());

        let mut next = batch;
        let mut value = worker.index();
        while value < records {
            if value >= next {
                handle.advance_to(records + next);
                handle.flush();
                next += batch;
                while probe.less_than(handle.time()) { worker.step(); }
                // println!("{:?}\tround {} unloaded", timer.elapsed(), next);
            }
            handle.advance_to(records + value);
            handle.insert(value);
            value += worker.peers();
        }

        handle.advance_to(records + records);
        handle.flush();
        while probe.less_than(handle.time()) { worker.step(); }

        println!("{:?}\tdata unloaded", timer.elapsed());

        while worker.step() { }

    }).unwrap();
}