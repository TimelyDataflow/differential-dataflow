extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::input::Input;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::operators::Consolidate;

fn main() {

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    // This computation demonstrates in-place accumulation of arbitrarily large 
    // volumes of input data, so long as the number of distinct keys is moderate.
    // 
    // Specifically, the program aims to accumulate (key, val) pairs where we are
    // interested in the sum of values for each key, as well as the count of the
    // number of records, so that we can produce the average.
    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let mut input = worker.dataflow::<(), _, _>(|scope| {

            let (input, data) = scope.new_collection::<_, isize>();

            // move `val` into the ring component.
            data.explode(|(x,y)| Some((x, DiffPair::new(y, 1))))
                .consolidate_u();

            input
        });

        let timer = ::std::time::Instant::now();

        let mut val = 0 as usize;
        loop {

            for _ in 0 .. batch {
        	    // introduce some data with bounded key.
        	    input.insert(((val % keys), (val as isize) % 32));
                val += 1;
            }

            worker.step(); 
            let elapsed = timer.elapsed();
            let secs = elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64)/1000000000.0;
            println!("tuples: {:?},\telts/sec: {:?}", val, val as f64 / secs);
        }

    }).unwrap();
}