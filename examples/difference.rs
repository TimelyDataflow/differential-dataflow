extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;

use timely::dataflow::operators::*;

use differential_dataflow::difference::DiffPair;
use differential_dataflow::AsCollection;
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

            let (input, data) = scope.new_input::<((usize, isize), isize)>();

            // move `val` into the ring component.
            data.map(|((x,y),d)| (x, Default::default(), DiffPair::new(y,d)))
				.as_collection()
                .consolidate();

            input
        });

        let timer = ::std::time::Instant::now();

        for val in 0 .. {
        	// introduce some data with bounded key.
        	input.send(((val % keys, (val as isize) % 100), 1 as isize));

        	// we must still give the computation the opportunity to act.
        	if val > 0 && val % batch == 0 { 
        		worker.step(); 
        		let elapsed = timer.elapsed();
        		let secs = elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64)/1000000000.0;
        		println!("tuples: {:?},\telts/sec: {:?}", val, val as f64 / secs);
        	}
        }

    }).unwrap();
}