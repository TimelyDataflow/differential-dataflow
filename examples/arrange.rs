extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
extern crate vec_map;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::trace::Cursor;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    // define a new timely dataflow computation. 
    timely::execute_from_args(std::env::args().skip(4), move |worker| {

    	let index = worker.index();
    	let peers = worker.peers();

    	// create a a degree counting differential dataflow
    	let (mut input, probe, trace) = worker.scoped(|scope| {

    		// create edge input, count a few ways.
    		let (input, edges) = scope.new_input();

    		// pull off source, and count.
    		let arranged = edges.as_collection()
    							.arrange_by_key_hashed();

		    (input, arranged.stream.probe().0, arranged.trace.clone())
    	});

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        // load up graph dataz
        let &time = input.time();
        for edge in 0..edges {
        	if edge % peers == index {
        		input.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), time, 1));
        	}

        	// move the data along a bit
        	if edge % 10000 == 9999 {
        		worker.step();
        	}
		}

		let timer = ::std::time::Instant::now();

		input.advance_to(1);
		worker.step_while(|| probe.lt(input.time()));

		if index == 0 {
			let timer = timer.elapsed();
			let nanos = timer.as_secs() * 1000000000 + timer.subsec_nanos() as u64;
			println!("Loading finished after {:?}", nanos);
		}

		// change graph, forever
		if batch > 0 {

			for edge in 0usize .. {
				let &time = input.time();
				if edge % peers == index {
	        		input.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), time, 1));
	        		input.send(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), time,-1));
				}

	        	if edge % batch == (batch - 1) {

	        		let timer = ::std::time::Instant::now();

	        		let next = input.epoch() + 1;
	        		input.advance_to(next);
					worker.step_while(|| probe.lt(input.time()));

					if index == 0 {
						let timer = timer.elapsed();
						let nanos = timer.as_secs() * 1000000000 + timer.subsec_nanos() as u64;
						println!("Round {} finished after {:?}", next - 1, nanos);

						let mut count = 0;
		        		let timer = ::std::time::Instant::now();
		        		let mut cursor = trace.cursor();
		        		while cursor.key_valid() {
		        			while cursor.val_valid() {
								let mut sum = 0;		        				
								cursor.map_times(|_,d| sum += d);
								if sum > 0 { count += 1; }
								cursor.step_val();
		        			}

		        			cursor.step_key()
		        		}

		        		println!("count: {} in {:?}", count, timer.elapsed());
					}
	        	}
	        }
	    }

    }).unwrap();
}