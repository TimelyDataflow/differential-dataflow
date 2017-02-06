extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
extern crate vec_map;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    let use_old = std::env::args().find(|x| x == "old").is_some();
    let use_new = std::env::args().find(|x| x == "new").is_some();


    timely::execute_from_args(std::env::args().skip(4), move |computation| {

    	let index = computation.index();
    	let peers = computation.peers();

    	// create a a degree counting differential dataflow
    	let (mut input, probe) = computation.scoped(|scope| {

    		// create edge input, count a few ways.
    		let (input, edges) = scope.new_input::<((u32, u32), i32)>();

    		// pull off source, and count.
    		let edges = edges.as_collection()
    						 // .inspect(|x| println!("{:?}", x))
    						 ;

    		let old = if use_old { 
    			Some(edges.map(|(src,_)| (src, true))
    					  .group(|_, input, output| { output.push((input.next().unwrap().1, 1)); }))
    		}
    		else {
    			None
    		};

    		let new = if use_new {
    			Some(edges.map(|(src,_)| (src, true))
    					  .group_alt(|_key, input, output| { output.push((input[0].1, 1)); }))
    		}
    		else {
    			None
    		};

    		let probe = match (old, new) {
    			(Some(old), Some(new)) => old.negate()
			    						     .concat(&new)
			    						     .consolidate()
			    						     .inspect_batch(|t,xs| panic!("{:?}:\terror: {:?}", t, xs))
			    						     .probe().0,
			    (Some(old), None)	   => old.probe().0,
			    (None, Some(new))	   => new.probe().0,
			    (None, None)		   => panic!("Specify either 'old' or 'new'"),
			};

		    (input, probe)
    	});

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        // load up graph dataz
        for edge in 0..edges {
        	if edge % peers == index {
        		input.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
        	}

        	// move the data along a bit
        	if edge % 10000 == 9999 {
        		computation.step();
        	}
		}

		// let timer = ::std::time::Instant::now();

		input.advance_to(1);
		computation.step_while(|| probe.lt(input.time()));

		if index == 0 {
			// let timer = timer.elapsed();
			// let nanos = timer.as_secs() * 1000000000 + timer.subsec_nanos() as u64;
			// println!("Loading finished after {:?}", nanos);
		}

		// change graph, forever
		if batch > 0 {

			for edge in 0usize .. 5000 {
				if edge % peers == index {
	        		input.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
	        		input.send(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
				}

	        	if edge % batch == (batch - 1) {

	        		let timer = ::std::time::Instant::now();

	        		let next = input.epoch() + 1;
	        		input.advance_to(next);
					computation.step_while(|| probe.lt(input.time()));

					if index == 0 {
						let timer = timer.elapsed();
						let nanos = timer.as_secs() * 1000000000 + timer.subsec_nanos() as u64;
						// println!("Round {} finished after {:?}", next - 1, nanos);
						println!("{}", nanos);
					}
	        	}
	        }
	    }

    }).unwrap();
}