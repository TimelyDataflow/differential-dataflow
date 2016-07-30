extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
extern crate vec_map;

use vec_map::VecMap;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use timely_sort::Unsigned;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::operators::*;
use differential_dataflow::operators::join::JoinArranged;
use differential_dataflow::operators::threshold::ThresholdArranged;
use differential_dataflow::operators::group::Count;
use differential_dataflow::lattice::Lattice;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let k: i32 = std::env::args().nth(4).unwrap().parse().unwrap();

    let kc1 = std::env::args().find(|x| x == "kcore1").is_some();
    let kc2 = std::env::args().find(|x| x == "kcore2").is_some();

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(5), move |computation| {

    	let index = computation.index();
    	let peers = computation.peers();

    	// create a a degree counting differential dataflow
    	let (mut input, probe) = computation.scoped(|scope| {

    		// create edge input, count a few ways.
    		let (input, edges) = scope.new_input();

    		
    		// pull off source, and count.
    		let mut edges = edges.as_collection();

    		if kc1 { edges = kcore1(&edges, k); }
    		if kc2 { edges = kcore2(&edges, k); }

    		let degrs = edges//.flat_map(|(src,dst)| Some(src).into_iter().chain(Some(dst).into_iter()))
    						 .map(|(src,_dst)| src)
    						 .count_u();

    		// pull of count, and count.
		    let distr = degrs.map(|(_, cnt)| cnt as u32)
    						 .count_u();

			// show us something about the collection, notice when done.
			let probe = distr//.inspect(|x| println!("observed: {:?}", x))
							.probe().0;

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

		let timer = ::std::time::Instant::now();

		input.advance_to(1);
		computation.step_while(|| probe.lt(input.time()));

		if index == 0 {
			let timer = timer.elapsed();
			let nanos = timer.as_secs() * 1000000000 + timer.subsec_nanos() as u64;
			println!("Loading finished after {:?}", nanos);
		}

		// change graph, forever
		if batch > 0 {

			for edge in 0usize .. {
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
						println!("Round {} finished after {:?}", next - 1, nanos);
					}
	        	}
	        }
	    }

    }).unwrap();
}


fn kcore1<G: Scope>(edges: &Collection<G, (u32, u32)>, k: i32) -> Collection<G, (u32, u32)> 
where G::Timestamp: Lattice {
	edges.iterate(|inner| {
		// determine active vertices
		let active = inner.flat_map(|(src,dst)| Some(src).into_iter().chain(Some(dst).into_iter()))
						  .threshold_u(move |_,cnt| if cnt >= k { 1 } else { 0 });

		// restrict edges active vertices, return result
	    edges.enter(&inner.scope())
	    	 .semijoin_u(&active)
	    	 .map(|(src,dst)| (dst,src))
     	     .semijoin_u(&active)
     	     .map(|(dst,src)| (src,dst))
	})
}

fn kcore2<G: Scope>(edges: &Collection<G, (u32, u32)>, k: i32) -> Collection<G, (u32, u32)> 
where G::Timestamp: Lattice+::std::hash::Hash {

	edges.iterate(move |inner| {
		// determine active vertices
		let active = inner.flat_map(|(src,dst)| Some(src).into_iter().chain(Some(dst).into_iter()))
						  .arrange_by_self(|k| k.as_u64(), |x| (VecMap::new(), x))
                		  .threshold(|k| k.as_u64(), |x| (VecMap::new(), x), move |_,cnt| if cnt >= k { 1 } else { 0 });

		// restrict edges active vertices, return result
	    edges.enter(&inner.scope())
	    	 .arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x))
	    	 .join(&active, |k,v,_| (k.clone(), v.clone()))
	    	 .map(|(src,dst)| (dst,src))
 	    	 .arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x))
 	    	 .join(&active, |k,v,_| (k.clone(), v.clone()))
     	     .map(|(dst,src)| (src,dst))
	})
}