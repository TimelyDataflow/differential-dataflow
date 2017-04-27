extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
// extern crate vec_map;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::trace::Trace;
use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::operators::*;
use differential_dataflow::operators::join::JoinArranged;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::hash::HashValSpine as Spine;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    // let kc1 = std::env::args().find(|x| x == "kcore1").is_some();
    // let kc2 = std::env::args().find(|x| x == "kcore2").is_some();

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(4), move |worker| {

    	let index = worker.index();
    	let peers = worker.peers();

    	// create a a degree counting differential dataflow
    	let (mut input, probe) = worker.dataflow(|scope| {

    		// create edge input, count a few ways.
    		let (input, edges) = scope.new_input();

    		// pull off source, and count.
    		let mut edges = edges.as_collection();

    		// if kc1 { edges = kcore1(&edges, std::env::args().nth(4).unwrap().parse().unwrap()); }
    		// if kc2 { edges = kcore2(&edges, std::env::args().nth(4).unwrap().parse().unwrap()); }

    		let degrs = edges.map(|(src, _dst)| src)
    						 .count_u();

    		// pull of count, and count.
		    let distr = degrs.map(|(_src, cnt)| cnt as usize)
    						 .count_u();

			// show us something about the collection, notice when done.
			let probe = distr//.inspect(|x| println!("observed: {:?}", x))
							 .probe();

		    (input, probe)
    	});

        let timer = ::std::time::Instant::now();

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

		input.advance_to(1);
		worker.step_while(|| probe.less_than(input.time()));

		if index == 0 {
			// let timer = timer.elapsed();
			// let nanos = timer.as_secs() * 1000000000 + timer.subsec_nanos() as u64;
			println!("Loading finished after {:?}", timer.elapsed());
		}

		if batch > 0 {

			// create a new input session; will batch data for us!
            let mut session = differential_dataflow::input::InputSession::from(&mut input);

            for round in 1 .. {

            	// only do the work of this worker.
            	if round % peers == index {
            		session.advance_to(round);
	        		session.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
	        		session.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
            	}

            	if round % batch == 0 {
            		// all workers indicate they have finished with `round`.
            		session.advance_to(round + 1);
					session.flush();

	        		let timer = ::std::time::Instant::now();
					worker.step_while(|| probe.less_than(session.time()));
					println!("worker {}, round {} finished after {:?}", index, round, timer.elapsed());
            	}
            }
		}
    }).unwrap();
}


// fn kcore1<G: Scope>(edges: &Collection<G, (u32, u32)>, k: isize) -> Collection<G, (u32, u32)> 
// where G::Timestamp: Lattice+Ord {

// 	edges.iterate(|inner| {
// 		// determine active vertices
// 		let active = inner.flat_map(|(src,dst)| Some((src,())).into_iter().chain(Some((dst,())).into_iter()))
// 						  .group(move |_k, s, t| { if s[0].1 > k { t.push(((),1)) } })
// 						  .map(|(k,_)| k);
// 						  // .threshold_u(move |_,cnt| if cnt >= k { 1 } else { 0 });

// 		// restrict edges active vertices, return result
// 	    edges.enter(&inner.scope())
// 	    	 .semijoin(&active)
// 	    	 .map(|(src,dst)| (dst,src))
//      	     .semijoin(&active)
//      	     .map(|(dst,src)| (src,dst))
// 	})
// }

// fn kcore2<G: Scope>(edges: &Collection<G, (u32, u32)>, k: isize) -> Collection<G, (u32, u32)> 
// where G::Timestamp: Lattice+::std::hash::Hash+Ord {

// 	edges.iterate(move |inner| {
// 		// determine active vertices
// 		let active = inner.flat_map(|(src,dst)| Some(src).into_iter().chain(Some(dst).into_iter()))
// 						  .arrange_by_self()
// 						  .group_arranged(move |_k, s, t| { if s[0].1 > k { t.push(((),1)) } }, Spine::new());

// 		// restrict edges active vertices, return result
// 	    edges.enter(&inner.scope())
// 	    	 .arrange_by_key_hashed()
// 	    	 .join_arranged(&active, |k,v,_| (k.item.clone(), v.clone()))
// 	    	 .map(|(src,dst)| (dst,src))
//  	    	 .arrange_by_key_hashed()
//  	    	 .join_arranged(&active, |k,v,_| (k.item.clone(), v.clone()))
//      	     .map(|(dst,src)| (src,dst))
// 	})
// }