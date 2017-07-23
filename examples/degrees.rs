extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;
// extern crate vec_map;

// use timely::dataflow::*;
// use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
// use differential_dataflow::trace::Trace;
// use differential_dataflow::{Collection, AsCollection};
// use differential_dataflow::operators::*;
// use differential_dataflow::operators::join::JoinArranged;
// use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::operators::count::CountTotal;
// use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
// use differential_dataflow::lattice::Lattice;
// use differential_dataflow::trace::implementations::hash::HashValSpine as Spine;

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
            let (input, edges) = scope.new_collection();

            // if kc1 { edges = kcore1(&edges, std::env::args().nth(4).unwrap().parse().unwrap()); }
            // if kc2 { edges = kcore2(&edges, std::env::args().nth(4).unwrap().parse().unwrap()); }

            let degrs = edges.map(|(src, _dst)| src)
                             .count_total_u();

            // pull of count, and count.
            let distr = degrs.map(|(_src, cnt)| cnt as u32)
                             .count_total_u();

            // show us something about the collection, notice when done.
            let probe = distr//.inspect(|x| println!("observed: {:?}", x))
                             .probe();

            (input, probe)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        // load up graph dataz
        for _ in 0 .. (edges / peers) + if index < (edges % peers) { 1 } else { 0 } {
            input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)))
        }

        input.advance_to(1);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        if batch > 0 {
            for wave in 1 .. {
                for round in 0 .. batch {
                    input.advance_to(((wave * batch) + round) * peers + index);
                    input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));                    
                }

                input.advance_to((wave + 1) * batch * peers);
                input.flush();

                let timer = ::std::time::Instant::now();
                worker.step_while(|| probe.less_than(input.time()));

                if index == 0 {
                    println!("round {} finished after {:?}", wave * batch * peers, timer.elapsed());
                }
            }
        }
    }).unwrap();
}

// fn kcore1<G: Scope>(edges: &Collection<G, (u32, u32)>, k: isize) -> Collection<G, (u32, u32)> 
// where G::Timestamp: Lattice+Ord {

//     edges.iterate(|inner| {
//         // determine active vertices
//         let active = inner.flat_map(|(src,dst)| Some((src,())).into_iter().chain(Some((dst,())).into_iter()))
//                           .group(move |_k, s, t| { if s[0].1 > k { t.push(((),1)) } })
//                           .map(|(k,_)| k);
//                           // .threshold_u(move |_,cnt| if cnt >= k { 1 } else { 0 });

//         // restrict edges active vertices, return result
//         edges.enter(&inner.scope())
//              .semijoin(&active)
//              .map(|(src,dst)| (dst,src))
//              .semijoin(&active)
//              .map(|(dst,src)| (src,dst))
//     })
// }

// fn kcore2<G: Scope>(edges: &Collection<G, (u32, u32)>, k: isize) -> Collection<G, (u32, u32)> 
// where G::Timestamp: Lattice+::std::hash::Hash+Ord {

//     edges.iterate(move |inner| {
//         // determine active vertices
//         let active = inner.flat_map(|(src,dst)| Some(src).into_iter().chain(Some(dst).into_iter()))
//                           .arrange_by_self()
//                           .group_arranged(move |_k, s, t| { if s[0].1 > k { t.push(((),1)) } }, Spine::new());

//         // restrict edges active vertices, return result
//         edges.enter(&inner.scope())
//              .arrange_by_key_hashed()
//              .join_core(&active, |k,v,_| Some((k.item.clone(), v.clone())))
//              .map(|(src,dst)| (dst,src))
//              .arrange_by_key_hashed()
//              .join_core(&active, |k,v,_| Some((k.item.clone(), v.clone())))
//              .map(|(dst,src)| (src,dst))
//     })
// }