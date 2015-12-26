// extern crate rand;
// extern crate time;
// extern crate columnar;
// extern crate timely;
// extern crate differential_dataflow;
//
// use std::mem;
//
// use std::hash::Hash;
// use timely::example_shared::*;
// use timely::example_shared::operators::*;
// use timely::communication::ThreadCommunicator;
//
// use rand::{Rng, SeedableRng, StdRng};
//
// use differential_dataflow::Collection;
// use differential_dataflow::collection_trace::lookup::UnsignedInt;
// use differential_dataflow::collection_trace::LeastUpperBound;
//
// use differential_dataflow::operators::*;
//
fn main() {
//
//             let start = time::precise_time_s();
//             let start2 = start.clone();
//             let mut computation = GraphRoot::new(ThreadCommunicator);
//
//             let mut input = computation.subcomputation(|builder| {
//
//                 let (input, mut edges) = builder.new_input();
//
//                 pagerank(&edges).consolidate(|||x| *x)
//                                 .inspect_batch(move |t, x| { println!("{}s:\tobserved at {:?}: {:?} changes",
//                                                              ((time::precise_time_s() - start2)) - (t.inner as f64),
//                                                              t, x.len()) });
//
//                 input
//             });
//
//             // let mut nodes = BufReader::new(File::open("/Users/mcsherry/Projects/Datasets/twitter-dedup.offsets").unwrap());
//             // let mut edges = BufReader::new(File::open("/Users/mcsherry/Projects/Datasets/twitter-dedup.targets").unwrap());
//
//             // let mut sent = 0;
//             // let mut buffer = Vec::new();
//             // let mut offset = nodes.read_u64::<LittleEndian>().unwrap();
//             // assert!(offset == 0);
//             // for node in (0..60000000) {
//             //     let read = nodes.read_u64::<LittleEndian>().unwrap();
//             //     for _ in 0.. (read - offset) {
//             //         let edge = edges.read_u32::<LittleEndian>().unwrap();
//             //         if node % 2 == 0 && edge % 2 == 0 {
//             //             buffer.push(((node / 2 as u32, edge / 2 as u32), 1));
//             //             if buffer.len() > 1000 {
//             //                 sent += buffer.len();
//             //                 input.send_at(0, buffer.drain(..));
//             //                 computation.step();
//             //             }
//             //         }
//             //     }
//             //     offset = read;
//             // }
//             //
//             // sent += buffer.len();
//             // input.send_at(0, buffer.drain(..));
//             //
//             // println!("sent {} edges", sent);
//
//             let nodes = 200_000u32;
//             let edges = 4_000_000;
//
//             println!("determining pagerank of {} nodes, {} edges:", nodes, edges);
//             println!("please note: not actually pagerank yet; don't get excited.");
//
//             let seed: &[_] = &[1, 2, 3, 4];
//             let mut rng1: StdRng = SeedableRng::from_seed(seed);
//             let mut rng2: StdRng = SeedableRng::from_seed(seed);
//
//             rng1.gen::<f64>();
//             rng2.gen::<f64>();
//
//             let mut left = edges;
//             while left > 0 {
//                 let next = if left < 1000 { left } else { 1000 };
//                 input.send_at(0, (0..next).map(|_| ((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1)));
//                 computation.step();
//                 left -= next;
//             }
//
//             println!("input ingested after {}", time::precise_time_s() - start);
//             //
//             // let mut round = 0 as u32;
//             // let mut changes = Vec::new();
//             // while computation.step() {
//             //     if time::precise_time_s() - start >= round as f64 {
//             //         let change_count = 1000;
//             //         for _ in 0..change_count {
//             //             changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
//             //             changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
//             //         }
//             //
//             //         input.send_at(round, changes.drain(..));
//             //         input.advance_to(round + 1);
//             //         round += 1;
//             //     }
//             // }
//
//             input.close();
//
//             while computation.step() { }
//             computation.step(); // shut down
}
//
// fn pagerank<G: GraphBuilder, U: UnsignedInt>(edges: &Collection<G, (U, U)>) -> Collection<G, U>
// where G::Timestamp: LeastUpperBound+Hash {
//
//     let degrs = edges.map(|(x,w)| (x.0,w))
//                      .consolidate(|||x| *x)
//                      .group_by_u(|x| (x,()), |k,v| (*k,*v), |_,s,t| t.push((s[0].1, 1)))
//                      .inspect_batch(|_t, xs| println!("degrees: {:?}", xs.len()))
//                      ;
//
//     // start everyone with 100 units of "rank".
//     edges.group_by_u(|x| (x.0,()), |k,_| *k, |_,_,t| { t.push(((), 10000)) })
//          .iterate(u32::max_value(), |||x| *x, |ranks| {
//
//              let degrs = degrs.enter(&ranks.scope());
//              let edges = edges.enter(&ranks.scope());
//
//              // pair surfers with the out-degree of their location
//              ranks.join_u(&degrs, |n| (n,()), |nc| nc, |n,_,c| (*n,*c))
//                   .inspect_batch(|t, xs| println!("join1ed at {:?}: {:?}", t, xs.len()))
//                   .group_by_u(|x| x, |k,_| *k, |_,s,t| t.push(((), s[0].1 / s[0].0)))
//                   .inspect_batch(|t, xs| println!("grouped at {:?}: {:?}", t, xs.len()))
//                   .join_u(&edges, |n| (n,()), |e| e, |_,_,d| *d)
//                   .inspect_batch(|t, xs| println!("join2ed at {:?}: {:?}\n", t, xs.len()))
//          })
// }
