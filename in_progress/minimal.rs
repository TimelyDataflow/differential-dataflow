extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;

// use timely::drain::DrainExt;

use differential_dataflow::operators::*;

fn main() {

    // let communicator = ThreadCommunicator;
    //
    // let start = time::precise_time_s();
    //
    // let mut computation = GraphRoot::new(communicator);
    // let (mut input, mut output) = computation.subcomputation(|builder| {
    //
    //     // start with a collection of inputs
    //     let (input, edges) = builder.new_input::<((u32,u32), i32)>();
    //
    //     // produce (node, degr) pairs for each node in the graph
    //     let degrs = edges.group_by_u(|x| x, |k,v| (*k,*v), |_k, s, t| { t.push((s.count() as u32, 1)); });
    //
    //     // produce (node, neig, degr) pairs for each (node, neig) edge
    //     let neigh = degrs.join_u(&edges, |x| x, |y| y, |k,x,y| (*k,*x,*y));
    //
    //     // collect those triples by neig; report the number of degree 1 neighbors.
    //     let dpdnt = neigh.group_by_u(|x| (x.1, x.2), |k,v| (*k,*v), |_k, s, t| { t.push((s.filter(|x| *x.1 > 1).count() as u32, 1)) });
    //
    //     dpdnt.inspect_batch(|t,x| println!("output at {:?}:\t{:?}", t, x));
    //
    //     let (output, degleaf) = builder.new_input::<(u32, i32)>();
    //
    //     edges.filter(|_| false)
    //          .iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {
    //
    //              let edges = edges.enter(&inner.scope());
    //
    //              // prep the same computation using the iterated data
    //              let degrs_o = degrs.enter(&inner.scope());
    //              let neigh_o = neigh.enter(&inner.scope());
    //              let dpdnt_o = dpdnt.enter(&inner.scope());
    //
    //              let degrs_i = inner.group_by_u(|x| x, |k,v| (*k,*v), |_k, s, t| { t.push((s.count() as u32, 1)); });
    //              let neigh_i = degrs_i.join_u(&inner, |x| x, |y| y, |k,x,y| (*k,*x,*y));
    //              let dpdnt_i = neigh_i.group_by_u(|x| (x.1, x.2), |k,v| (*k,*v), |_k, s, t| { t.push((s.filter(|x| *x.0 > 1).count() as u32, 1)) });
    //
    //              // list of keys (neighbors) for each output vertex
    //              let dpdnt_k = dpdnt_o.concat(&dpdnt_i).map(|((k,(v1,v2)),w)| ((v1,k),w));
    //
    //              // list of
    //              let neigh_k = neigh_o.concat(&neigh_i).map(|((k,x,y),w)| ((x,k),w));
    //
    //              let edgesa = edges.concat(&inner);
    //
    //              let outs = inner.builder.enter(&degleaf).join_u();
    //
    //              // monotonically increase the required elements
    //              edges.join_u(&)
    //                   .concat(&inner)
    //          })
    //          .inspect_batch(|t,x| println!("explain at {:?}:\t{:?}", t, x));
    //
    //     (input, output)
    // });
    //
    // input.advance_to(1);
    // input.close();
    //
    // output.advance_to(1);
    // output.close();
    //
    // while computation.step() { }
    // computation.step(); // shut down
    //
    // println!("computation finished at {}", time::precise_time_s() - start);
}
