//! Differential dataflow is a high-throughput, low-latency data-parallel programming framework.
//!
//! Differential dataflow programs are written using primitives operations like `map`, `filter`,
//! `join`, and `group_by`, as well as higher-order operations like `iterate`. Having defined a
//! differential dataflow computation, you may add or remove records from its inputs, and the
//! system will automatically update the results of your computation with any corresponding additions
//! and removals.
//!
//! Differential dataflow built on top of the timely dataflow framework for data-parallel programming
//! and so is automatically parallelizable across multiple threads, processes, and computers.
//! Moreover, because it uses timely dataflow's primitives, it seamlessly inter-operates with other
//! timely dataflow computations.
//!
//! It should be stressed that differential dataflow is still very much a work in progress, with
//! features and ergonomics still wildly in development. It is generally improving, though.
//!
//! # Examples
//!
//! The following example is a fragment for computing the breadth-first-search depth in a graph.
//!
//! ```ignore
//! // an example BFS computation fragment in differential dataflow
//! let (e_in, edges) = dataflow.new_input::<((u32, u32), i32)>();
//! let (r_in, roots) = dataflow.new_input::<(u32, i32)>();
//!
//! // initialize roots at distance 0
//! let start = roots.map(|(x, w)| ((x, 0), w));
//!
//! // repeatedly update minimal distances to each node,
//! // by describing how to do one round of updates, and then repeating.
//! let limit = start.iterate(u32::max_value(), |x| x.0, |x| x.0, |dists| {
//!
//!     // bring the invariant edges into the loop
//!     let edges = dists.builder().enter(&edges);
//!
//!     // join current distances with edges to get +1 distances,
//!     // include the current distances in the set as well,
//!     // group by node id and keep minimum distance.
//!     dists.join_u(&edges, |d| d, |e| e, |_,d,n| (*n, d+1))
//!          .concat(&dists)
//!          .group_by_u(|x| x, |k,v| (*k, *v), |_, mut s, t| {
//!              t.push((*s.peek().unwrap().0, 1))
//!          })
//! });
//!
//! limit.inspect(|x| println!("observed: {:?}", x));
//! ```


extern crate timely;
extern crate itertools;
extern crate radix_sort;

pub mod collection_trace;
pub mod sort;
pub mod operators;
pub mod iterators;
