//! Differential dataflow is a high-throughput, low-latency data-parallel programming framework.
//!
//! Differential dataflow programs are written in a collection-oriented style, where multisets of
//! records are transformed and combined using primitives operations like `map`, `filter`,
//! `join`, and `group_by`. Differential dataflow also includes a higher-order operation `iterate`.
//!
//! Having defined a differential dataflow computation, you may then add or remove records from its
//! inputs, and the system will automatically update the computation's outputs with the appropriate
//! corresponding additions and removals.
//!
//! Differential dataflow is built on the timely dataflow framework for data-parallel programming
//! and so is automatically parallelizable across multiple threads, processes, and computers.
//! Moreover, because it uses timely dataflow's primitives, it seamlessly inter-operates with other
//! timely dataflow computations.
//!
//! Differential dataflow is still very much a work in progress, with features and ergonomics still
//! wildly in development. It is generally improving, though.
//!
//! # Examples
//!
//! ```ignore
//! extern crate timely;
//! use timely::*;
//! use timely::dataflow::Scope;
//! use timely::dataflow::operators::{Input, Inspect};
//!
//! use differential_dataflow::operators::*;
//!
//! // construct and execute a timely dataflow
//! timely::execute(Configuration::Thread, |root| {
//!
//!     // construct an input and group its records
//!     // keeping only the smallest values.
//!     let mut input = root.scoped(|scope| {
//!         let (handle, stream) = scope.new_input();
//!         stream.group(|key, vals, output| output.push(vals.next().unwrap()))
//!               .inspect(|val| println!("observed: {:?}", val));
//!
//!         handle
//!     });
//!
//!     // introduce many records
//!     for i in 0..1000 {
//!         input.send((i % 10, i % 3));
//!         input.advance_to(i + 1);
//!         root.step();
//!     }
//! });
//! ```
//!
//!
//!
//! For a more complicated example, the following fragment computes the breadth-first-search depth
//! in a graph.
//!
//! ```ignore
//! extern crate timely;
//! use timely::*;
//! use timely::dataflow::Scope;
//! use timely::dataflow::operators::{Input, Inspect};
//!
//! use differential_dataflow::operators::*;
//!
//! // construct and execute a timely dataflow
//! timely::execute(Configuration::Thread, |root| {
//!
//!     let (edges, roots) = root.scoped(|scope| {
//!
//!         let (e_in, edges) = scope.new_input(); // ((u32, u32), isize);
//!         let (r_in, roots) = scope.new_input(); // (u32, isize);
//!
//!         // initialize roots at distance 0
//!         let start = roots.map(|(x, w)| ((x, 0), w));
//!
//!         // repeatedly update minimal distances to each node,
//!         // by describing how to do one round of updates, and then repeating.
//!         let limit = start.iterate(|dists| {
//!
//!             // bring the invariant edges into the loop
//!             let edges = edges.enter(&dists.scope());
//!
//!             // join current distances with edges to get +1 distances,
//!             // include the current distances in the set as well,
//!             // group by node id and keep minimum distance.
//!             dists.join_map(&edges, |_,&d,&n| (n,d+1))
//!                  .concat(&dists)
//!                  .group(|_, s, t| t.push((s[0].0, 1))
//!         });
//!
//!         // inspect distances!
//!         limit.inspect(|x| println!("observed: {:?}", x));
//!
//!         (e_in, r_in)
//!     });
//!
//!     edges.send(((0,1), 1));
//!     edges.send(((1,2), 1));
//!     edges.send(((2,3), 1));
//!
//!     roots.send((0, 1));
//! });
//! ```

#![forbid(missing_docs)]

use std::fmt::Debug;

/// A change in count.
pub type Delta = isize;

pub use stream::{Collection, AsCollection};
pub use hashable::Hashable;

/// A composite trait for data types usable in differential dataflow.
///
/// This trait differs from Rust's `Hash` trait, which is designed to allow general hash functions 
/// to operate over a type. Here, instead, we are just about providing a value, and allowing the 
/// type to override that behavior if appropriate.
///
/// The trait also requires the specification of an output type. This can be helpful in situations 
/// where we want to cache the hash value, or in situations like radix sorting where we can more 
/// efficiently sort data if we know there are fewer bytes in the integer keys.
pub trait Data : timely::ExchangeData + Ord + Debug { }
impl<T: timely::ExchangeData + Ord + Debug> Data for T { }

extern crate fnv;
extern crate timely;
extern crate vec_map;
extern crate itertools;
extern crate linear_map;
extern crate timely_sort;
extern crate timely_communication;

#[macro_use]
extern crate abomonation;

pub mod hashable;
pub mod operators;
pub mod lattice;
pub mod trace;
mod stream;

