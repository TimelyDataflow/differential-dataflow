//! Differential dataflow is a high-throughput, low-latency data-parallel programming framework.
//!
//! Differential dataflow programs are written in a collection-oriented style, where you transform
//! collections of records using traditional operations like `map`, `filter`, `join`, and `group_by`. 
//! Differential dataflow also includes the less traditional operation `iterate`, which allows you
//! to repeatedly apply differential dataflow transformations to collections.
//!
//! Once you have defined a differential dataflow computation, you may then add records to or remove 
//! records from its inputs; the system will automatically update the computation's outputs with the 
//! appropriate corresponding additions and removals, and report these changes to you.
//!
//! Differential dataflow is built on the [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) 
//! framework for data-parallel programming which automatically parallelizes across multiple threads, 
//! processes, and computers. Furthermore, because it uses timely dataflow's primitives, it seamlessly 
//! inter-operates with other timely dataflow computations.
//!
//! Differential dataflow is still very much a work in progress, with features and ergonomics still
//! wildly in development. It is generally improving, though.
//!
//! # Examples
//!
//! This fragment creates a collection of pairs of integers, imagined as graph edges, and then counts 
//! first the number of times the source coordinate occurs, and then the number of times each count 
//! occurs, giving us a sense for the distribution of degrees in the graph.
//! 
//! ```ignore
//! // create a a degree counting differential dataflow
//! let (mut input, probe) = worker.dataflow(|scope| {
//!
//!     // create edge input, count a few ways.
//!     let (input, edges) = scope.new_collection();
//!
//!     // extract the source field, and then count.
//!     let degrs = edges.map(|(src, _dst)| src)
//!                      .count();
//!
//!     // extract the count field, and then count them.
//!     let distr = degrs.map(|(_src, cnt)| cnt)
//!                      .count();
//!
//!     // report the changes to the count collection, notice when done.
//!     let probe = distr.inspect(|x| println!("observed: {:?}", x))
//!                      .probe();
//!
//!     (input, probe)
//! });
//! ```
//! 
//! Now assembled, we can drive the computation like a timely dataflow computation, by pushing update
//! records (triples of data, time, and change in count) at the `input` stream handle. The `probe` is
//! how timely dataflow tells us that we have seen all corresponding output updates (in case there are
//! none).
//!
//! ```ignore
//! loop {
//!     let time = input.epoch();
//!     for round in time .. time + 100 {
//!         input.advance_to(round);
//!         input.insert((round % 13, round % 7));
//!     }
//!
//!     input.flush();
//!     while probe.less_than(input.time()) { 
//!        worker.step(); 
//!     }
//! }
//! ```
//! 
//! This example should print out the 100 changes in the output, in this case each reflecting the increase
//! of some node degree by one (typically four output changes, corresponding to the addition and deletion 
//! of the new and old counts of the old and new degrees of the affected node).

#![forbid(missing_docs)]

use std::fmt::Debug;

pub use collection::{Collection, AsCollection};
pub use hashable::Hashable;
pub use difference::Diff;

/// A composite trait for data types usable in differential dataflow.
///
/// Most differential dataflow operators require the ability to cancel corresponding updates, and the
/// way that they do this is by putting the data in a canonical form. The `Ord` trait allows us to sort
/// the data, at which point we can consolidate updates for equivalent records.
pub trait Data : timely::ExchangeData + Ord + Debug { }
impl<T: timely::ExchangeData + Ord + Debug> Data for T { }

extern crate fnv;
extern crate timely;
extern crate timely_sort;
extern crate timely_communication;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

pub mod hashable;
pub mod operators;
pub mod lattice;
pub mod trace;
pub mod input;
pub mod difference;
pub mod collection;