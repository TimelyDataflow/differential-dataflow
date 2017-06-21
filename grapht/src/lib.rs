extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;

use std::rc::Rc;
use std::collections::HashMap;

use timely_communication::Allocator;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle as ProbeHandle;

// stuff for talking about shared trace types ...
use differential_dataflow::hashable::UnsignedWrapper;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;

pub type RootTime = timely::progress::nested::product::Product<timely::progress::timestamp::RootTimestamp, usize>;
type TraceBatch = OrdValBatch<UnsignedWrapper<usize>, usize, RootTime, isize>;
type TraceSpine = Spine<UnsignedWrapper<usize>, usize, RootTime, isize, Rc<TraceBatch>>;
pub type TraceHandle = TraceAgent<UnsignedWrapper<usize>, usize, RootTime, isize, TraceSpine>;

// &mut Child<Root<Allocator>,usize>, 
//     handles: &mut HashMap<String, TraceHandle>, 
//     probe: &mut ProbeHandle<RootTime>,
//     args: &[String]) 

pub type Environment<'a, 'b> = (&'a mut Child<'b, Root<Allocator>,usize>, &'a mut HashMap<String, TraceHandle>, &'a mut ProbeHandle<RootTime>, &'a [String]);

// pub struct Environment<'a, 'b> {
//     pub worker: &'a mut Child<'b, Root<Allocator>,usize>,
//     pub handles: &'a mut HashMap<String, TraceHandle>,
//     pub probe: &'a mut ProbeHandle<RootTime>,
//     pub args: &'a [String],
// }