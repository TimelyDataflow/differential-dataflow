extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;

// use timely_communication::Allocator;
// use timely::dataflow::scopes::{Child, Root};

// use differential_dataflow::operators::CountTotal;

// stuff for talking about shared trace types ...
use differential_dataflow::hashable::UnsignedWrapper;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
// use timely::dataflow::operators::probe::Handle as ProbeHandle;

pub type RootTime = timely::progress::nested::product::Product<timely::progress::timestamp::RootTimestamp, usize>;
type TraceBatch = OrdValBatch<UnsignedWrapper<usize>, usize, RootTime, isize>;
type TraceSpine = Spine<UnsignedWrapper<usize>, usize, RootTime, isize, TraceBatch>;
pub type TraceHandle = TraceAgent<UnsignedWrapper<usize>, usize, RootTime, isize, TraceSpine>;