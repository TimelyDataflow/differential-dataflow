extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;

use std::any::Any;
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

// These are all defined here so that users can be assured a common layout.
pub type RootTime = timely::progress::nested::product::Product<timely::progress::timestamp::RootTimestamp, usize>;
type TraceBatch = OrdValBatch<UnsignedWrapper<usize>, usize, RootTime, isize>;
type TraceSpine = Spine<UnsignedWrapper<usize>, usize, RootTime, isize, Rc<TraceBatch>>;
pub type TraceHandle = TraceAgent<UnsignedWrapper<usize>, usize, RootTime, isize, TraceSpine>;

/// Arguments provided to each shared library to help build their dataflows and register their results.
pub type Environment<'a, 'b> = (
    &'a mut Child<'b, Root<Allocator>,usize>, 
    &'a mut TraceHandler, 
    &'a mut ProbeHandle<RootTime>, 
    &'a [String]
);

/// A wrapper around a `HashMap<String, Box<Any>>` that handles downcasting.
pub struct TraceHandler {
    handles: HashMap<String, Box<Any>>,
}

impl TraceHandler {
    /// Create a new trace handler.
    pub fn new() -> Self { TraceHandler { handles: HashMap::new() } }
    /// Acquire a mutable borrow of the value for `name`, if it is of type `T`.
    pub fn get_mut<'a, T: Any>(&'a mut self, name: &str) -> Result<&'a mut T, String> {
        let boxed = self.handles.get_mut(name).ok_or(format!("failed to find handle: {:?}", name))?;
        boxed.downcast_mut::<T>().ok_or(format!("failed to downcast: {}", name))
    }
    /// Assign a thing to key `name`, boxed as `Box<Any>`.
    pub fn set<T: Any>(&mut self, name: String, thing: T) {
        let boxed: Box<Any> = Box::new(thing);
        assert!(boxed.downcast_ref::<T>().is_some());
        self.handles.insert(name, boxed);
    }
    pub fn remove(&mut self, name: &str) {
        self.handles.remove(name);
    }
}