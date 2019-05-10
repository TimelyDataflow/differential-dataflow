//! Shared read access to a trace.

use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::default::Default;
use std::collections::VecDeque;

use timely::dataflow::Scope;
use timely::dataflow::operators::generic::source;
use timely::progress::Timestamp;
use timely::dataflow::operators::CapabilitySet;

use lattice::Lattice;
use trace::{Trace, TraceReader, Batch, BatchReader, Cursor};

use trace::wrappers::rc::TraceBox;

use timely::scheduling::Activator;

use super::{TraceWriter, TraceAgentQueueWriter, TraceAgentQueueReader, Arranged};
use super::TraceReplayInstruction;

/// A `TraceReader` wrapper which can be imported into other dataflows.
///
/// The `TraceAgent` is the default trace type produced by `arranged`, and it can be extracted
/// from the dataflow in which it was defined, and imported into other dataflows.
pub struct TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    trace: Rc<RefCell<TraceBox<Tr>>>,
    queues: Weak<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
    advance: Vec<Tr::Time>,
    through: Vec<Tr::Time>,
}

impl<Tr> TraceReader for TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = Tr::Batch;
    type Cursor = Tr::Cursor;
    fn advance_by(&mut self, frontier: &[Tr::Time]) {
        self.trace.borrow_mut().adjust_advance_frontier(&self.advance[..], frontier);
        self.advance.clear();
        self.advance.extend(frontier.iter().cloned());
    }
    fn advance_frontier(&mut self) -> &[Tr::Time] {
        &self.advance[..]
    }
    fn distinguish_since(&mut self, frontier: &[Tr::Time]) {
        self.trace.borrow_mut().adjust_through_frontier(&self.through[..], frontier);
        self.through.clear();
        self.through.extend(frontier.iter().cloned());
    }
    fn distinguish_frontier(&mut self) -> &[Tr::Time] {
        &self.through[..]
    }
    fn cursor_through(&mut self, frontier: &[Tr::Time]) -> Option<(Tr::Cursor, <Tr::Cursor as Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>>::Storage)> {
        self.trace.borrow_mut().trace.cursor_through(frontier)
    }
    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, f: F) { self.trace.borrow_mut().trace.map_batches(f) }
}

impl<Tr> TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Timestamp+Lattice,
{
    /// Creates a new agent from a trace reader.
    pub fn new(trace: Tr) -> (Self, TraceWriter<Tr>)
    where
        Tr: Trace,
        Tr::Batch: Batch<Tr::Key,Tr::Val,Tr::Time,Tr::R>,
    {
        let trace = Rc::new(RefCell::new(TraceBox::new(trace)));
        let queues = Rc::new(RefCell::new(Vec::new()));

        let reader = TraceAgent {
            trace: trace.clone(),
            queues: Rc::downgrade(&queues),
            advance: trace.borrow().advance_frontiers.frontier().to_vec(),
            through: trace.borrow().through_frontiers.frontier().to_vec(),
        };

        let writer = TraceWriter::new(
            vec![Default::default()],
            Rc::downgrade(&trace),
            queues,
        );

        (reader, writer)
    }

    /// Attaches a new shared queue to the trace.
    ///
    /// The queue is first populated with existing batches from the trace,
    /// The queue will be immediately populated with existing historical batches from the trace, and until the reference
    /// is dropped the queue will receive new batches as produced by the source `arrange` operator.
    pub fn new_listener(&mut self, activator: Activator) -> TraceAgentQueueReader<Tr>
    where
        Tr::Time: Default
    {
        // create a new queue for progress and batch information.
        let mut new_queue = VecDeque::new();

        // add the existing batches from the trace
        let mut upper = None;
        self.trace
            .borrow_mut()
            .trace
            .map_batches(|batch| {
                new_queue.push_back(TraceReplayInstruction::Batch(batch.clone(), Some(Default::default())));
                upper = Some(batch.upper().to_vec());
                // new_queue.push_back((vec![Default::default()], batch.clone(), Some(Default::default())));
            });

        if let Some(upper) = upper {
            new_queue.push_back(TraceReplayInstruction::Frontier(upper));
        }

        let reference = Rc::new((activator, RefCell::new(new_queue)));

        // wraps the queue in a ref-counted ref cell and enqueue/return it.
        if let Some(queue) = self.queues.upgrade() {
            queue.borrow_mut().push(Rc::downgrade(&reference));
        }
        reference.0.activate();
        reference
    }
}

impl<Tr> TraceAgent<Tr>
where
    Tr: TraceReader+'static,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    /// Copies an existing collection into the supplied scope.
    ///
    /// This method creates an `Arranged` collection that should appear indistinguishable from applying `arrange`
    /// directly to the source collection brought into the local scope. The only caveat is that the initial state
    /// of the collection is its current state, and updates occur from this point forward. The historical changes
    /// the collection experienced in the past are accumulated, and the distinctions from the initial collection
    /// are no longer evident.
    ///
    /// The current behavior is that the introduced collection accumulates updates to some times less or equal
    /// to `self.advance_frontier()`. There is *not* currently a guarantee that the updates are accumulated *to*
    /// the frontier, and the resulting collection history may be weirdly partial until this point. In particular,
    /// the historical collection may move through configurations that did not actually occur, even if eventually
    /// arriving at the correct collection. This is probably a bug; although we get to the right place in the end,
    /// the intermediate computation could do something that the original computation did not, like diverge.
    ///
    /// I would expect the semantics to improve to "updates are advanced to `self.advance_frontier()`", which
    /// means the computation will run as if starting from exactly this frontier. It is not currently clear whose
    /// responsibility this should be (the trace/batch should only reveal these times, or an operator should know
    /// to advance times before using them).
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Configuration;
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeBySelf;
    /// use differential_dataflow::operators::reduce::Reduce;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    /// use differential_dataflow::hashable::OrdWrapper;
    ///
    /// fn main() {
    ///     ::timely::execute(Configuration::Thread, |worker| {
    ///
    ///         // create a first dataflow
    ///         let mut trace = worker.dataflow::<u32,_,_>(|scope| {
    ///             // create input handle and collection.
    ///             scope.new_collection_from(0 .. 10).1
    ///                  .arrange_by_self()
    ///                  .trace
    ///         });
    ///
    ///         // do some work.
    ///         worker.step();
    ///         worker.step();
    ///
    ///         // create a second dataflow
    ///         worker.dataflow(move |scope| {
    ///             trace.import(scope)
    ///                  .reduce(move |_key, src, dst| dst.push((*src[0].0, 1)));
    ///         });
    ///
    ///     }).unwrap();
    /// }
    /// ```
    pub fn import<G>(&mut self, scope: &G) -> Arranged<G, TraceAgent<Tr>>
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp,
    {
        self.import_named(scope, "ArrangedSource")
    }

    /// Same as `import`, but allows to name the source.
    pub fn import_named<G>(&mut self, scope: &G, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp,
    {
        // Drop ShutdownButton and return only the arrangement.
        self.import_core(scope, name).0
    }
    /// Imports an arrangement into the supplied scope.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Configuration;
    /// use timely::dataflow::ProbeHandle;
    /// use timely::dataflow::operators::Probe;
    /// use differential_dataflow::input::InputSession;
    /// use differential_dataflow::operators::arrange::ArrangeBySelf;
    /// use differential_dataflow::operators::reduce::Reduce;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    /// use differential_dataflow::hashable::OrdWrapper;
    ///
    /// fn main() {
    ///     ::timely::execute(Configuration::Thread, |worker| {
    ///
    ///         let mut input = InputSession::<_,(),isize>::new();
    ///         let mut probe = ProbeHandle::new();
    ///
    ///         // create a first dataflow
    ///         let mut trace = worker.dataflow::<u32,_,_>(|scope| {
    ///             // create input handle and collection.
    ///             input.to_collection(scope)
    ///                  .arrange_by_self()
    ///                  .trace
    ///         });
    ///
    ///         // do some work.
    ///         worker.step();
    ///         worker.step();
    ///
    ///         // create a second dataflow
    ///         let mut shutdown = worker.dataflow(|scope| {
    ///             let (arrange, button) = trace.import_core(scope, "Import");
    ///             arrange.stream.probe_with(&mut probe);
    ///             button
    ///         });
    ///
    ///         worker.step();
    ///         worker.step();
    ///         assert!(!probe.done());
    ///
    ///         shutdown.press();
    ///
    ///         worker.step();
    ///         worker.step();
    ///         assert!(probe.done());
    ///
    ///     }).unwrap();
    /// }
    /// ```
    pub fn import_core<G>(&mut self, scope: &G, name: &str) -> (Arranged<G, TraceAgent<Tr>>, ShutdownButton<CapabilitySet<Tr::Time>>)
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp,
    {
        let trace = self.clone();

        // Capabilities shared with a shutdown button.
        // let shutdown_button = ShutdownButton::new(capabilities.clone());

        let mut shutdown_button = None;

        let stream = {

            let mut shutdown_button_ref = &mut shutdown_button;
            source(scope, name, move |capability, info| {

                let capabilities = Rc::new(RefCell::new(Some(CapabilitySet::new())));

                let activator = scope.activator_for(&info.address[..]);
                let queue = self.new_listener(activator);

                let activator = scope.activator_for(&info.address[..]);
                *shutdown_button_ref = Some(ShutdownButton::new(capabilities.clone(), activator));

                capabilities.borrow_mut().as_mut().unwrap().insert(capability);

                move |output| {

                    let mut capabilities = capabilities.borrow_mut();
                    if let Some(ref mut capabilities) = *capabilities {

                        let mut borrow = queue.1.borrow_mut();
                        for instruction in borrow.drain(..) {
                            match instruction {
                                TraceReplayInstruction::Frontier(frontier) => {
                                    // println!("DOWNGRADE: {:?}", frontier);
                                    capabilities.downgrade(&frontier[..]);
                                },
                                TraceReplayInstruction::Batch(batch, hint) => {
                                    if let Some(time) = hint {
                                        // println!("TIME: {:?}", time);
                                        let delayed = capabilities.delayed(&time);
                                        output.session(&delayed).give(batch);
                                    }
                                }
                            }
                        }
                        // for (frontier, batch, hint) in borrow.drain(..) {

                        //     println!("REPLAY\t{:?}, {:?}, {:?}", frontier, batch.description(), hint);

                        //     if let Some(time) = hint {
                        //         let delayed = capabilities.delayed(&time);
                        //         output.session(&delayed).give(batch);
                        //     }

                        //     capabilities.downgrade(&frontier[..]);
                        // }
                    }
                }
            })
        };

        (Arranged { stream, trace }, shutdown_button.unwrap())
    }
}



/// Wrapper than can drop shared references.
pub struct ShutdownButton<T> {
    reference: Rc<RefCell<Option<T>>>,
    activator: Activator,
}

impl<T> ShutdownButton<T> {
    /// Creates a new ShutdownButton.
    pub fn new(reference: Rc<RefCell<Option<T>>>, activator: Activator) -> Self {
        Self { reference, activator }
    }
    /// Push the shutdown button, dropping the shared objects.
    pub fn press(&mut self) {
        *self.reference.borrow_mut() = None;
        self.activator.activate();
    }
}

impl<Tr> Clone for TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    fn clone(&self) -> Self {

        // increase counts for wrapped `TraceBox`.
        self.trace.borrow_mut().adjust_advance_frontier(&[], &self.advance[..]);
        self.trace.borrow_mut().adjust_through_frontier(&[], &self.through[..]);

        TraceAgent {
            trace: self.trace.clone(),
            queues: self.queues.clone(),
            advance: self.advance.clone(),
            through: self.through.clone(),
        }
    }
}


impl<Tr> Drop for TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    fn drop(&mut self) {
        // decrement borrow counts to remove all holds
        self.trace.borrow_mut().adjust_advance_frontier(&self.advance[..], &[]);
        self.trace.borrow_mut().adjust_through_frontier(&self.through[..], &[]);
    }
}