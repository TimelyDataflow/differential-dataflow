//! Program inputs and diagnostics source dataflows.

use super::*;
use timely::dataflow::operators::CapabilityTrait;
use timely::progress::{ChangeBatch, Stamp};

/// A container of corgi columns at the program time, as an input takes them.
pub(super) type InputContainer = HostContainer;

type InputHandle = timely::dataflow::operators::core::unordered_input::UnorderedHandle<HostTime, timely::container::CapacityContainerBuilder<InputContainer>>;
type InputCapability = timely::dataflow::operators::ActivateCapability<HostTime>;

/// Unordered inputs return activating capabilities. Keep their wake-on-drop behavior
/// and stamp each outgoing container with the whole set, as Timely's CapabilitySet does.
struct InputCapabilities(Vec<InputCapability>);

impl CapabilityTrait<HostTime> for InputCapabilities {
    fn stamp(&self) -> Stamp<HostTime> {
        self.0.iter().map(|cap| cap.time().clone()).collect()
    }

    fn valid_for_output(&self, buffer: &Rc<RefCell<ChangeBatch<HostTime>>>, port: usize) -> bool {
        self.0.iter().all(|cap| cap.valid_for_output(buffer, port))
    }
}

/// The number of rows an input converts to columns at once.
pub(super) const COLUMN_BATCH: usize = 1 << 16;

/// An installed program's positional `input N`, or a diagnostic source's: an input of corgi
/// containers at one shape, the input's declared one or its source's.
pub(super) struct ServerInput {
    handle: InputHandle,
    capabilities: InputCapabilities,
    /// The shape of every update the input holds.
    pub(super) shape: RowShape,
    pub(super) width: usize,
}

impl ServerInput {
    pub(super) fn new((handle, capability): (InputHandle, InputCapability), shape: RowShape, width: usize) -> Self {
        ServerInput { handle, capabilities: InputCapabilities(vec![capability]), shape, width }
    }

    /// Add fed `columns` at `time`. Their shapes must be the input's, as its declaration says.
    pub(super) fn give_columns(&mut self, columns: Updates, time: OuterTime) -> Result<(), String> {
        self.give_columns_at(columns, &host_time(time))
    }

    pub(super) fn give_columns_at(&mut self, columns: Updates, time: &HostTime) -> Result<(), String> {
        let shape = (corgi::shape_of_value(&columns.keys), corgi::shape_of_value(&columns.vals));
        if shape != self.shape {
            return Err(format!("a feed of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape)));
        }
        let Updates { keys, vals, diffs } = columns;
        let n = diffs.len();
        let times = crate::runtime::col_times::ColTimes::from_raw_lanes((0..self.width).map(|j| vec![time.get(j).copied().unwrap_or(0); n]).collect(), n);
        self.give(InputContainer { keys, vals, times, diffs });
        Ok(())
    }

    /// Send a container, which must have the input's shape.
    pub(super) fn give(&mut self, mut container: InputContainer) {
        if !container.times.is_empty() {
            assert!(!self.capabilities.0.is_empty(), "a finished input cannot send rows");
            let shape = (corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals));
            assert!(shape == self.shape, "a container of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape));
            self.handle.activate().session(&self.capabilities).give_container(&mut container);
        }
    }

    pub(super) fn frontier(&self) -> impl Iterator<Item = &HostTime> {
        self.capabilities.0.iter().map(|cap| cap.time())
    }

    pub(super) fn time(&self) -> Option<&HostTime> {
        match &self.capabilities.0[..] { [cap] => Some(cap.time()), _ => None }
    }

    /// The caller has checked that every new point is reachable. Derive the entire
    /// replacement from the old set before releasing any of its capabilities.
    pub(super) fn advance_to_frontier(&mut self, frontier: &Antichain<HostTime>) {
        if let ([cap], [point]) = (&mut self.capabilities.0[..], frontier.elements()) {
            cap.downgrade(point);
        } else {
            self.capabilities.0 = frontier.iter().map(|point| {
                self.capabilities.0.iter().find(|cap| cap.time().less_equal(point))
                    .expect("validated input frontier").delayed(point)
            }).collect();
        }
    }

    pub(super) fn advance_to_point(&mut self, time: &HostTime) {
        self.advance_to_frontier(&Antichain::from_elem(time.clone()));
    }

}

impl Server {
    /// Build a diagnostic source's dataflow, one input arranged and published as trace `name` of `shape`, as
    /// corgi chunks. Returns the input, the dataflow's id, and the probe on its arrangement.
    pub(super) fn install_source(&mut self, worker: &mut Worker, name: &str, shape: RowShape) -> (ServerInput, usize, ProbeHandle<HostTime>) {
        let probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        self.note_dataflow(dataflow_id, name);
        let (handle, trace) = host_dataflow(worker, |scope| {
            use timely::dataflow::operators::core::UnorderedInput as _;
            use timely::dataflow::operators::Probe;
            use differential_dataflow::AsCollection;
            let (handle, stream) = scope.new_unordered_input::<timely::container::CapacityContainerBuilder<InputContainer>>();
            let arranged = crate::backend::corgi::arrange_export(stream.as_collection());
            arranged.stream.probe_with(&probe);
            (handle, arranged.trace)
        });
        self.traces.insert(name.to_string(), trace);
        let input = ServerInput::new(handle, shape.clone(), 1);
        self.shapes.insert(name.to_string(), shape);
        self.outputs.insert(name.to_string(), Output { width: 1, owner: name.to_string(), probe: probe.clone(), since: Antichain::from_elem(host_time(self.diagnostics_epoch)) });
        (input, dataflow_id, probe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Inspect the message on the input edge itself. Splitting a container by time
    // would give the same final collection but a different delivery contract.
    #[test]
    fn input_sends_one_message_stamped_with_the_whole_frontier() {
        use timely::dataflow::operators::{Inspect, Probe};
        use timely::dataflow::operators::core::UnorderedInput;
        timely::execute_directly(|worker| {
            let probe = ProbeHandle::new();
            let received = Rc::new(RefCell::new(Vec::new()));
            let shape = (corgi::Shape::Int, corgi::Shape::Unit);
            let mut handle = host_dataflow(worker, |scope| {
                let (handle, stream) = scope.new_unordered_input::<timely::container::CapacityContainerBuilder<InputContainer>>();
                let received = Rc::clone(&received);
                stream.inspect_core(move |event| {
                    if let Ok((stamp, rows)) = event {
                        received.borrow_mut().push((stamp.clone(), rows.times.to_vec()));
                    }
                }).probe_with(&probe);
                ServerInput::new(handle, shape.clone(), 2)
            });
            let x = host_time(2);
            let y = HostTime::new(smallvec::smallvec![0, 2]);
            let frontier = [x.clone(), y.clone()].into_iter().collect();
            handle.advance_to_frontier(&frontier);
            let mut rows = crate::runtime::feed::FeedDecoder::new(&shape.0, &shape.1);
            rows.push("1", None, 1).unwrap();
            rows.push("2", None, -1).unwrap();
            let Updates { keys, vals, diffs } = rows.finish();
            let points = vec![x, y];
            handle.give(InputContainer { keys, vals, diffs, times: points.iter().collect() });
            handle.advance_to_frontier(&Antichain::new());
            while !probe.done() { worker.step(); }
            assert_eq!(*received.borrow(), [(Stamp::from(frontier), points)]);
        });
    }

}
