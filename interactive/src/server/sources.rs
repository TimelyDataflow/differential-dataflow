//! Program inputs and diagnostics source dataflows.

use super::*;

/// A container of corgi columns at the program time, as an input takes them.
pub(super) type InputContainer = HostContainer;

type InputHandle = timely::dataflow::operators::core::unordered_input::UnorderedHandle<HostTime, timely::container::CapacityContainerBuilder<InputContainer>>;
type InputCapability = timely::dataflow::operators::ActivateCapability<HostTime>;

/// The number of rows an input converts to columns at once.
pub(super) const COLUMN_BATCH: usize = 1 << 16;

/// An installed program's positional `input N`, or a diagnostic source's: an input of corgi
/// containers at one shape, the input's declared one or its source's.
pub(super) struct ServerInput {
    handle: InputHandle,
    capability: InputCapability,
    /// The shape of every update the input holds.
    pub(super) shape: RowShape,
    pub(super) width: usize,
}

impl ServerInput {
    pub(super) fn new((handle, capability): (InputHandle, InputCapability), shape: RowShape, width: usize) -> Self {
        ServerInput { handle, capability, shape, width }
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
    fn give(&mut self, mut container: InputContainer) {
        if !container.times.is_empty() {
            let shape = (corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals));
            assert!(shape == self.shape, "a container of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape));
            self.handle.activate().session(&self.capability).give_container(&mut container);
        }
    }

    /// Hold the frontier at this epoch; dropping the input releases its capability.
    pub(super) fn advance_to(&mut self, time: OuterTime) {
        self.advance_to_point(&host_time(time));
    }

    pub(super) fn time(&self) -> &HostTime { self.capability.time() }

    pub(super) fn advance_to_point(&mut self, time: &HostTime) {
        self.capability.downgrade(time);
    }
}

impl Server {
    /// Build a source's dataflow, one input arranged and published as trace `name` of `shape`, as
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
        self.outputs.insert(name.to_string(), Output { width: 1, owner: name.to_string(), probe: probe.clone(), since: self.import_as_of() });
        (input, dataflow_id, probe)
    }
}
