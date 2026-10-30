//! Wiring the `diag.*` sources (see `crate::diag`): turning them on, noting which dataflow runs
//! which program, and feeding them at each `tick`.

use super::*;

impl Server {
    /// Turn diagnostics on with this worker's `diagnostics` (see [`crate::diag::register`]):
    /// programs may then import the `diag.*` sources.
    pub fn set_diagnostics(&mut self, diagnostics: Rc<RefCell<crate::diag::Diagnostics>>) {
        self.diagnostics = Some(diagnostics);
    }

    /// Record that dataflow `index` runs the installed `name`, for diagnostics.
    pub(super) fn note_dataflow(&self, index: usize, name: &str) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.borrow_mut().note_dataflow(index, name);
        }
    }

    /// Bring diagnostics up to date with the worker's logging: flush the loggers and fold their
    /// events into the pending changes.
    pub(super) fn process_diagnostics(&self, worker: &Worker) {
        if let Some(diagnostics) = &self.diagnostics {
            crate::diag::flush(worker);
            let installed: std::collections::HashSet<usize> = worker.installed_dataflows().into_iter().collect();
            diagnostics.borrow_mut().process(&installed);
        }
    }

    /// Give each installed `diag.*` source its pending changes at `time`, and forget the rest.
    pub(super) fn feed_diagnostics(&mut self, time: OuterTime) {
        let Some(diagnostics) = &self.diagnostics else { return };
        let mut diagnostics = diagnostics.borrow_mut();
        for (name, installed) in self.programs.iter_mut() {
            if installed.origin != Origin::Diagnostics { continue; }
            if let (Some(rows), Some(input)) = (diagnostics.take_pending(name), installed.inputs.get_mut(&0)) {
                input.give_columns(rows, time).expect("diagnostics rows have their source's shape");
            }
        }
        diagnostics.clear_pending();
    }

    /// Install diagnostics source `name` (see [`crate::diag`]): published like any source, fed by
    /// each worker from its own logging at every `tick`. A source installed after others are
    /// running starts from the whole of what it describes now: the installed sources first take
    /// the changes pending for them, so nothing is counted twice.
    pub(super) fn install_diagnostics(&mut self, worker: &mut Worker, name: &str) {
        self.process_diagnostics(worker);
        self.feed_diagnostics(self.epoch);
        let shape = crate::diag::shape(name).expect("a diagnostics source");
        let (mut input, dataflow_id, _probe) = self.install_source(worker, name, shape);
        let seed = self.diagnostics.as_ref().and_then(|d| d.borrow().snapshot(name));
        if let Some(rows) = seed {
            input.give_columns(rows, self.epoch).expect("diagnostics rows have their source's shape");
        }
        input.advance_to(self.epoch);

        let mut inputs = HashMap::new();
        inputs.insert(0usize, input);
        self.programs.insert(
            name.to_string(),
            Installed {
                inputs,
                imports: Vec::new(),
                exports: vec![name.to_string()],
                dataflow_id,
                root_width: 1,
                manual: false,
                now: host_time(self.epoch),
                origin: Origin::Diagnostics,
                generators: HashMap::new(),
            },
        );
    }
}
