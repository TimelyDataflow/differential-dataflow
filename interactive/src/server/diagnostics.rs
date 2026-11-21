//! Wiring the `diag.*` sources (see `crate::diag`): turning them on, noting which dataflow runs
//! which program, and feeding them at each sample.

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
            if let (Some(rows), Some(input)) = (diagnostics.take_pending(name), installed.inputs.get_mut(&InputId::Number(0))) {
                input.give_columns(rows, time).expect("diagnostics rows have their source's shape");
            }
        }
        diagnostics.clear_pending();
    }

    /// Sample worker logs at the diagnostic clock and advance only the diagnostic inputs.
    /// A subsequent spin waits for readers to incorporate this sample.
    pub fn sample_diagnostics(&mut self, worker: &Worker) -> Result<OuterTime, String> {
        if self.diagnostics.is_none() { return Err("diagnostics are off; start with DDIR_DIAGNOSTICS=1".into()); }
        let next = self.diagnostics_epoch.checked_add(1).ok_or("diagnostic clock overflow")?;
        self.process_diagnostics(worker);
        self.feed_diagnostics(self.diagnostics_epoch);
        for source in self.programs.values_mut().filter(|p| p.origin == Origin::Diagnostics) {
            source.inputs.get_mut(&InputId::Number(0)).expect("diagnostic source input").advance_to_point(&host_time(next));
        }
        self.diagnostics_epoch = next;
        Ok(next)
    }

    /// Install diagnostics source `name` (see [`crate::diag`]): published like any source, fed by
    /// each worker from its own logging at each sample. A source installed after others are
    /// running starts from the whole of what it describes now: the installed sources first take
    /// the changes pending for them, so nothing is counted twice.
    pub(super) fn install_diagnostics(&mut self, worker: &mut Worker, name: &str) {
        self.process_diagnostics(worker);
        self.feed_diagnostics(self.diagnostics_epoch);
        let shape = crate::diag::shape(name).expect("a diagnostics source");
        let (mut input, dataflow_id, _probe) = self.install_source(worker, name, shape);
        let seed = self.diagnostics.as_ref().and_then(|d| d.borrow().snapshot(name));
        if let Some(rows) = seed {
            input.give_columns(rows, self.diagnostics_epoch).expect("diagnostics rows have their source's shape");
        }
        input.advance_to_point(&host_time(self.diagnostics_epoch));

        let mut inputs = HashMap::new();
        inputs.insert(InputId::Number(0), input);
        self.programs.insert(
            name.to_string(),
            Installed {
                inputs,
                imports: Vec::new(),
                exports: vec![name.to_string()],
                dataflow_id,
                origin: Origin::Diagnostics,
            },
        );
    }
}
