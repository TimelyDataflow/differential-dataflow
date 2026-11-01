//! Bindings: a published trace's changes fed into a program's input, one epoch later.

use super::*;

/// A live export→input binding: a persistent tap on a published trace whose
/// buffered changes are fed into a program's positional input at each tick.
///
/// This is the discrete-time feedback primitive: the target input receives
/// the source's *changes*, one epoch delayed — and since changes telescope,
/// the input's accumulation MIRRORS the source as of the previous epoch
/// (plus whatever else was fed to it), with no client round-trip.
///
/// The state-machine idiom: give the program a seed input and a dedicated
/// feedback input, `let state = seed + feedback;`, and bind the export
/// `f(state) + (seed | negate)` to the feedback input. Then
/// `state(t) = seed + f(state(t-1)) - seed = f(state(t-1))` — one step of
/// the recursion per tick, entirely inside the server, while later seed
/// changes still inject as perturbations.
pub(super) struct Binding {
    /// Canonical name of the tapped trace.
    pub(super) source: String,
    /// Target program name.
    pub(super) target: String,
    /// Target positional input.
    pub(super) input: usize,
    /// Changes captured since the last drain, times collapsed. Filled by the
    /// tap dataflow's inspect as the worker steps; drained by `tick`.
    pub(super) buffer: Captured,
    /// The tap dataflow's id, for teardown on `unbind`.
    dataflow_id: usize,
    /// The tap's probe: `tick` must wait on it so the buffer holds every
    /// change through the just-closed epoch before draining.
    pub(super) probe: ProbeHandle<HostTime>,
    /// Keeps the tap's import alive; dropped (deactivating the operator)
    /// together with the binding.
    _shutdown: ShutdownButton<CapabilitySet<HostTime>>,
}

impl Server {
    /// Bind trace `trace` to positional `input` of program `prog`: from now
    /// on, every tick delivers the trace's *changes* into that input at the
    /// next epoch, so the input mirrors the trace one epoch delayed. The
    /// write path for programs — an installed dataflow can now act on the
    /// world (or on itself, see [`Binding`]) without any client in the loop.
    ///
    /// Sharding: each worker's tap sees its shard of the trace and feeds its
    /// local input handle, so the union across workers delivers the full
    /// delta exactly once; the input's exchange re-routes as usual.
    ///
    /// The bound source gains an importer (it cannot be dropped while
    /// bound); the target cannot be dropped either (see `drop_program`).
    /// Errors: unknown trace or program, non-writable target (generated or
    /// clock), no such input, or the identical binding already exists.
    pub fn bind(
        &mut self,
        worker: &mut Worker,
        trace: &str,
        prog: &str,
        input: usize,
    ) -> Result<(), String> {
        let source = canonical_source_name(trace);
        let target = prog.to_string();
        if !self.is_published(&source) {
            return Err(format!("no trace {:?}", source));
        }
        let installed = self
            .programs
            .get(&target)
            .ok_or_else(|| format!("no program {:?}", target))?;
        if installed.origin != Origin::Program {
            return Err(format!("{:?} is not a writable program", target));
        }
        if !installed.inputs.contains_key(&input) {
            return Err(format!("program {:?} has no input {}", target, input));
        }
        if installed.manual || self.programs[&self.outputs[&source].owner].manual {
            return Err("tick bindings require legacy mode on both ends".into());
        }
        let declared = &installed.inputs[&input].shape;
        let shape = &self.shapes[&source];
        if declared != shape {
            return Err(format!(
                "program {:?} declares input {} as {} but trace {:?} is published as {}",
                target, input, fmt_shape(declared), source, fmt_shape(shape)
            ));
        }
        if self
            .bindings
            .iter()
            .any(|b| b.source == source && b.target == target && b.input == input)
        {
            return Err(format!(
                "trace {:?} is already bound to {:?} input {}",
                source, target, input
            ));
        }

        let mut probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        // Capture the trace's containers, never forming rows.
        let buffer: Captured = Rc::new(RefCell::new(Vec::new()));
        let buffer_in = buffer.clone();
        let mut published = self.published(&source).expect("checked above");
        let as_of = self.import_as_of();
        let shutdown = host_dataflow(worker, |scope| {
            use timely::dataflow::operators::{Inspect, Probe};
            let (containers, shutdown) = published.import_containers(scope, "BindImport", as_of);
            containers
                .inner
                .inspect_core(move |event| {
                    if let Ok((_, container)) = event {
                        buffer_in.borrow_mut().push(container.clone());
                    }
                })
                .probe_with(&mut probe);
            shutdown
        });

        *self.importers.entry(source.clone()).or_insert(0) += 1;
        self.bindings.push(Binding {
            source,
            target,
            input,
            buffer,
            dataflow_id,
            probe,
            _shutdown: shutdown,
        });
        Ok(())
    }

    /// Remove the binding of `trace` into `prog`'s `input`, dropping its tap
    /// dataflow and releasing the source's importer count.
    pub fn unbind(
        &mut self,
        worker: &mut Worker,
        trace: &str,
        prog: &str,
        input: usize,
    ) -> Result<(), String> {
        let source = canonical_source_name(trace);
        let pos = self
            .bindings
            .iter()
            .position(|b| b.source == source && b.target == prog && b.input == input)
            .ok_or_else(|| {
                format!(
                    "no binding of {:?} to {:?} input {}",
                    source, prog, input
                )
            })?;
        let binding = self.bindings.remove(pos);
        if let Some(count) = self.importers.get_mut(&binding.source) {
            *count = count.saturating_sub(1);
        }
        worker.drop_dataflow(binding.dataflow_id);
        Ok(())
    }

    /// The live bindings, as `(source-trace, target-program, input)`.
    pub fn binding_info(&self) -> Vec<(String, String, usize)> {
        self.bindings
            .iter()
            .map(|b| (b.source.clone(), b.target.clone(), b.input))
            .collect()
    }
}
