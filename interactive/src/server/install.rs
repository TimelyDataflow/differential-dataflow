//! Installing a program: shapes checked, imports bound, the dataflow built, exports published.

use super::*;

impl Server {
    /// Install `prog` under `name`: build its dataflow in `worker`, wiring each
    /// root `Source::Trace` to a registered trace and registering each export's
    /// trace for later imports. New inputs are advanced to the current epoch so
    /// they are consistent with the traces they may already see.
    ///
    /// A `Source::Trace` that names a *recipe* (e.g. `random:nodes=8,edges=12`)
    /// is installed on demand if absent — generated sources are content-addressed,
    /// so two importers of the same recipe share one source. Any other
    /// unregistered import errors (install its producer first). Also errors if
    /// the name is taken or it would republish an existing export name.
    pub fn install(
        &mut self,
        worker: &mut Worker,
        name: &str,
        prog: &st::Program,
    ) -> Result<(), String> {
        self.install_with_root(worker, name, prog, 1, self.import_as_of())
    }

    /// Install with `root_width` root coordinates and an explicit import/read floor.
    /// Wider imports are refused; narrower imports share their trace unchanged.
    /// New inputs start at `as_of` (at the compatibility epoch for width 1).
    pub fn install_with_root(
        &mut self, worker: &mut Worker, name: &str, prog: &st::Program,
        root_width: usize, as_of: HostTime,
    ) -> Result<(), String> {
        if root_width == 0 || as_of.len() > root_width {
            return Err("root width must be positive and contain the as-of point".into());
        }
        if self.programs.contains_key(name) {
            return Err(format!("a program named {:?} is already installed", name));
        }
        // The root is not a loop: a published trace holds what its program computes at each
        // epoch, not a round of an iteration. Loops live in named scopes.
        if let Some(var) = prog.root.vars.first() {
            return Err(format!(
                "program {:?} has feedback variable `{}` at its root; `set` belongs in a named scope (`name: {{ set {} = ...; }}`)",
                name, var.name, var.name
            ));
        }
        // An input is fed from outside, so it says what it holds, as a table declares its columns.
        for imp in &prog.root.imports {
            if let (st::Source::Input(n), None) = (&imp.from, &imp.shape) {
                return Err(format!(
                    "program {:?} reads input {n} without declaring its shape; write `input {n} : (key, val)`",
                    name
                ));
            }
        }
        // Resolve trace imports against canonical names. A name that is not registered must be a
        // generated source (`clock`, or a recipe such as `random:...`), installed on demand below.
        // Everything is checked before anything is installed, so a rejected program leaves no trace.
        let mut generated: Vec<String> = Vec::new();
        for imp in &prog.root.imports {
            if let st::Source::Trace(t) = &imp.from {
                let key = canonical_source_name(t);
                if let Some(output) = self.outputs.get(&key) {
                    if output.width > root_width {
                        return Err(format!("cannot import trace {key:?} of width {} into width {root_width}", output.width));
                    }
                    if !output.since.less_equal(&as_of) || !self.traces.get_mut(&key).unwrap().get_logical_compaction().less_equal(&as_of) {
                        return Err(format!("as-of {as_of:?} is before trace {key:?}'s compaction frontier"));
                    }
                } else if !self.import_as_of().less_equal(&as_of) {
                    return Err(format!("as-of {as_of:?} is before generated source {key:?}'s compaction frontier"));
                }
                if !self.is_published(&key) {
                    if crate::diag::is_source(&key) && self.diagnostics.is_none() {
                        return Err(format!(
                            "program {:?} imports {:?}, but diagnostics are off; start the server with DDIR_DIAGNOSTICS=1",
                            name, t
                        ));
                    }
                    if key != "clock" && Recipe::parse(&key).is_none() && !crate::diag::is_source(&key) {
                        return Err(format!(
                            "program {:?} imports unknown trace {:?}; install its producer first",
                            name, t
                        ));
                    }
                    generated.push(key);
                }
            }
        }
        for e in &prog.root.exports {
            if self.is_published(&e.name) || generated.contains(&e.name) {
                return Err(format!("export name {:?} is already published; choose another name or drop its producer", e.name));
            }
        }
        self.check_shapes(name, prog, root_width)?;
        // Validate the submitted program before any pass can remove an invalid binding.
        // Workers have the current provider shapes and as-of. A pass panic must return
        // an install error, before creating sources or changing the server's catalog.
        let mut optimized = prog.clone();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::passes::run_with_root(&mut optimized, &|source| match source {
                st::Source::Trace(t) => self.source_shape(t),
                _ => None,
            }, &self.passes, &as_of, root_width, &mut |_, _, _| {});
        })).map_err(|_| format!("program {:?} panicked during preparation", name))?;
        let prog = &optimized;
        let (scope_shapes, export_shapes) = self.check_shapes(name, prog, root_width)?;
        for key in generated {
            if self.is_published(&key) {
                continue; // imported twice
            }
            if key == "clock" {
                self.install_clock(worker);
            } else if crate::diag::is_source(&key) {
                self.install_diagnostics(worker, &key);
            } else {
                let recipe = Recipe::parse(&key).expect("checked above");
                self.install_generated(worker, &key, recipe);
            }
        }

        let import_names: Vec<String> = prog
            .root
            .imports
            .iter()
            .filter_map(|imp| match &imp.from {
                st::Source::Trace(t) => Some(canonical_source_name(t)),
                _ => None,
            })
            .collect();
        let export_names: Vec<String> = prog.root.exports.iter().map(|e| e.name.clone()).collect();

        let mut output_probes = HashMap::new();
        let root = &prog.root;
        let arranged_imports = self.arranged_imports;
        let tapped = self.export_taps;
        let traces = &mut self.traces;

        // The id this dataflow will get; captured so `drop` can remove it.
        let dataflow_id = worker.next_dataflow_index();
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.borrow_mut().note_dataflow(dataflow_id, name);
        }

        #[allow(clippy::type_complexity)]
        let (published, inputs): (Vec<(String, crate::backend::corgi::ExportTrace, Option<crate::backend::corgi::ExportTap>)>, Vec<(usize, ServerInput)>) =
            host_dataflow(worker, |scope| {
                use timely::dataflow::operators::core::UnorderedInput as _;
                use timely::dataflow::operators::Probe;
                use differential_dataflow::AsCollection;
                use crate::backend::corgi::{export_containers, Arr, CorgiImport};
                let mut inputs: Vec<(usize, ServerInput)> = Vec::new();

                // One value per root import, rendered without rows. An input converts rows to
                // columns as they are fed, and a published trace is read as its chunks, or with
                // arranged imports as the producer's arrangement itself.
                let imports: Vec<CorgiImport> = root
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        st::Source::Input(n) => {
                            let (handle, stream) = scope.new_unordered_input::<timely::container::CapacityContainerBuilder<InputContainer>>();
                            let shape = imp.shape.clone().expect("an input declares its shape (checked above)");
                            let mut input = ServerInput::new(handle, shape);
                            input.width = root_width;
                            inputs.push((*n, input));
                            CorgiImport::Columns(stream.as_collection())
                        }
                        st::Source::Trace(t) => {
                            // The first binding point: resolve a named trace by importing it, as
                            // of the program's start.
                            let key = canonical_source_name(t);
                            let trace = traces.get_mut(&key).expect("validated above");
                            debug_assert!(trace.get_logical_compaction().less_equal(&as_of), "an import as of {as_of:?}, before its trace's compaction frontier");
                            let arranged = trace.import(scope);
                            if arranged_imports {
                                CorgiImport::Export(Arr { arranged, since: as_of.clone() })
                            } else {
                                CorgiImport::Columns(export_containers(arranged, as_of.clone()))
                            }
                        }
                        st::Source::Parent(_) => unreachable!("root import from a parent scope"),
                    })
                    .collect();

                // Each export is published as the arrangement the program built, or arranged once.
                let exports = crate::backend::corgi::render_program(root, scope, imports, &scope_shapes, root_width);
                let published: Vec<_> = root
                    .exports
                    .iter()
                    .zip(exports)
                    .map(|(e, arranged)| {
                        let tap: Option<crate::backend::corgi::ExportTap> = tapped.then(Default::default);
                        let stream = match &tap {
                            Some(t) => crate::backend::corgi::tap_export(&arranged, t.clone()),
                            None => arranged.stream.clone(),
                        };
                        let probe = ProbeHandle::new();
                        stream.probe_with(&probe);
                        output_probes.insert(e.name.clone(), probe);
                        (e.name.clone(), arranged.trace, tap)
                    })
                    .collect();

                (published, inputs)
            });

        for (export_name, shape) in export_shapes {
            self.shapes.insert(export_name, shape);
        }
        for (export_name, trace, tap) in published {
            self.outputs.insert(export_name.clone(), Output { width: root_width, owner: name.to_string(), probe: output_probes.remove(&export_name).unwrap(), since: as_of.clone() });
            if let Some(tap) = tap { self.taps.insert(export_name.clone(), tap); }
            self.traces.insert(export_name, trace);
        }
        for t in &import_names {
            *self.importers.entry(t.clone()).or_insert(0) += 1;
        }
        let now = if root_width == 1 { as_of.join(&host_time(self.epoch)) } else { as_of.clone() };
        let mut by_pos: HashMap<usize, ServerInput> = HashMap::new();
        for (pos, mut handle) in inputs {
            handle.advance_to_point(&now);
            by_pos.insert(pos, handle);
        }
        self.programs.insert(
            name.to_string(),
            Installed {
                inputs: by_pos,
                imports: import_names,
                exports: export_names,
                dataflow_id,
                root_width,
                now,
                origin: Origin::Program,
                generators: HashMap::new(),
            },
        );
        Ok(())
    }

    /// Infer the shapes of `prog`'s collections, before anything is installed. An imported trace
    /// takes the shape it is published at; a declared import must match it. Returns the shapes,
    /// for rendering, and the shape of each export; or the conflicts that leave the program
    /// without meaning, or the collections whose shape nothing determines.
    fn check_shapes(&self, name: &str, prog: &st::Program, root_width: usize) -> Result<(crate::shapes::ScopeShapes, Vec<(String, RowShape)>), String> {
        let published = |t: &str| self.source_shape(t);
        for imp in &prog.root.imports {
            if let (st::Source::Trace(t), Some(declared)) = (&imp.from, &imp.shape) {
                if let Some(shape) = published(t) {
                    if shape != *declared {
                        return Err(format!(
                            "program {:?} declares import {:?} as {} but it is published as {}",
                            name, t, fmt_shape(declared), fmt_shape(&shape)
                        ));
                    }
                }
            }
        }
        let (shapes, problems) = crate::shapes::infer_with_root(prog, &|src| match src {
            st::Source::Trace(t) => published(t),
            _ => None,
        }, root_width);
        if !problems.conflicts.is_empty() {
            return Err(format!("program {:?} has conflicting shapes: {}", name, problems.conflicts.join("; ")));
        }
        if !problems.unknown.is_empty() {
            return Err(format!("program {:?} has collections without a shape: {}", name, problems.unknown.join("; ")));
        }
        let exports = prog.root.exports.iter().zip(&shapes.exports)
            .map(|(e, s)| {
                let c = s.as_ref().expect("every collection has a shape (checked above)");
                (e.name.clone(), (c.key.clone(), c.val.clone()))
            })
            .collect();
        Ok((shapes, exports))
    }
}
