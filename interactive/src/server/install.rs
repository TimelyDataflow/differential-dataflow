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
        if self.programs.contains_key(name) {
            return Err(format!("a program named {:?} is already installed", name));
        }
        // The root is not a loop: a published trace holds what its program computes at each
        // epoch, not a round of an iteration. Loops live in named scopes.
        if let Some(var) = prog.root.vars.first() {
            return Err(format!(
                "program {:?} declares `var {}` at its root; a `var` belongs in a named scope (`name: {{ var {} = ...; }}`)",
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
        let (mut scope_shapes, export_shapes) = self.check_shapes(name, prog)?;
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

        // Run the optional rewrite only after the original program has passed
        // install's checks and generated imports have their published shapes.
        // A rejected candidate must never turn a valid install into an error.
        let projected = if std::env::var_os("DDIR_PROJECTION").is_some() {
            let mut candidate = prog.clone();
            let masked = crate::projection::pushdown(&mut candidate, &|source| match source {
                st::Source::Trace(trace_name) => self.shapes.get(&canonical_source_name(trace_name)).cloned(),
                _ => None,
            }).unwrap_or(0);
            if masked > 0 {
                match self.check_shapes(name, &candidate) {
                    Ok((shapes, exports)) if exports == export_shapes => {
                        scope_shapes = shapes;
                        Some(candidate)
                    }
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };
        let prog = projected.as_ref().unwrap_or(prog);

        // Updates at the as-of time that only paths through a strict half join read produce
        // nothing there (see `as_of`); drop them where they are lifted. The rewrite adds filters,
        // so the shapes are inferred again, and a candidate that fails that is not used.
        let as_of = self.import_as_of();
        let mut dropped = prog.clone();
        let dropped = if crate::as_of::drop_dead_as_of(&mut dropped, &as_of) > 0 {
            match self.check_shapes(name, &dropped) {
                Ok((shapes, exports)) if exports == export_shapes => {
                    scope_shapes = shapes;
                    Some(dropped)
                }
                _ => None,
            }
        } else {
            None
        };
        let prog = dropped.as_ref().unwrap_or(prog);

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

        let probe = ProbeHandle::new();
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
            host_dataflow(worker, |outer, scope| {
                use timely::dataflow::operators::core::Input as _;
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
                            let (handle, stream) = outer.new_input::<InputContainer>();
                            let shape = imp.shape.clone().expect("an input declares its shape (checked above)");
                            inputs.push((*n, ServerInput::new(handle, shape)));
                            CorgiImport::Columns(stream.as_collection().enter(scope))
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
                let exports = crate::backend::corgi::render_program(root, scope, imports, &scope_shapes);
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
                        stream.probe_with(&probe);
                        (e.name.clone(), arranged.trace, tap)
                    })
                    .collect();

                (published, inputs)
            });

        for (export_name, shape) in export_shapes {
            self.shapes.insert(export_name, shape);
        }
        for (export_name, trace, tap) in published {
            if let Some(tap) = tap { self.taps.insert(export_name.clone(), tap); }
            self.traces.insert(export_name, trace);
        }
        for t in &import_names {
            *self.importers.entry(t.clone()).or_insert(0) += 1;
        }
        let mut by_pos: HashMap<usize, ServerInput> = HashMap::new();
        for (pos, mut handle) in inputs {
            handle.advance_to(self.epoch);
            handle.flush();
            by_pos.insert(pos, handle);
        }
        self.programs.insert(
            name.to_string(),
            Installed {
                inputs: by_pos,
                imports: import_names,
                exports: export_names,
                dataflow_id,
                probe,
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
    fn check_shapes(&self, name: &str, prog: &st::Program) -> Result<(crate::shapes::ScopeShapes, Vec<(String, RowShape)>), String> {
        let published = |t: &str| -> Option<RowShape> {
            let key = canonical_source_name(t);
            self.shapes.get(&key).cloned().or_else(|| generated_shape(&key)).or_else(|| crate::diag::shape(&key))
        };
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
        let (shapes, problems) = crate::shapes::infer(prog, &|src| match src {
            st::Source::Trace(t) => published(t),
            _ => None,
        });
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
