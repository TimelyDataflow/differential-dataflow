//! A live, multi-worker DDIR server.
//!
//! Hosts a long-running timely worker group into which interpreted DDIR
//! programs are *installed* one at a time, and lets them share results by name.
//! This is the interpreter-driven successor to the legacy `dd_server` crate,
//! which hot-loaded compiled `.so`s via `libloading`; here "install" means
//! parse → lower → render an [`crate::scope_ir::Program`] against a live
//! registry — no machine code, no `dlopen`.
//!
//! # The two binding points
//!
//! The named-trace IR (`import "x"` / `export "y"`) already flows through parse
//! → lower → `scope_ir`; every batch backend simply `panic!`s on a non-`Input`
//! source because it has no registry. The server supplies that registry and
//! resolves the two ends:
//!
//! - **`Source::Trace(name)`** — instead of panicking, `import_core` the
//!   registered [`ServerTrace`] into the new dataflow and feed it as a root
//!   collection (keeping the [`ShutdownButton`] so the import can be torn down).
//! - **`Export(name, _)`** — arrange the exported collection and register its
//!   trace under `name`, so a later install can import it.
//!
//! # Lifecycle
//!
//! - **install** builds a dataflow that may import published traces and
//!   publishes its own exports.
//! - **tick** advances all inputs to the next epoch, runs to quiescence, then
//!   lets every trace compact (an importer's own handle holds the shared
//!   `TraceBox` back to what it still needs, so compaction sheds only history
//!   no live reader requires).
//! - **drop** evicts a program — but only one whose published traces have *no
//!   live importers* (a refcount gate). That keeps teardown local and avoids
//!   the "is the frontier emptying because the source finished, or because it's
//!   being torn down?" ambiguity: we never pull a trace out from under a live
//!   consumer. Pressing the import [`ShutdownButton`]s and dropping the input
//!   handles lets timely reclaim the dataflow and release its upstream holds.
//!
//! # Scope (still deferred)
//!
//! Vec substrate only; direct arrangement import (vs. the `as_collection`
//! round-trip); and on-demand trace peeking.

use std::collections::HashMap;

use timely::worker::Worker;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::CapabilitySet;
use timely::progress::Antichain;
use differential_dataflow::VecCollection;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::{TraceAgent, ShutdownButton};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::dynamic::pointstamp::PointStamp;

use crate::ir::{Value, Diff};
use crate::scope_ir as st;
use crate::backend::vec::render_tree;

/// The host (outer) timestamp shared across all installed programs.
pub type OuterTime = u64;

/// A registered, shareable arrangement: the published form of an `export`,
/// arranged by key at the host time so any later install can `import` it.
pub type ServerTrace = TraceAgent<ValSpine<Value, Value, OuterTime, Diff>>;

/// An input handle into an installed program's positional `input N`.
type ServerInput = InputSession<OuterTime, (Value, Value), Diff>;

/// The shutdown handle for one imported trace; pressing it releases the import
/// operator's capability so the dataflow can be reclaimed.
type ImportToken = ShutdownButton<CapabilitySet<OuterTime>>;

/// Everything the server holds for one installed program.
struct Installed {
    /// Positional input index -> handle.
    inputs: HashMap<usize, ServerInput>,
    /// Names of traces this program imports (for the importer refcount).
    imports: Vec<String>,
    /// Names of traces this program publishes (registry entries it owns).
    exports: Vec<String>,
    /// One shutdown button per import, pressed on drop.
    tokens: Vec<ImportToken>,
}

/// A live registry of installed programs and the traces they publish.
pub struct Server {
    /// Published export name -> shareable trace.
    traces: HashMap<String, ServerTrace>,
    /// Installed program name -> its handles and lifecycle tokens.
    programs: HashMap<String, Installed>,
    /// Trace name -> number of installed programs importing it (the drop gate).
    importers: HashMap<String, usize>,
    /// One probe shared by every export, so `tick` can wait for quiescence.
    probe: ProbeHandle<OuterTime>,
    /// The current open epoch; inputs sit here until `tick` closes it.
    epoch: OuterTime,
}

impl Server {
    /// A fresh server with the host clock at epoch 0.
    pub fn new() -> Self {
        Server {
            traces: HashMap::new(),
            programs: HashMap::new(),
            importers: HashMap::new(),
            probe: ProbeHandle::new(),
            epoch: 0,
        }
    }

    /// The current epoch (the open host time).
    pub fn epoch(&self) -> OuterTime { self.epoch }

    /// Whether a trace is registered under `name`.
    pub fn has_trace(&self, name: &str) -> bool { self.traces.contains_key(name) }

    /// Install `prog` under `name`: build its dataflow in `worker`, wiring each
    /// root `Source::Trace` to a registered trace and registering each export's
    /// trace for later imports. New inputs are advanced to the current epoch so
    /// they are consistent with the traces they may already see.
    ///
    /// Errors (without building anything) if the name is taken, if it imports a
    /// trace that is not yet registered, or if it would publish an export name
    /// that already exists — install producers before consumers, and keep
    /// published names unique so the registry's name→producer map is unambiguous.
    pub fn install(&mut self, worker: &mut Worker, name: &str, prog: &st::Program) -> Result<(), String> {
        if self.programs.contains_key(name) {
            return Err(format!("a program named {:?} is already installed", name));
        }
        for imp in &prog.root.imports {
            if let st::Source::Trace(t) = &imp.from {
                if !self.traces.contains_key(t) {
                    return Err(format!("program {:?} imports unknown trace {:?}; install its producer first", name, t));
                }
            }
        }
        for e in &prog.root.exports {
            if self.traces.contains_key(&e.name) {
                return Err(format!("export name {:?} is already published; choose another name or drop its producer", e.name));
            }
        }

        let import_names: Vec<String> = prog.root.imports.iter()
            .filter_map(|imp| match &imp.from { st::Source::Trace(t) => Some(t.clone()), _ => None })
            .collect();
        let export_names: Vec<String> = prog.root.exports.iter().map(|e| e.name.clone()).collect();

        let probe = self.probe.clone();
        let root = &prog.root;
        let traces = &mut self.traces;

        // Build the dataflow; return the exports' traces, the new inputs, and
        // the import shutdown tokens.
        let (published, inputs, tokens): (Vec<(String, ServerTrace)>, Vec<(usize, ServerInput)>, Vec<ImportToken>) =
            worker.dataflow::<OuterTime, _, _>(|outer| {
                let mut inputs: Vec<(usize, ServerInput)> = Vec::new();
                let mut tokens: Vec<ImportToken> = Vec::new();

                // One outer (host-time) collection per root import.
                let outer_cols: Vec<VecCollection<OuterTime, (Value, Value), Diff>> =
                    root.imports.iter().map(|imp| match &imp.from {
                        st::Source::Input(n) => {
                            let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                            inputs.push((*n, handle));
                            col
                        }
                        st::Source::Trace(t) => {
                            // The first binding point: resolve a named trace by
                            // importing it; keep its shutdown button for teardown.
                            let (arranged, button) = traces.get_mut(t).expect("validated above")
                                .import_core(outer.clone(), &imp.name);
                            tokens.push(button);
                            arranged.as_collection(|k, v| (k.clone(), v.clone()))
                        }
                        st::Source::Parent(_) => unreachable!("root import from a parent scope"),
                    }).collect();

                // Render the program body in its own iterative scope, then bring
                // every export back out to the host time (mirrors `vec::evaluate`).
                let leaved: Vec<VecCollection<OuterTime, (Value, Value), Diff>> =
                    outer.iterative::<PointStamp<OuterTime>, _, _>(|inner| {
                        let entered: Vec<_> = outer_cols.iter().map(|c| c.clone().enter(inner)).collect();
                        let exports = render_tree(root, inner.clone(), 0, entered);
                        exports.into_iter().map(|c| c.leave(outer)).collect::<Vec<_>>()
                    });

                // The second binding point: probe and publish each export's trace.
                let published: Vec<(String, ServerTrace)> = root.exports.iter().zip(leaved)
                    .map(|(e, col)| (e.name.clone(), col.probe_with(&probe).arrange_by_key().trace))
                    .collect();

                (published, inputs, tokens)
            });

        for (export_name, trace) in published {
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
        self.programs.insert(name.to_string(), Installed {
            inputs: by_pos,
            imports: import_names,
            exports: export_names,
            tokens,
        });
        Ok(())
    }

    /// Stage an update to positional input `input` of installed program `prog`.
    /// Takes effect at the next [`tick`](Self::tick).
    pub fn feed(&mut self, prog: &str, input: usize, key: Value, val: Value, diff: Diff) -> Result<(), String> {
        let installed = self.programs.get_mut(prog).ok_or_else(|| format!("no program {:?}", prog))?;
        let handle = installed.inputs.get_mut(&input).ok_or_else(|| format!("program {:?} has no input {}", prog, input))?;
        handle.update((key, val), diff);
        Ok(())
    }

    /// Drop installed program `name`, releasing its dataflow.
    ///
    /// Refuses (changing nothing) if any trace the program publishes still has a
    /// live importer — drop the consumers first. Otherwise it presses the
    /// program's import shutdown buttons, closes its inputs, unregisters its
    /// published traces, and steps the worker so timely can reclaim the
    /// now-orphaned dataflow.
    pub fn drop_program(&mut self, worker: &mut Worker, name: &str) -> Result<(), String> {
        let installed = self.programs.get(name).ok_or_else(|| format!("no program {:?}", name))?;
        for ex in &installed.exports {
            let live = self.importers.get(ex).copied().unwrap_or(0);
            if live > 0 {
                return Err(format!("cannot drop {:?}: its trace {:?} has {} live importer(s); drop them first", name, ex, live));
            }
        }

        let mut installed = self.programs.remove(name).unwrap();
        // Release this program's holds on the traces it imports.
        for token in installed.tokens.iter_mut() {
            token.press();
        }
        for t in &installed.imports {
            if let Some(c) = self.importers.get_mut(t) {
                *c = c.saturating_sub(1);
            }
        }
        // Unregister the traces this program published.
        for ex in &installed.exports {
            self.traces.remove(ex);
        }
        // Close the inputs; dropping `installed` releases the pressed tokens.
        installed.inputs.clear();
        drop(installed);
        // Nudge timely to reclaim the orphaned operators.
        for _ in 0..3 { worker.step(); }
        Ok(())
    }

    /// Close the current epoch: advance every input to the next epoch, step the
    /// worker until all exports have caught up, then let every trace compact.
    pub fn tick(&mut self, worker: &mut Worker) {
        let next = self.epoch + 1;
        for installed in self.programs.values_mut() {
            for handle in installed.inputs.values_mut() {
                handle.advance_to(next);
                handle.flush();
            }
        }
        self.epoch = next;

        let probe = self.probe.clone();
        let epoch = self.epoch;
        while probe.less_than(&epoch) {
            worker.step();
        }

        // Allow every published trace to compact up to the new epoch. This is
        // safe even while another program is importing the trace: each importer
        // is a separate `TraceAgent` whose contribution holds the shared
        // `TraceBox` compaction back to what it still needs (the meet across all
        // handles), so the trace only sheds history no live reader requires.
        let frontier = Antichain::from_elem(self.epoch);
        for trace in self.traces.values_mut() {
            trace.set_logical_compaction(frontier.borrow());
            trace.set_physical_compaction(frontier.borrow());
        }
    }

    /// Print the registry: epoch, published traces (with importer counts),
    /// installed programs.
    pub fn list(&self) {
        println!("epoch: {}", self.epoch);
        println!("traces ({}):", self.traces.len());
        let mut names: Vec<&String> = self.traces.keys().collect();
        names.sort();
        for n in names {
            println!("  {} (importers: {})", n, self.importers.get(n).copied().unwrap_or(0));
        }
        println!("programs ({}):", self.programs.len());
        let mut progs: Vec<&String> = self.programs.keys().collect();
        progs.sort();
        for p in progs {
            let installed = &self.programs[p];
            let mut ins: Vec<usize> = installed.inputs.keys().copied().collect();
            ins.sort();
            println!("  {} (inputs: {:?}, imports: {:?}, exports: {:?})", p, ins, installed.imports, installed.exports);
        }
    }
}

impl Default for Server {
    fn default() -> Self { Server::new() }
}
