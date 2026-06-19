//! A live, multi-worker DDIR server.
//!
//! Hosts a long-running timely worker group into which interpreted DDIR
//! programs are *installed* one at a time, and lets them share results by name.
//! This is the interpreter-driven successor to the legacy `dd_server` crate,
//! which hot-loaded compiled `.so`s via `libloading`; here "install" means
//! parse → lower → render an [`crate::scope_ir::Program`] against a live
//! registry — no machine code, no `dlopen`.
//!
//! # Typed commands
//!
//! The server executes a [`Command`] — already parsed, lowered, and validated.
//! Programs are parsed *off the worker threads* (on the intake side) and shipped
//! here as `scope_ir::Program`s; a malformed program is rejected before it ever
//! reaches a worker, so bad input can't panic the computation. `Command` is
//! serializable precisely so it can ride a timely `Sequencer` to every worker.
//!
//! # The two binding points
//!
//! The named-trace IR (`import "x"` / `export "y"`) flows through parse → lower
//! → `scope_ir`; every batch backend simply `panic!`s on a non-`Input` source
//! because it has no registry. The server resolves both ends:
//!
//! - **`Source::Trace(name)`** — `import` the registered [`ServerTrace`] into the
//!   new dataflow and feed it as a root collection.
//! - **`Export(name, _)`** — arrange the exported collection and register its
//!   trace under `name`, so a later install can import it.
//!
//! # Lifecycle
//!
//! - **install** builds a dataflow over imported traces + positional inputs,
//!   publishing its exports. The dataflow's id (`next_dataflow_index`) is kept
//!   for teardown.
//! - **feed** stages an input update at a chosen time (default: the current
//!   epoch) via `update_at`, so inputs can be scheduled into the future.
//! - **tick** advances all inputs to the next epoch, runs to quiescence, then
//!   lets every trace compact (an importer's own handle holds the shared
//!   `TraceBox` back to what it still needs).
//! - **drop** evicts a program — gated on its published traces having no live
//!   importer — and calls `worker.drop_dataflow`, which removes the operators
//!   outright and frees their state immediately. The gate is what makes that
//!   unilateral removal safe: nothing live still reads the dropped traces.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use timely::worker::Worker;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use differential_dataflow::VecCollection;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::TraceAgent;
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

/// A unit of server work, already parsed/lowered/validated on the intake side.
///
/// Serializable so it can be circulated to every worker through a timely
/// `Sequencer`; the workers execute it without any further parsing.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum Command {
    /// Install `program` under `name`.
    Install { name: String, program: st::Program },
    /// Update positional `input` of `prog`: add `(key, val)` with `diff` at
    /// `time` (default the current epoch when `None`).
    Feed { prog: String, input: usize, key: Value, val: Value, time: Option<OuterTime>, diff: Diff },
    /// Close the current epoch and run to quiescence.
    Tick,
    /// Drop the named program.
    Drop { name: String },
    /// Snapshot a registered trace (optionally one key) and print it (worker 0).
    Peek { trace: String, key: Option<Value> },
    /// Print the registry (worker 0).
    List,
    /// Print the command help (worker 0).
    Help,
    /// Stop the server.
    Exit,
}

/// Everything the server holds for one installed program.
struct Installed {
    /// Positional input index -> handle.
    inputs: HashMap<usize, ServerInput>,
    /// Names of traces this program imports (for the importer refcount).
    imports: Vec<String>,
    /// Names of traces this program publishes (registry entries it owns).
    exports: Vec<String>,
    /// The timely dataflow id, used to `drop_dataflow` on teardown.
    dataflow_id: usize,
    /// This program's own probe (every export is probed with it), so `tick`
    /// waits per-program. A shared probe would strand a dropped program's
    /// handle at its last frontier and wedge `tick` forever.
    probe: ProbeHandle<OuterTime>,
}

/// A live registry of installed programs and the traces they publish.
pub struct Server {
    /// Published export name -> shareable trace.
    traces: HashMap<String, ServerTrace>,
    /// Installed program name -> its handles and lifecycle bookkeeping.
    programs: HashMap<String, Installed>,
    /// Trace name -> number of installed programs importing it (the drop gate).
    importers: HashMap<String, usize>,
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

        let probe = ProbeHandle::new();
        let root = &prog.root;
        let traces = &mut self.traces;

        // The id this dataflow will get; captured so `drop` can remove it.
        let dataflow_id = worker.next_dataflow_index();

        let (published, inputs): (Vec<(String, ServerTrace)>, Vec<(usize, ServerInput)>) =
            worker.dataflow::<OuterTime, _, _>(|outer| {
                let mut inputs: Vec<(usize, ServerInput)> = Vec::new();

                // One outer (host-time) collection per root import.
                let outer_cols: Vec<VecCollection<OuterTime, (Value, Value), Diff>> =
                    root.imports.iter().map(|imp| match &imp.from {
                        st::Source::Input(n) => {
                            let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                            inputs.push((*n, handle));
                            col
                        }
                        st::Source::Trace(t) => {
                            // The first binding point: resolve a named trace by importing it.
                            let arranged = traces.get_mut(t).expect("validated above").import(outer.clone());
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

                (published, inputs)
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
            dataflow_id,
            probe,
        });
        Ok(())
    }

    /// Stage an update to positional input `input` of installed program `prog`:
    /// add `(key, val)` with multiplicity `diff` at `time` (default: the current
    /// epoch). The time must be at or after the current epoch — you cannot
    /// insert into the closed past. Takes effect once `tick` advances the input
    /// frontier past `time`.
    pub fn feed(&mut self, prog: &str, input: usize, key: Value, val: Value, time: Option<OuterTime>, diff: Diff) -> Result<(), String> {
        let t = time.unwrap_or(self.epoch);
        if t < self.epoch {
            return Err(format!("cannot feed at time {} < current epoch {}", t, self.epoch));
        }
        let installed = self.programs.get_mut(prog).ok_or_else(|| format!("no program {:?}", prog))?;
        let handle = installed.inputs.get_mut(&input).ok_or_else(|| format!("program {:?} has no input {}", prog, input))?;
        handle.update_at((key, val), t, diff);
        Ok(())
    }

    /// Read a snapshot of a registered trace and print it (worker 0).
    ///
    /// Builds a transient dataflow that imports the trace, optionally filters to
    /// a single `key`, **exchanges every row to worker 0**, and accumulates net
    /// multiplicities as of the current epoch — so the result is the complete,
    /// consolidated contents even when the trace is sharded across workers, not
    /// each worker's slice. The dataflow is dropped as soon as it has drained.
    pub fn peek(&mut self, worker: &mut Worker, name: &str, key: Option<Value>) -> Result<(), String> {
        use timely::dataflow::operators::{Exchange, Inspect, Probe};

        if !self.traces.contains_key(name) {
            return Err(format!("no trace {:?}", name));
        }
        let epoch = self.epoch;
        // Net multiplicity per (key, val); filled on worker 0 after the exchange.
        let acc: Rc<RefCell<HashMap<(Value, Value), Diff>>> = Rc::new(RefCell::new(HashMap::new()));
        let acc_in = acc.clone();
        let key_filter = key.clone();
        let mut peek_probe = ProbeHandle::new();

        let trace = self.traces.get_mut(name).unwrap();
        let peek_id = worker.next_dataflow_index();
        worker.dataflow::<OuterTime, _, _>(|scope| {
            let imported = trace.import(scope.clone());
            let coll = imported.as_collection(|k, v| (k.clone(), v.clone()));
            let coll = match key_filter {
                Some(k) => coll.filter(move |(kk, _)| kk == &k),
                None => coll,
            };
            coll.inner
                .exchange(|_| 0u64) // gather every shard onto worker 0
                .inspect(move |((k, v), t, d)| {
                    // The snapshot as of `epoch`: the closed past (t < epoch).
                    if *t < epoch {
                        *acc_in.borrow_mut().entry((k.clone(), v.clone())).or_insert(0) += *d;
                    }
                })
                .probe_with(&mut peek_probe);
        });
        // Drain the transient dataflow up to the current epoch, then drop it.
        while peek_probe.less_than(&epoch) {
            worker.step();
        }
        worker.drop_dataflow(peek_id);

        if worker.index() == 0 {
            let acc = acc.borrow();
            let mut rows: Vec<(&(Value, Value), &Diff)> = acc.iter().filter(|(_, d)| **d != 0).collect();
            rows.sort_by(|a, b| a.0.cmp(b.0));
            match &key {
                Some(k) => println!("peek {:?} key={:?} ({} rows):", name, k, rows.len()),
                None => println!("peek {:?} ({} rows):", name, rows.len()),
            }
            for ((k, v), d) in rows {
                println!("  ({:?}, {:?})  x{}", k, v, d);
            }
        }
        Ok(())
    }

    /// Drop installed program `name`, releasing its dataflow immediately.
    ///
    /// Refuses (changing nothing) if any trace the program publishes still has a
    /// live importer — drop the consumers first. Otherwise it unregisters the
    /// program's published traces, closes its inputs, and calls
    /// `worker.drop_dataflow`, which removes the operators and frees their state
    /// at once. Safe because the gate guarantees no live dataflow still reads it.
    pub fn drop_program(&mut self, worker: &mut Worker, name: &str) -> Result<(), String> {
        let installed = self.programs.get(name).ok_or_else(|| format!("no program {:?}", name))?;
        for ex in &installed.exports {
            let live = self.importers.get(ex).copied().unwrap_or(0);
            if live > 0 {
                return Err(format!("cannot drop {:?}: its trace {:?} has {} live importer(s); drop them first", name, ex, live));
            }
        }

        let installed = self.programs.remove(name).unwrap();
        for t in &installed.imports {
            if let Some(c) = self.importers.get_mut(t) {
                *c = c.saturating_sub(1);
            }
        }
        for ex in &installed.exports {
            self.traces.remove(ex);
        }
        let id = installed.dataflow_id;
        // Drop the input handles first (closes the inputs while the operators
        // still exist), then remove the dataflow outright.
        drop(installed);
        worker.drop_dataflow(id);
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

        // Wait for every *live* program to catch up. Per-program probes mean a
        // dropped program leaves nothing behind to wait on.
        let epoch = self.epoch;
        while self.programs.values().any(|p| p.probe.less_than(&epoch)) {
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
