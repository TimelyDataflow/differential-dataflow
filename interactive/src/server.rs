//! A live, multi-worker DDIR server.
//!
//! Hosts a long-running timely worker group into which interpreted DDIR
//! programs are *installed* one at a time, and lets them share results by name.
//! An install parses, lowers, and renders a [`crate::scope_ir::Program`]
//! against a live registry of shared traces.
//!
//! # Typed commands
//!
//! The server executes a [`Command`] — already parsed, lowered, and validated.
//! Programs are parsed *off the worker threads* (on the intake side) and shipped
//! here as `scope_ir::Program`s; a malformed program is rejected before it ever
//! reaches a worker, so bad input can't panic the computation. [`Command`] is
//! serializable so worker 0 can broadcast one ordered command stream to the
//! whole worker group.
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
//! - **load** fills an input in bulk from a recipe or a file, each worker
//!   feeding its own shard; a churning recipe then changes the input on every
//!   tick, which is how a program is run under standing change.
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

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::{ShutdownButton, TraceAgent};
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::VecCollection;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::backend::corgi::render_tree_rows;
use crate::backend::vec::render_tree;
use crate::ir::{Diff, Value};
use crate::scope_ir as st;

/// The host (outer) timestamp shared across all installed programs.
pub type OuterTime = u64;

/// The substrate used to render newly installed DDIR programs.
///
/// One backend is selected for the whole server. Imports and exports currently
/// pass through a transitional row-speaking registry; the Corgi backend stays
/// columnar within each program, but a native Corgi registry is the intended
/// production shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenderBackend {
    Vec,
    Corgi,
}

/// One row in the server's compatibility input path. The enclosing command
/// supplies the program, positional input, and current server epoch once for
/// the whole batch. Native bulk ingress need not materialize this row form.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct InputUpdate {
    pub key: Value,
    pub val: Value,
    pub diff: Diff,
}

impl std::str::FromStr for RenderBackend {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "vec" => Ok(Self::Vec),
            "corgi" => Ok(Self::Corgi),
            other => Err(format!("backend must be vec or corgi, got {other:?}")),
        }
    }
}

/// A registered, shareable arrangement: the published form of an `export`,
/// arranged by key at the host time so any later install can `import` it.
pub type ServerTrace = TraceAgent<ValSpine<Value, Value, OuterTime, Diff>>;

/// An input handle into an installed program's positional `input N`.
type ServerInput = InputSession<OuterTime, (Value, Value), Diff>;

/// A generated, content-addressed source. Importing such a name installs the
/// generator on demand, and two imports of the same recipe share one source.
#[derive(Clone, Copy)]
enum Recipe {
    /// `random:nodes=N,edges=E[,arity=A][,seed=S][,churn=C]` — a deterministic
    /// random graph: a window of `E` rows of `A` fields, every field in `0..N`.
    /// Each tick replaces `C` rows (default zero).
    Random {
        nodes: u64,
        edges: u64,
        arity: usize,
        seed: u64,
        churn: u64,
    },
    /// `iota:N` — the rows `(0) .. (N-1)`, each a one-field `Tuple`. The minimal
    /// index source from which richer generators are derived in-language (with
    /// `hash`).
    Iota { n: u64 },
}

impl Recipe {
    /// Parse a recipe name, or `None` if it isn't one (then it's an ordinary
    /// trace lookup). Unknown keys, missing required keys, or non-numbers reject.
    fn parse(name: &str) -> Option<Recipe> {
        if let Some(params) = name.strip_prefix("random:") {
            let (mut nodes, mut edges, mut arity, mut seed, mut churn) =
                (None, None, 2usize, 0u64, 0u64);
            for kv in params.split(',') {
                let (k, v) = kv.split_once('=')?;
                match k.trim() {
                    "nodes" => nodes = Some(v.trim().parse().ok()?),
                    "edges" => edges = Some(v.trim().parse().ok()?),
                    "arity" => arity = v.trim().parse().ok()?,
                    "seed" => seed = v.trim().parse().ok()?,
                    "churn" => churn = v.trim().parse().ok()?,
                    _ => return None,
                }
            }
            Some(Recipe::Random {
                nodes: nodes?,
                edges: edges?,
                arity,
                seed,
                churn,
            })
        } else if let Some(n) = name.strip_prefix("iota:") {
            Some(Recipe::Iota {
                n: n.trim().parse().ok()?,
            })
        } else {
            None
        }
    }

    /// The canonical name: fixed key order, defaults filled — so reorderings and
    /// omitted defaults address the same source.
    fn canonical(&self) -> String {
        match self {
            Recipe::Random {
                nodes,
                edges,
                arity,
                seed,
                churn,
            } => format!(
                "random:nodes={},edges={},arity={},seed={},churn={}",
                nodes, edges, arity, seed, churn
            ),
            Recipe::Iota { n } => format!("iota:{}", n),
        }
    }

    /// The number of rows the source contains.
    fn rows_len(&self) -> u64 {
        match self {
            Recipe::Random { edges, .. } => *edges,
            Recipe::Iota { n } => *n,
        }
    }

    /// The generated row at index `e`.
    fn row(&self, e: u64) -> (Value, Value) {
        match self {
            Recipe::Random {
                nodes, arity, seed, ..
            } => crate::gen_row_seeded(*seed, e, *nodes, *arity),
            Recipe::Iota { .. } => (Value::Tuple(vec![Value::Int(e as i64)]), Value::unit()),
        }
    }
}

/// Where an installed entry came from. Only `Program` is writable by `feed`;
/// `Clock` additionally has its single row advanced each `tick`.
#[derive(Clone, Copy, PartialEq)]
enum Origin {
    Program,
    Generated,
    Clock,
}

/// The single `clock` row for epoch `t`: `(Tuple[t] ; ())`.
fn clock_row(t: OuterTime) -> Value {
    Value::Tuple(vec![Value::Int(t as i64)])
}

/// Map a source name to its canonical form: a recipe canonicalizes, any other
/// name is returned unchanged. Used everywhere a source is looked up, so
/// generated sources are shared by content regardless of how they're spelled.
fn canonical_source_name(name: &str) -> String {
    Recipe::parse(name)
        .map(|r| r.canonical())
        .unwrap_or_else(|| name.to_string())
}

/// A unit of server work, already parsed/lowered/validated on the intake side.
///
/// Serializable so worker 0 can circulate it to every worker; the workers
/// execute it without any further parsing.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Command {
    /// Install `program` under `name`.
    Install { name: String, program: st::Program },
    /// Update positional `input` of `prog`: add `(key, val)` with `diff` at
    /// `time` (default the current epoch when `None`).
    Feed {
        prog: String,
        input: usize,
        key: Value,
        val: Value,
        time: Option<OuterTime>,
        diff: Diff,
    },
    /// Stage several rows into one positional input at the current epoch.
    FeedBatch {
        prog: String,
        input: usize,
        updates: Vec<InputUpdate>,
    },
    /// Fill positional `input` of `prog` from `source` — a recipe name or a
    /// file path — at the current epoch. Collective: every worker feeds its
    /// own shard of the rows (see [`Server::load`]).
    Load {
        prog: String,
        input: usize,
        source: String,
    },
    /// Close `n` epochs, running to quiescence after each one.
    Tick { n: u64 },
    /// Drop the named program.
    Drop { name: String },
    /// Snapshot a registered trace (optionally one key) and print it (worker 0).
    Peek { trace: String, key: Option<Value> },
    /// Bind trace `trace`'s changes into input `input` of `prog` at each tick.
    Bind {
        trace: String,
        prog: String,
        input: usize,
    },
    /// Remove a binding installed by `Bind`.
    Unbind {
        trace: String,
        prog: String,
        input: usize,
    },
    /// Print the registry (worker 0).
    List,
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
    /// Whether this is a user program, a generated source, or the clock — see
    /// [`Origin`]. Generated/clock entries advance and drop like any program but
    /// are not writable by `feed`.
    origin: Origin,
    /// Per input: the recipe whose rows it holds and the next row to retract,
    /// for inputs that churn each `tick` (a generated `random:` source's own
    /// input, or a program input bulk-loaded from such a recipe).
    generators: HashMap<usize, (Recipe, u64)>,
}

/// A stable, transport-friendly description of one installed dataflow.
#[derive(Clone, Debug)]
pub struct ProgramInfo {
    pub name: String,
    pub inputs: Vec<usize>,
    pub imports: Vec<String>,
    pub exports: Vec<String>,
    pub origin: &'static str,
}

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
struct Binding {
    /// Canonical name of the tapped trace.
    source: String,
    /// Target program name.
    target: String,
    /// Target positional input.
    input: usize,
    /// Changes captured since the last drain, times collapsed. Filled by the
    /// tap dataflow's inspect as the worker steps; drained by `tick`.
    buffer: Rc<RefCell<Vec<((Value, Value), Diff)>>>,
    /// The tap dataflow's id, for teardown on `unbind`.
    dataflow_id: usize,
    /// The tap's probe: `tick` must wait on it so the buffer holds every
    /// change through the just-closed epoch before draining.
    probe: ProbeHandle<OuterTime>,
    /// Keeps the tap's import alive; dropped (deactivating the operator)
    /// together with the binding.
    _shutdown: ShutdownButton<CapabilitySet<OuterTime>>,
}

/// A live registry of installed programs and the traces they publish.
pub struct Server {
    /// Published export name -> shareable trace.
    traces: HashMap<String, ServerTrace>,
    /// Installed program name -> its handles and lifecycle bookkeeping.
    programs: HashMap<String, Installed>,
    /// Trace name -> number of installed programs importing it (the drop gate).
    /// Bindings count here too: a bound source cannot be dropped.
    importers: HashMap<String, usize>,
    /// Live export→input bindings, drained by each `tick`.
    bindings: Vec<Binding>,
    /// The current open epoch; inputs sit here until `tick` closes it.
    epoch: OuterTime,
    /// Rendering substrate for subsequently installed programs.
    backend: RenderBackend,
}

impl Server {
    /// A fresh server with the host clock at epoch 0.
    pub fn new() -> Self {
        Self::with_backend(RenderBackend::Vec)
    }

    /// A fresh server using `backend` for installed DDIR programs.
    pub fn with_backend(backend: RenderBackend) -> Self {
        Server {
            traces: HashMap::new(),
            programs: HashMap::new(),
            importers: HashMap::new(),
            bindings: Vec::new(),
            epoch: 0,
            backend,
        }
    }

    /// The current epoch (the open host time).
    pub fn epoch(&self) -> OuterTime {
        self.epoch
    }

    /// Whether a trace is registered under `name`.
    pub fn has_trace(&self, name: &str) -> bool {
        self.traces.contains_key(name)
    }

    /// Clone a trace reader for a transient peek or subscription dataflow.
    pub fn trace(&self, name: &str) -> Option<ServerTrace> {
        self.traces.get(&canonical_source_name(name)).cloned()
    }

    /// Return registry state without coupling a caller to stdout formatting.
    pub fn program_info(&self) -> Vec<ProgramInfo> {
        let mut result: Vec<_> = self
            .programs
            .iter()
            .map(|(name, installed)| {
                let mut inputs: Vec<_> = installed.inputs.keys().copied().collect();
                inputs.sort();
                ProgramInfo {
                    name: name.clone(),
                    inputs,
                    imports: installed.imports.clone(),
                    exports: installed.exports.clone(),
                    origin: match installed.origin {
                        Origin::Program => "program",
                        Origin::Generated => "generated",
                        Origin::Clock => "clock",
                    },
                }
            })
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        result
    }

    pub fn trace_info(&self) -> Vec<(String, usize)> {
        let mut result: Vec<_> = self
            .traces
            .keys()
            .map(|name| (name.clone(), self.importers.get(name).copied().unwrap_or(0)))
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

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
        // Resolve trace imports against canonical names, installing generated
        // sources (e.g. `random:...`) on demand. A name that is neither
        // registered nor a recipe is an error.
        for imp in &prog.root.imports {
            if let st::Source::Trace(t) = &imp.from {
                let key = canonical_source_name(t);
                if !self.traces.contains_key(&key) {
                    if key == "clock" {
                        self.install_clock(worker);
                    } else if let Some(recipe) = Recipe::parse(&key) {
                        self.install_generated(worker, &key, recipe);
                    } else {
                        return Err(format!(
                            "program {:?} imports unknown trace {:?}; install its producer first",
                            name, t
                        ));
                    }
                }
            }
        }
        for e in &prog.root.exports {
            if self.traces.contains_key(&e.name) {
                return Err(format!("export name {:?} is already published; choose another name or drop its producer", e.name));
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

        let probe = ProbeHandle::new();
        let root = &prog.root;
        let traces = &mut self.traces;
        let backend = self.backend;

        // The id this dataflow will get; captured so `drop` can remove it.
        let dataflow_id = worker.next_dataflow_index();

        let (published, inputs): (Vec<(String, ServerTrace)>, Vec<(usize, ServerInput)>) =
            worker.dataflow::<OuterTime, _, _>(|outer| {
                let mut inputs: Vec<(usize, ServerInput)> = Vec::new();

                // One outer (host-time) collection per root import.
                let outer_cols: Vec<VecCollection<OuterTime, (Value, Value), Diff>> = root
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        st::Source::Input(n) => {
                            let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                            inputs.push((*n, handle));
                            col
                        }
                        st::Source::Trace(t) => {
                            // The first binding point: resolve a named trace by importing it.
                            let key = canonical_source_name(t);
                            let arranged = traces
                                .get_mut(&key)
                                .expect("validated above")
                                .import(outer.clone());
                            arranged.as_collection(|k, v| (k.clone(), v.clone()))
                        }
                        st::Source::Parent(_) => unreachable!("root import from a parent scope"),
                    })
                    .collect();

                // Render the program body in its own iterative scope, then bring
                // every export back out to the host time (mirrors `vec::evaluate`).
                let leaved: Vec<VecCollection<OuterTime, (Value, Value), Diff>> = outer
                    .iterative::<PointStamp<OuterTime>, _, _>(|inner| {
                        let entered: Vec<_> =
                            outer_cols.iter().map(|c| c.clone().enter(inner)).collect();
                        let exports = match backend {
                            RenderBackend::Vec => render_tree(root, inner.clone(), 0, entered),
                            RenderBackend::Corgi => {
                                render_tree_rows(root, inner.clone(), 0, entered)
                            }
                        };
                        exports
                            .into_iter()
                            .map(|c| c.leave(outer))
                            .collect::<Vec<_>>()
                    });

                // The second binding point: probe and publish each export's trace.
                let published: Vec<(String, ServerTrace)> = root
                    .exports
                    .iter()
                    .zip(leaved)
                    .map(|(e, col)| {
                        (
                            e.name.clone(),
                            col.probe_with(&probe).arrange_by_key().trace,
                        )
                    })
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

    /// Install a generated source under its canonical `name`: a one-input
    /// dataflow pre-filled with the recipe's rows at time 0 and published as a
    /// trace. Content-addressed, so a later importer of the same recipe shares
    /// it; not writable (see `feed`); dropped like any program once unused.
    fn install_generated(&mut self, worker: &mut Worker, name: &str, recipe: Recipe) {
        let probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        let (index, peers) = (worker.index(), worker.peers());

        let (trace, mut input): (ServerTrace, ServerInput) =
            worker.dataflow::<OuterTime, _, _>(|outer| {
                let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                let trace = col.probe_with(&probe).arrange_by_key().trace;
                (trace, handle)
            });

        // Each worker emits its shard (e % peers == index) at time 0, so the
        // union is the full source exactly once.
        for e in 0..recipe.rows_len() {
            if (e as usize) % peers == index {
                input.update_at(recipe.row(e), 0, 1);
            }
        }
        input.advance_to(self.epoch);
        input.flush();

        self.traces.insert(name.to_string(), trace);
        let mut inputs = HashMap::new();
        inputs.insert(0usize, input);
        self.programs.insert(
            name.to_string(),
            Installed {
                inputs,
                imports: Vec::new(),
                exports: vec![name.to_string()],
                dataflow_id,
                probe,
                origin: Origin::Generated,
                generators: HashMap::from([(0usize, (recipe, 0u64))]),
            },
        );
    }

    /// Install the `clock` source: a single row holding the current epoch, which
    /// advances by one each `tick` (an O(1) change, not an O(n) regeneration).
    /// Produced on worker 0 only; `tick` advances it (see [`Server::tick`]).
    fn install_clock(&mut self, worker: &mut Worker) {
        let probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        let w0 = worker.index() == 0;

        let (trace, mut input): (ServerTrace, ServerInput) =
            worker.dataflow::<OuterTime, _, _>(|outer| {
                let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                let trace = col.probe_with(&probe).arrange_by_key().trace;
                (trace, handle)
            });

        if w0 {
            input.update_at((clock_row(self.epoch), Value::unit()), self.epoch, 1);
        }
        input.advance_to(self.epoch);
        input.flush();

        self.traces.insert("clock".to_string(), trace);
        let mut inputs = HashMap::new();
        inputs.insert(0usize, input);
        self.programs.insert(
            "clock".to_string(),
            Installed {
                inputs,
                imports: Vec::new(),
                exports: vec!["clock".to_string()],
                dataflow_id,
                probe,
                origin: Origin::Clock,
                generators: HashMap::new(),
            },
        );
    }

    /// Stage an update to positional input `input` of installed program `prog`:
    /// add `(key, val)` with multiplicity `diff` at `time` (default: the current
    /// epoch). The time must be at or after the current epoch — you cannot
    /// insert into the closed past. Takes effect once `tick` advances the input
    /// frontier past `time`.
    pub fn feed(
        &mut self,
        prog: &str,
        input: usize,
        key: Value,
        val: Value,
        time: Option<OuterTime>,
        diff: Diff,
    ) -> Result<(), String> {
        let t = time.unwrap_or(self.epoch);
        self.validate_feed(prog, input, t)?;
        self.apply_feed(prog, input, key, val, t, diff);
        Ok(())
    }

    /// Atomically stage several rows into one input at the current open epoch.
    ///
    /// The target is validated before its handle changes. The batch does not
    /// advance time; all rows become visible together when a later `tick`
    /// closes this epoch.
    pub fn feed_batch(
        &mut self,
        prog: &str,
        input: usize,
        updates: Vec<InputUpdate>,
    ) -> Result<(), String> {
        let time = self.epoch;
        self.validate_feed(prog, input, time)?;
        for update in updates {
            self.apply_feed(
                prog,
                input,
                update.key,
                update.val,
                time,
                update.diff,
            );
        }
        Ok(())
    }

    /// Bulk-load rows into positional `input` of `prog` at the current epoch.
    ///
    /// `source` is either a recipe (`random:…`, `iota:N` — the same names an
    /// `import` accepts) or the path of a text file with one row per line of
    /// whitespace-separated integers (`(Tuple[ints] ; ())`). Collective: every
    /// worker must call this, and each feeds only its shard (`row % peers ==
    /// index`) through its own handle, so the union is the source exactly once
    /// and the exchange places each row on its key's owner.
    ///
    /// A `random:` recipe with `churn=C` keeps churning: every later `tick`
    /// retracts the next `C` rows of the window and adds `C` fresh ones, the
    /// standing-change regime a program is benchmarked under. Returns the
    /// number of rows in the source (across all workers).
    ///
    /// A file is validated in full on every worker before any of its rows is
    /// applied: a malformed line fails the load on every worker, so a source
    /// is fed whole or not at all, and the workers agree because they read the
    /// same file (the source must be visible, and identical, to all of them).
    pub fn load(
        &mut self,
        worker: &Worker,
        prog: &str,
        input: usize,
        source: &str,
    ) -> Result<u64, String> {
        let time = self.epoch;
        self.validate_feed(prog, input, time)?;
        let (index, peers) = (worker.index(), worker.peers());
        let mine = |e: u64| (e as usize) % peers == index;
        let recipe = Recipe::parse(source);
        let (total, rows): (u64, Vec<(Value, Value)>) = match recipe {
            Some(recipe) => (
                recipe.rows_len(),
                (0..recipe.rows_len()).filter(|e| mine(*e)).map(|e| recipe.row(e)).collect(),
            ),
            None => {
                let text = std::fs::read_to_string(source)
                    .map_err(|e| format!("load: cannot read {:?}: {}", source, e))?;
                let mut total = 0;
                let mut rows = Vec::new();
                // Every line is parsed, whichever worker's shard it falls in: a
                // malformed line must fail the load everywhere, or the other
                // workers would apply their shards and the client would see
                // "loaded" over partial data.
                for (e, line) in text.lines().filter(|l| !l.trim().is_empty()).enumerate() {
                    total += 1;
                    let fields = line
                        .split_whitespace()
                        .map(|t| t.parse::<i64>().map(Value::Int))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| format!("load: line {} of {:?} is not a row of integers: {:?}", e + 1, source, line))?;
                    if mine(e as u64) {
                        rows.push((Value::Tuple(fields), Value::unit()));
                    }
                }
                (total, rows)
            }
        };
        let installed = self
            .programs
            .get_mut(&canonical_source_name(prog))
            .expect("load target was prevalidated");
        if let Some(recipe @ Recipe::Random { churn: 1.., .. }) = recipe {
            installed.generators.insert(input, (recipe, 0));
        }
        let handle = installed.inputs.get_mut(&input).expect("load input was prevalidated");
        for row in rows {
            handle.update_at(row, time, 1);
        }
        Ok(total)
    }

    /// Check everything about an input target that can fail without changing
    /// its handle. Both singular and batched feeds validate before applying.
    fn validate_feed(&self, prog: &str, input: usize, time: OuterTime) -> Result<(), String> {
        if time < self.epoch {
            return Err(format!(
                "cannot feed at time {} < current epoch {}",
                time, self.epoch
            ));
        }
        let prog = canonical_source_name(prog);
        let installed = self
            .programs
            .get(&prog)
            .ok_or_else(|| format!("no program {:?}", prog))?;
        if installed.origin != Origin::Program {
            let kind = if installed.origin == Origin::Clock {
                "clock"
            } else {
                "generated"
            };
            return Err(format!(
                "{:?} is a {} source and is not writable",
                prog, kind
            ));
        }
        if !installed.inputs.contains_key(&input) {
            return Err(format!("program {:?} has no input {}", prog, input));
        }
        Ok(())
    }

    /// Apply a feed whose target was already validated.
    fn apply_feed(
        &mut self,
        prog: &str,
        input: usize,
        key: Value,
        val: Value,
        time: OuterTime,
        diff: Diff,
    ) {
        let prog = canonical_source_name(prog);
        self.programs
            .get_mut(&prog)
            .expect("feed target was prevalidated")
            .inputs
            .get_mut(&input)
            .expect("feed input was prevalidated")
            .update_at((key, val), time, diff);
    }

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
        if !self.traces.contains_key(&source) {
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

        let buffer: Rc<RefCell<Vec<((Value, Value), Diff)>>> = Rc::new(RefCell::new(Vec::new()));
        let buffer_in = buffer.clone();
        let mut probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        let trace_handle = self.traces.get_mut(&source).expect("checked above");
        let shutdown = worker.dataflow::<OuterTime, _, _>(|scope| {
            let (arranged, shutdown) = trace_handle.import_core(scope.clone(), "BindImport");
            arranged
                .as_collection(|k, v| (k.clone(), v.clone()))
                .inspect(move |((key, val), _time, diff)| {
                    buffer_in
                        .borrow_mut()
                        .push(((key.clone(), val.clone()), *diff));
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

    /// Read a snapshot of a registered trace and print it (worker 0).
    ///
    /// Builds a transient dataflow that imports the trace, optionally filters to
    /// a single `key`, **exchanges every row to worker 0**, and accumulates net
    /// multiplicities as of the current epoch — so the result is the complete,
    /// consolidated contents even when the trace is sharded across workers, not
    /// each worker's slice. The dataflow is dropped as soon as it has drained.
    pub fn peek(
        &mut self,
        worker: &mut Worker,
        name: &str,
        key: Option<Value>,
    ) -> Result<(), String> {
        use timely::dataflow::operators::{Exchange, Inspect, Probe};

        let canon = canonical_source_name(name);
        let name = canon.as_str();
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
                        *acc_in
                            .borrow_mut()
                            .entry((k.clone(), v.clone()))
                            .or_insert(0) += *d;
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
            let mut rows: Vec<(&(Value, Value), &Diff)> =
                acc.iter().filter(|(_, d)| **d != 0).collect();
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

    /// Return the consolidated closed-past contents of a trace on worker 0.
    /// This is the structured counterpart to [`Server::peek`] for protocols.
    pub fn snapshot(
        &mut self,
        worker: &mut Worker,
        name: &str,
    ) -> Result<Vec<(Value, Value, Diff)>, String> {
        use timely::dataflow::operators::{Exchange, Inspect, Probe};

        let name = canonical_source_name(name);
        let epoch = self.epoch;
        let mut trace = self
            .trace(&name)
            .ok_or_else(|| format!("no trace {:?}", name))?;
        let acc: Rc<RefCell<HashMap<(Value, Value), Diff>>> = Rc::new(RefCell::new(HashMap::new()));
        let acc_in = acc.clone();
        let mut probe = ProbeHandle::new();
        let id = worker.next_dataflow_index();
        worker.dataflow::<OuterTime, _, _>(|scope| {
            trace
                .import(scope.clone())
                .as_collection(|k, v| (k.clone(), v.clone()))
                .inner
                .exchange(|_| 0u64)
                .inspect(move |((k, v), t, d)| {
                    if *t < epoch {
                        *acc_in
                            .borrow_mut()
                            .entry((k.clone(), v.clone()))
                            .or_insert(0) += *d;
                    }
                })
                .probe_with(&mut probe);
        });
        while probe.less_than(&epoch) {
            worker.step();
        }
        worker.drop_dataflow(id);
        let mut rows: Vec<_> = acc
            .borrow()
            .iter()
            .filter(|(_, d)| **d != 0)
            .map(|((k, v), d)| (k.clone(), v.clone(), *d))
            .collect();
        rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        Ok(rows)
    }

    /// Drop installed program `name`, releasing its dataflow immediately.
    ///
    /// Refuses (changing nothing) if any trace the program publishes still has a
    /// live importer — drop the consumers first. Otherwise it unregisters the
    /// program's published traces, closes its inputs, and calls
    /// `worker.drop_dataflow`, which removes the operators and frees their state
    /// at once. Safe because the gate guarantees no live dataflow still reads it.
    pub fn drop_program(&mut self, worker: &mut Worker, name: &str) -> Result<(), String> {
        if let Some(binding) = self.bindings.iter().find(|b| b.target == name) {
            return Err(format!(
                "cannot drop {:?}: its input {} is bound from trace {:?}; unbind first",
                name, binding.input, binding.source
            ));
        }
        let canon = canonical_source_name(name);
        let name = canon.as_str();
        let installed = self
            .programs
            .get(name)
            .ok_or_else(|| format!("no program {:?}", name))?;
        for ex in &installed.exports {
            let live = self.importers.get(ex).copied().unwrap_or(0);
            if live > 0 {
                return Err(format!(
                    "cannot drop {:?}: its trace {:?} has {} live importer(s); drop them first",
                    name, ex, live
                ));
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
        // Generated sources are installed on demand and have no independent
        // owner. Reclaim any whose last importing program was just removed.
        let garbage: Vec<_> = self
            .programs
            .iter()
            .filter(|(source, program)| {
                program.origin != Origin::Program
                    && self.importers.get(*source).copied().unwrap_or(0) == 0
            })
            .map(|(source, _)| source.clone())
            .collect();
        for source in garbage {
            self.drop_program(worker, &source)?;
        }
        Ok(())
    }

    /// Close the current epoch: advance every input to the next epoch, step the
    /// worker until all exports have caught up, then let every trace compact.
    pub fn tick(&mut self, worker: &mut Worker) {
        let cur = self.epoch;
        let next = cur + 1;
        let w0 = worker.index() == 0;
        for installed in self.programs.values_mut() {
            // The clock's single row advances by one each tick (worker 0 owns
            // it): retract the current epoch, add the next — an O(1) change.
            if w0 && installed.origin == Origin::Clock {
                if let Some(h) = installed.inputs.get_mut(&0) {
                    h.update_at((clock_row(cur), Value::unit()), next, -1);
                    h.update_at((clock_row(next), Value::unit()), next, 1);
                }
            }
            // A random source denotes an infinite deterministic row stream.
            // Each tick replaces `churn` members of its fixed-size window.
            for (input, (recipe, cursor)) in installed.generators.iter_mut() {
                let recipe = *recipe;
                if let Recipe::Random { edges, churn, .. } = recipe {
                    if let Some(h) = installed.inputs.get_mut(input) {
                        for _ in 0..churn {
                            let old = *cursor;
                            let new = edges + *cursor;
                            if (old as usize) % worker.peers() == worker.index() {
                                h.update(recipe.row(old), -1);
                            }
                            if (new as usize) % worker.peers() == worker.index() {
                                h.update(recipe.row(new), 1);
                            }
                            *cursor += 1;
                        }
                    }
                }
            }
            for handle in installed.inputs.values_mut() {
                handle.advance_to(next);
                handle.flush();
            }
        }
        self.epoch = next;

        // Wait for every *live* program to catch up. Per-program probes mean a
        // dropped program leaves nothing behind to wait on. Binding taps are
        // waited on too, so each buffer holds every change through the epoch
        // just closed before it is drained below.
        let epoch = self.epoch;
        while self.programs.values().any(|p| p.probe.less_than(&epoch))
            || self.bindings.iter().any(|b| b.probe.less_than(&epoch))
        {
            worker.step();
        }

        // Feedback: deliver each binding's buffered source changes into its
        // target input at the (new, open) epoch — they become visible when
        // the NEXT tick closes it. One-epoch delay is what makes the loop
        // well-founded: each tick performs exactly one step of any
        // program-to-program (or program-to-self) recursion.
        let bindings = &self.bindings;
        let programs = &mut self.programs;
        for binding in bindings {
            let mut buffer = binding.buffer.borrow_mut();
            if buffer.is_empty() {
                continue;
            }
            let handle = programs
                .get_mut(&binding.target)
                .and_then(|p| p.inputs.get_mut(&binding.input));
            if let Some(handle) = handle {
                for ((key, val), diff) in buffer.drain(..) {
                    handle.update_at((key, val), epoch, diff);
                }
            } else {
                // The target vanished; drop_program refuses while bound, so
                // this is unreachable — but never let the buffer grow.
                buffer.clear();
            }
        }

        // Allow every published trace to compact up to the previous epoch. This
        // is safe even while another program is importing the trace: each
        // importer is a separate `TraceAgent` whose contribution holds the shared
        // `TraceBox` compaction back to what it still needs (the meet across all
        // handles), so the trace only sheds history no live reader requires.
        //
        // We hold one epoch back (`epoch - 1`, not `epoch`): `peek` reconstructs
        // its snapshot from the closed past (`t < epoch`), so the trace must
        // still distinguish times up to `epoch - 1`. Compacting to `[epoch]`
        // would let a batch merge advance that closed-past history forward onto
        // `epoch`, sliding it out of peek's window and leaving peek with only the
        // latest epoch's delta. (`saturating_sub` guards `epoch == 0`, though
        // tick only runs at epoch >= 1.)
        let frontier = Antichain::from_elem(self.epoch.saturating_sub(1));
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
            println!(
                "  {} (importers: {})",
                n,
                self.importers.get(n).copied().unwrap_or(0)
            );
        }
        println!("programs ({}):", self.programs.len());
        let mut progs: Vec<&String> = self.programs.keys().collect();
        progs.sort();
        for p in progs {
            let installed = &self.programs[p];
            let mut ins: Vec<usize> = installed.inputs.keys().copied().collect();
            ins.sort();
            let tag = match installed.origin {
                Origin::Program => "",
                Origin::Generated => " [generated]",
                Origin::Clock => " [clock]",
            };
            println!(
                "  {}{} (inputs: {:?}, imports: {:?}, exports: {:?})",
                p, tag, ins, installed.imports, installed.exports
            );
        }
        if !self.bindings.is_empty() {
            println!("bindings ({}):", self.bindings.len());
            for b in &self.bindings {
                println!("  {} -> {} input {}", b.source, b.target, b.input);
            }
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Server::new()
    }
}

/// Evaluate `program` on explicit inputs through a throwaway server: every
/// export's consolidated final contents (rows with non-zero net multiplicity),
/// by name.
///
/// This is the data-in/data-out entry point the test suites build on, and it
/// takes the same path a live install does — `install`, one `feed` of each
/// positional input (`inputs[i]` holds input `i`'s rows, each at multiplicity
/// +1, dealt round-robin across the workers), one `tick`, then a `snapshot` of
/// each export. `config` picks the worker group: `Config::process(n)` hands
/// exchanged containers between threads as typed values, while
/// `CommunicationConfig::ProcessBinary(n)` sends every one through the wire
/// format. The answer must not depend on either choice.
pub fn evaluate(
    backend: RenderBackend,
    config: timely::Config,
    program: &st::Program,
    inputs: &[Vec<(Value, Value)>],
) -> std::collections::BTreeMap<String, Vec<((Value, Value), Diff)>> {
    let program = program.clone();
    let inputs = inputs.to_vec();
    let guards = timely::execute(config, move |worker| {
        let mut server = Server::with_backend(backend);
        server.install(worker, "evaluate", &program).expect("evaluate: install");
        let has_input = |i: usize| program.root.imports.iter().any(|imp| matches!(imp.from, st::Source::Input(n) if n == i));
        for (input, rows) in inputs.iter().enumerate().filter(|(i, _)| has_input(*i)) {
            let shard = rows
                .iter()
                .skip(worker.index())
                .step_by(worker.peers())
                .map(|(key, val)| InputUpdate { key: key.clone(), val: val.clone(), diff: 1 })
                .collect();
            server.feed_batch("evaluate", input, shard).expect("evaluate: feed");
        }
        server.tick(worker);
        // `snapshot` gathers to worker 0; the other workers' results are empty.
        program
            .root
            .exports
            .iter()
            .map(|e| {
                let rows = server.snapshot(worker, &e.name).expect("evaluate: snapshot");
                (e.name.clone(), rows.into_iter().map(|(k, v, d)| ((k, v), d)).collect())
            })
            .collect()
    })
    .expect("evaluate: worker startup");
    guards
        .join()
        .into_iter()
        .next()
        .expect("evaluate: worker 0")
        .expect("evaluate: worker 0 returned")
}
