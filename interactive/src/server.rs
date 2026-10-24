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
//! reaches a worker. Every input declares its shape, and data is checked at it
//! before anything is staged: a feed or load of rows of another shape is
//! refused, as is an import or binding of a trace published at another shape.
//! There is no per-program failure isolation: a panic inside a dataflow operator
//! can take down the shared server, disconnecting other clients. [`Command`]
//! is serializable so worker 0 can broadcast one ordered command stream to the
//! whole worker group.
//!
//! # The two binding points
//!
//! The named-trace IR (`import "x"` / `export "y"`) flows through parse → lower
//! → `scope_ir`. The server resolves both ends:
//!
//! - **`Source::Trace(name)`** — `import` the registered trace into the new
//!   dataflow, as the producer's arrangement or as a root collection.
//! - **`Export(name, _)`** — arrange the exported collection and register its
//!   trace under `name`, so a later install can import it.
//!
//! # Lifecycle
//!
//! - **install** builds a dataflow over imported traces + positional inputs,
//!   publishing its exports. The dataflow's id (`next_dataflow_index`) is kept
//!   for teardown.
//! - **feed** stages updates, as text read at the input's declared shapes or
//!   as columns, at a chosen time (default: the current epoch), so inputs can
//!   be scheduled into the future.
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
use differential_dataflow::operators::arrange::ShutdownButton;
use differential_dataflow::trace::TraceReader;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::corgi::container::Updates;
use crate::ir::Diff;
use crate::scope_ir as st;

/// The host epoch: the server's clock, which `tick` advances.
pub type OuterTime = u64;

/// The time programs and published traces run at: an epoch, then one coordinate per enclosing
/// loop. A program's root collections, and so its published traces, are at `(e, [])`.
pub type HostTime = crate::ir::Time;

/// Epoch `e` as a program time, `(e, [])`.
pub fn host_time(e: OuterTime) -> HostTime {
    timely::order::Product::new(e, PointStamp::default())
}

/// Build a dataflow whose operators run at [`HostTime`]: `build` gets a scope at that time.
///
/// Timely roots a dataflow at a time that refines `()`, which `HostTime` does not, so the scope
/// sits one level in, under a root at the epoch. Nothing iterates there: only inputs, which a
/// root must hold, cross into it (see [`ServerInput`]), and every trace is built and read at
/// `HostTime`.
pub fn host_dataflow<R>(
    worker: &mut Worker,
    build: impl FnOnce(timely::dataflow::Scope<'_, OuterTime>, timely::dataflow::Scope<'_, HostTime>) -> R,
) -> R {
    worker.dataflow::<OuterTime, _, _>(|outer| outer.iterative::<PointStamp<OuterTime>, _, _>(|scope| build(outer, scope)))
}

/// A published export as its readers see it: a trace of corgi chunks.
#[derive(Clone)]
pub struct Published(pub crate::backend::corgi::ExportTrace);

impl Published {
    /// Import into `scope` as containers, one per chunk of each batch, with the import's shutdown
    /// button. Each worker reads its own share.
    pub fn import_containers<'s>(
        &mut self,
        scope: timely::dataflow::Scope<'s, HostTime>,
        name: &str,
    ) -> (
        differential_dataflow::Collection<'s, HostTime, HostContainer>,
        ShutdownButton<CapabilitySet<HostTime>>,
    ) {
        let (arranged, shutdown) = self.0.import_core(scope, name);
        (crate::backend::corgi::export_containers(arranged), shutdown)
    }
}

/// The shape of a collection's rows: its key's and its value's.
pub type RowShape = (corgi::Shape, corgi::Shape);

/// A container of corgi columns at the program time, as a published trace's readers see them.
pub type HostContainer = crate::corgi::container::CorgiContainer<HostTime, Diff>;

/// A container of corgi columns at an epoch, as an input takes them.
type InputContainer = crate::corgi::container::CorgiContainer<OuterTime, Diff>;

/// The number of rows an input converts to columns at once, and a generator emits at once.
const COLUMN_BATCH: usize = 1 << 16;

/// An installed program's positional `input N`, or a generated source's: an input of corgi
/// containers at one shape, the input's declared one or its source's.
struct ServerInput {
    handle: timely::dataflow::operators::core::input::Handle<OuterTime, timely::container::CapacityContainerBuilder<InputContainer>>,
    /// The shape of every update the input holds.
    shape: RowShape,
}

impl ServerInput {
    fn new(
        handle: timely::dataflow::operators::core::input::Handle<OuterTime, timely::container::CapacityContainerBuilder<InputContainer>>,
        shape: RowShape,
    ) -> Self {
        ServerInput { handle, shape }
    }

    /// Add `recipe`'s rows at `indices`, each with multiplicity `diff` at `time`, as columns.
    fn generate(&mut self, recipe: Recipe, indices: impl Iterator<Item = u64>, time: OuterTime, diff: Diff) {
        let mut indices = indices.peekable();
        let mut batch = Vec::with_capacity(COLUMN_BATCH);
        while indices.peek().is_some() {
            batch.clear();
            batch.extend(indices.by_ref().take(COLUMN_BATCH));
            self.give(recipe.container(&batch, time, diff));
        }
    }

    /// Add fed `columns` at `time`. Their shapes must be the input's, as its declaration says.
    fn give_columns(&mut self, columns: Updates, time: OuterTime) -> Result<(), String> {
        let shape = (corgi::shape_of_value(&columns.keys), corgi::shape_of_value(&columns.vals));
        if shape != self.shape {
            return Err(format!("a feed of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape)));
        }
        let Updates { keys, vals, diffs } = columns;
        let n = diffs.len();
        let times = crate::corgi::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n);
        self.give(InputContainer { keys, vals, times, diffs });
        Ok(())
    }

    /// Add the updates of `container`, each at `time` whatever time it carries.
    fn give_at(&mut self, container: HostContainer, time: OuterTime) {
        let HostContainer { keys, vals, times, diffs } = container;
        let n = times.len();
        let times = crate::corgi::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n);
        self.give(InputContainer { keys, vals, times, diffs });
    }

    /// Send a container, which must have the input's shape.
    fn give(&mut self, mut container: InputContainer) {
        if !container.times.is_empty() {
            let shape = (corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals));
            assert!(shape == self.shape, "a container of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(&self.shape));
            self.handle.send_batch(&mut container);
        }
    }

    fn advance_to(&mut self, time: OuterTime) {
        self.handle.advance_to(time);
    }

    fn flush(&mut self) {
        self.handle.flush();
    }
}

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

    /// The shape of every row the source contains: `arity` integers for `random`, one for
    /// `iota`, and a unit value for both.
    fn shape(&self) -> RowShape {
        use corgi::Shape;
        let key = match self {
            Recipe::Random { arity: 0, .. } => Shape::Unit,
            Recipe::Random { arity, .. } => Shape::Prod(vec![Shape::Int; *arity]),
            Recipe::Iota { .. } => Shape::Prod(vec![Shape::Int]),
        };
        (key, Shape::Unit)
    }

    /// The generated row at index `e`, as a feed writes it: what [`Recipe::container`] must hold,
    /// formed one row at a time.
    #[cfg(test)]
    fn row(&self, e: u64) -> (String, String) {
        let fields: Vec<String> = match self {
            Recipe::Random { nodes, arity, seed, .. } => (0..*arity).map(|col| gen_field_seeded(*seed, e, *nodes, col).to_string()).collect(),
            Recipe::Iota { .. } => vec![e.to_string()],
        };
        (format!("tuple({})", fields.join(",")), "tuple()".into())
    }

    /// The generated rows at `indices`, as columns of [`Recipe::shape`], each with multiplicity
    /// `diff` at `time`.
    fn container(&self, indices: &[u64], time: OuterTime, diff: Diff) -> InputContainer {
        use corgi::Value as CValue;
        let n = indices.len();
        let keys = match self {
            Recipe::Random { arity: 0, .. } => CValue::Unit(n),
            Recipe::Random { nodes, arity, seed, .. } => CValue::Prod(
                (0..*arity)
                    .map(|col| CValue::i64(indices.iter().map(|&e| gen_field_seeded(*seed, e, *nodes, col)).collect()))
                    .collect(),
            ),
            Recipe::Iota { .. } => CValue::Prod(vec![CValue::i64(indices.iter().map(|&e| e as i64).collect())]),
        };
        InputContainer {
            keys,
            vals: CValue::Unit(n),
            times: crate::corgi::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n),
            diffs: vec![diff; n],
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
    /// A `diag.*` source, fed from the server's own logging at each `tick` (see [`crate::diag`]).
    Diagnostics,
}

/// `clock` rows for these epochs, each with its multiplicity: `((t), ())`.
fn clock_rows(rows: &[(OuterTime, Diff)]) -> Updates {
    Updates {
        keys: corgi::Value::Prod(vec![corgi::Value::int(rows.iter().map(|&(t, _)| t as i64))]),
        vals: corgi::Value::Unit(rows.len()),
        diffs: rows.iter().map(|&(_, diff)| diff).collect(),
    }
}

/// The shape of the `clock` row.
fn clock_shape() -> RowShape {
    (corgi::Shape::Prod(vec![corgi::Shape::Int]), corgi::Shape::Unit)
}

/// Deterministic hash: maps an index to a pseudorandom u64 (splitmix64).
fn hash_u64(index: u64) -> u64 {
    let mut x = index.wrapping_mul(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// Field `col` of the `random:` source's row `edge_index`: a hash-derived `Int` of magnitude below
/// `nodes`. Distinct seeds give distinct graphs.
pub fn gen_field_seeded(seed: u64, edge_index: u64, nodes: u64, col: usize) -> i64 {
    let base = edge_index.wrapping_mul(31).wrapping_add(seed.wrapping_mul(0x9e3779b97f4a7c15));
    (hash_u64(base.wrapping_add(col as u64)) % nodes) as i64
}

/// The shape of a generated source's rows, by its canonical name.
fn generated_shape(name: &str) -> Option<RowShape> {
    if name == "clock" { Some(clock_shape()) } else { Recipe::parse(name).map(|r| r.shape()) }
}

fn fmt_shape((k, v): &RowShape) -> String {
    format!("({k}, {v})")
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
    /// Update positional `input` of `prog` with one row, read into columns, at
    /// `time` (default the current epoch when `None`).
    Feed {
        prog: String,
        input: usize,
        columns: Updates,
        time: Option<OuterTime>,
    },
    /// Stage several rows, read into columns, into one positional input at the
    /// current epoch.
    FeedColumns {
        prog: String,
        input: usize,
        columns: Updates,
    },
    /// Fill positional `input` of `prog` from `source` — a recipe name, a
    /// published trace, or a file path — at the current epoch, each row's
    /// multiplicity scaled by `diff`. Collective: every worker feeds its own
    /// shard of the rows (see [`Server::load`]).
    Load {
        prog: String,
        input: usize,
        source: String,
        diff: Diff,
    },
    /// Close `n` epochs, running to quiescence after each one.
    Tick { n: u64 },
    /// Drop the named program.
    Drop { name: String },
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
    /// Report the registry. The driver assembles it from [`Server::program_info`],
    /// [`Server::trace_info`] and [`Server::binding_info`].
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
    probe: ProbeHandle<HostTime>,
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
    buffer: Captured,
    /// The tap dataflow's id, for teardown on `unbind`.
    dataflow_id: usize,
    /// The tap's probe: `tick` must wait on it so the buffer holds every
    /// change through the just-closed epoch before draining.
    probe: ProbeHandle<HostTime>,
    /// Keeps the tap's import alive; dropped (deactivating the operator)
    /// together with the binding.
    _shutdown: ShutdownButton<CapabilitySet<HostTime>>,
}

/// A binding's captured changes: the containers of its source trace, whose times `tick`
/// replaces.
type Captured = Rc<RefCell<Vec<HostContainer>>>;

/// A live registry of installed programs and the traces they publish.
pub struct Server {
    /// Published export name -> trace of corgi chunks, which every reader reads as containers.
    traces: HashMap<String, crate::backend::corgi::ExportTrace>,
    /// With export taps: each export's new batches on this worker, drained by `take_changes`: a
    /// standing change stream without a snapshot.
    taps: HashMap<String, crate::backend::corgi::ExportTap>,
    /// `set_export_taps`.
    export_taps: bool,
    /// Installed program name -> its handles and lifecycle bookkeeping.
    programs: HashMap<String, Installed>,
    /// Published trace name -> the shape of its rows: inferred from its producer's program, or
    /// fixed by its recipe for a generated source.
    shapes: HashMap<String, RowShape>,
    /// Trace name -> number of installed programs importing it (the drop gate).
    /// Bindings count here too: a bound source cannot be dropped.
    importers: HashMap<String, usize>,
    /// Live export→input bindings, drained by each `tick`.
    bindings: Vec<Binding>,
    /// The current open epoch; inputs sit here until `tick` closes it.
    epoch: OuterTime,
    /// Hand published traces to importing programs as arrangements, so an importer shares the
    /// producer's trace instead of copying it.
    arranged_imports: bool,
    /// This worker's diagnostics, when they are on: the source of the `diag.*` collections.
    diagnostics: Option<Rc<RefCell<crate::diag::Diagnostics>>>,
}

impl Server {
    /// A fresh server with the host clock at epoch 0.
    pub fn new() -> Self {
        Server {
            traces: HashMap::new(),
            taps: HashMap::new(),
            export_taps: false,
            programs: HashMap::new(),
            shapes: HashMap::new(),
            importers: HashMap::new(),
            bindings: Vec::new(),
            epoch: 0,
            arranged_imports: true,
            diagnostics: None,
        }
    }

    /// Turn diagnostics on with this worker's `diagnostics` (see [`crate::diag::register`]):
    /// programs may then import the `diag.*` sources.
    pub fn set_diagnostics(&mut self, diagnostics: Rc<RefCell<crate::diag::Diagnostics>>) {
        self.diagnostics = Some(diagnostics);
    }

    /// Record that dataflow `index` runs the installed `name`, for diagnostics.
    fn note_dataflow(&self, index: usize, name: &str) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.borrow_mut().note_dataflow(index, name);
        }
    }

    /// Bring diagnostics up to date with the worker's logging: flush the loggers and fold their
    /// events into the pending changes.
    fn process_diagnostics(&self, worker: &Worker) {
        if let Some(diagnostics) = &self.diagnostics {
            crate::diag::flush(worker);
            let installed: std::collections::HashSet<usize> = worker.installed_dataflows().into_iter().collect();
            diagnostics.borrow_mut().process(&installed);
        }
    }

    /// Give each installed `diag.*` source its pending changes at `time`, and forget the rest.
    fn feed_diagnostics(&mut self, time: OuterTime) {
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

    /// The current epoch (the open host time).
    pub fn epoch(&self) -> OuterTime {
        self.epoch
    }

    /// Whether `name` (canonical) is published.
    fn is_published(&self, name: &str) -> bool {
        self.traces.contains_key(name)
    }

    /// Import published traces into programs as arrangements (default on). Joins, reduces, and
    /// `arrange`s of an import then read the producer's trace, at the root and inside iterative
    /// scopes, rather than each importer rebuilding it. Off, imports arrive as containers.
    /// Affects programs installed afterwards.
    pub fn set_arranged_imports(&mut self, on: bool) {
        self.arranged_imports = on;
    }

    /// Also keep each export's new batches for [`Server::take_changes`] (default off). Affects
    /// programs installed afterwards.
    pub fn set_export_taps(&mut self, on: bool) {
        self.export_taps = on;
    }

    /// A reader for published trace `name`, for a transient peek or subscription dataflow.
    pub fn published(&self, name: &str) -> Option<Published> {
        self.traces.get(&canonical_source_name(name)).cloned().map(Published)
    }

    /// The shape of a published trace's rows.
    pub fn trace_shape(&self, name: &str) -> Option<RowShape> {
        self.shapes.get(&canonical_source_name(name)).cloned()
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
                        Origin::Diagnostics => "diagnostics",
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
                use crate::backend::corgi::{export_containers, CorgiImport};
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
                            // The first binding point: resolve a named trace by importing it.
                            let key = canonical_source_name(t);
                            let arranged = traces.get_mut(&key).expect("validated above").import(scope);
                            if arranged_imports {
                                CorgiImport::Export(arranged)
                            } else {
                                CorgiImport::Columns(export_containers(arranged))
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

    /// Install a generated source under its canonical `name`: a one-input
    /// dataflow pre-filled with the recipe's rows at time 0 and published as a
    /// trace. Content-addressed, so a later importer of the same recipe shares
    /// it; not writable (see `feed`); dropped like any program once unused.
    fn install_generated(&mut self, worker: &mut Worker, name: &str, recipe: Recipe) {
        let (index, peers) = (worker.index(), worker.peers());
        let (mut input, dataflow_id, probe) = self.install_source(worker, name, recipe.shape());

        // Each worker emits its shard (e % peers == index) at time 0, so the
        // union is the full source exactly once.
        input.generate(recipe, (0..recipe.rows_len()).filter(|e| (*e as usize) % peers == index), 0, 1);
        input.advance_to(self.epoch);
        input.flush();

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
        let w0 = worker.index() == 0;
        let (mut input, dataflow_id, probe) = self.install_source(worker, "clock", clock_shape());

        if w0 {
            input.give_columns(clock_rows(&[(self.epoch, 1)]), self.epoch).expect("clock rows have the clock's shape");
        }
        input.advance_to(self.epoch);
        input.flush();

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

    /// Install diagnostics source `name` (see [`crate::diag`]): published like any source, fed by
    /// each worker from its own logging at every `tick`. A source installed after others are
    /// running starts from the whole of what it describes now: the installed sources first take
    /// the changes pending for them, so nothing is counted twice.
    fn install_diagnostics(&mut self, worker: &mut Worker, name: &str) {
        self.process_diagnostics(worker);
        self.feed_diagnostics(self.epoch);
        let shape = crate::diag::shape(name).expect("a diagnostics source");
        let (mut input, dataflow_id, probe) = self.install_source(worker, name, shape);
        let seed = self.diagnostics.as_ref().and_then(|d| d.borrow().snapshot(name));
        if let Some(rows) = seed {
            input.give_columns(rows, self.epoch).expect("diagnostics rows have their source's shape");
        }
        input.advance_to(self.epoch);
        input.flush();
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
                origin: Origin::Diagnostics,
                generators: HashMap::new(),
            },
        );
    }

    /// Build a source's dataflow, one input arranged and published as trace `name` of `shape`, as
    /// corgi chunks. Returns the input, the dataflow's id, and the probe on its arrangement.
    fn install_source(&mut self, worker: &mut Worker, name: &str, shape: RowShape) -> (ServerInput, usize, ProbeHandle<HostTime>) {
        let probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        self.note_dataflow(dataflow_id, name);
        let (handle, trace) = host_dataflow(worker, |outer, scope| {
            use timely::dataflow::operators::core::Input as _;
            use timely::dataflow::operators::Probe;
            use differential_dataflow::AsCollection;
            let (handle, stream) = outer.new_input::<InputContainer>();
            let arranged = crate::backend::corgi::arrange_export(stream.as_collection().enter(scope));
            arranged.stream.probe_with(&probe);
            (handle, arranged.trace)
        });
        self.traces.insert(name.to_string(), trace);
        let input = ServerInput::new(handle, shape.clone());
        self.shapes.insert(name.to_string(), shape);
        (input, dataflow_id, probe)
    }

    /// Stage rows written as text into positional input `input` of installed program `prog`, at
    /// `time` (default the current epoch), which must not be in the closed past. Each line of
    /// `rows` is one row, `<key> [val=<v>] [diff=<d>]`, its values read at the input's declared
    /// shapes as a feed reads them ([`crate::corgi::feed`]); blank lines and `#` comments are
    /// skipped. Nothing is staged if any row is not of those shapes. The rows take effect once a
    /// `tick` advances the input past `time`.
    pub fn feed(&mut self, prog: &str, input: usize, rows: &str, time: Option<OuterTime>) -> Result<(), String> {
        self.validate_feed(prog, input, time.unwrap_or(self.epoch))?;
        let (key, val) = &self.programs[&canonical_source_name(prog)].inputs[&input].shape;
        let mut decoder = crate::corgi::feed::FeedDecoder::new(key, val);
        for (number, line) in rows.lines().map(str::trim).enumerate() {
            if !line.is_empty() && !line.starts_with('#') {
                decoder.push_line(line).map_err(|error| format!("row {}: {error}", number + 1))?;
            }
        }
        self.feed_columns(prog, input, decoder.finish(), time)
    }

    /// Stage rows already read into columns at `time` (default the current epoch), each with
    /// its own multiplicity. Their shapes must be the input's declared shapes; nothing is staged
    /// otherwise.
    pub fn feed_columns(&mut self, prog: &str, input: usize, columns: Updates, time: Option<OuterTime>) -> Result<(), String> {
        let time = time.unwrap_or(self.epoch);
        self.validate_feed(prog, input, time)?;
        self.programs
            .get_mut(&canonical_source_name(prog))
            .expect("feed target was prevalidated")
            .inputs
            .get_mut(&input)
            .expect("feed input was prevalidated")
            .give_columns(columns, time)
    }

    /// Bulk-load rows into positional `input` of `prog` at the current epoch.
    ///
    /// `source` is a recipe (`random:…`, `iota:N` — the same names an
    /// `import` accepts), a published trace, or the path of a text file with one
    /// row per line of whitespace-separated integers, the fields of the input's
    /// key (its value is unit), read into columns at the input's declared shape.
    /// Collective: every worker must call this, and each feeds only its shard
    /// (`row % peers == index`) through its own handle, so the union is the
    /// source exactly once and the exchange places each row on its key's owner.
    /// Each row enters with its multiplicity times `diff`.
    ///
    /// A trace is read as its consolidated contents before the current epoch,
    /// each worker reading its own share of the arrangement directly, with no
    /// dataflow and no exchange (the input's own exchange routes the rows).
    /// `feed bank 0 from retire diff=-1` is the maintenance idiom: a program
    /// exports the rows it no longer needs, and the server deletes them from its
    /// input without a client round trip.
    ///
    /// A `random:` recipe with `churn=C` keeps churning: every later `tick`
    /// retracts the next `C` rows of the window and adds `C` fresh ones, the
    /// standing-change regime a program is benchmarked under. Returns the
    /// number of rows in the source (across all workers), or for a trace the
    /// number in this worker's share.
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
        diff: Diff,
    ) -> Result<u64, String> {
        let time = self.epoch;
        self.validate_feed(prog, input, time)?;
        if self.loads_from_trace(source) {
            return self.load_trace(prog, input, source, diff);
        }
        let (index, peers) = (worker.index(), worker.peers());
        let mine = |e: u64| (e as usize) % peers == index;
        let recipe = Recipe::parse(source);
        let target = &self.programs[&canonical_source_name(prog)].inputs[&input];
        if let Some(recipe) = recipe {
            if recipe.shape() != target.shape {
                return Err(format!(
                    "program {:?} declares input {} as {} but {:?} generates {}",
                    prog, input, fmt_shape(&target.shape), source, fmt_shape(&recipe.shape())
                ));
            }
        }
        // A recipe's rows are generated into the input below; a file's are read here.
        let (total, rows) = match recipe {
            Some(recipe) => (recipe.rows_len(), None),
            None => {
                let text = std::fs::read_to_string(source)
                    .map_err(|e| format!("load: cannot read {:?}: {}", source, e))?;
                // Every line is read, whichever worker's shard it falls in: a
                // malformed line must fail the load everywhere, or the other
                // workers would apply their shards and the client would see
                // "loaded" over partial data. Only this worker's lines are
                // kept; the others are read into a scratch decoder, emptied
                // as it fills, so that holding them does not grow with the file.
                use crate::corgi::feed::FeedDecoder;
                let (key, val) = &target.shape;
                let (mut rows, mut others) = (FeedDecoder::new(key, val), FeedDecoder::new(key, val));
                let mut total = 0;
                for (e, line) in text.lines().filter(|l| !l.trim().is_empty()).enumerate() {
                    let fields = line.split_whitespace().collect::<Vec<_>>().join(",");
                    let decoder = if mine(e as u64) { &mut rows } else { &mut others };
                    decoder.push(&fields, None, diff).map_err(|error| format!("load: line {} of {:?}: {error}", e + 1, source))?;
                    if others.len() >= COLUMN_BATCH {
                        others = FeedDecoder::new(key, val);
                    }
                    total += 1;
                }
                (total, Some(rows.finish()))
            }
        };
        let installed = self
            .programs
            .get_mut(&canonical_source_name(prog))
            .expect("load target was prevalidated");
        if let Some(recipe @ Recipe::Random { churn: 1.., .. }) = recipe {
            if diff != 1 {
                return Err(format!("load: a churning recipe {:?} must be fed with diff=1", source));
            }
            installed.generators.insert(input, (recipe, 0));
        }
        let handle = installed.inputs.get_mut(&input).expect("load input was prevalidated");
        if let Some(recipe) = recipe {
            handle.generate(recipe, (0..recipe.rows_len()).filter(|e| mine(*e)), time, diff);
        }
        if let Some(rows) = rows {
            handle.give_columns(rows, time)?;
        }
        Ok(total)
    }

    /// Whether [`Server::load`] reads `source` as a published trace (and so counts only this
    /// worker's share of its rows), rather than as a recipe or a file.
    pub fn loads_from_trace(&self, source: &str) -> bool {
        Recipe::parse(source).is_none() && self.is_published(&canonical_source_name(source))
    }

    /// [`Server::load`] from published trace `source`: this worker's share of its contents
    /// before the current epoch, scaled by `diff`, into its own handle for `prog`'s `input`.
    fn load_trace(&mut self, prog: &str, input: usize, source: &str, diff: Diff) -> Result<u64, String> {
        let name = canonical_source_name(source);
        let declared = &self.programs[&canonical_source_name(prog)].inputs[&input].shape;
        let shape = self.shapes[&name].clone();
        if *declared != shape {
            return Err(format!(
                "program {:?} declares input {} as {} but trace {:?} is published as {}",
                prog, input, fmt_shape(declared), name, fmt_shape(&shape)
            ));
        }
        let mut updates = self.snapshot_local(&name)?;
        // Every check above reads state all workers share, so they agree on it. Only scaling
        // reads this worker's rows; an overflow there cannot be refused on every worker alike.
        for d in updates.diffs.iter_mut() {
            *d = d.checked_mul(diff).unwrap_or_else(|| {
                panic!("feed from {name:?}: multiplicity {d} overflows when scaled by {diff}")
            });
        }
        let rows = updates.len() as u64;
        self.programs
            .get_mut(&canonical_source_name(prog))
            .expect("load target was prevalidated")
            .inputs
            .get_mut(&input)
            .expect("load input was prevalidated")
            .give_columns(updates, self.epoch)?;
        Ok(rows)
    }

    /// This worker's share of trace `name`, consolidated as of the current epoch, read from its
    /// batches without a dataflow. Unlike [`Server::snapshot`], nothing moves between workers.
    pub fn snapshot_local(&mut self, name: &str) -> Result<Updates, String> {
        use differential_dataflow::trace::TraceReader;
        let name = canonical_source_name(name);
        let epoch = self.epoch;
        let mut trace = self.traces.get(&name).cloned().ok_or_else(|| format!("no trace {:?}", name))?;
        let batches = trace
            .batches_through(Antichain::from_elem(host_time(epoch)).borrow())
            .ok_or_else(|| format!("trace {:?} has no readable cut at epoch {}", name, epoch))?;
        let parts = crate::backend::corgi::batch_containers(batches)
            .into_iter()
            .map(|HostContainer { keys, vals, times, diffs }| {
                let part = Updates { keys, vals, diffs };
                let before: Vec<usize> = (0..times.len()).filter(|&i| times.get(i).outer < epoch).collect();
                if before.len() == part.len() { part } else { part.select(&before) }
            })
            .collect();
        Ok(Updates::consolidate(parts, &self.shapes[&name]))
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
        let Published(mut trace) = self.published(&source).expect("checked above");
        let shutdown = host_dataflow(worker, |_, scope| {
            use timely::dataflow::operators::{Inspect, Probe};
            let (arranged, shutdown) = trace.import_core(scope.clone(), "BindImport");
            crate::backend::corgi::export_containers(arranged)
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

    /// Return the consolidated closed-past contents of a trace on worker 0, as columns.
    ///
    /// Builds a transient dataflow that imports the trace, **sends every
    /// container whole to worker 0**, and consolidates them as of the current
    /// epoch — so the result is the complete contents even when the trace is
    /// sharded across workers, not each worker's slice, in key-then-value order.
    /// The other workers get no updates. The dataflow is dropped as soon as it
    /// has drained. Filtering and rendering are the caller's.
    pub fn snapshot(
        &mut self,
        worker: &mut Worker,
        name: &str,
    ) -> Result<Updates, String> {
        use timely::dataflow::operators::Probe;
        use timely::dataflow::operators::generic::Operator;

        let name = canonical_source_name(name);
        let epoch = self.epoch;
        let Published(mut trace) = self.published(&name).ok_or_else(|| format!("no trace {:?}", name))?;
        let shape = self.shapes[&name].clone();
        let parts: Rc<RefCell<Vec<Updates>>> = Rc::new(RefCell::new(Vec::new()));
        let parts_in = parts.clone();
        let mut probe = ProbeHandle::new();
        let id = worker.next_dataflow_index();
        host_dataflow(worker, |_, scope| {
            // The dataflow is dropped once drained, which releases the import.
            let (arranged, _shutdown) = trace.import_core(scope.clone(), "SnapshotImport");
            crate::backend::corgi::export_containers(arranged)
                .inner
                .unary::<timely::container::CapacityContainerBuilder<HostContainer>, _, _, _>(crate::corgi::exchange::GatherPact, "Snapshot", move |_, _| {
                    move |input, _output| {
                        input.for_each(|_cap, data| {
                            let HostContainer { keys, vals, times, diffs } = std::mem::take(data);
                            let part = Updates { keys, vals, diffs };
                            let before: Vec<usize> = (0..times.len()).filter(|&i| times.get(i).outer < epoch).collect();
                            parts_in.borrow_mut().push(if before.len() == part.len() { part } else { part.select(&before) });
                        });
                    }
                })
                .probe_with(&mut probe);
        });
        while probe.less_than(&host_time(epoch)) {
            // Remote data or progress wakes idle workers; do not busy-poll.
            worker.step_or_park(None);
        }
        worker.drop_dataflow(id);
        let parts = std::mem::take(&mut *parts.borrow_mut());
        Ok(Updates::consolidate(parts, &shape))
    }

    /// [`Server::snapshot`], with only the updates whose key is `key`, if one is given: text read
    /// at the trace's key shape, as a feed reads a key.
    pub fn peek(&mut self, worker: &mut Worker, name: &str, key: Option<&str>) -> Result<Updates, String> {
        let updates = self.snapshot(worker, name)?;
        let Some(text) = key else { return Ok(updates) };
        let (kshape, _) = self.shapes[&canonical_source_name(name)].clone();
        let mut decoder = crate::corgi::feed::FeedDecoder::new(&kshape, &corgi::Shape::Unit);
        decoder.push(text, None, 1)?;
        let wanted = decoder.finish().keys;
        let rows: Vec<usize> = (0..updates.len()).collect();
        let equal = corgi::arrange::equal_idx(&updates.keys, &wanted, &rows, &vec![0; rows.len()]);
        let keep: Vec<usize> = rows.into_iter().filter(|&row| equal[row] == 0).collect();
        Ok(updates.select(&keep))
    }

    /// The changes to export `name` that this worker's share of its arrangement has produced
    /// since the last call, one container per chunk (with [`Server::set_export_taps`]). Each worker
    /// drains its own.
    pub fn take_changes(&mut self, name: &str) -> Vec<HostContainer> {
        match self.taps.get(name) {
            Some(tap) => crate::backend::corgi::batch_containers(std::mem::take(&mut *tap.borrow_mut())),
            None => Vec::new(),
        }
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
            self.shapes.remove(ex);
            self.taps.remove(ex);
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
            // Dropping one source reclaims any others it leaves unused, so some may be gone.
            if self.programs.contains_key(&source) {
                self.drop_program(worker, &source)?;
            }
        }
        Ok(())
    }

    /// Close the current epoch: advance every input to the next epoch, step the
    /// worker until all exports have caught up, then let every trace compact.
    pub fn tick(&mut self, worker: &mut Worker) {
        let cur = self.epoch;
        let next = cur + 1;
        let w0 = worker.index() == 0;
        let (index, peers) = (worker.index(), worker.peers());
        let mine = |e: &u64| (*e as usize) % peers == index;
        // Diagnostics of everything since the last tick enter at the epoch being closed.
        self.process_diagnostics(worker);
        self.feed_diagnostics(cur);
        for installed in self.programs.values_mut() {
            // The clock's single row advances by one each tick (worker 0 owns
            // it): retract the current epoch, add the next — an O(1) change.
            if w0 && installed.origin == Origin::Clock {
                if let Some(h) = installed.inputs.get_mut(&0) {
                    h.give_columns(clock_rows(&[(cur, -1), (next, 1)]), next).expect("clock rows have the clock's shape");
                }
            }
            // A random source denotes an infinite deterministic row stream.
            // Each tick replaces `churn` members of its fixed-size window.
            for (input, (recipe, cursor)) in installed.generators.iter_mut() {
                let recipe = *recipe;
                if let Recipe::Random { edges, churn, .. } = recipe {
                    if let Some(h) = installed.inputs.get_mut(input) {
                        // Retract rows `cursor ..` of the window, and add the rows `edges` later.
                        let window = *cursor .. *cursor + churn;
                        h.generate(recipe, window.clone().filter(mine), cur, -1);
                        h.generate(recipe, window.map(|e| e + edges).filter(mine), cur, 1);
                        *cursor += churn;
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
        while self.programs.values().any(|p| p.probe.less_than(&host_time(epoch)))
            || self.bindings.iter().any(|b| b.probe.less_than(&host_time(epoch)))
        {
            // Remote data or progress wakes idle workers; do not busy-poll.
            worker.step_or_park(None);
        }

        // Feedback: deliver each binding's buffered source changes into its
        // target input at the (new, open) epoch — they become visible when
        // the NEXT tick closes it. One-epoch delay is what makes the loop
        // well-founded: each tick performs exactly one step of any
        // program-to-program (or program-to-self) recursion.
        let bindings = &self.bindings;
        let programs = &mut self.programs;
        for binding in bindings {
            let handle = programs
                .get_mut(&binding.target)
                .and_then(|p| p.inputs.get_mut(&binding.input));
            // The target vanished; drop_program refuses while bound, so
            // this is unreachable — but never let the buffer grow.
            match handle {
                Some(handle) => {
                    for container in binding.buffer.borrow_mut().drain(..) {
                        handle.give_at(container, epoch);
                    }
                }
                None => binding.buffer.borrow_mut().clear(),
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
        let frontier = Antichain::from_elem(host_time(self.epoch.saturating_sub(1)));
        for trace in self.traces.values_mut() {
            trace.set_logical_compaction(frontier.borrow());
            trace.set_physical_compaction(frontier.borrow());
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
/// positional input (`inputs[i]` holds input `i`'s rows, one feed line each,
/// `<key> [val=<v>] [diff=<d>]`, dealt round-robin across the workers), one
/// `tick`, then a `snapshot` of each export. `config` picks the worker group: `Config::process(n)` hands
/// exchanged containers between threads as typed values, while
/// `CommunicationConfig::ProcessBinary(n)` sends every one through the wire
/// format. The answer must not depend on either choice.
pub fn evaluate(
    config: timely::Config,
    program: &st::Program,
    inputs: &[Vec<String>],
) -> std::collections::BTreeMap<String, Updates> {
    let program = program.clone();
    let inputs = inputs.to_vec();
    let guards = timely::execute(config, move |worker| {
        let mut server = Server::new();
        server.install(worker, "evaluate", &program).expect("evaluate: install");
        let has_input = |i: usize| program.root.imports.iter().any(|imp| matches!(imp.from, st::Source::Input(n) if n == i));
        for (input, rows) in inputs.iter().enumerate().filter(|(i, _)| has_input(*i)) {
            let shard: Vec<&str> = rows.iter().skip(worker.index()).step_by(worker.peers()).map(String::as_str).collect();
            server.feed("evaluate", input, &shard.join("\n"), None).expect("evaluate: feed");
        }
        server.tick(worker);
        // `snapshot` gathers to worker 0; the other workers' results are empty.
        program
            .root
            .exports
            .iter()
            .map(|e| (e.name.clone(), server.snapshot(worker, &e.name).expect("evaluate: snapshot")))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A recipe's columns hold the rows it generates, at its shape.
    #[test]
    fn recipe_columns_are_its_rows() {
        let recipes = ["random:nodes=10,edges=50,arity=0", "random:nodes=10,edges=50", "random:nodes=7,edges=50,arity=3,seed=5", "iota:50"];
        for name in recipes {
            let recipe = Recipe::parse(name).unwrap();
            let indices: Vec<u64> = (0..recipe.rows_len()).filter(|e| e % 3 != 1).collect();
            let container = recipe.container(&indices, 4, -1);
            let (k, v) = recipe.shape();
            assert_eq!((corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals)), (k, v), "{name}");
            let rows: Vec<_> = indices.iter().map(|&e| (recipe.row(e), 4, -1)).collect();
            assert_eq!(container.into_updates(), rows, "{name}");
        }
    }
}
