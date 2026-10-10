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
//! reaches a worker. This does not make execution safe for arbitrary data:
//! violating an input/import shape ascription panics inside a dataflow operator
//! on either backend and can take down the shared server, disconnecting other
//! clients. Feed acknowledgements do not check these contracts, and there is
//! no per-program failure isolation. [`Command`] is serializable so worker 0
//! can broadcast one ordered command stream to the whole worker group.
//!
//! # The two binding points
//!
//! The named-trace IR (`import "x"` / `export "y"`) flows through parse → lower
//! → `scope_ir`. The server resolves both ends:
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
/// One backend is selected for the whole server. With the Corgi backend and
/// columnar exports ([`Server::set_columnar_exports`]), inputs, generated
/// sources, published traces, imports, and binds hold corgi columns, and rows
/// appear only where a client reads them. Otherwise imports and exports pass
/// through a row registry, and the Corgi backend is columnar only within each
/// program.
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

/// A published export as its readers see it: a row trace, or with columnar exports
/// ([`Server::set_columnar_exports`]) a trace of corgi chunks. Either reads as rows.
#[derive(Clone)]
pub enum Published {
    Rows(ServerTrace),
    Columnar(crate::backend::corgi::ExportTrace),
}

impl Published {
    /// Import into `scope` as a stream of `((key, val), time, diff)` rows, with the
    /// import's shutdown button. A columnar trace converts to rows as it is read.
    pub fn import_rows<'s>(
        &mut self,
        scope: timely::dataflow::Scope<'s, OuterTime>,
        name: &str,
    ) -> (
        timely::dataflow::Stream<'s, OuterTime, Vec<((Value, Value), OuterTime, Diff)>>,
        ShutdownButton<CapabilitySet<OuterTime>>,
    ) {
        match self {
            Published::Rows(trace) => {
                let (arranged, shutdown) = trace.import_core(scope, name);
                (arranged.as_collection(|k, v| (k.clone(), v.clone())).inner, shutdown)
            }
            Published::Columnar(trace) => {
                let (arranged, shutdown) = trace.import_core(scope, name);
                (crate::backend::corgi::export_rows(arranged), shutdown)
            }
        }
    }
}

/// The shape of a collection's rows: its key's and its value's.
pub type RowShape = (corgi::Shape, corgi::Shape);

/// A container of corgi columns at the host time.
type HostContainer = crate::corgi::container::CorgiContainer<OuterTime, Diff>;

/// The number of rows an input converts to columns at once, and a generator emits at once.
const COLUMN_BATCH: usize = 1 << 16;

/// An input handle into an installed program's positional `input N`, or a generated source's:
/// rows, or with columnar exports ([`Server::set_columnar_exports`]) corgi containers.
enum ServerInput {
    Rows(InputSession<OuterTime, (Value, Value), Diff>),
    Columns(ColumnInput),
}

impl ServerInput {
    /// Add `row` with multiplicity `diff` at `time`, which must not precede the input's time.
    fn update_at(&mut self, row: (Value, Value), time: OuterTime, diff: Diff) {
        match self {
            ServerInput::Rows(session) => session.update_at(row, time, diff),
            ServerInput::Columns(input) => input.update_at(row, time, diff),
        }
    }

    /// Add `recipe`'s rows at `indices`, each with multiplicity `diff` at `time`. A column input
    /// receives them as columns, never as rows.
    fn generate(&mut self, recipe: Recipe, indices: impl Iterator<Item = u64>, time: OuterTime, diff: Diff) {
        match self {
            ServerInput::Rows(session) => {
                for e in indices {
                    session.update_at(recipe.row(e), time, diff);
                }
            }
            ServerInput::Columns(input) => {
                let mut indices = indices.peekable();
                let mut batch = Vec::with_capacity(COLUMN_BATCH);
                while indices.peek().is_some() {
                    batch.clear();
                    batch.extend(indices.by_ref().take(COLUMN_BATCH));
                    input.give(recipe.container(&batch, time, diff));
                }
            }
        }
    }

    /// Add the updates of `container`, each at `time` whatever time it carries.
    fn give_at(&mut self, mut container: HostContainer, time: OuterTime) {
        match self {
            ServerInput::Rows(session) => {
                for (row, _, diff) in container.into_updates() {
                    session.update_at(row, time, diff);
                }
            }
            ServerInput::Columns(input) => {
                let n = container.times.len();
                container.times = crate::corgi::col_times::ColTimes::from_raw_lanes(vec![vec![time; n]], n);
                input.give(container);
            }
        }
    }

    fn advance_to(&mut self, time: OuterTime) {
        match self {
            ServerInput::Rows(session) => session.advance_to(time),
            ServerInput::Columns(input) => input.advance_to(time),
        }
    }

    fn flush(&mut self) {
        match self {
            ServerInput::Rows(session) => session.flush(),
            ServerInput::Columns(input) => input.flush(),
        }
    }
}

/// An input of corgi containers. Rows given one at a time are buffered and converted to columns
/// a batch at a time, at the input's shape: declared, fixed by a recipe, or else the first row's.
struct ColumnInput {
    handle: timely::dataflow::operators::core::input::Handle<OuterTime, timely::container::CapacityContainerBuilder<HostContainer>>,
    /// The shape rows are converted at, once known.
    shape: Option<RowShape>,
    /// Whether `shape` was declared, so that each row must have it.
    declared: bool,
    /// Rows not yet converted to columns.
    rows: Vec<((Value, Value), OuterTime, Diff)>,
}

impl ColumnInput {
    fn new(
        handle: timely::dataflow::operators::core::input::Handle<OuterTime, timely::container::CapacityContainerBuilder<HostContainer>>,
        shape: Option<RowShape>,
        declared: bool,
    ) -> Self {
        ColumnInput { handle, shape, declared, rows: Vec::new() }
    }

    fn update_at(&mut self, row: (Value, Value), time: OuterTime, diff: Diff) {
        assert!(*self.handle.time() <= time, "update at {time} precedes the input's time {}", self.handle.time());
        if self.declared {
            let (k, v) = self.shape.as_ref().expect("a declared input has a shape");
            assert!(row.0.has_shape(k) && row.1.has_shape(v), "input does not match its shape ascription");
        }
        self.rows.push((row, time, diff));
        if self.rows.len() >= COLUMN_BATCH {
            self.send_rows();
        }
    }

    /// Convert the buffered rows to columns and send them.
    fn send_rows(&mut self) {
        let Some(((k, v), _, _)) = self.rows.first() else { return };
        let (ks, vs) = self.shape.get_or_insert_with(|| {
            let pin = |r: &Value, what: &str| crate::corgi::logic::shape_of_row(r).unwrap_or_else(|e| panic!("input {what}: {e}"));
            (pin(k, "key"), pin(v, "value"))
        });
        let mut container = HostContainer::from_updates(std::mem::take(&mut self.rows), ks, vs);
        self.handle.send_batch(&mut container);
    }

    /// Send a container, after any buffered rows. Its shape must be the input's, if that is
    /// known, and otherwise becomes it.
    fn give(&mut self, mut container: HostContainer) {
        self.send_rows();
        if !container.times.is_empty() {
            let shape = (corgi::shape_of_value(&container.keys), corgi::shape_of_value(&container.vals));
            let pinned = self.shape.get_or_insert(shape.clone());
            assert!(shape == *pinned, "a container of shape {} reached an input of shape {}", fmt_shape(&shape), fmt_shape(pinned));
            self.handle.send_batch(&mut container);
        }
    }

    fn advance_to(&mut self, time: OuterTime) {
        self.send_rows();
        self.handle.advance_to(time);
    }

    fn flush(&mut self) {
        self.send_rows();
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

    /// The generated row at index `e`.
    fn row(&self, e: u64) -> (Value, Value) {
        match self {
            Recipe::Random {
                nodes, arity, seed, ..
            } => crate::gen_row_seeded(*seed, e, *nodes, *arity),
            Recipe::Iota { .. } => (Value::Tuple(vec![Value::Int(e as i64)]), Value::unit()),
        }
    }

    /// The generated rows at `indices`, as columns of [`Recipe::shape`], each with multiplicity
    /// `diff` at `time`. The same rows as [`Recipe::row`], without forming them.
    fn container(&self, indices: &[u64], time: OuterTime, diff: Diff) -> HostContainer {
        use corgi::Value as CValue;
        let n = indices.len();
        let keys = match self {
            Recipe::Random { arity: 0, .. } => CValue::Unit(n),
            Recipe::Random { nodes, arity, seed, .. } => CValue::Prod(
                (0..*arity)
                    .map(|col| CValue::i64(indices.iter().map(|&e| crate::gen_field_seeded(*seed, e, *nodes, col)).collect()))
                    .collect(),
            ),
            Recipe::Iota { .. } => CValue::Prod(vec![CValue::i64(indices.iter().map(|&e| e as i64).collect())]),
        };
        HostContainer {
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
}

/// The single `clock` row for epoch `t`: `(Tuple[t] ; ())`.
fn clock_row(t: OuterTime) -> Value {
    Value::Tuple(vec![Value::Int(t as i64)])
}

/// The shape of the `clock` row.
fn clock_shape() -> RowShape {
    (corgi::Shape::Prod(vec![corgi::Shape::Int]), corgi::Shape::Unit)
}

/// The shape of a generated source's rows, by its canonical name.
fn generated_shape(name: &str) -> Option<RowShape> {
    if name == "clock" { Some(clock_shape()) } else { Recipe::parse(name).map(|r| r.shape()) }
}

fn fmt_shape((k, v): &RowShape) -> String {
    format!("({k} ; {v})")
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
    /// Snapshot a registered trace, optionally keeping one key. The driver
    /// reads the rows with [`Server::snapshot`] and renders them.
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
    /// Positional input index -> its declared shape, for inputs that declare one.
    input_shapes: HashMap<usize, RowShape>,
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
    buffer: Captured,
    /// The tap dataflow's id, for teardown on `unbind`.
    dataflow_id: usize,
    /// The tap's probe: `tick` must wait on it so the buffer holds every
    /// change through the just-closed epoch before draining.
    probe: ProbeHandle<OuterTime>,
    /// Keeps the tap's import alive; dropped (deactivating the operator)
    /// together with the binding.
    _shutdown: ShutdownButton<CapabilitySet<OuterTime>>,
}

/// A binding's captured changes, in the form its source trace holds them: rows, or the
/// containers of a columnar trace (whose times `tick` replaces).
enum Captured {
    Rows(Rc<RefCell<Vec<((Value, Value), Diff)>>>),
    Columns(Rc<RefCell<Vec<HostContainer>>>),
}

/// A live registry of installed programs and the traces they publish.
pub struct Server {
    /// Published export name -> shareable trace.
    traces: HashMap<String, ServerTrace>,
    /// With columnar exports: published export name -> trace of corgi chunks. Columnar imports
    /// and binds read the chunks as containers; other readers (row imports, binds into row
    /// inputs, snapshots, subscriptions) see rows, converted as they read.
    ctraces: HashMap<String, crate::backend::corgi::ExportTrace>,
    /// With export taps: each columnar export's new batches on this worker, drained by
    /// `take_changes` / `for_each_change_batch`: a standing change stream without a snapshot.
    taps: HashMap<String, crate::backend::corgi::ExportTap>,
    /// `set_columnar_exports`.
    columnar_exports: bool,
    /// `set_export_taps`.
    export_taps: bool,
    /// Installed program name -> its handles and lifecycle bookkeeping.
    programs: HashMap<String, Installed>,
    /// Published trace name -> the shape of its rows, where it is known: inferred from its
    /// producer's program, or fixed by its recipe for a generated source.
    shapes: HashMap<String, RowShape>,
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
            ctraces: HashMap::new(),
            taps: HashMap::new(),
            columnar_exports: true,
            export_taps: false,
            programs: HashMap::new(),
            shapes: HashMap::new(),
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

    /// Whether `name` (canonical) is published, as a row or a columnar trace.
    fn is_published(&self, name: &str) -> bool {
        self.traces.contains_key(name) || self.ctraces.contains_key(name)
    }

    /// Keep corgi-backend data columnar (default on): a program's exports leave its scope as
    /// corgi containers and are arranged as corgi chunks, as are generated sources; inputs convert
    /// fed rows to columns; imports and binds read chunks as containers. Readers convert to rows
    /// only when they need rows (`snapshot`, a row input). Affects programs and sources installed
    /// afterwards.
    pub fn set_columnar_exports(&mut self, on: bool) {
        self.columnar_exports = on;
    }

    /// Whether newly installed programs and sources keep their data as corgi columns.
    fn columnar(&self) -> bool {
        self.backend == RenderBackend::Corgi && self.columnar_exports
    }

    /// With columnar exports, also keep each export's new batches for [`Server::take_changes`]
    /// and [`Server::for_each_change_batch`] (default off). Affects programs installed afterwards.
    pub fn set_export_taps(&mut self, on: bool) {
        self.export_taps = on;
    }

    /// Clone a trace reader for a transient peek or subscription dataflow.
    pub fn trace(&self, name: &str) -> Option<ServerTrace> {
        self.traces.get(&canonical_source_name(name)).cloned()
    }

    /// A reader for published trace `name`, row or columnar, for a transient peek or
    /// subscription dataflow.
    pub fn published(&self, name: &str) -> Option<Published> {
        let name = canonical_source_name(name);
        self.traces.get(&name).cloned().map(Published::Rows)
            .or_else(|| self.ctraces.get(&name).cloned().map(Published::Columnar))
    }

    /// The shape of a published trace's rows, where it is known.
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
            .chain(self.ctraces.keys())
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
        // Resolve trace imports against canonical names. A name that is not registered must be a
        // generated source (`clock`, or a recipe such as `random:...`), installed on demand below.
        // Everything is checked before anything is installed, so a rejected program leaves no trace.
        let mut generated: Vec<String> = Vec::new();
        for imp in &prog.root.imports {
            if let st::Source::Trace(t) = &imp.from {
                let key = canonical_source_name(t);
                if !self.is_published(&key) {
                    if key != "clock" && Recipe::parse(&key).is_none() {
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
        let (scope_shapes, export_shapes) = self.check_shapes(name, prog)?;
        for key in generated {
            if self.is_published(&key) {
                continue; // imported twice
            }
            if key == "clock" {
                self.install_clock(worker);
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

        // A declared import of a trace whose shape is unknown is checked as its data arrives.
        let unchecked: Vec<Option<RowShape>> = prog.root.imports.iter()
            .map(|imp| match &imp.from {
                st::Source::Trace(t) if !self.shapes.contains_key(&canonical_source_name(t)) => imp.shape.clone(),
                _ => None,
            })
            .collect();

        let probe = ProbeHandle::new();
        let root = &prog.root;
        let columnar = self.columnar();
        let traces = &mut self.traces;
        let backend = self.backend;
        let tapped = columnar && self.export_taps;
        let ctraces = &mut self.ctraces;

        // The id this dataflow will get; captured so `drop` can remove it.
        let dataflow_id = worker.next_dataflow_index();

        #[allow(clippy::type_complexity)]
        let (published, cpublished, inputs): (Vec<(String, ServerTrace)>, Vec<(String, crate::backend::corgi::ExportTrace, Option<crate::backend::corgi::ExportTap>)>, Vec<(usize, ServerInput)>) =
            worker.dataflow::<OuterTime, _, _>(|outer| {
                let mut inputs: Vec<(usize, ServerInput)> = Vec::new();

                // Columnar: one outer (host-time) collection of corgi containers per root import,
                // rendered without rows. A columnar trace is read as its chunks, and an input
                // converts rows to columns as they are fed.
                let (leaved, cleaved) = if columnar {
                    use timely::dataflow::operators::core::Input as _;
                    use crate::backend::corgi::{assert_shape, export_containers, rows_to_corgi};
                    let outer_cols: Vec<differential_dataflow::Collection<OuterTime, HostContainer>> = root
                        .imports
                        .iter()
                        .enumerate()
                        .map(|(k, imp)| {
                            // The declared shape, or the one inferred at install (an imported trace's).
                            let shape = imp.shape.clone()
                                .or_else(|| scope_shapes.imports[k].clone().map(|c| (c.key, c.val)));
                            match &imp.from {
                                st::Source::Input(n) => {
                                    let (handle, stream) = outer.new_input::<HostContainer>();
                                    inputs.push((*n, ServerInput::Columns(ColumnInput::new(handle, shape, imp.shape.is_some()))));
                                    differential_dataflow::AsCollection::as_collection(stream)
                                }
                                st::Source::Trace(t) => {
                                    // The first binding point: resolve a named trace by importing it.
                                    let key = canonical_source_name(t);
                                    let cols = match ctraces.get_mut(&key) {
                                        Some(ctrace) => export_containers(ctrace.import(outer.clone())),
                                        None => {
                                            let arranged = traces
                                                .get_mut(&key)
                                                .expect("validated above")
                                                .import(outer.clone());
                                            rows_to_corgi(arranged.as_collection(|k, v| (k.clone(), v.clone())), shape)
                                        }
                                    };
                                    match unchecked[k].clone() {
                                        Some(declared) => assert_shape(cols, declared),
                                        None => cols,
                                    }
                                }
                                st::Source::Parent(_) => unreachable!("root import from a parent scope"),
                            }
                        })
                        .collect();

                    // Render the program body in its own iterative scope, then bring
                    // every export back out to the host time.
                    let exports = outer.iterative::<PointStamp<OuterTime>, _, _>(|inner| {
                        let entered: Vec<_> = outer_cols.iter().map(|c| c.clone().enter(inner)).collect();
                        crate::backend::corgi::render_tree(root, inner.clone(), 0, entered, Some(&scope_shapes))
                            .into_iter()
                            .map(|c| c.leave(outer))
                            .collect::<Vec<_>>()
                    });
                    (Vec::new(), exports)
                } else {
                    // One outer (host-time) collection per root import.
                    let outer_cols: Vec<VecCollection<OuterTime, (Value, Value), Diff>> = root
                        .imports
                        .iter()
                        .map(|imp| match &imp.from {
                            st::Source::Input(n) => {
                                let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                                inputs.push((*n, ServerInput::Rows(handle)));
                                col
                            }
                            st::Source::Trace(t) => {
                                // The first binding point: resolve a named trace by importing it.
                                let key = canonical_source_name(t);
                                // A columnar export is read as rows by its importers.
                                if let Some(ctrace) = ctraces.get_mut(&key) {
                                    let rows = crate::backend::corgi::export_rows(ctrace.import(outer.clone()));
                                    return differential_dataflow::AsCollection::as_collection(rows);
                                }
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
                    // every export back out to the host time.
                    let exports = outer.iterative::<PointStamp<OuterTime>, _, _>(|inner| {
                        let entered: Vec<_> =
                            outer_cols.iter().map(|c| c.clone().enter(inner)).collect();
                        let exports = match backend {
                            RenderBackend::Vec => render_tree(root, inner.clone(), 0, entered, Some(&scope_shapes)),
                            RenderBackend::Corgi => {
                                render_tree_rows(root, inner.clone(), 0, entered, Some(&scope_shapes))
                            }
                        };
                        exports
                            .into_iter()
                            .map(|c| c.leave(outer))
                            .collect::<Vec<_>>()
                    });
                    (exports, Vec::new())
                };
                let cpublished: Vec<_> = root
                    .exports
                    .iter()
                    .zip(cleaved)
                    .map(|(e, col)| {
                        use timely::dataflow::operators::Probe;
                        let arranged = crate::backend::corgi::arrange_export(col);
                        let tap: Option<crate::backend::corgi::ExportTap> = tapped.then(Default::default);
                        let stream = match &tap {
                            Some(t) => crate::backend::corgi::tap_export(&arranged, t.clone()),
                            None => arranged.stream.clone(),
                        };
                        stream.probe_with(&probe);
                        (e.name.clone(), arranged.trace, tap)
                    })
                    .collect();

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

                (published, cpublished, inputs)
            });

        for (export_name, shape) in export_shapes {
            self.shapes.insert(export_name, shape);
        }
        let input_shapes: HashMap<usize, RowShape> = prog.root.imports.iter()
            .filter_map(|imp| match (&imp.from, &imp.shape) {
                (st::Source::Input(n), Some(shape)) => Some((*n, shape.clone())),
                _ => None,
            })
            .collect();
        for (export_name, trace) in published {
            self.traces.insert(export_name, trace);
        }
        for (export_name, trace, tap) in cpublished {
            if let Some(tap) = tap { self.taps.insert(export_name.clone(), tap); }
            self.ctraces.insert(export_name, trace);
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
                input_shapes,
            },
        );
        Ok(())
    }

    /// Infer the shapes of `prog`'s collections, before anything is installed. An imported trace
    /// takes the shape it is published at; a declared import must match it. Returns the shapes,
    /// for rendering, and the shape of each export that has one; or the conflicts that leave the
    /// program without meaning.
    /// Collections whose shape is unknown (an undeclared input upstream) are not an error.
    fn check_shapes(&self, name: &str, prog: &st::Program) -> Result<(crate::shapes::ScopeShapes, Vec<(String, RowShape)>), String> {
        let published = |t: &str| -> Option<RowShape> {
            let key = canonical_source_name(t);
            self.shapes.get(&key).cloned().or_else(|| generated_shape(&key))
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
        let exports = prog.root.exports.iter().zip(&shapes.exports)
            .filter_map(|(e, s)| s.as_ref().map(|c| (e.name.clone(), (c.key.clone(), c.val.clone()))))
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
                input_shapes: HashMap::new(),
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
            input.update_at((clock_row(self.epoch), Value::unit()), self.epoch, 1);
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
                input_shapes: HashMap::new(),
            },
        );
    }

    /// Build a source's dataflow, one input arranged and published as trace `name` of `shape`: as
    /// corgi chunks if installs are columnar, and otherwise as rows. Returns the input, the
    /// dataflow's id, and the probe on its arrangement.
    fn install_source(&mut self, worker: &mut Worker, name: &str, shape: RowShape) -> (ServerInput, usize, ProbeHandle<OuterTime>) {
        let probe = ProbeHandle::new();
        let dataflow_id = worker.next_dataflow_index();
        let input = if self.columnar() {
            let (handle, trace) = worker.dataflow::<OuterTime, _, _>(|outer| {
                use timely::dataflow::operators::core::Input as _;
                use timely::dataflow::operators::Probe;
                let (handle, stream) = outer.new_input::<HostContainer>();
                let arranged = crate::backend::corgi::arrange_export(differential_dataflow::AsCollection::as_collection(stream));
                arranged.stream.probe_with(&probe);
                (handle, arranged.trace)
            });
            self.ctraces.insert(name.to_string(), trace);
            ServerInput::Columns(ColumnInput::new(handle, Some(shape.clone()), false))
        } else {
            let (handle, trace) = worker.dataflow::<OuterTime, _, _>(|outer| {
                let (handle, col) = outer.new_collection::<(Value, Value), Diff>();
                (handle, col.probe_with(&probe).arrange_by_key().trace)
            });
            self.traces.insert(name.to_string(), trace);
            ServerInput::Rows(handle)
        };
        self.shapes.insert(name.to_string(), shape);
        (input, dataflow_id, probe)
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
        let handle = self.programs
            .get_mut(&canonical_source_name(prog))
            .expect("feed target was prevalidated")
            .inputs
            .get_mut(&input)
            .expect("feed input was prevalidated");
        for update in updates {
            handle.update_at((update.key, update.val), time, update.diff);
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
        if let (Some(recipe), Some(declared)) = (recipe, self.programs.get(&canonical_source_name(prog)).and_then(|p| p.input_shapes.get(&input))) {
            if recipe.shape() != *declared {
                return Err(format!(
                    "program {:?} declares input {} as {} but {:?} generates {}",
                    prog, input, fmt_shape(declared), source, fmt_shape(&recipe.shape())
                ));
            }
        }
        // A recipe's rows are generated into the input below; a file's are parsed here.
        let (total, rows): (u64, Vec<(Value, Value)>) = match recipe {
            Some(recipe) => (recipe.rows_len(), Vec::new()),
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
        if let Some(recipe) = recipe {
            handle.generate(recipe, (0..recipe.rows_len()).filter(|e| mine(*e)), time, 1);
        }
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
        if let (Some(declared), Some(shape)) = (installed.input_shapes.get(&input), self.shapes.get(&source)) {
            if declared != shape {
                return Err(format!(
                    "program {:?} declares input {} as {} but trace {:?} is published as {}",
                    target, input, fmt_shape(declared), source, fmt_shape(shape)
                ));
            }
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
        let (buffer, shutdown) = match self.published(&source).expect("checked above") {
            // A columnar trace into a column input: capture its containers, never forming rows.
            Published::Columnar(mut trace) if matches!(installed.inputs[&input], ServerInput::Columns(_)) => {
                let buffer: Rc<RefCell<Vec<HostContainer>>> = Rc::new(RefCell::new(Vec::new()));
                let buffer_in = buffer.clone();
                let shutdown = worker.dataflow::<OuterTime, _, _>(|scope| {
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
                (Captured::Columns(buffer), shutdown)
            }
            mut trace => {
                let buffer: Rc<RefCell<Vec<((Value, Value), Diff)>>> = Rc::new(RefCell::new(Vec::new()));
                let buffer_in = buffer.clone();
                let shutdown = worker.dataflow::<OuterTime, _, _>(|scope| {
                    use timely::dataflow::operators::{Inspect, Probe};
                    let (rows, shutdown) = trace.import_rows(scope.clone(), "BindImport");
                    rows
                        .inspect(move |((key, val), _time, diff)| {
                            buffer_in
                                .borrow_mut()
                                .push(((key.clone(), val.clone()), *diff));
                        })
                        .probe_with(&mut probe);
                    shutdown
                });
                (Captured::Rows(buffer), shutdown)
            }
        };

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

    /// Return the consolidated closed-past contents of a trace on worker 0.
    ///
    /// Builds a transient dataflow that imports the trace, **exchanges every
    /// row to worker 0**, and accumulates net multiplicities as of the current
    /// epoch — so the result is the complete, consolidated contents even when
    /// the trace is sharded across workers, not each worker's slice. The
    /// dataflow is dropped as soon as it has drained. Filtering and rendering
    /// are the caller's: this returns rows, it does not print them.
    pub fn snapshot(
        &mut self,
        worker: &mut Worker,
        name: &str,
    ) -> Result<Vec<(Value, Value, Diff)>, String> {
        use timely::dataflow::operators::{Exchange, Inspect, Probe};

        let name = canonical_source_name(name);
        let epoch = self.epoch;
        let mut trace = self.published(&name).ok_or_else(|| format!("no trace {:?}", name))?;
        let acc: Rc<RefCell<HashMap<(Value, Value), Diff>>> = Rc::new(RefCell::new(HashMap::new()));
        let acc_in = acc.clone();
        let mut probe = ProbeHandle::new();
        let id = worker.next_dataflow_index();
        worker.dataflow::<OuterTime, _, _>(|scope| {
            // The dataflow is dropped once drained, which releases the import.
            let (rows, _shutdown) = trace.import_rows(scope.clone(), "SnapshotImport");
            rows
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
            // Remote data or progress wakes idle workers; do not busy-poll.
            worker.step_or_park(None);
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

    /// The changes to export `name` that this worker's share of its arrangement has produced
    /// since the last call, as rows (with [`Server::set_export_taps`]). Each worker drains its own.
    pub fn take_changes(&mut self, name: &str) -> Vec<((Value, Value), OuterTime, Diff)> {
        match self.taps.get(name) {
            Some(tap) => crate::backend::corgi::batch_rows(std::mem::take(&mut *tap.borrow_mut())),
            None => Vec::new(),
        }
    }

    /// Drain this worker's tapped export changes in row batches no larger than
    /// `rows_per_batch`. Callbacks run in export order; rows are consumed once.
    /// Unlike `take_changes`, this does not materialize the entire export as rows.
    /// The limit counts rows, not bytes (nested payloads may be large).
    pub fn for_each_change_batch(
        &mut self,
        name: &str,
        rows_per_batch: usize,
        consume: impl FnMut(Vec<((Value, Value), OuterTime, Diff)>),
    ) {
        assert!(rows_per_batch > 0, "export row batch size must be positive");
        if let Some(tap) = self.taps.get(name) {
            let batches = std::mem::take(&mut *tap.borrow_mut());
            crate::backend::corgi::for_each_batch_rows(batches, rows_per_batch, consume);
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
            self.ctraces.remove(ex);
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
        let (index, peers) = (worker.index(), worker.peers());
        let mine = |e: &u64| (*e as usize) % peers == index;
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
        while self.programs.values().any(|p| p.probe.less_than(&epoch))
            || self.bindings.iter().any(|b| b.probe.less_than(&epoch))
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
            match (&binding.buffer, handle) {
                (Captured::Rows(buffer), Some(handle)) => {
                    for ((key, val), diff) in buffer.borrow_mut().drain(..) {
                        handle.update_at((key, val), epoch, diff);
                    }
                }
                (Captured::Columns(buffer), Some(handle)) => {
                    for container in buffer.borrow_mut().drain(..) {
                        handle.give_at(container, epoch);
                    }
                }
                (Captured::Rows(buffer), None) => buffer.borrow_mut().clear(),
                (Captured::Columns(buffer), None) => buffer.borrow_mut().clear(),
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
        for trace in self.ctraces.values_mut() {
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
