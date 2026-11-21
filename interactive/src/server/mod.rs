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
//! The named-trace IR (an input bound to a trace, and `output "y"`) flows through parse → lower
//! → `scope_ir`. The server resolves both ends:
//!
//! - **`Source::Trace(name)`** — import the registered trace into the new
//!   dataflow, as the producer's arrangement or as a root collection. A load's binding
//!   ([`Program::bind`](crate::scope_ir::Program::bind)) makes an input this source.
//! - **`Export(name, _)`** — arrange the output collection and register its
//!   trace under `<program>::<name>` ([`output_name`]), so a later install can import it.
//!
//! # Lifecycle
//!
//! - **install** builds a dataflow over imported traces + positional inputs,
//!   publishing its exports. The dataflow's id (`next_dataflow_index`) is kept
//!   for teardown.
//! - **feed** stages updates, as text read at the input's declared shapes or
//!   as columns, at a chosen time or explicitly at the input’s singleton frontier. Updates can
//!   be scheduled into the future.
//! - **load** fills an input from a file or a published trace.
//! - **seal** moves one input’s write frontier or finishes the input.
//! - **spin** waits for each export to reach its commanded write frontier.
//! - **trim** releases readable history on one output.
//! - **drop** evicts a program — gated on its published traces having no live
//!   importer — and calls `worker.drop_dataflow`, which removes the operators
//!   outright and frees their state immediately. The gate is what makes that
//!   unilateral removal safe: nothing live still reads the dropped traces.
//!
//! # Layout
//!
//! This file holds the registry: [`Server`] and what it keeps per program, [`Command`], the
//! published-trace handle [`Published`], and program teardown ([`Server::drop_program`]).
//! The rest is one file per concern, each adding methods to [`Server`]:
//!
//! - `time`: scalar and product timestamps ([`OuterTime`], [`HostTime`], [`host_dataflow`]);
//! - `sources`: positional inputs and diagnostic sources;
//! - `install`: installing a program;
//! - `feed`: feeding and loading inputs;
//! - `read`: snapshots, peeks and export taps;
//! - `diagnostics`: the `diag.*` sources' wiring;
//! - `evaluate`: [`evaluate`], the test suites' entry point.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use differential_dataflow::operators::arrange::ShutdownButton;
use differential_dataflow::trace::TraceReader;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::PartialOrder;
use differential_dataflow::lattice::Lattice;
use timely::worker::Worker;

use crate::runtime::container::Updates;
use crate::ir::Diff;
use crate::scope_ir as st;

mod diagnostics;
mod evaluate;
mod feed;
mod install;
mod read;
mod sources;
mod time;
mod progress;

pub use self::evaluate::evaluate;
pub use self::time::{host_dataflow, host_time, HostTime, OuterTime};
use self::sources::{InputContainer, ServerInput, COLUMN_BATCH};

/// The name an installed program's output is published under: `<program>::<output>`. Every
/// program has its own namespace, so two programs may name their outputs alike, and one program
/// text may be installed several times under different names. Diagnostic sources (`diag.*`) belong to no program and keep their
/// own names.
pub fn output_name(program: &str, output: &str) -> String {
    format!("{program}::{output}")
}

/// A published export as its readers see it: a trace of corgi chunks.
/// Its declared root width is available from [`Server::trace_width`].
#[derive(Clone)]
pub struct Published(pub crate::backend::corgi::ExportTrace);

impl Published {
    /// Import into `scope` as containers, one per chunk of each batch, read as of `as_of`
    /// (see [`Server::install_read_floor`]), with the import's shutdown button. Each worker reads its own
    /// share.
    pub fn import_containers<'s>(
        &mut self,
        scope: timely::dataflow::Scope<'s, HostTime>,
        name: &str,
        as_of: HostTime,
    ) -> (
        differential_dataflow::Collection<'s, HostTime, HostContainer>,
        ShutdownButton<CapabilitySet<HostTime>>,
    ) {
        debug_assert!(self.0.get_logical_compaction().less_equal(&as_of), "an import as of {as_of:?}, before its trace's compaction frontier");
        let (arranged, shutdown) = self.0.import_core(scope, name);
        (crate::backend::corgi::export_containers(arranged, as_of), shutdown)
    }
}

/// The shape of a collection's rows: its key's and its value's.
pub type RowShape = (corgi::Shape, corgi::Shape);

/// A container of corgi columns at the program time, as a published trace's readers see them.
pub type HostContainer = crate::runtime::container::CorgiContainer<HostTime, Diff>;

/// Where an installed entry came from. Only `Program` is writable by `feed`;
#[derive(Clone, Copy, PartialEq)]
enum Origin {
    Program,
    /// A `diag.*` source, fed from the server's own logging at each sample (see [`crate::diag`]).
    Diagnostics,
}

fn fmt_shape((k, v): &RowShape) -> String {
    format!("({k}, {v})")
}

/// A unit of server work, already parsed/lowered/validated on the intake side.
///
/// A program's input, as a client names it: by its label, a number (`input 0`), which is also its
/// position, or a name (`input "edges"`). Workers resolve a name against the program installed,
/// so a client's view of the program cannot misdirect a feed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InputId {
    Position(usize),
    Name(String),
}

impl InputId {
    /// Digits are a position, and anything else a name.
    pub fn parse(text: &str) -> Self {
        let digits = !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
        match text.parse() {
            Ok(position) if digits => InputId::Position(position),
            _ => InputId::Name(text.to_string()),
        }
    }

    /// Whether `text` may name an input: an identifier, so that it never reads as a number.
    pub fn is_name(text: &str) -> bool {
        let mut chars = text.chars();
        chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
}

impl From<usize> for InputId {
    fn from(position: usize) -> Self { InputId::Position(position) }
}

/// Update times for a batch. Every shard of a per-row batch retains its global admission bounds.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum FeedTimes {
    /// One explicit point or, for `time=now`, the input’s sole frontier element.
    Shared(Option<HostTime>),
    /// One lane per coordinate, accompanied by the least points and width of the whole batch.
    PerRow { lanes: Vec<Vec<u64>>, least: Vec<HostTime>, width: usize },
}

/// Serializable so worker 0 can circulate it to every worker; the workers
/// execute it without any further parsing.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Command {
    /// Install `program` under `name`.
    Install { name: String, program: st::Program, width: usize },
    /// Update `input` of `prog` with one row, read into columns, at
    /// `time` (`None` for `time=now`).
    Feed {
        prog: String,
        input: InputId,
        columns: Updates,
        time: Option<HostTime>,
    },
    /// Stage one batch of updates, partitioned across the workers.
    FeedBatch { prog: String, input: InputId, columns: Updates, times: FeedTimes },
    /// Move `input` of program `prog` to `frontier` (see [`Server::seal`]).
    Seal { prog: String, input: InputId, frontier: Antichain<HostTime> },
    /// Fill `input` of `prog` from `source` — a published trace or a file path — at the given destination time (the sole input frontier point for `time=now`), each row's
    /// multiplicity scaled by `diff`. Collective: every worker feeds its own
    /// shard of the rows (see [`Server::load`]).
    Load {
        prog: String,
        input: InputId,
        time: Option<HostTime>,
        source: String,
        diff: Diff,
    },
    /// Run until every program's outputs have caught up with their inputs (see [`Server::spin`]).
    Spin,
    /// Sample the diagnostics inputs and advance their clock.
    SampleDiagnostics,
    /// Release the output's history before this time.
    Trim { trace: String, frontier: Option<HostTime> },
    /// Drop the named program.
    Drop { name: String },
    /// Report the registry. The driver assembles it from [`Server::program_info`],
    /// [`Server::trace_info`].
    List,
    /// Stop the server.
    Exit,
}

/// Everything the server holds for one installed program.
struct Installed {
    /// Positional input index -> handle.
    inputs: HashMap<usize, ServerInput>,
    /// The positions of its inputs, by the names they are written with.
    input_names: HashMap<String, usize>,
    /// Names of traces this program imports (for the importer refcount).
    imports: Vec<String>,
    /// Names of traces this program publishes (registry entries it owns).
    exports: Vec<String>,
    /// The timely dataflow id, used to `drop_dataflow` on teardown.
    dataflow_id: usize,
    /// Distinguishes writable program inputs from diagnostic sources.
    origin: Origin,

}

/// Progress and read validity belong to each export, including shared sources.
struct Output {
    /// Number of root time coordinates in this export.
    width: usize,
    /// Installed program that publishes this trace.
    owner: String,
    /// This worker's observed frontier of the export arrangement.
    probe: ProbeHandle<HostTime>,
    /// Logical compaction frontier. Empty once the publisher releases its history.
    since: Antichain<HostTime>,
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

/// A live registry of installed programs and the traces they publish.
pub struct Server {
    outputs: HashMap<String, Output>,
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
    /// fixed by the diagnostic schema.
    shapes: HashMap<String, RowShape>,
    /// Trace name -> number of installed programs importing it (the drop gate).
    importers: HashMap<String, usize>,
    /// Hand published traces to importing programs as arrangements, so an importer shares the
    /// producer's trace instead of copying it.
    arranged_imports: bool,
    /// The passes install runs on programs installed afterwards (default all except projection).
    passes: crate::passes::Config,
    /// Next epoch for a sample of worker diagnostics.
    diagnostics_epoch: OuterTime,
    /// This worker's diagnostics, when they are on: the source of the `diag.*` collections.
    diagnostics: Option<Rc<RefCell<crate::diag::Diagnostics>>>,
}

impl Server {
    /// A fresh server with no installed programs.
    pub fn new() -> Self {
        Server {
            outputs: HashMap::new(),
            traces: HashMap::new(),
            taps: HashMap::new(),
            export_taps: false,
            programs: HashMap::new(),
            shapes: HashMap::new(),
            importers: HashMap::new(),
            arranged_imports: true,
            passes: crate::passes::Config::default(),
            diagnostics_epoch: 0,
            diagnostics: None,
        }
    }

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

    /// The passes install runs on programs installed afterwards (default all except projection). None of them is
    /// needed to run a program; they change how, not what, it computes.
    pub fn set_passes(&mut self, passes: crate::passes::Config) {
        self.passes = passes;
    }

    /// The passes install runs, for reports that describe an install.
    pub fn passes(&self) -> crate::passes::Config {
        self.passes
    }

    /// Also keep each export's new batches for [`Server::take_changes`] (default off). Affects
    /// programs installed afterwards.
    pub fn set_export_taps(&mut self, on: bool) {
        self.export_taps = on;
    }

    /// A reader for published trace `name`, for a transient peek or subscription dataflow.
    pub fn published(&self, name: &str) -> Option<Published> {
        self.traces.get(name).cloned().map(Published)
    }

    /// The shape of a published trace's rows.
    pub fn trace_shape(&self, name: &str) -> Option<RowShape> {
        self.shapes.get(name).cloned()
    }

    /// The shape of a published trace or a source install can create on demand.
    pub fn source_shape(&self, name: &str) -> Option<RowShape> {
        self.trace_shape(name).or_else(|| crate::diag::shape(name))
    }

    /// The position of `prog`'s `input`, resolving a name against the program installed.
    pub fn input_position(&self, prog: &str, input: &InputId) -> Result<usize, String> {
        match input {
            InputId::Position(n) => Ok(*n),
            InputId::Name(name) => {
                let installed = self.programs.get(prog).ok_or_else(|| format!("no program {prog:?}"))?;
                installed.input_names.get(name).copied().ok_or_else(|| format!("program {prog:?} has no input named {name:?}"))
            }
        }
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

    /// Drop installed program `name`, releasing its dataflow immediately.
    ///
    /// Refuses (changing nothing) if any trace the program publishes still has a
    /// live importer — drop the consumers first. Otherwise it unregisters the
    /// program's published traces, closes its inputs, and calls
    /// `worker.drop_dataflow`, which removes the operators and frees their state
    /// at once. Safe because the gate guarantees no live dataflow still reads it.
    pub fn drop_program(&mut self, worker: &mut Worker, name: &str) -> Result<(), String> {
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
            self.outputs.remove(ex);
            self.shapes.remove(ex);
            self.taps.remove(ex);
        }
        let id = installed.dataflow_id;
        // Drop the input handles first (closes the inputs while the operators
        // still exist), then remove the dataflow outright.
        drop(installed);
        worker.drop_dataflow(id);
        // Diagnostic sources are installed on demand and have no independent
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

}

impl Default for Server {
    fn default() -> Self {
        Server::new()
    }
}
