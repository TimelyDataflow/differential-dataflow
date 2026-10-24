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
//!
//! # Layout
//!
//! This file holds the registry: [`Server`] and what it keeps per program, [`Command`], the
//! published-trace handle [`Published`], and the lifecycle's ends, `tick` and `drop_program`.
//! The rest is one file per concern, each adding methods to [`Server`]:
//!
//! - `time`: the host epoch and the program time ([`OuterTime`], [`HostTime`], [`host_dataflow`]);
//! - `sources`: positional inputs, the `random:`/`iota:` recipes and the `clock`;
//! - `install`: installing a program;
//! - `feed`: feeding and loading inputs;
//! - `bind`: bindings from published traces to inputs;
//! - `read`: snapshots, peeks and export taps;
//! - `diagnostics`: the `diag.*` sources' wiring;
//! - `evaluate`: [`evaluate`], the test suites' entry point.

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

use crate::runtime::container::Updates;
use crate::ir::Diff;
use crate::scope_ir as st;

mod bind;
mod diagnostics;
mod evaluate;
mod feed;
mod install;
mod read;
mod sources;
mod time;

pub use self::evaluate::evaluate;
pub use self::sources::gen_field_seeded;
pub use self::time::{host_dataflow, host_time, HostTime, OuterTime};
use self::bind::Binding;
use self::sources::{canonical_source_name, clock_rows, generated_shape, InputContainer, Recipe, ServerInput, COLUMN_BATCH};

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
pub type HostContainer = crate::runtime::container::CorgiContainer<HostTime, Diff>;

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

fn fmt_shape((k, v): &RowShape) -> String {
    format!("({k}, {v})")
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
