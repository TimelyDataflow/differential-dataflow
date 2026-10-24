//! Command and response shapes for the line-oriented protocol.
//!
//! Each request line is `<reqid> <cmd> [args...]`.
//! Each response line starts with the same `<reqid>` followed by one of:
//!   - `ok [body...]` — terminal success line
//!   - `err [body...]` — terminal error line
//!   - `data <fields...>` — one streamed body line (peek/tail batches)
//!   - `end` — terminator of a `tail` stream, once it is stopped
//!
//! Multi-line bodies use two-phase framing: `load ... begin` accepts literal
//! DDIR through `end-load`, while `feed <prog> <in#> begin` accepts row updates
//! through `end-feed` and becomes one non-interleavable server command.
//!
//! `feed <prog> <in#> frame <nbytes>` is followed by exactly `nbytes` bytes: the
//! updates as columns, in the binary frame format `peek … as corgi` answers with
//! (`interactive::runtime::frame`), with no time lanes. On WebSocket the header
//! line and its bytes are one binary message.

use std::any::{type_name_of_val, Any};
use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use interactive::runtime::container::Updates;
use interactive::runtime::feed::FeedDecoder;
use interactive::ir::Diff;
use interactive::scope_ir::{Program, Source};
use interactive::server::{Command as ServerCommand, OuterTime, RowShape};

pub type ReqId = String;

/// One client session's outbound stream. Cloned into each `Request` so
/// dispatch can route responses back to the originating client (and so
/// long-lived subscriptions like `tail` capture the right sender).
pub type RespSender = std::sync::mpsc::Sender<Out>;

/// One message to a client: a protocol line, or a binary frame.
pub enum Out {
    /// A text line, newline included.
    Line(String),
    /// A binary frame: on TCP and stdin the header line `<reqid> frame <kind> <nbytes>\n`
    /// followed by exactly `nbytes` bytes; on WebSocket one binary message holding the same
    /// header line and bytes. `pending` counts a subscription's frame bytes not yet written;
    /// the writer subtracts each frame's bytes once it is on the wire.
    Frame {
        header: String,
        bytes: Vec<u8>,
        pending: Option<Arc<AtomicUsize>>,
    },
}

impl Out {
    /// The message as it goes on the wire, releasing a frame's pending bytes.
    pub fn into_wire(self) -> Vec<u8> {
        match self {
            Out::Line(line) => line.into_bytes(),
            Out::Frame { header, bytes, pending } => {
                if let Some(pending) = pending {
                    pending.fetch_sub(bytes.len(), Ordering::Relaxed);
                }
                let mut wire = header.into_bytes();
                wire.extend_from_slice(&bytes);
                wire
            }
        }
    }
}

/// Per-session identity. Lets the worker tear down long-lived
/// subscriptions when a connection disappears without an explicit stop.
pub type ConnectionId = u64;

#[derive(Debug)]
pub enum Cmd {
    /// Install a dataflow.
    /// `id_hint` — the client-chosen name the dataflow is installed under.
    /// `bindings` — `import-name -> binding`, where the binding is either a
    /// registered trace name or a builtin call (`random(...)`).
    /// `program` — DDIR text, inline (`load … begin` … `end-load`) or read
    /// from a file (`load <name> from <path>`).
    /// `explain` — `explain` or `explain=debug`: apply the explanation rewrite,
    /// with `debug` tapping every demand collection with an inspect; the query
    /// input is the one after the program's own, the demand sets its exports.
    Load {
        id_hint: String,
        bindings: BTreeMap<String, String>,
        program: String,
        explain: Option<bool>,
    },
    /// Drop the named dataflow. Fails if any export of this dataflow is
    /// still imported by another live dataflow or held by a reader.
    Drop { name: String },
    /// List held names.
    List,
    /// One-shot snapshot of a named trace, optionally of one key (text, read at the trace's key
    /// shape): a text line of its columns, or with `as corgi` binary frames.
    Peek { name: String, key: Option<String>, columnar: bool },
    /// Persistent subscription to a named trace: text lines of its changes' columns, or with
    /// `as corgi` binary frames.
    Tail { name: String, columnar: bool },
    /// Cancel a previous `tail` (matched by its reqid).
    Stop { tail_reqid: ReqId },
    /// Update positional `input` of `prog` with one row, read into columns at
    /// the input's declared shapes, at `time` (default the current epoch).
    /// Identity and ordering of writes are convention, not enforcement:
    /// cooperating clients include their session name and an ordering id in
    /// the data, and programs resolve contention over those facts (see
    /// demo/claims.txt, demo/txn.txt).
    Feed {
        prog: String,
        input: usize,
        columns: Updates,
        time: Option<OuterTime>,
    },
    /// Atomically stage many rows, read into columns at the input's declared
    /// shapes, into one program input at the current epoch.
    FeedColumns {
        prog: String,
        input: usize,
        columns: Updates,
    },
    /// Fill one program input from a source the server reads itself — a recipe
    /// (`random:…`, `iota:N`), a published trace, or a file of integer rows —
    /// each worker taking its shard, so no row crosses the wire.
    /// `feed <prog> <in#> from <source> [diff=<d>]`.
    Source {
        prog: String,
        input: usize,
        source: String,
        diff: Diff,
    },
    /// Bind a trace's changes into `prog`'s positional `input`, delivered at
    /// each tick one epoch delayed — the write path for installed programs.
    Bind {
        trace: String,
        prog: String,
        input: usize,
    },
    /// Remove a binding installed by `bind`.
    Unbind {
        trace: String,
        prog: String,
        input: usize,
    },
    /// Advance ambient time by `n` (default 1).
    Tick { n: u64 },
    /// End the session.
    Exit,
}

/// A command ready to broadcast to the worker group. Protocol-only tail
/// lifecycle wraps the typed server command vocabulary rather than leaking
/// response channels into the dataflow.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PreparedCommand {
    Server(ServerCommand),
    Peek { name: String, key: Option<String>, columnar: bool },
    Tail { name: String, columnar: bool },
    Stop { tail_reqid: ReqId },
}

/// The declared shapes of loaded programs' inputs, by program name, shared by every session so
/// that a feed is read into columns before it reaches the workers. A `load` records its program's
/// shapes as it is prepared, replacing any earlier program's of that name; nothing is forgotten,
/// as a feed to a program that is not installed is refused by the workers, which check the
/// shapes again.
#[derive(Clone, Default)]
pub struct Catalog(Arc<Mutex<HashMap<String, HashMap<usize, RowShape>>>>);

impl Catalog {
    fn record(&self, name: &str, program: &Program) {
        let inputs = program.root.imports.iter().filter_map(|import| match (&import.from, &import.shape) {
            (Source::Input(n), Some(shape)) => Some((*n, shape.clone())),
            _ => None,
        });
        self.0.lock().unwrap().insert(name.to_string(), inputs.collect());
    }

    /// The declared shapes of `prog`'s `input`.
    fn shape(&self, prog: &str, input: usize) -> Result<RowShape, String> {
        let programs = self.0.lock().unwrap();
        let inputs = programs.get(prog).ok_or_else(|| format!("no program {prog:?}"))?;
        inputs.get(&input).cloned().ok_or_else(|| format!("program {prog:?} has no input {input}"))
    }

    /// A reader of rows for `prog`'s `input`, at its declared shapes.
    fn decoder(&self, prog: &str, input: usize) -> Result<FeedDecoder, String> {
        let (key, val) = self.shape(prog, input)?;
        Ok(FeedDecoder::new(&key, &val))
    }
}

/// Parse and lower the expensive parts of a protocol command on its session
/// thread. Every worker receives the same typed program, and malformed DDIR
/// never reaches a Timely worker. A load's input shapes go into `catalog`.
pub fn prepare(command: Cmd, catalog: &Catalog) -> Result<PreparedCommand, String> {
    let command = match command {
        Cmd::Load {
            id_hint,
            bindings,
            program,
            explain,
        } => {
            let mut program = catch_unwind(AssertUnwindSafe(|| {
                interactive::lower::lower_tree(interactive::parse::pipe::parse(&program))
            }))
            .map_err(panic_message)?;
            apply_bindings(&mut program, &bindings)?;
            // The rewrite precedes optimization (its rules assume single-op Linears).
            if let Some(debug) = explain {
                let options = interactive::explain::Options { debug_inspects: debug };
                program = catch_unwind(AssertUnwindSafe(|| interactive::explain::explain_with(&program, options)))
                    .map_err(panic_message)?;
            }
            program.optimize();
            catalog.record(&id_hint, &program);
            ServerCommand::Install {
                name: id_hint,
                program,
            }
        }
        Cmd::Drop { name } => ServerCommand::Drop { name },
        Cmd::List => ServerCommand::List,
        Cmd::Peek { name, key, columnar } => return Ok(PreparedCommand::Peek { name, key, columnar }),
        Cmd::Tail { name, columnar } => return Ok(PreparedCommand::Tail { name, columnar }),
        Cmd::Stop { tail_reqid } => return Ok(PreparedCommand::Stop { tail_reqid }),
        Cmd::Feed { prog, input, columns, time } => ServerCommand::Feed { prog, input, columns, time },
        Cmd::FeedColumns { prog, input, columns } => ServerCommand::FeedColumns { prog, input, columns },
        Cmd::Source { prog, input, source, diff } => ServerCommand::Load { prog, input, source, diff },
        Cmd::Bind { trace, prog, input } => ServerCommand::Bind { trace, prog, input },
        Cmd::Unbind { trace, prog, input } => ServerCommand::Unbind { trace, prog, input },
        Cmd::Tick { n } => ServerCommand::Tick { n },
        Cmd::Exit => ServerCommand::Exit,
    };
    Ok(PreparedCommand::Server(command))
}

fn apply_bindings(
    program: &mut Program,
    bindings: &BTreeMap<String, String>,
) -> Result<(), String> {
    for (local, binding) in bindings {
        let import = program
            .root
            .imports
            .iter_mut()
            .find(|import| import.name == *local)
            .ok_or_else(|| format!("binding names no import {:?}", local))?;
        import.from = Source::Trace(random_binding(binding)?);
    }
    Ok(())
}

/// Translate the call spelling of a binding (`random(...)`) into its
/// content-addressed source name.
fn random_binding(binding: &str) -> Result<String, String> {
    let Some(body) = binding
        .strip_prefix("random(")
        .and_then(|s| s.strip_suffix(')'))
    else {
        return Ok(binding.to_string());
    };
    let mut values: HashMap<&str, &str> = HashMap::new();
    for field in body.split(',') {
        let (key, value) = field
            .trim()
            .split_once('=')
            .ok_or_else(|| format!("malformed random field {:?}", field))?;
        values.insert(key.trim(), value.trim());
    }
    let nodes = values.remove("range").ok_or("random requires range")?;
    let edges = values.remove("count").ok_or("random requires count")?;
    let arity = values.remove("arity").unwrap_or("2");
    let seed = values.remove("seed").unwrap_or("0");
    let churn = values.remove("churn").unwrap_or("0");
    if !values.is_empty() {
        return Err(format!("unknown random fields: {:?}", values.keys()));
    }
    Ok(format!(
        "random:nodes={},edges={},arity={},seed={},churn={}",
        nodes, edges, arity, seed, churn
    ))
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        format!("DDIR parser panicked ({})", type_name_of_val(&panic))
    }
}

/// A prepared request: reqid plus the command (or a parse/lowering error).
#[derive(Debug)]
pub struct Request {
    pub reqid: ReqId,
    pub kind: Result<PreparedCommand, String>,
    /// Where to route responses for this request (and, for `tail`, all
    /// subsequent batches until `stop`). Cloned from the per-connection
    /// outbound sender.
    pub resp: RespSender,
    /// Originating session; lets the worker tear down per-connection
    /// state (tails) when this session ends.
    pub connection_id: ConnectionId,
}

/// State carried between lines so the parser can splice a multi-line load or
/// feed body together. The parser hands back a request id with its parsed
/// command (or parse error), or `None` when more lines are required.
#[derive(Default)]
pub struct LineParser {
    /// The input shapes feeds are read at.
    catalog: Catalog,
    pending_load: Option<PendingLoad>,
    pending_feed: Option<PendingFeed>,
    pending_frame: Option<PendingFrame>,
    auto_reqid_counter: u64,
    max_load_bytes: usize,
    max_feed_bytes: usize,
}

/// Tokens that introduce a command. If a line begins with one of these
/// instead of an explicit reqid, the parser synthesizes a reqid.
const COMMAND_KEYWORDS: &[&str] = &[
    "load", "drop", "list", "peek", "tail", "stop", "tick", "exit", "feed", "bind",
    "unbind",
];

/// Program-size gate: a `load` body larger than this is rejected at intake,
/// before parsing — installs are cheap to request and costly to render, so
/// the cap is the first line of defense against installation as denial of
/// service. Override with `DDIR_MAX_PROGRAM_BYTES`.
fn max_program_bytes() -> usize {
    std::env::var("DDIR_MAX_PROGRAM_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(65536)
}

/// A framed feed is accumulated on its session thread before admission. Keep
/// that buffering bounded while allowing the 100k-row performance regime.
fn max_feed_bytes() -> usize {
    std::env::var("DDIR_MAX_FEED_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16 * 1024 * 1024)
}

struct PendingLoad {
    reqid: ReqId,
    id_hint: String,
    bindings: BTreeMap<String, String>,
    explain: Option<bool>,
    body: String,
    /// Set once the body exceeds the size gate; the rest of the body is
    /// swallowed (not stored) and `end-load` reports this error, so an
    /// oversized upload cannot make the parser misread body text as commands.
    poisoned: Option<String>,
}

struct PendingFeed {
    reqid: ReqId,
    prog: String,
    input: usize,
    /// The rows so far, read at the input's declared shapes; `None` for an input with none.
    rows: Option<FeedDecoder>,
    bytes: usize,
    /// Malformed and oversized bodies are swallowed through `end-feed`, then
    /// reported once; no partial command reaches the worker group.
    poisoned: Option<String>,
}

/// A `feed … frame <nbytes>` whose bytes are still to come.
struct PendingFrame {
    reqid: ReqId,
    prog: String,
    input: usize,
    nbytes: usize,
    /// The input's declared shapes, or why the frame will be refused: its bytes are read
    /// past (not kept) and the error reported in its place.
    shape: Result<RowShape, String>,
}

impl LineParser {
    pub fn new(catalog: Catalog) -> Self {
        LineParser {
            catalog,
            max_load_bytes: max_program_bytes(),
            max_feed_bytes: max_feed_bytes(),
            ..Self::default()
        }
    }

    /// Test hook: a parser with an explicit program-size cap.
    #[cfg(test)]
    fn with_cap(max_load_bytes: usize) -> Self {
        LineParser {
            max_load_bytes,
            max_feed_bytes: max_load_bytes,
            ..Self::default()
        }
    }

    /// Feed one input line; return `Some((reqid, parsed))` if the line
    /// completes a command (single-line or end of a multi-line body),
    /// else `None` to indicate more input is required. The caller pairs
    /// the result with a per-connection response sender to form a
    /// `Request`.
    pub fn feed(&mut self, line: &str) -> Option<(ReqId, Result<Cmd, String>)> {
        // A framed feed fixes its target once, then accepts compact rows until
        // `end-feed`. Parse and buffer the whole body on the session thread so
        // the worker group sees either one complete command or no command.
        if let Some(ref mut pending) = self.pending_feed {
            let trimmed = line.trim();
            let mut parts = trimmed.split_whitespace();
            let first = parts.next();
            let second = parts.next();
            let third = parts.next();
            let explicit_end = first == Some(pending.reqid.as_str())
                && second == Some("end-feed")
                && third.is_none();
            let bare_end = first == Some("end-feed") && second.is_none();
            if explicit_end || bare_end {
                let done = self.pending_feed.take().unwrap();
                if let Some(err) = done.poisoned {
                    return Some((done.reqid, Err(err)));
                }
                let columns = done.rows.expect("an unpoisoned feed has its input's shapes").finish();
                let (prog, input) = (done.prog, done.input);
                return Some((done.reqid, Ok(Cmd::FeedColumns { prog, input, columns })));
            }

            pending.bytes = pending.bytes.saturating_add(line.len() + 1);
            if pending.poisoned.is_none() && pending.bytes > self.max_feed_bytes {
                pending.poisoned = Some(format!(
                    "feed: body exceeds {} bytes (DDIR_MAX_FEED_BYTES)",
                    self.max_feed_bytes
                ));
                pending.rows = None;
            }
            if pending.poisoned.is_some() || trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let rows = pending.rows.as_mut().expect("an unpoisoned feed has its input's shapes");
            let row = rows.len() + 1;
            if let Err(error) = rows.push_line(trimmed) {
                pending.poisoned = Some(format!("feed row {row}: {error}"));
                pending.rows = None;
            }
            return None;
        }

        // Inside a pending load body: every line is literal program text
        // until `<reqid> end-load` or `<id_hint> end-load`. The id_hint
        // form is the friendly default when the load was auto-reqid'd
        // (so the user can type `gen end-load` after `load gen … begin`
        // without having to know the minted reqid).
        if let Some(ref mut pl) = self.pending_load {
            let trimmed = line.trim_end_matches(['\r', '\n']);
            let mut parts = trimmed.split_whitespace();
            if let (Some(tok0), Some(tok1), None) = (parts.next(), parts.next(), parts.next()) {
                if (tok0 == pl.reqid || tok0 == pl.id_hint) && tok1 == "end-load" {
                    let done = self.pending_load.take().unwrap();
                    if let Some(err) = done.poisoned {
                        return Some((done.reqid, Err(err)));
                    }
                    return Some((
                        done.reqid,
                        Ok(Cmd::Load {
                            id_hint: done.id_hint,
                            bindings: done.bindings,
                            program: done.body,
                            explain: done.explain,
                        }),
                    ));
                }
            }
            if pl.poisoned.is_none() {
                pl.body.push_str(trimmed);
                pl.body.push('\n');
                if pl.body.len() > self.max_load_bytes {
                    pl.poisoned = Some(format!(
                        "load: program body exceeds {} bytes (DDIR_MAX_PROGRAM_BYTES)",
                        self.max_load_bytes
                    ));
                    pl.body = String::new();
                }
            }
            return None;
        }

        let trimmed = line.trim();
        // Blank lines and `#` comments are skipped between commands. Body
        // handling above decides whether they are literal or ignorable.
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        let mut toks = trimmed.split_whitespace();
        let first = toks.next()?;
        // If the line starts with a known command, mint a reqid so the
        // user can type bare `list`, `tick 5`, `peek foo` without a
        // hand-rolled tag. The minted reqid is echoed in the response
        // so it can still be used with e.g. `stop <reqid>`.
        let (reqid, cmd) = if COMMAND_KEYWORDS.contains(&first) {
            self.auto_reqid_counter += 1;
            (format!("_{}", self.auto_reqid_counter), first)
        } else {
            // Normal `<reqid> <cmd> ...` form.
            let cmd = match toks.next() {
                Some(c) => c,
                None => return Some((first.to_string(), Err("missing command".into()))),
            };
            (first.to_string(), cmd)
        };
        let rest: Vec<&str> = toks.collect();
        match parse_cmd(cmd, &rest, &self.catalog) {
            ParseOutcome::Cmd(c) => Some((reqid, Ok(c))),
            ParseOutcome::Err(e) => Some((reqid, Err(e))),
            ParseOutcome::BeginLoad {
                id_hint,
                bindings,
                explain,
            } => {
                self.pending_load = Some(PendingLoad {
                    reqid,
                    id_hint,
                    bindings,
                    explain,
                    body: String::new(),
                    poisoned: None,
                });
                None
            }
            ParseOutcome::BeginFeed { prog, input } => {
                // A feed to an input with no declared shapes is read through to its end, then refused.
                let (rows, poisoned) = match self.catalog.decoder(&prog, input) {
                    Ok(rows) => (Some(rows), None),
                    Err(error) => (None, Some(format!("feed: {error}"))),
                };
                self.pending_feed = Some(PendingFeed { reqid, prog, input, rows, bytes: 0, poisoned });
                None
            }
            ParseOutcome::BeginFrame { prog, input, nbytes } => {
                let shape = if nbytes > self.max_feed_bytes {
                    Err(format!("feed: a frame of {nbytes} bytes exceeds {} bytes (DDIR_MAX_FEED_BYTES)", self.max_feed_bytes))
                } else {
                    input.clone().and_then(|input| self.catalog.shape(&prog, input).map_err(|error| format!("feed: {error}")))
                };
                let input = input.unwrap_or_default();
                self.pending_frame = Some(PendingFrame { reqid, prog, input, nbytes, shape });
                None
            }
        }
    }

    /// True if waiting for the terminator of a multi-line body. WS transport
    /// uses this to forward blank-line body content verbatim.
    pub fn awaiting_body(&self) -> bool {
        self.pending_load.is_some() || self.pending_feed.is_some()
    }

    /// The bytes a `feed … frame` header announced, which must be read next and handed to
    /// [`frame`](Self::frame), and whether they are wanted: a frame to be refused (too large, or
    /// for an input with no declared shapes) is read past, not kept.
    pub fn frame_bytes(&self) -> Option<(usize, bool)> {
        self.pending_frame.as_ref().map(|frame| (frame.nbytes, frame.shape.is_ok()))
    }

    /// Complete a pending `feed … frame` with its bytes (none, for one read past): the updates,
    /// checked against the input's declared shapes, as one command, or why they are refused.
    pub fn frame(&mut self, payload: &[u8]) -> Option<(ReqId, Result<Cmd, String>)> {
        let frame = self.pending_frame.take()?;
        let columns = frame.shape.and_then(|shape| {
            if payload.len() != frame.nbytes {
                return Err(format!("feed: a frame of {} bytes, but {} arrived", frame.nbytes, payload.len()));
            }
            interactive::runtime::frame::decode_feed(payload, &shape, self.max_feed_bytes as u64).map_err(|error| format!("feed: {error}"))
        });
        let (prog, input) = (frame.prog, frame.input);
        Some((frame.reqid, columns.map(|columns| Cmd::FeedColumns { prog, input, columns })))
    }
}

enum ParseOutcome {
    Cmd(Cmd),
    Err(String),
    BeginLoad {
        id_hint: String,
        bindings: BTreeMap<String, String>,
        explain: Option<bool>,
    },
    BeginFeed {
        prog: String,
        input: usize,
    },
    BeginFrame {
        prog: String,
        input: Result<usize, String>,
        nbytes: usize,
    },
}


/// `args` without a trailing `as corgi`, and whether it was there.
fn as_corgi<'a, 'b>(args: &'a [&'b str]) -> (&'a [&'b str], bool) {
    match args {
        [rest @ .., "as", "corgi"] => (rest, true),
        _ => (args, false),
    }
}

fn parse_cmd(cmd: &str, args: &[&str], catalog: &Catalog) -> ParseOutcome {
    match cmd {
        "load" => {
            // Two forms. `load <name> from <path> [options]` installs a program
            // file, read here on the session thread. `load <name> [options] begin`
            // switches the parser into
            // body-collection mode; subsequent lines are program text terminated by
            // `<reqid> end-load`. Options: `explain`, `explain=debug`, `name=binding`.
            const USAGE: &str = "load: expected `<name> from <path> [options]` or `<name> [options] begin`";
            let Some((id_hint, rest)) = args.split_first() else {
                return ParseOutcome::Err(USAGE.into());
            };
            let (path, options): (Option<&str>, &[&str]) = match rest {
                ["from", path, options @ ..] => (Some(path), options),
                [options @ .., "begin"] => (None, options),
                _ => return ParseOutcome::Err(USAGE.into()),
            };
            let mut bindings = BTreeMap::new();
            let mut explain = None;
            for tok in options {
                match *tok {
                    "explain" => { explain = Some(false); continue; }
                    "explain=debug" => { explain = Some(true); continue; }
                    _ => {}
                }
                let Some((k, v)) = tok.split_once('=') else {
                    return ParseOutcome::Err(format!(
                        "load: argument {:?} must be `explain`, `explain=debug`, or `name=binding`",
                        tok
                    ));
                };
                if bindings.insert(k.to_string(), v.to_string()).is_some() {
                    return ParseOutcome::Err(format!("load: duplicate binding for {:?}", k));
                }
            }
            let id_hint = id_hint.to_string();
            match path {
                // A file the server reads itself is not an upload, so the program-size
                // gate (an intake defense) does not apply to it.
                Some(path) => match std::fs::read_to_string(path) {
                    Ok(program) => ParseOutcome::Cmd(Cmd::Load {
                        id_hint,
                        bindings,
                        program,
                        explain,
                    }),
                    Err(error) => ParseOutcome::Err(format!("load: cannot read {path:?}: {error}")),
                },
                None => ParseOutcome::BeginLoad {
                    id_hint,
                    bindings,
                    explain,
                },
            }
        }
        "feed" => {
            if let [prog, input, "begin"] = args {
                let input = match input.parse() {
                    Ok(input) => input,
                    Err(_) => {
                        return ParseOutcome::Err(format!(
                            "feed: <in#> must be a number, got {input:?}"
                        ))
                    }
                };
                return ParseOutcome::BeginFeed {
                    prog: (*prog).to_string(),
                    input,
                };
            }
            // Syntax: `feed <prog> <in#> frame <nbytes>`, then exactly `nbytes` bytes: a binary frame.
            // A bad `<in#>` still has its bytes read past; a bad `<nbytes>` leaves none to find.
            if let [prog, input, "frame", nbytes] = args {
                let Ok(nbytes) = nbytes.parse() else {
                    return ParseOutcome::Err(format!("feed: <nbytes> must be a number, got {nbytes:?}"));
                };
                let input = input.parse().map_err(|_| format!("feed: <in#> must be a number, got {input:?}"));
                return ParseOutcome::BeginFrame { prog: (*prog).to_string(), input, nbytes };
            }
            // Syntax: `feed <prog> <in#> from <recipe-trace-or-file> [diff=<d>]` — the server
            // sources the rows.
            if let [prog, input, "from", source, rest @ ..] = args {
                let diff = match rest {
                    [] => Ok(1),
                    [arg] => arg
                        .strip_prefix("diff=")
                        .and_then(|d| d.parse().ok())
                        .ok_or_else(|| format!("feed: expected diff=<integer>, got {arg:?}")),
                    _ => Err("feed: expected `<prog> <in#> from <source> [diff=<d>]`".to_string()),
                };
                return match (input.parse(), diff) {
                    (Ok(input), Ok(diff)) => ParseOutcome::Cmd(Cmd::Source {
                        prog: (*prog).to_string(),
                        input,
                        source: (*source).to_string(),
                        diff,
                    }),
                    (Err(_), _) => ParseOutcome::Err(format!("feed: <in#> must be a number, got {input:?}")),
                    (_, Err(error)) => ParseOutcome::Err(error),
                };
            }
            // Syntax: `feed <prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]`
            // A `<v>`/`<key>` is a value of the input's declared shape, written
            // without spaces (see `interactive::runtime::feed`).
            if args.len() < 3 {
                return ParseOutcome::Err(
                    "feed: expected `<prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]`".into(),
                );
            }
            let prog = args[0].to_string();
            let input: usize = match args[1].parse() {
                Ok(n) => n,
                Err(_) => {
                    return ParseOutcome::Err(format!(
                        "feed: <in#> must be a number, got {:?}",
                        args[1]
                    ))
                }
            };
            let key = args[2];
            let mut val = None;
            let mut time = None;
            let mut diff: Diff = 1;
            for tok in &args[3..] {
                if let Some(v) = tok.strip_prefix("val=") {
                    val = Some(v);
                } else if let Some(t) = tok.strip_prefix("time=") {
                    time = match t.parse() {
                        Ok(t) => Some(t),
                        Err(_) => {
                            return ParseOutcome::Err(format!(
                                "feed: time= must be a number, got {:?}",
                                t
                            ))
                        }
                    };
                } else if let Some(d) = tok.strip_prefix("diff=") {
                    diff = match d.parse() {
                        Ok(d) => d,
                        Err(_) => {
                            return ParseOutcome::Err(format!(
                                "feed: diff= must be an integer, got {:?}",
                                d
                            ))
                        }
                    };
                } else {
                    return ParseOutcome::Err(format!("feed: unrecognized argument {:?}", tok));
                }
            }
            let mut row = match catalog.decoder(&prog, input) {
                Ok(row) => row,
                Err(e) => return ParseOutcome::Err(format!("feed: {e}")),
            };
            if let Err(e) = row.push(key, val, diff) {
                return ParseOutcome::Err(format!("feed: {e}"));
            }
            ParseOutcome::Cmd(Cmd::Feed { prog, input, columns: row.finish(), time })
        }
        "bind" | "unbind" => match args {
            [trace, prog, input] => match input.parse::<usize>() {
                Ok(input) => {
                    let (trace, prog) = ((*trace).to_string(), (*prog).to_string());
                    if cmd == "bind" {
                        ParseOutcome::Cmd(Cmd::Bind { trace, prog, input })
                    } else {
                        ParseOutcome::Cmd(Cmd::Unbind { trace, prog, input })
                    }
                }
                Err(_) => {
                    ParseOutcome::Err(format!("{}: <in#> must be a number, got {:?}", cmd, input))
                }
            },
            _ => ParseOutcome::Err(format!("{}: expected `<trace> <prog> <in#>`", cmd)),
        },
        "drop" => match args {
            [name] => ParseOutcome::Cmd(Cmd::Drop { name: (*name).to_string() }),
            _ => ParseOutcome::Err("drop: expected `<name>`".into()),
        },
        "list" => match args {
            [] => ParseOutcome::Cmd(Cmd::List),
            _ => ParseOutcome::Err("list: takes no arguments".into()),
        },
        "peek" => {
            let (args, columnar) = as_corgi(args);
            match args {
                [name] => ParseOutcome::Cmd(Cmd::Peek { name: (*name).to_string(), key: None, columnar }),
                [name, key] => ParseOutcome::Cmd(Cmd::Peek { name: (*name).to_string(), key: Some((*key).to_string()), columnar }),
                _ => ParseOutcome::Err("peek: expected `<name> [key] [as corgi]`".into()),
            }
        }
        "tail" => match as_corgi(args) {
            ([name], columnar) => ParseOutcome::Cmd(Cmd::Tail { name: (*name).to_string(), columnar }),
            _ => ParseOutcome::Err("tail: expected `<name> [as corgi]`".into()),
        },
        "stop" => match args {
            [rid] => ParseOutcome::Cmd(Cmd::Stop {
                tail_reqid: (*rid).to_string(),
            }),
            _ => ParseOutcome::Err("stop: expected `<tail-reqid>`".into()),
        },
        "tick" => match args {
            [] => ParseOutcome::Cmd(Cmd::Tick { n: 1 }),
            [n] => match n.parse::<u64>() {
                Ok(n) => ParseOutcome::Cmd(Cmd::Tick { n }),
                Err(_) => ParseOutcome::Err(format!("tick: bad count {:?}", n)),
            },
            _ => ParseOutcome::Err("tick: expected `[n]`".into()),
        },
        "exit" => ParseOutcome::Cmd(Cmd::Exit),
        other => ParseOutcome::Err(format!("unknown command {:?}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A parser that has prepared a load of `world`, whose inputs are fed below.
    fn world() -> LineParser {
        let program = r#"
            type T = A int | B int | C (int, int);
            export "pairs" = input 0 : ((int, int), ());
            export "both" = input 1 : ((int, int), (int, int));
            export "tagged" = input 2 : ((), T);
            export "ones" = input 3 : ((int), (int));
        "#;
        let catalog = Catalog::default();
        let load = Cmd::Load { id_hint: "world".into(), bindings: BTreeMap::new(), program: program.into(), explain: None };
        prepare(load, &catalog).expect("the program lowers");
        LineParser::new(catalog)
    }

    fn feed_all(p: &mut LineParser, lines: &[&str]) -> Vec<(ReqId, Result<Cmd, String>)> {
        let mut out = Vec::new();
        for l in lines {
            if let Some(r) = p.feed(l) {
                out.push(r);
            }
        }
        out
    }

    /// A `feed … frame` header and its bytes are one command: the columns, checked against the
    /// input's declared shapes. A frame of other shapes, cut short, too large, or for an input with
    /// no shapes is refused, and the bytes of the last two are read past, not kept.
    #[test]
    fn framed_feeds_are_columns_at_the_input_shapes() {
        let mut p = world();
        let mut rows = p.catalog.decoder("world", 0).unwrap();
        rows.push("tuple(1,2)", None, 1).unwrap();
        rows.push("tuple(3,-4)", None, 2).unwrap();
        let columns = rows.finish();
        let bytes = interactive::runtime::frame::encode(&columns, &[]);
        assert!(p.feed(&format!("r1 feed world 0 frame {}", bytes.len())).is_none());
        assert_eq!(p.frame_bytes(), Some((bytes.len(), true)));
        match p.frame(&bytes) {
            Some((reqid, Ok(Cmd::FeedColumns { prog, input, columns: got }))) => {
                assert_eq!((reqid.as_str(), prog.as_str(), input), ("r1", "world", 0));
                assert_eq!(got, columns);
            }
            other => panic!("unexpected parse: {other:?}"),
        }
        assert!(p.frame_bytes().is_none(), "the frame is complete");
        assert!(p.feed(&format!("r2 feed world 1 frame {}", bytes.len())).is_none());
        assert!(matches!(p.frame(&bytes), Some((_, Err(e))) if e.contains("shape")));
        assert!(p.feed(&format!("r3 feed world 0 frame {}", bytes.len())).is_none());
        assert!(matches!(p.frame(&bytes[..bytes.len() - 8]), Some((_, Err(_)))), "cut short");
        assert!(p.feed("r4 feed world 0 frame 999999999999").is_none());
        assert_eq!(p.frame_bytes(), Some((999999999999, false)));
        assert!(matches!(p.frame(&[]), Some((_, Err(e))) if e.contains("exceeds")));
        assert!(p.feed("r5 feed world 9 frame 64").is_none());
        assert_eq!(p.frame_bytes(), Some((64, false)));
        assert!(matches!(p.frame(&[]), Some((_, Err(_)))));
        assert!(p.feed("r6 feed world x frame 64").is_none(), "a bad input index still reads past its bytes");
        assert_eq!(p.frame_bytes(), Some((64, false)));
        assert!(matches!(p.frame(&[]), Some((_, Err(_)))));
        assert!(matches!(feed_all(&mut p, &["r7 feed world 0 frame x"])[..], [(_, Err(_))]));
    }

    #[test]
    fn feed_from_sources_rows_server_side() {
        let mut p = LineParser::new(Catalog::default());
        let out = feed_all(&mut p, &["r1 feed world 0 from iota:3\n"]);
        match &out[..] {
            [(reqid, Ok(Cmd::Source { prog, input, source, diff: 1 }))] => {
                assert_eq!(reqid.as_str(), "r1");
                assert_eq!((prog.as_str(), *input, source.as_str()), ("world", 0, "iota:3"));
            }
            other => panic!("unexpected parse: {other:?}"),
        }
        let out = feed_all(&mut p, &["r3 feed bank 0 from retire diff=-1\n"]);
        assert!(
            matches!(&out[..], [(_, Ok(Cmd::Source { source, diff: -1, .. }))] if source == "retire"),
            "a trace feed carries its scale: {out:?}"
        );
        let out = feed_all(&mut p, &["r4 feed bank 0 from retire diff=nope\n"]);
        assert!(matches!(&out[..], [(_, Err(_))]), "a bad diff must be rejected: {out:?}");
        let out = feed_all(&mut p, &["r2 feed world x from iota:3\n"]);
        assert!(matches!(&out[..], [(_, Err(_))]), "a bad input index must be rejected: {out:?}");
    }

    #[test]
    fn simple_commands() {
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(
            &mut p,
            &["r0 list", "r1 tick 5", "r2 drop 3", "r3 peek foo"],
        );
        assert_eq!(got.len(), 4);
        assert!(matches!(got[0].1, Ok(Cmd::List)));
        assert!(matches!(got[1].1, Ok(Cmd::Tick { n: 5 })));
        assert!(matches!(got[2].1, Ok(Cmd::Drop { ref name }) if name == "3"));
        assert!(matches!(got[3].1, Ok(Cmd::Peek { ref name, key: None, columnar: false }) if name == "foo"));
    }

    #[test]
    fn multiline_load() {
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(
            &mut p,
            &[
                "r0 load gen edges=random(seed=1) begin",
                "let edges = import \"edges/v1\";",
                "export \"reach\" = edges;",
                "r0 end-load",
            ],
        );
        assert_eq!(got.len(), 1);
        match &got[0].1 {
            Ok(Cmd::Load {
                id_hint,
                bindings,
                program,
                explain,
            }) => {
                assert_eq!(id_hint, "gen");
                assert_eq!(
                    bindings.get("edges").map(String::as_str),
                    Some("random(seed=1)")
                );
                assert!(program.contains("import \"edges/v1\""));
                assert!(program.contains("export \"reach\""));
                assert!(explain.is_none());
            }
            _ => panic!("expected Load, got {:?}", got[0].1),
        }
    }

    #[test]
    fn load_is_lowered_before_worker_admission() {
        let mut p = LineParser::new(Catalog::default());
        assert!(p.feed("r0 load world begin").is_none());
        assert!(p.feed("let rows = input 0;").is_none());
        assert!(p.feed("export \"rows\" = rows;").is_none());
        let (_, parsed) = p.feed("r0 end-load").expect("load is complete");
        let prepared = prepare(parsed.expect("protocol parse succeeds"), &Catalog::default())
            .expect("DDIR parsing and lowering succeed");
        match prepared {
            PreparedCommand::Server(ServerCommand::Install { name, program }) => {
                assert_eq!(name, "world");
                assert_eq!(program.root.exports[0].name, "rows");
            }
            other => panic!("expected prepared install, got {other:?}"),
        }
    }

    #[test]
    fn load_from_file_reads_the_program_here() {
        let dir = std::env::temp_dir().join(format!("ddir-load-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ddp = dir.join("p.ddp");
        std::fs::write(&ddp, "export \"r\" = input 0;\n").unwrap();
        let ddp2 = dir.join("q.ddp");
        std::fs::write(&ddp2, "export \"s\" = input 0;\n").unwrap();
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(&mut p, &[
            &format!("r1 load p from {} explain=debug e=random(seed=1,arity=2,range=8,count=12)\n", ddp.display()),
            &format!("r2 load q from {}\n", ddp2.display()),
            "r3 load p from /nonexistent/p.ddp\n",
            "r4 load p --explain begin\n",
            &format!("r5 load p from {} explain\n", ddp.display()),
        ]);
        match &got[0].1 {
            Ok(Cmd::Load { id_hint, bindings, program, explain }) => {
                assert_eq!(id_hint, "p");
                assert_eq!(program, "export \"r\" = input 0;\n");
                assert_eq!(*explain, Some(true));
                assert_eq!(bindings.get("e").map(String::as_str), Some("random(seed=1,arity=2,range=8,count=12)"));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(&got[1].1, Ok(Cmd::Load { explain: None, .. })));
        assert!(matches!(&got[2].1, Err(e) if e.starts_with("load: cannot read")));
        assert!(matches!(&got[3].1, Err(e) if e.contains("`explain`")));
        assert!(matches!(&got[4].1, Ok(Cmd::Load { explain: Some(false), .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn peek_and_tail_take_a_key_and_as_corgi() {
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(&mut p, &[
            "r1 peek t 2,3\n", "r2 peek t\n", "r3 peek t 2,3 as corgi\n", "r4 peek t as corgi\n", "r5 peek t 1 2\n",
            "r6 tail t\n", "r7 tail t as corgi\n", "r8 tail t as rows\n",
        ]);
        let peek = |i: usize| match &got[i].1 { Ok(Cmd::Peek { key, columnar, .. }) => Some((key.clone(), *columnar)), _ => None };
        assert_eq!(peek(0), Some((Some("2,3".into()), false)));
        assert_eq!(peek(1), Some((None, false)));
        assert_eq!(peek(2), Some((Some("2,3".into()), true)));
        assert_eq!(peek(3), Some((None, true)));
        assert!(got[4].1.is_err());
        assert!(matches!(&got[5].1, Ok(Cmd::Tail { columnar: false, .. })));
        assert!(matches!(&got[6].1, Ok(Cmd::Tail { columnar: true, .. })));
        assert!(got[7].1.is_err());
    }

    #[test]
    fn auto_reqid_for_bare_command() {
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(&mut p, &["list", "tick 3", "rA peek foo", "exit"]);
        assert_eq!(got.len(), 4);
        // Bare commands get auto-minted reqids; mixed-in explicit reqids
        // pass through unchanged.
        assert_eq!(got[0].0, "_1"); // list
        assert!(matches!(got[0].1, Ok(Cmd::List)));
        assert_eq!(got[1].0, "_2"); // tick 3
        assert!(matches!(got[1].1, Ok(Cmd::Tick { n: 3 })));
        assert_eq!(got[2].0, "rA"); // explicit reqid preserved
        assert!(matches!(got[2].1, Ok(Cmd::Peek { ref name, key: None, columnar: false }) if name == "foo"));
        assert_eq!(got[3].0, "_3"); // exit
        assert!(matches!(got[3].1, Ok(Cmd::Exit)));
    }

    #[test]
    fn feed_cmd() {
        let mut p = world();
        let got = feed_all(
            &mut p,
            &[
                "r0 feed world 0 1,2",
                "r1 feed world 1 3,4 val=7,8 time=5 diff=-1",
                "r2 feed world 2 _ val=inject(2,tuple(3,4))",
                "r3 feed world zero 1,2",     // bad input index
                "r4 feed world 0 1,2 wat=7",  // unknown argument
                "r5 feed world 0 tuple(1,",   // malformed value
                "r6 feed world 0 1,2 val=3",  // a value where the input has unit
                "r7 feed world 9 1,2",        // an input the program does not have
                "r8 feed nowhere 0 1,2",      // a program never loaded
                "r9 feed world 0 tuple(1,2)",
            ],
        );
        assert_eq!(got.len(), 10);
        let Ok(Cmd::Feed { columns: wrapped, .. }) = &got[9].1 else { panic!("expected Feed, got {:?}", got[9].1) };
        match &got[0].1 {
            Ok(Cmd::Feed { prog, input, columns, time }) => {
                assert_eq!((prog.as_str(), *input, *time), ("world", 0, None));
                assert_eq!((&columns.keys, &columns.diffs), (&wrapped.keys, &vec![1]));
            }
            other => panic!("expected Feed, got {:?}", other),
        }
        match &got[1].1 {
            Ok(Cmd::Feed { time, columns, .. }) => {
                assert_eq!(*time, Some(5));
                assert_eq!(columns.diffs, vec![-1]);
            }
            other => panic!("expected Feed, got {:?}", other),
        }
        assert!(matches!(&got[2].1, Ok(Cmd::Feed { columns, .. }) if columns.len() == 1));
        assert!(got[3].1.is_err());
        assert!(got[4].1.is_err());
        assert!(got[5].1.as_ref().is_err_and(|e| e.starts_with("feed: key \"tuple(1,\"")));
        assert!(got[6].1.as_ref().is_err_and(|e| e.starts_with("feed: val \"3\"")));
        assert_eq!(got[7].1.as_ref().unwrap_err(), r#"feed: program "world" has no input 9"#);
        assert_eq!(got[8].1.as_ref().unwrap_err(), r#"feed: no program "nowhere""#);
    }

    #[test]
    fn framed_feed_becomes_one_typed_command() {
        let mut p = world();
        assert!(p.feed("r0 feed world 3 begin").is_none());
        assert!(p.awaiting_body());
        assert!(p.feed("# rows use the enclosing command's epoch").is_none());
        assert!(p.feed("1 val=10").is_none());
        assert!(p.feed("2 val=-20 diff=-1").is_none());
        let (reqid, parsed) = p.feed("r0 end-feed").expect("feed is complete");
        assert_eq!(reqid, "r0");
        assert!(!p.awaiting_body());

        let prepared = prepare(parsed.expect("protocol parse succeeds"), &Catalog::default()).unwrap();
        let PreparedCommand::Server(ServerCommand::FeedColumns {
            prog,
            input,
            columns,
        }) = prepared
        else {
            panic!("expected prepared columnar feed, got {prepared:?}");
        };
        assert_eq!((prog.as_str(), input), ("world", 3));
        assert_eq!(columns.diffs, vec![1, -1]);
        // The same rows, written out in full.
        let wrapped = feed_all(&mut p, &["r1 feed world 3 begin", "tuple(1) val=tuple(10)", "tuple(2) val=tuple(-20) diff=-1", "end-feed"]);
        let [(_, Ok(Cmd::FeedColumns { columns: wrapped, .. }))] = &wrapped[..] else { panic!("expected a feed, got {wrapped:?}") };
        assert_eq!((&columns.keys, &columns.vals), (&wrapped.keys, &wrapped.vals));
    }

    /// A row that is not of the input's shapes refuses the whole feed, naming the row.
    #[test]
    fn framed_feed_refuses_a_misfit_row() {
        let mut p = world();
        let got = feed_all(&mut p, &["r0 feed world 3 begin", "1 val=10", "2 val=inject(0,3)", "3 val=30", "r0 end-feed"]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1.as_ref().unwrap_err(), r#"feed row 2: val "inject(0,3)": expected `tuple(` at byte 0, found `inject(0,3)`"#);
        let elsewhere = feed_all(&mut p, &["r1 feed elsewhere 0 begin", "1 val=10", "end-feed"]);
        assert_eq!(elsewhere[0].1.as_ref().unwrap_err(), r#"feed: no program "elsewhere""#);
    }

    #[test]
    fn malformed_framed_feed_is_rejected_atomically() {
        let mut p = world();
        assert!(p.feed("r0 feed world 3 begin").is_none());
        assert!(p.feed("1 val=10").is_none());
        assert!(p.feed("2 time=3").is_none());
        // Once poisoned, otherwise command-looking lines are body and cannot
        // leak a partial batch or desynchronize the protocol.
        assert!(p.feed("r1 list").is_none());
        let (reqid, result) = p.feed("end-feed").expect("bare terminator works");
        assert_eq!(reqid, "r0");
        assert!(result
            .as_ref()
            .is_err_and(|error| error.contains("time= is not allowed")));
        assert!(matches!(p.feed("r2 list"), Some((_, Ok(Cmd::List)))));
    }

    #[test]
    fn oversized_framed_feed_is_rejected_cleanly() {
        let mut p = LineParser { max_feed_bytes: 32, ..world() };
        assert!(p.feed("r0 feed world 3 begin").is_none());
        for key in 0..16 {
            assert!(p.feed(&format!("{key} val={key}")).is_none());
        }
        let (reqid, result) = p.feed("r0 end-feed").expect("feed is complete");
        assert_eq!(reqid, "r0");
        assert!(result
            .as_ref()
            .is_err_and(|error| error.contains("exceeds 32 bytes")));
        assert!(matches!(p.feed("r1 list"), Some((_, Ok(Cmd::List)))));
    }

    #[test]
    fn bind_cmd() {
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(
            &mut p,
            &["r0 bind next counter 1", "r1 unbind next counter 1", "r2 bind next counter one"],
        );
        assert_eq!(got.len(), 3);
        assert!(matches!(
            &got[0].1,
            Ok(Cmd::Bind { trace, prog, input: 1 }) if trace == "next" && prog == "counter"
        ));
        assert!(matches!(&got[1].1, Ok(Cmd::Unbind { input: 1, .. })));
        assert!(got[2].1.is_err());
    }

    #[test]
    fn oversized_load_is_rejected_cleanly() {
        let mut p = LineParser::with_cap(64);
        assert!(p.feed("r0 load big begin").is_none());
        // Push well past the cap; every body line is swallowed, none parse
        // as commands, and the terminator reports one clean error.
        for _ in 0..16 {
            assert!(p.feed("let x = input 0; -- padding padding padding").is_none());
        }
        let got = p.feed("r0 end-load").expect("end-load completes the upload");
        assert_eq!(got.0, "r0");
        assert!(got.1.as_ref().is_err_and(|e| e.contains("exceeds 64 bytes")));
        // The parser is usable again afterwards.
        assert!(matches!(p.feed("r1 list"), Some((_, Ok(Cmd::List)))));
    }

    #[test]
    fn drop_by_id_or_name() {
        let mut p = LineParser::new(Catalog::default());
        let got = feed_all(
            &mut p,
            &[
                "r0 drop 3",
                "r1 drop my_reach",
                "r2 drop", // missing arg → err
            ],
        );
        assert_eq!(got.len(), 3);
        assert!(matches!(got[0].1, Ok(Cmd::Drop { ref name }) if name == "3"));
        assert!(matches!(got[1].1, Ok(Cmd::Drop { ref name }) if name == "my_reach"));
        assert!(matches!(got[2].1, Err(_)));
    }
}
