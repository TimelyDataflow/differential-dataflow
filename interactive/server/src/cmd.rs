//! Command and response shapes for the line-oriented protocol.
//!
//! Each request line is `<reqid> <cmd> [args...]`.
//! Each response line starts with the same `<reqid>` followed by one of:
//!   - `ok [body...]` — terminal success line
//!   - `err [body...]` — terminal error line
//!   - `data <fields...>` — one streamed body line (peek/tail batches)
//!   - `end` — terminator after a stream of `data` lines
//!
//! Multi-line bodies use two-phase framing: `load ... begin` accepts literal
//! DDIR through `end-load`, while `feed <prog> <in#> begin` accepts row updates
//! through `end-feed` and becomes one non-interleavable server command.

use std::any::{type_name_of_val, Any};
use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};

use interactive::ir::{eval, Diff, Value};
use interactive::scope_ir::{Program, Source};
use interactive::server::{Command as ServerCommand, InputUpdate, OuterTime};

pub type ReqId = String;

/// One client session's outbound stream. Cloned into each `Request` so
/// dispatch can route responses back to the originating client (and so
/// long-lived subscriptions like `tail` capture the right sender).
pub type RespSender = std::sync::mpsc::Sender<String>;

/// Per-session identity. Lets the worker tear down long-lived
/// subscriptions when a connection disappears without an explicit stop.
pub type ConnectionId = u64;

#[derive(Debug)]
pub enum Cmd {
    /// Install a dataflow.
    /// `id_hint` — a client-chosen name; the server may keep it or assign
    /// a fresh id (echo'd in the response).
    /// `bindings` — `import-name -> binding`, where the binding is either a
    /// registered trace name or a builtin call (`random(...)`).
    /// `program` — DDIR text.
    /// `explain` — request the explain rewrite (reserved; the server
    /// reports an error rather than improvising a meaning).
    Load {
        id_hint: String,
        bindings: BTreeMap<String, String>,
        program: String,
        explain: bool,
    },
    /// Drop the dataflow named by id or by `id_hint`. Fails if any
    /// export of this dataflow is still imported by another live
    /// dataflow or held by a reader.
    Drop { target: DataflowRef },
    /// List held names.
    List,
    /// One-shot snapshot of a named trace.
    Peek { name: String },
    /// Persistent subscription to a named trace.
    Tail { name: String },
    /// Cancel a previous `tail` (matched by its reqid).
    Stop { tail_reqid: ReqId },
    /// Update positional `input` of `prog`: add `(key, val)` with `diff` at
    /// `time` (default the current epoch). Identity and ordering of writes
    /// are convention, not enforcement: cooperating clients include their
    /// session name and an ordering id in the data, and programs resolve
    /// contention over those facts (see demo/claims.txt, demo/txn.txt).
    Feed {
        prog: String,
        input: usize,
        key: Value,
        val: Value,
        time: Option<OuterTime>,
        diff: Diff,
    },
    /// Atomically stage many rows into one program input at the current epoch.
    FeedBatch {
        prog: String,
        input: usize,
        updates: Vec<InputUpdate>,
    },
    /// Fill one program input from a source the server reads itself — a recipe
    /// (`random:…`, `iota:N`) or a file of integer rows — each worker taking its
    /// shard, so no row crosses the wire. `feed <prog> <in#> from <source>`.
    Source {
        prog: String,
        input: usize,
        source: String,
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
    /// Push a row into the query input of a `--explain` dataflow
    /// (reserved; unimplemented). Sign is `+1` for `add`, `-1` for `del`.
    #[allow(dead_code)]
    Query {
        target: DataflowRef,
        kind: QueryKind,
        key: Vec<i64>,
        val: Vec<i64>,
    },
    /// Advance ambient time by `n` (default 1).
    Tick { n: u64 },
    /// End the session.
    Exit,
}

#[derive(Debug, Clone, Copy)]
pub enum QueryKind {
    Add,
    Del,
}

/// A reference to a registered dataflow, used by both `drop` and
/// `query`. Either a numeric dataflow id or a name (the load's
/// `id_hint`). Parsed by reading the token as a `u64` first, then
/// falling back to a string name. So `drop 5` and `drop my_reach`
/// both work, and `drop 5_alt` (which fails to parse as u64) falls
/// through to the name lookup.
#[derive(Debug, Clone)]
pub enum DataflowRef {
    Id(u64),
    Name(String),
}

/// A command ready to broadcast to the worker group. Protocol-only tail
/// lifecycle wraps the typed server command vocabulary rather than leaking
/// response channels into the dataflow.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PreparedCommand {
    Server(ServerCommand),
    Tail { name: String },
    Stop { tail_reqid: ReqId },
}

/// Parse and lower the expensive parts of a protocol command on its session
/// thread. Every worker receives the same typed program, and malformed DDIR
/// never reaches a Timely worker.
pub fn prepare(command: Cmd) -> Result<PreparedCommand, String> {
    let command = match command {
        Cmd::Load {
            id_hint,
            bindings,
            program,
            explain,
        } => {
            if explain {
                return Err(
                    "load --explain is reserved; explanation is not implemented here yet".into(),
                );
            }
            let mut program = catch_unwind(AssertUnwindSafe(|| {
                let statements = interactive::parse::pipe::parse(&program);
                interactive::lower::lower_tree(statements)
            }))
            .map_err(panic_message)?;
            apply_bindings(&mut program, &bindings)?;
            program.optimize();
            ServerCommand::Install {
                name: id_hint,
                program,
            }
        }
        Cmd::Drop { target } => ServerCommand::Drop {
            name: match target {
                DataflowRef::Name(name) => name,
                DataflowRef::Id(id) => {
                    return Err(format!(
                        "numeric dataflow id {} is no longer exposed; use its name",
                        id
                    ))
                }
            },
        },
        Cmd::List => ServerCommand::List,
        Cmd::Peek { name } => ServerCommand::Peek {
            trace: name,
            key: None,
        },
        Cmd::Tail { name } => return Ok(PreparedCommand::Tail { name }),
        Cmd::Stop { tail_reqid } => return Ok(PreparedCommand::Stop { tail_reqid }),
        Cmd::Feed {
            prog,
            input,
            key,
            val,
            time,
            diff,
        } => ServerCommand::Feed {
            prog,
            input,
            key,
            val,
            time,
            diff,
        },
        Cmd::FeedBatch {
            prog,
            input,
            updates,
        } => ServerCommand::FeedBatch {
            prog,
            input,
            updates,
        },
        Cmd::Source { prog, input, source } => ServerCommand::Load { prog, input, source },
        Cmd::Bind { trace, prog, input } => ServerCommand::Bind { trace, prog, input },
        Cmd::Unbind { trace, prog, input } => ServerCommand::Unbind { trace, prog, input },
        Cmd::Query { .. } => {
            return Err("query is reserved for --explain dataflows and is not implemented".into())
        }
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
/// feed body together. The parser hands back either a complete `Request` or
/// `None` (more lines required).
#[derive(Default)]
pub struct LineParser {
    pending_load: Option<PendingLoad>,
    pending_feed: Option<PendingFeed>,
    auto_reqid_counter: u64,
    max_load_bytes: usize,
    max_feed_bytes: usize,
}

/// Tokens that introduce a command. If a line begins with one of these
/// instead of an explicit reqid, the parser synthesizes a reqid.
const COMMAND_KEYWORDS: &[&str] = &[
    "load", "drop", "list", "peek", "tail", "stop", "tick", "query", "exit", "feed", "bind",
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
    explain: bool,
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
    updates: Vec<InputUpdate>,
    bytes: usize,
    /// Malformed and oversized bodies are swallowed through `end-feed`, then
    /// reported once; no partial command reaches the worker group.
    poisoned: Option<String>,
}

impl LineParser {
    pub fn new() -> Self {
        LineParser {
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
                return Some((
                    done.reqid,
                    Ok(Cmd::FeedBatch {
                        prog: done.prog,
                        input: done.input,
                        updates: done.updates,
                    }),
                ));
            }

            pending.bytes = pending.bytes.saturating_add(line.len() + 1);
            if pending.poisoned.is_none() && pending.bytes > self.max_feed_bytes {
                pending.poisoned = Some(format!(
                    "feed: body exceeds {} bytes (DDIR_MAX_FEED_BYTES)",
                    self.max_feed_bytes
                ));
                pending.updates.clear();
            }
            if pending.poisoned.is_some() || trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            match parse_input_update(trimmed) {
                Ok(update) => pending.updates.push(update),
                Err(error) => {
                    pending.poisoned = Some(format!("feed row: {error}"));
                    pending.updates.clear();
                }
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
        match parse_cmd(cmd, &rest) {
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
                self.pending_feed = Some(PendingFeed {
                    reqid,
                    prog,
                    input,
                    updates: Vec::new(),
                    bytes: 0,
                    poisoned: None,
                });
                None
            }
        }
    }

    /// True if waiting for the terminator of a multi-line body. WS transport
    /// uses this to forward blank-line body content verbatim.
    pub fn awaiting_body(&self) -> bool {
        self.pending_load.is_some() || self.pending_feed.is_some()
    }
}

enum ParseOutcome {
    Cmd(Cmd),
    Err(String),
    BeginLoad {
        id_hint: String,
        bindings: BTreeMap<String, String>,
        explain: bool,
    },
    BeginFeed {
        prog: String,
        input: usize,
    },
}

/// A `<value>` for `feed`: a comma-separated integer row → `Tuple`, `_` or
/// empty → unit, else a closed scalar term (one whitespace-free token, e.g.
/// `inject(2,tuple(3,4))`) evaluated to a constant. Term-parser panics are
/// caught here on the session thread and become clean errors — malformed
/// input never reaches the worker.
fn parse_value(s: &str) -> Result<Value, String> {
    let s = s.trim();
    if s.is_empty() || s == "_" {
        return Ok(Value::unit());
    }
    if s.chars()
        .all(|c| c.is_ascii_digit() || c == ',' || c == '-')
    {
        if let Ok(ints) = s
            .split(',')
            .map(|t| t.trim().parse::<i64>())
            .collect::<Result<Vec<_>, _>>()
        {
            return Ok(Value::Tuple(ints.into_iter().map(Value::Int).collect()));
        }
    }
    catch_unwind(AssertUnwindSafe(|| {
        eval(&interactive::parse::pipe::parse_term(s), &mut Vec::new())
    }))
    .map_err(|panic| {
        if let Some(msg) = panic.downcast_ref::<&str>() {
            (*msg).to_string()
        } else if let Some(msg) = panic.downcast_ref::<String>() {
            msg.clone()
        } else {
            format!("malformed value {:?}", s)
        }
    })
}

/// Parse one row in a framed feed. Its target and timestamp belong to the
/// enclosing command, which keeps large requests compact and atomic.
fn parse_input_update(line: &str) -> Result<InputUpdate, String> {
    let mut args = line.split_whitespace();
    let Some(key) = args.next() else {
        return Err("expected `<key> [val=<v>] [diff=<d>]`".into());
    };
    let key = parse_value(key).map_err(|error| format!("key: {error}"))?;
    let mut val = Value::unit();
    let mut diff: Diff = 1;
    for tok in args {
        if let Some(value) = tok.strip_prefix("val=") {
            val = parse_value(value).map_err(|error| format!("val: {error}"))?;
        } else if let Some(value) = tok.strip_prefix("diff=") {
            diff = value
                .parse()
                .map_err(|_| format!("diff= must be an integer, got {value:?}"))?;
        } else if tok.starts_with("time=") {
            return Err("time= is not allowed; a framed feed uses the current epoch".into());
        } else {
            return Err(format!("unrecognized argument {tok:?}"));
        }
    }
    Ok(InputUpdate { key, val, diff })
}

fn parse_cmd(cmd: &str, args: &[&str]) -> ParseOutcome {
    match cmd {
        "load" => {
            // Syntax: `load <id-hint> [--explain] [name=binding ...] begin`
            // The trailing `begin` switches the parser into body-collection
            // mode; subsequent input lines are program text terminated by
            // `<reqid> end-load`.
            if args.is_empty() {
                return ParseOutcome::Err(
                    "load: expected `<id-hint> [--explain] [name=binding ...] begin`".into(),
                );
            }
            if args.last() != Some(&"begin") {
                return ParseOutcome::Err(
                    "load: must end with `begin` (multi-line body required)".into(),
                );
            }
            let id_hint = args[0].to_string();
            let middle = &args[1..args.len() - 1];
            let mut bindings = BTreeMap::new();
            let mut explain = false;
            for tok in middle {
                if *tok == "--explain" {
                    explain = true;
                    continue;
                }
                let Some((k, v)) = tok.split_once('=') else {
                    return ParseOutcome::Err(format!(
                        "load: argument {:?} must be `--explain` or `name=binding`",
                        tok
                    ));
                };
                if bindings.insert(k.to_string(), v.to_string()).is_some() {
                    return ParseOutcome::Err(format!("load: duplicate binding for {:?}", k));
                }
            }
            ParseOutcome::BeginLoad {
                id_hint,
                bindings,
                explain,
            }
        }
        "query" => {
            // Syntax: `query <df-id-or-name> add|del <k-fields> ; <v-fields>`
            // Where k/v-fields are comma-separated i64. Empty side allowed
            // (write nothing before/after the `;`).
            if args.len() < 3 {
                return ParseOutcome::Err(
                    "query: expected `<df-id-or-name> add|del <k1,k2,..> ; <v1,v2,..>`".into(),
                );
            }
            let target = match args[0].parse::<u64>() {
                Ok(n) => DataflowRef::Id(n),
                Err(_) => DataflowRef::Name(args[0].to_string()),
            };
            let kind = match args[1] {
                "add" => QueryKind::Add,
                "del" => QueryKind::Del,
                other => {
                    return ParseOutcome::Err(format!(
                        "query: kind must be add|del, got {:?}",
                        other
                    ))
                }
            };
            // Find the `;` separator among the remaining tokens.
            let rest = &args[2..];
            let sep = rest.iter().position(|t| *t == ";");
            let (k_toks, v_toks): (&[&str], &[&str]) = match sep {
                Some(i) => (&rest[..i], &rest[i + 1..]),
                None => (rest, &[]),
            };
            fn parse_fields(toks: &[&str]) -> Result<Vec<i64>, String> {
                let mut out = Vec::new();
                for t in toks {
                    for piece in t.split(',') {
                        if piece.is_empty() {
                            continue;
                        }
                        out.push(piece.parse().map_err(|_| format!("bad i64 {:?}", piece))?);
                    }
                }
                Ok(out)
            }
            let key = match parse_fields(k_toks) {
                Ok(v) => v,
                Err(e) => return ParseOutcome::Err(format!("query key: {}", e)),
            };
            let val = match parse_fields(v_toks) {
                Ok(v) => v,
                Err(e) => return ParseOutcome::Err(format!("query val: {}", e)),
            };
            ParseOutcome::Cmd(Cmd::Query {
                target,
                kind,
                key,
                val,
            })
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
            // Syntax: `feed <prog> <in#> from <recipe-or-file>` — the server sources the rows.
            if let [prog, input, "from", source] = args {
                return match input.parse() {
                    Ok(input) => ParseOutcome::Cmd(Cmd::Source {
                        prog: (*prog).to_string(),
                        input,
                        source: (*source).to_string(),
                    }),
                    Err(_) => ParseOutcome::Err(format!("feed: <in#> must be a number, got {input:?}")),
                };
            }
            // Syntax: `feed <prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]`
            // A `<v>`/`<key>` is a comma-separated integer row (`1,2` → tuple;
            // `_`/empty → unit) or a closed scalar term written without
            // spaces (`inject(2,tuple(3,4))`), as in the ddir_server example.
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
            let key = match parse_value(args[2]) {
                Ok(v) => v,
                Err(e) => return ParseOutcome::Err(format!("feed key: {}", e)),
            };
            let mut val = Value::unit();
            let mut time = None;
            let mut diff: Diff = 1;
            for tok in &args[3..] {
                if let Some(v) = tok.strip_prefix("val=") {
                    val = match parse_value(v) {
                        Ok(v) => v,
                        Err(e) => return ParseOutcome::Err(format!("feed val: {}", e)),
                    };
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
            ParseOutcome::Cmd(Cmd::Feed {
                prog,
                input,
                key,
                val,
                time,
                diff,
            })
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
            [tok] => {
                let target = match tok.parse::<u64>() {
                    Ok(n) => DataflowRef::Id(n),
                    Err(_) => DataflowRef::Name((*tok).to_string()),
                };
                ParseOutcome::Cmd(Cmd::Drop { target })
            }
            _ => ParseOutcome::Err("drop: expected `<dataflow-id-or-name>`".into()),
        },
        "list" => match args {
            [] => ParseOutcome::Cmd(Cmd::List),
            _ => ParseOutcome::Err("list: takes no arguments".into()),
        },
        "peek" => match args {
            [name] => ParseOutcome::Cmd(Cmd::Peek {
                name: (*name).to_string(),
            }),
            _ => ParseOutcome::Err("peek: expected `<name>`".into()),
        },
        "tail" => match args {
            [name] => ParseOutcome::Cmd(Cmd::Tail {
                name: (*name).to_string(),
            }),
            _ => ParseOutcome::Err("tail: expected `<name>`".into()),
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

    fn feed_all(p: &mut LineParser, lines: &[&str]) -> Vec<(ReqId, Result<Cmd, String>)> {
        let mut out = Vec::new();
        for l in lines {
            if let Some(r) = p.feed(l) {
                out.push(r);
            }
        }
        out
    }

    #[test]
    fn feed_from_sources_rows_server_side() {
        let mut p = LineParser::default();
        let out = feed_all(&mut p, &["r1 feed world 0 from iota:3\n"]);
        match &out[..] {
            [(reqid, Ok(Cmd::Source { prog, input, source }))] => {
                assert_eq!(reqid.as_str(), "r1");
                assert_eq!((prog.as_str(), *input, source.as_str()), ("world", 0, "iota:3"));
            }
            other => panic!("unexpected parse: {other:?}"),
        }
        let out = feed_all(&mut p, &["r2 feed world x from iota:3\n"]);
        assert!(matches!(&out[..], [(_, Err(_))]), "a bad input index must be rejected: {out:?}");
    }

    #[test]
    fn simple_commands() {
        let mut p = LineParser::new();
        let got = feed_all(
            &mut p,
            &["r0 list", "r1 tick 5", "r2 drop 3", "r3 peek foo"],
        );
        assert_eq!(got.len(), 4);
        assert!(matches!(got[0].1, Ok(Cmd::List)));
        assert!(matches!(got[1].1, Ok(Cmd::Tick { n: 5 })));
        assert!(matches!(
            got[2].1,
            Ok(Cmd::Drop {
                target: DataflowRef::Id(3)
            })
        ));
        assert!(matches!(got[3].1, Ok(Cmd::Peek { ref name }) if name == "foo"));
    }

    #[test]
    fn multiline_load() {
        let mut p = LineParser::new();
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
                assert!(!*explain);
            }
            _ => panic!("expected Load, got {:?}", got[0].1),
        }
    }

    #[test]
    fn load_is_lowered_before_worker_admission() {
        let mut p = LineParser::new();
        assert!(p.feed("r0 load world begin").is_none());
        assert!(p.feed("let rows = input 0;").is_none());
        assert!(p.feed("export \"rows\" = rows;").is_none());
        let (_, parsed) = p.feed("r0 end-load").expect("load is complete");
        let prepared = prepare(parsed.expect("protocol parse succeeds"))
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
    fn load_explain() {
        let mut p = LineParser::new();
        let got = feed_all(&mut p, &[
            "rE load reach --explain edges=random(seed=1,arity=2,range=10,count=2,churn=0) begin",
            "export \"reach_out\" = import \"edges\";",
            "rE end-load",
        ]);
        assert_eq!(got.len(), 1);
        match &got[0].1 {
            Ok(Cmd::Load { explain, .. }) => assert!(*explain),
            _ => panic!("expected explain Load, got {:?}", got[0].1),
        }
    }

    #[test]
    fn query_cmd() {
        let mut p = LineParser::new();
        let got = feed_all(&mut p, &["rQ query 3 add 1,2 ; 99"]);
        assert_eq!(got.len(), 1);
        match &got[0].1 {
            Ok(Cmd::Query {
                target,
                kind,
                key,
                val,
            }) => {
                assert!(matches!(target, DataflowRef::Id(3)));
                assert!(matches!(kind, QueryKind::Add));
                assert_eq!(key, &vec![1, 2]);
                assert_eq!(val, &vec![99]);
            }
            _ => panic!("expected Query, got {:?}", got[0].1),
        }
    }

    #[test]
    fn auto_reqid_for_bare_command() {
        let mut p = LineParser::new();
        let got = feed_all(&mut p, &["list", "tick 3", "rA peek foo", "exit"]);
        assert_eq!(got.len(), 4);
        // Bare commands get auto-minted reqids; mixed-in explicit reqids
        // pass through unchanged.
        assert_eq!(got[0].0, "_1"); // list
        assert!(matches!(got[0].1, Ok(Cmd::List)));
        assert_eq!(got[1].0, "_2"); // tick 3
        assert!(matches!(got[1].1, Ok(Cmd::Tick { n: 3 })));
        assert_eq!(got[2].0, "rA"); // explicit reqid preserved
        assert!(matches!(got[2].1, Ok(Cmd::Peek { ref name }) if name == "foo"));
        assert_eq!(got[3].0, "_3"); // exit
        assert!(matches!(got[3].1, Ok(Cmd::Exit)));
    }

    #[test]
    fn feed_cmd() {
        let mut p = LineParser::new();
        let got = feed_all(
            &mut p,
            &[
                "r0 feed world 0 1,2",
                "r1 feed world 0 3,4 val=7,8 time=5 diff=-1",
                "r2 feed world 0 _ val=inject(2,tuple(3,4))",
                "r3 feed world zero 1,2",     // bad input index
                "r4 feed world 0 1,2 wat=7",  // unknown argument
                "r5 feed world 0 tuple(1,",   // malformed term -> caught, not a panic
            ],
        );
        assert_eq!(got.len(), 6);
        match &got[0].1 {
            Ok(Cmd::Feed { prog, input, key, val, time, diff }) => {
                assert_eq!(prog, "world");
                assert_eq!(*input, 0);
                assert_eq!(*key, Value::Tuple(vec![Value::Int(1), Value::Int(2)]));
                assert_eq!(*val, Value::unit());
                assert_eq!(*time, None);
                assert_eq!(*diff, 1);
            }
            other => panic!("expected Feed, got {:?}", other),
        }
        match &got[1].1 {
            Ok(Cmd::Feed { time, diff, .. }) => {
                assert_eq!(*time, Some(5));
                assert_eq!(*diff, -1);
            }
            other => panic!("expected Feed, got {:?}", other),
        }
        assert!(matches!(&got[2].1, Ok(Cmd::Feed { key, .. }) if *key == Value::unit()));
        assert!(got[3].1.is_err());
        assert!(got[4].1.is_err());
        assert!(got[5].1.is_err());
    }

    #[test]
    fn framed_feed_becomes_one_typed_command() {
        let mut p = LineParser::new();
        assert!(p.feed("r0 feed world 2 begin").is_none());
        assert!(p.awaiting_body());
        assert!(p.feed("# rows use the enclosing command's epoch").is_none());
        assert!(p.feed("1 val=10").is_none());
        assert!(p.feed("2 val=-20 diff=-1").is_none());
        let (reqid, parsed) = p.feed("r0 end-feed").expect("feed is complete");
        assert_eq!(reqid, "r0");
        assert!(!p.awaiting_body());

        let prepared = prepare(parsed.expect("protocol parse succeeds")).unwrap();
        let PreparedCommand::Server(ServerCommand::FeedBatch {
            prog,
            input,
            updates,
        }) = prepared
        else {
            panic!("expected prepared feed batch, got {prepared:?}");
        };
        assert_eq!(prog, "world");
        assert_eq!(input, 2);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].key, Value::Tuple(vec![Value::Int(1)]));
        assert_eq!(updates[0].val, Value::Tuple(vec![Value::Int(10)]));
        assert_eq!(updates[0].diff, 1);
        assert_eq!(updates[1].diff, -1);
    }

    #[test]
    fn malformed_framed_feed_is_rejected_atomically() {
        let mut p = LineParser::new();
        assert!(p.feed("r0 feed world 0 begin").is_none());
        assert!(p.feed("1 val=10").is_none());
        assert!(p.feed("2 time=3").is_none());
        // Once poisoned, otherwise command-looking lines are body and cannot
        // leak a partial batch or desynchronize the protocol.
        assert!(p.feed("r1 list").is_none());
        let (reqid, result) = p.feed("end-feed").expect("bare terminator works");
        assert_eq!(reqid, "r0");
        assert!(result
            .as_ref()
            .is_err_and(|error| error.contains("uses the current epoch")));
        assert!(matches!(p.feed("r2 list"), Some((_, Ok(Cmd::List)))));
    }

    #[test]
    fn oversized_framed_feed_is_rejected_cleanly() {
        let mut p = LineParser::with_cap(32);
        assert!(p.feed("r0 feed world 0 begin").is_none());
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
        let mut p = LineParser::new();
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
        let mut p = LineParser::new();
        let got = feed_all(
            &mut p,
            &[
                "r0 drop 3",
                "r1 drop my_reach",
                "r2 drop", // missing arg → err
            ],
        );
        assert_eq!(got.len(), 3);
        assert!(matches!(
            got[0].1,
            Ok(Cmd::Drop {
                target: DataflowRef::Id(3)
            })
        ));
        assert!(
            matches!(got[1].1, Ok(Cmd::Drop { target: DataflowRef::Name(ref n) }) if n == "my_reach")
        );
        assert!(matches!(got[2].1, Err(_)));
    }
}
