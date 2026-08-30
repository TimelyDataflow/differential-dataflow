//! Command and response shapes for the line-oriented protocol.
//!
//! Each request line is `<reqid> <cmd> [args...]`.
//! Each response line starts with the same `<reqid>` followed by one of:
//!   - `ok [body...]` — terminal success line
//!   - `err [body...]` — terminal error line
//!   - `data <fields...>` — one streamed body line (peek/tail batches)
//!   - `end` — terminator after a stream of `data` lines
//!
//! Multi-line bodies use a two-phase upload. `load ... begin` accepts literal
//! program text through `end-load`; `batch begin` accepts only `feed` lines
//! through `end-batch` and becomes one non-interleavable worker command.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

use interactive::ir::{eval, Diff, Value};
use interactive::server::{InputUpdate, OuterTime};

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
    /// Atomically stage a prevalidated group of feeds at the current epoch.
    /// The batch does not tick; all updates become visible together when a
    /// later tick closes that epoch.
    Batch { updates: Vec<InputUpdate> },
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

/// A parsed request: reqid plus the command (or a parse error).
#[derive(Debug)]
pub struct Request {
    pub reqid: ReqId,
    pub kind: Result<Cmd, String>,
    /// Where to route responses for this request (and, for `tail`, all
    /// subsequent batches until `stop`). Cloned from the per-connection
    /// outbound sender.
    pub resp: RespSender,
    /// Originating session; lets the worker tear down per-connection
    /// state (tails) when this session ends.
    pub connection_id: ConnectionId,
}

/// State carried between lines so the parser can splice a multi-line
/// `load <id> ... begin` body together. The parser hands back either a
/// complete `Request` or `None` (more lines required).
#[derive(Default)]
pub struct LineParser {
    pending_load: Option<PendingLoad>,
    pending_batch: Option<PendingBatch>,
    auto_reqid_counter: u64,
    max_load_bytes: usize,
    max_batch_bytes: usize,
}

/// Tokens that introduce a command. If a line begins with one of these
/// instead of an explicit reqid, the parser synthesizes a reqid.
const COMMAND_KEYWORDS: &[&str] = &[
    "load", "batch", "drop", "list", "peek", "tail", "stop", "tick", "query", "exit", "feed",
    "bind", "unbind",
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

/// Batch-size gate. A batch is cheap to apply after validation, but it is
/// accumulated on an intake thread before being sent to the worker.
fn max_batch_bytes() -> usize {
    std::env::var("DDIR_MAX_BATCH_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(65536)
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

struct PendingBatch {
    reqid: ReqId,
    updates: Vec<InputUpdate>,
    bytes: usize,
    /// A malformed or oversized body is swallowed through `end-batch`, then
    /// reported as one error; no partial command reaches the worker.
    poisoned: Option<String>,
}

impl LineParser {
    pub fn new() -> Self {
        LineParser {
            max_load_bytes: max_program_bytes(),
            max_batch_bytes: max_batch_bytes(),
            ..Self::default()
        }
    }

    /// Test hook: a parser with an explicit program-size cap.
    #[cfg(test)]
    fn with_cap(max_load_bytes: usize) -> Self {
        LineParser {
            max_load_bytes,
            max_batch_bytes: max_load_bytes,
            ..Self::default()
        }
    }

    /// Feed one input line; return `Some((reqid, parsed))` if the line
    /// completes a command (single-line or end of a multi-line body),
    /// else `None` to indicate more input is required. The caller pairs
    /// the result with a per-connection response sender to form a
    /// `Request`.
    pub fn feed(&mut self, line: &str) -> Option<(ReqId, Result<Cmd, String>)> {
        // A batch body is parsed off-worker, but only as feed commands. It is
        // returned as one Cmd::Batch at the terminator, so no other session's
        // command can interleave between its members on the worker.
        if let Some(ref mut batch) = self.pending_batch {
            let trimmed = line.trim();
            let mut parts = trimmed.split_whitespace();
            let first = parts.next();
            let second = parts.next();
            let third = parts.next();
            let explicit_end = first == Some(batch.reqid.as_str())
                && second == Some("end-batch")
                && third.is_none();
            let bare_end = first == Some("end-batch") && second.is_none();
            if explicit_end || bare_end {
                let done = self.pending_batch.take().unwrap();
                if let Some(err) = done.poisoned {
                    return Some((done.reqid, Err(err)));
                }
                return Some((
                    done.reqid,
                    Ok(Cmd::Batch {
                        updates: done.updates,
                    }),
                ));
            }

            // Blank and comment lines are furniture between the feed rows.
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            batch.bytes = batch.bytes.saturating_add(line.len() + 1);
            if batch.bytes > self.max_batch_bytes {
                if batch.poisoned.is_none() {
                    batch.poisoned = Some(format!(
                        "batch: body exceeds {} bytes (DDIR_MAX_BATCH_BYTES)",
                        self.max_batch_bytes
                    ));
                    batch.updates.clear();
                }
                return None;
            }
            if batch.poisoned.is_some() {
                return None;
            }

            let Some("feed") = first else {
                batch.poisoned =
                    Some("batch: body accepts only bare `feed ...` commands".to_string());
                batch.updates.clear();
                return None;
            };
            let args: Vec<&str> = trimmed.split_whitespace().skip(1).collect();
            match parse_feed(&args) {
                Ok(feed) if feed.time.is_none() => batch.updates.push(InputUpdate {
                    prog: feed.prog,
                    input: feed.input,
                    key: feed.key,
                    val: feed.val,
                    diff: feed.diff,
                }),
                Ok(_) => {
                    batch.poisoned = Some(
                        "batch: `time=` is not allowed; every feed uses the current epoch"
                            .to_string(),
                    );
                    batch.updates.clear();
                }
                Err(err) => {
                    batch.poisoned = Some(err);
                    batch.updates.clear();
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
        // Blank lines and `#` comments are skipped between commands (inside
        // a load body every line is literal program text, handled above).
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
            ParseOutcome::BeginBatch => {
                self.pending_batch = Some(PendingBatch {
                    reqid,
                    updates: Vec::new(),
                    bytes: 0,
                    poisoned: None,
                });
                None
            }
        }
    }

    /// True if waiting for a multi-line body terminator. WS transport uses
    /// this to forward blank body content verbatim.
    pub fn awaiting_body(&self) -> bool {
        self.pending_load.is_some() || self.pending_batch.is_some()
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
    BeginBatch,
}

struct ParsedFeed {
    prog: String,
    input: usize,
    key: Value,
    val: Value,
    time: Option<OuterTime>,
    diff: Diff,
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

fn parse_feed(args: &[&str]) -> Result<ParsedFeed, String> {
    // Syntax: `feed <prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]`
    // A `<v>`/`<key>` is a comma-separated integer row (`1,2` -> tuple;
    // `_`/empty -> unit) or a closed scalar term written without spaces.
    if args.len() < 3 {
        return Err("feed: expected `<prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]`".into());
    }
    let prog = args[0].to_string();
    let input: usize = args[1]
        .parse()
        .map_err(|_| format!("feed: <in#> must be a number, got {:?}", args[1]))?;
    let key = parse_value(args[2]).map_err(|e| format!("feed key: {}", e))?;
    let mut val = Value::unit();
    let mut time = None;
    let mut diff: Diff = 1;
    for tok in &args[3..] {
        if let Some(v) = tok.strip_prefix("val=") {
            val = parse_value(v).map_err(|e| format!("feed val: {}", e))?;
        } else if let Some(t) = tok.strip_prefix("time=") {
            time = Some(
                t.parse()
                    .map_err(|_| format!("feed: time= must be a number, got {:?}", t))?,
            );
        } else if let Some(d) = tok.strip_prefix("diff=") {
            diff = d
                .parse()
                .map_err(|_| format!("feed: diff= must be an integer, got {:?}", d))?;
        } else {
            return Err(format!("feed: unrecognized argument {:?}", tok));
        }
    }
    Ok(ParsedFeed {
        prog,
        input,
        key,
        val,
        time,
        diff,
    })
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
        "batch" => match args {
            ["begin"] => ParseOutcome::BeginBatch,
            _ => ParseOutcome::Err("batch: expected `begin`".into()),
        },
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
        "feed" => match parse_feed(args) {
            Ok(ParsedFeed {
                prog,
                input,
                key,
                val,
                time,
                diff,
            }) => ParseOutcome::Cmd(Cmd::Feed {
                prog,
                input,
                key,
                val,
                time,
                diff,
            }),
            Err(err) => ParseOutcome::Err(err),
        },
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
    fn batch_collects_only_current_epoch_feeds() {
        let mut p = LineParser::new();
        let got = feed_all(
            &mut p,
            &[
                "rB batch begin",
                "# world update and audit declaration share one command",
                "feed world 0 1,2 val=7 diff=-1",
                "",
                "feed ledger 0 9 val=1,2,7",
                "rB end-batch",
            ],
        );
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "rB");
        let Ok(Cmd::Batch { updates }) = &got[0].1 else {
            panic!("expected Batch, got {:?}", got[0].1);
        };
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].prog, "world");
        assert_eq!(updates[0].input, 0);
        assert_eq!(
            updates[0].key,
            Value::Tuple(vec![Value::Int(1), Value::Int(2)])
        );
        assert_eq!(updates[0].val, Value::Tuple(vec![Value::Int(7)]));
        assert_eq!(updates[0].diff, -1);
        assert_eq!(updates[1].prog, "ledger");
    }

    #[test]
    fn bare_batch_has_a_bare_terminator() {
        let mut p = LineParser::new();
        let got = feed_all(&mut p, &["batch begin", "feed world 0 1", "end-batch"]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "_1");
        assert!(matches!(&got[0].1, Ok(Cmd::Batch { updates }) if updates.len() == 1));
    }

    #[test]
    fn malformed_batch_is_swallowed_and_rejected_as_one_command() {
        let mut p = LineParser::new();
        assert!(p.feed("rB batch begin").is_none());
        assert!(p.feed("feed world 0 1").is_none());
        assert!(p.feed("tick").is_none());
        // Even syntactically valid feeds after the poison remain body lines.
        assert!(p.feed("feed ledger 0 2").is_none());
        let got = p.feed("rB end-batch").expect("terminator completes batch");
        assert_eq!(got.0, "rB");
        assert!(got
            .1
            .as_ref()
            .is_err_and(|e| e.contains("only bare `feed ...`")));
        assert!(matches!(p.feed("r1 list"), Some((_, Ok(Cmd::List)))));
    }

    #[test]
    fn batch_rejects_explicit_times() {
        let mut p = LineParser::new();
        assert!(p.feed("rB batch begin").is_none());
        assert!(p.feed("feed world 0 1 time=3").is_none());
        let got = p.feed("rB end-batch").expect("terminator completes batch");
        assert!(got.1.as_ref().is_err_and(|e| e.contains("current epoch")));
    }

    #[test]
    fn oversized_batch_is_rejected_cleanly() {
        let mut p = LineParser::with_cap(32);
        assert!(p.feed("rB batch begin").is_none());
        for _ in 0..8 {
            assert!(p.feed("feed world 0 1,2 val=3,4").is_none());
        }
        let got = p.feed("rB end-batch").expect("terminator completes batch");
        assert_eq!(got.0, "rB");
        assert!(got
            .1
            .as_ref()
            .is_err_and(|e| e.contains("exceeds 32 bytes")));
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
