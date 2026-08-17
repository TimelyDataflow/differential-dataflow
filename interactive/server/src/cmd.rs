//! Command and response shapes for the line-oriented protocol.
//!
//! Each request line is `<reqid> <cmd> [args...]`.
//! Each response line starts with the same `<reqid>` followed by one of:
//!   - `ok [body...]` — terminal success line
//!   - `err [body...]` — terminal error line
//!   - `data <fields...>` — one streamed body line (peek/tail batches)
//!   - `end` — terminator after a stream of `data` lines
//!
//! Multi-line bodies (a DDIR program) come via a two-phase upload:
//! `<reqid> load <id> <bindings...> begin` opens; subsequent lines are
//! literal program text terminated by `<reqid> end-load`.

use std::collections::BTreeMap;

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
    auto_reqid_counter: u64,
}

/// Tokens that introduce a command. If a line begins with one of these
/// instead of an explicit reqid, the parser synthesizes a reqid.
const COMMAND_KEYWORDS: &[&str] = &[
    "load", "drop", "list", "peek", "tail", "stop", "tick", "query", "exit",
];

struct PendingLoad {
    reqid: ReqId,
    id_hint: String,
    bindings: BTreeMap<String, String>,
    explain: bool,
    body: String,
}

impl LineParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one input line; return `Some((reqid, parsed))` if the line
    /// completes a command (single-line or end of a multi-line body),
    /// else `None` to indicate more input is required. The caller pairs
    /// the result with a per-connection response sender to form a
    /// `Request`.
    pub fn feed(&mut self, line: &str) -> Option<(ReqId, Result<Cmd, String>)> {
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
            pl.body.push_str(trimmed);
            pl.body.push('\n');
            return None;
        }

        let trimmed = line.trim();
        // Blank lines and `#` comments are skipped between commands (inside
        // a load body every line is literal program text, handled above), so
        // annotated command scripts can be piped straight to stdin.
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
                });
                None
            }
        }
    }

    /// True if waiting for a `<reqid> end-load`. WS transport uses this
    /// to forward blank-line program body content verbatim.
    pub fn awaiting_body(&self) -> bool {
        self.pending_load.is_some()
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
