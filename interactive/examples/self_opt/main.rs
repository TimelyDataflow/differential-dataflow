//! DDIR optimizing DDIR: run `optimize.ddp` (the scope-IR optimizer written in DDIR)
//! against `Scope::optimize` (the Rust one) over every `.ddp` program named on the
//! command line, and check that the two produce the same IR.
//!
//! Each program is lowered (unoptimized), exported as `nodes`/`edges` rows (see the
//! header of `optimize.ddp` for the encoding), fed into one long-running server
//! install of the optimizer, and read back after a `tick`. Programs accumulate: the
//! optimizer holds every program fed so far and each `tick` pays for the new one.
//! At the end every program is retracted, one per tick, as a check that the
//! optimizer's state unwinds.
//!
//! ```text
//! cargo run --release --example self_opt -- $(find examples tests server -name '*.ddp')
//! ```
//!
//! Environment: `SELF_OPT_DDP=<file>` substitutes another optimizer program; `SELF_OPT_SHOW` dumps
//! each Rust-optimized program and `SELF_OPT_DUMP` dumps both sides of a disagreement.
//! `SELF_OPT_SHARED` puts every program's root scope in one dedup domain (root imports
//! named by source) and reports how much the programs share, instead of comparing.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use interactive::scope_ir::{Item, Node, Ref, Scope, Source};
use interactive::server::Server;
use interactive::{lower, parse};

#[path = "graph.rs"]
mod graph;
use graph::*;

/// The optimizer's output, indexed for read-back.
struct Output {
    nodes: HashMap<i64, (i64, i64, Vec<i64>)>,
    edges: HashMap<i64, BTreeMap<i64, Edge>>,
}

impl Output {
    fn refs(&self, holder: i64, base: i64) -> Vec<Ref> {
        self.edges
            .get(&holder)
            .map(|m| m.values().map(|e| dec(*e, base)).collect())
            .unwrap_or_default()
    }
    fn one(&self, holder: i64, base: i64) -> Ref {
        let mut refs = self.refs(holder, base);
        assert_eq!(refs.len(), 1, "holder {holder} has {} refs", refs.len());
        refs.pop().unwrap()
    }
}

/// Rebuild `s` from the optimizer's output: same walk as `export_scope`, so the
/// numbering lines up. Items absent from the output are dead; then compact.
fn import_scope(s: &mut Scope, num: &mut Numbering, pay: &Payloads, res: &Output) {
    let scope = num.scope();
    let base = scope * SCOPE_STRIDE;
    let mut dead = vec![false; s.items.len()];
    for (i, item) in s.items.iter_mut().enumerate() {
        let id = base + i as i64;
        match item {
            Item::Op(node) => match res.nodes.get(&id) {
                None => dead[i] = true,
                Some((kind, pid, ops)) => {
                    let mut refs = res.refs(id, base).into_iter();
                    *node = match *kind {
                        LINEAR => Node::Linear {
                            input: refs.next().unwrap(),
                            ops: ops.iter().map(|o| pay.ops[o].clone()).collect(),
                        },
                        CONCAT => Node::Concat(refs.collect()),
                        ARRANGE => Node::Arrange(refs.next().unwrap()),
                        JOIN => Node::Join {
                            left: refs.next().unwrap(),
                            right: refs.next().unwrap(),
                            projection: pay.projections[pid].clone(),
                        },
                        HALF_JOIN => {
                            let (strict, projection) = &pay.half_joins[pid];
                            Node::HalfJoin {
                                stream: refs.next().unwrap(),
                                arrangement: refs.next().unwrap(),
                                strict: *strict,
                                projection: projection.clone(),
                            }
                        }
                        REDUCE => Node::Reduce {
                            input: refs.next().unwrap(),
                            reducer: pay.reducers[pid].clone(),
                        },
                        INSPECT => Node::Inspect {
                            input: refs.next().unwrap(),
                            label: pay.labels[pid].clone(),
                        },
                        k => panic!("bad kind {k}"),
                    };
                }
            },
            Item::Sub(child) => {
                for imp in child.imports.iter_mut() {
                    if let Source::Parent(r) = &mut imp.from {
                        *r = res.one(num.holder(), base);
                    }
                }
                import_scope(child, num, pay, res);
            }
        }
    }
    for b in s.binds.iter_mut() {
        b.value = res.one(num.holder(), base);
    }
    for e in s.exports.iter_mut() {
        e.value = res.one(num.holder(), base);
    }
    compact(s, &dead);
}

/// `Scope::compact`, which is private: drop dead items, remap item indices.
fn compact(s: &mut Scope, dead: &[bool]) {
    let mut remap = vec![usize::MAX; s.items.len()];
    let mut next = 0;
    for (i, &d) in dead.iter().enumerate() {
        if !d {
            remap[i] = next;
            next += 1;
        }
    }
    let mut keep = dead.iter().map(|d| !d);
    s.items.retain(|_| keep.next().unwrap());
    let fix = |r: &mut Ref| match r {
        Ref::Local(i) | Ref::ChildExport(i, _) => {
            assert!(remap[*i] != usize::MAX, "a reference points at a dead item");
            *i = remap[*i];
        }
        Ref::Import(_) | Ref::Var(_) => {}
    };
    for item in s.items.iter_mut() {
        match item {
            Item::Op(node) => match node {
                Node::Linear { input, .. }
                | Node::Arrange(input)
                | Node::Reduce { input, .. }
                | Node::Inspect { input, .. } => fix(input),
                Node::Join { left, right, .. } => {
                    fix(left);
                    fix(right);
                }
                Node::HalfJoin {
                    stream,
                    arrangement,
                    ..
                } => {
                    fix(stream);
                    fix(arrangement);
                }
                Node::Concat(refs) => refs.iter_mut().for_each(fix),
            },
            Item::Sub(child) => {
                for imp in child.imports.iter_mut() {
                    if let Source::Parent(r) = &mut imp.from {
                        fix(r);
                    }
                }
            }
        }
    }
    s.binds.iter_mut().for_each(|b| fix(&mut b.value));
    s.exports.iter_mut().for_each(|e| fix(&mut e.value));
}

fn count_kinds(s: &Scope, kinds: &mut [usize; 7]) {
    for item in &s.items {
        match item {
            Item::Op(node) => {
                kinds[match node {
                    Node::Linear { .. } => 0,
                    Node::Concat(_) => 1,
                    Node::Arrange(_) => 2,
                    Node::Join { .. } => 3,
                    Node::Reduce { .. } => 4,
                    Node::Inspect { .. } => 5,
                    Node::HalfJoin { .. } => 6,
                }] += 1
            }
            Item::Sub(child) => count_kinds(child, kinds),
        }
    }
}

fn ints(text: &str) -> Vec<i64> {
    text.split(|c: char| !c.is_ascii_digit() && c != '-')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect()
}

fn read_result(server: &mut Server, worker: &mut timely::worker::Worker) -> Output {
    let mut nodes = HashMap::new();
    for (k, v, d) in server.snapshot(worker, "nodes").unwrap().rows() {
        assert_eq!(d, 1, "node row with multiplicity {d}: {k:?} {v:?}");
        let fields = ints(&v);
        nodes.insert(ints(&k)[0], (fields[1], fields[2], fields[3..].to_vec()));
    }
    let mut edges: HashMap<i64, BTreeMap<i64, Edge>> = HashMap::new();
    for (k, v, d) in server.snapshot(worker, "edges").unwrap().rows() {
        assert_eq!(d, 1, "edge row with multiplicity {d}: {k:?} {v:?}");
        let k = ints(&k);
        let v = ints(&v);
        let prev = edges
            .entry(k[0])
            .or_default()
            .insert(k[1], (v[0], v[1], v[2]));
        assert!(prev.is_none(), "two edges at {k:?}");
    }
    Output { nodes, edges }
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let opt_path =
            std::env::var("SELF_OPT_DDP").unwrap_or("examples/self_opt/optimize.ddp".into());
        let src = std::fs::read_to_string(&opt_path).expect("run from interactive/");
        let mut optimizer = lower::lower_tree(parse::pipe::parse(&src));
        optimizer.optimize();
        server.install(worker, "opt", &optimizer).unwrap();

        // Shared mode: one domain for every program's root scope, to measure cross-program
        // sharing. Read-back is per program, so it (and the comparison) is skipped.
        let shared = std::env::var("SELF_OPT_SHARED").is_ok();
        let mut separate = [0usize; 7];
        let mut pay = Payloads::default();
        let mut num = Numbering {
            next_scope: 0,
            next_holder: HOLDER_BASE,
        };
        let mut loaded: Vec<(String, Rows)> = Vec::new();
        let (mut agree, mut differ) = (0, 0);
        println!(
            "{:<44} {:>6} {:>6} {:>6} {:>9} {:>9}  verdict",
            "program", "before", "rust", "ddir", "rust µs", "tick ms"
        );
        for path in &paths {
            let Ok(text) = std::fs::read_to_string(path) else {
                continue;
            };
            // Programs that need a function registry or otherwise fail to lower are skipped.
            let Ok(program) =
                std::panic::catch_unwind(|| lower::lower_tree(parse::pipe::parse(&text)))
            else {
                println!("{path:<44} (does not lower; skipped)");
                continue;
            };

            let mut rust = program.clone();
            let optimized = Instant::now();
            rust.optimize();
            let rust_us = optimized.elapsed().as_micros();

            let first = Numbering {
                next_scope: num.next_scope,
                next_holder: num.next_holder,
            };
            let mut rows = Rows::default();
            let sources = shared.then(|| {
                program
                    .root
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        Source::Trace(name) => pay.intern(format!("trace {name}")),
                        Source::Input(i) => pay.intern(format!("input {path} {i}")),
                        Source::Parent(_) => unreachable!("a root import names an external source"),
                    })
                    .collect::<Vec<_>>()
            });
            export_scope(
                &program.root,
                &mut num,
                &mut pay,
                &mut rows,
                sources.as_deref(),
            );
            let (n, e) = updates(&rows, 1);
            server.feed_columns("opt", 0, n, None).unwrap();
            server.feed_columns("opt", 1, e, None).unwrap();
            let ticked = Instant::now();
            server.tick(worker);
            let tick_ms = ticked.elapsed().as_secs_f64() * 1e3;

            if shared {
                let mut kinds = [0usize; 7];
                count_kinds(&rust.root, &mut kinds);
                for (s, k) in separate.iter_mut().zip(kinds) {
                    *s += k;
                }
                println!(
                    "{:<44} {:>6} {:>6} {:>6} {:>9} {:>9.1}  (shared)",
                    path,
                    program.op_count(),
                    rust.op_count(),
                    "",
                    rust_us,
                    tick_ms
                );
                loaded.push((path.clone(), rows));
                continue;
            }
            let res = read_result(&mut server, worker);
            let mut ddir = program.clone();
            let mut replay = first;
            import_scope(&mut ddir.root, &mut replay, &pay, &res);

            let same = format!("{:?}", rust) == format!("{:?}", ddir);
            if same {
                agree += 1
            } else {
                differ += 1
            }
            println!(
                "{:<44} {:>6} {:>6} {:>6} {:>9} {:>9.1}  {}",
                path,
                program.op_count(),
                rust.op_count(),
                ddir.op_count(),
                rust_us,
                tick_ms,
                if same { "same" } else { "DIFFERENT" },
            );
            if std::env::var("SELF_OPT_SHOW").is_ok() {
                rust.dump();
            }
            if !same && std::env::var("SELF_OPT_DUMP").is_ok() {
                println!("--- rust");
                rust.dump();
                println!("--- ddir");
                ddir.dump();
            }
            loaded.push((path.clone(), rows));
        }
        if shared {
            let res = read_result(&mut server, worker);
            let mut joint = [0usize; 7];
            for (kind, _, _) in res.nodes.values() {
                if *kind != HOLDER {
                    joint[*kind as usize] += 1;
                }
            }
            let names = [
                "linear",
                "concat",
                "arrange",
                "join",
                "reduce",
                "inspect",
                "half_join",
            ];
            for (i, name) in names.iter().enumerate() {
                println!(
                    "{name:>8}: {:>5} optimized separately, {:>5} optimized jointly",
                    separate[i], joint[i]
                );
            }
            println!(
                "   total: {:>5} optimized separately, {:>5} optimized jointly",
                separate.iter().sum::<usize>(),
                joint.iter().sum::<usize>()
            );
        } else {
            println!("{agree} same, {differ} different");
            assert_eq!(differ, 0, "the DDIR optimizer differs from Scope::optimize");
        }

        // Unwind: retract each program; the optimizer's output should empty out.
        let unwound = Instant::now();
        for (_, rows) in &loaded {
            let (n, e) = updates(rows, -1);
            server.feed_columns("opt", 0, n, None).unwrap();
            server.feed_columns("opt", 1, e, None).unwrap();
            server.tick(worker);
        }
        let left = read_result(&mut server, worker);
        println!(
            "retracted {} programs in {:.1} ms; {} nodes and {} edges remain",
            loaded.len(),
            unwound.elapsed().as_secs_f64() * 1e3,
            left.nodes.len(),
            left.edges.len(),
        );
        assert!(
            left.nodes.is_empty() && left.edges.is_empty(),
            "retraction left optimizer state behind"
        );
    });
}
