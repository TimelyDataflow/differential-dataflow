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
//! Environment: `DDIR_BACKEND=corgi` renders the optimizer on the columnar backend;
//! `SELF_OPT_DDP=<file>` substitutes another optimizer program; `SELF_OPT_SHOW` dumps
//! each Rust-optimized program and `SELF_OPT_DUMP` dumps both sides of a disagreement.
//! `SELF_OPT_SHARED` puts every program's root scope in one dedup domain (root imports
//! named by source) and reports how much the programs share, instead of comparing.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use interactive::ir::{LinearOp, Projection, Reducer, Value};
use interactive::scope_ir::{Item, Node, Ref, Scope, Source};
use interactive::server::{InputUpdate, Server};
use interactive::{lower, parse};

const SCOPE_STRIDE: i64 = 1 << 20;
const HOLDER_BASE: i64 = 1 << 40;

const LINEAR: i64 = 0;
const CONCAT: i64 = 1;
const ARRANGE: i64 = 2;
const JOIN: i64 = 3;
const REDUCE: i64 = 4;
const INSPECT: i64 = 5;
const HOLDER: i64 = 9;

/// Payloads, interned by their `Debug` text (which is what the Rust dedup compares).
#[derive(Default)]
struct Payloads {
    ids: HashMap<String, i64>,
    ops: HashMap<i64, LinearOp>,
    projections: HashMap<i64, Projection>,
    reducers: HashMap<i64, Reducer>,
    labels: HashMap<i64, String>,
}

impl Payloads {
    fn intern(&mut self, text: String) -> i64 {
        let next = self.ids.len() as i64 + 1;
        *self.ids.entry(text).or_insert(next)
    }
}

type Edge = (i64, i64, i64);

/// Rows for one program, plus the numbering needed to read the result back.
#[derive(Default)]
struct Rows {
    nodes: Vec<(i64, (i64, i64, i64, Vec<i64>))>,
    edges: Vec<((i64, i64), Edge)>,
}

/// Scope and holder numbering, shared by the export walk and the read-back walk:
/// both visit the tree in the same order and so allocate the same numbers.
struct Numbering {
    next_scope: i64,
    next_holder: i64,
}

impl Numbering {
    fn scope(&mut self) -> i64 { let s = self.next_scope; self.next_scope += 1; s }
    fn holder(&mut self) -> i64 { let h = self.next_holder; self.next_holder += 1; h }
}

fn enc(r: &Ref, base: i64) -> Edge {
    match r {
        Ref::Local(i) => (0, base + *i as i64, 0),
        Ref::Import(k) => (1, *k as i64, 0),
        Ref::Var(v) => (2, *v as i64, 0),
        Ref::ChildExport(c, j) => (3, base + *c as i64, *j as i64),
    }
}

fn dec((tag, a, b): Edge, base: i64) -> Ref {
    match tag {
        0 => Ref::Local((a - base) as usize),
        1 => Ref::Import(a as usize),
        2 => Ref::Var(a as usize),
        3 => Ref::ChildExport((a - base) as usize, b as usize),
        _ => panic!("bad ref tag {tag}"),
    }
}

/// `sources`, when present, marks `s` as a root scope in *shared* mode: the root scopes of
/// all programs form one domain (so dedup can merge across programs), and a root import is
/// named by its source rather than by its position.
fn export_scope(s: &Scope, num: &mut Numbering, pay: &mut Payloads, rows: &mut Rows, sources: Option<&[i64]>) {
    let scope = num.scope();
    let base = scope * SCOPE_STRIDE;
    let domain = if sources.is_some() { -1 } else { scope };
    let encode = |r: &Ref| match (r, sources) {
        (Ref::Import(k), Some(src)) => (1, src[*k], 0),
        _ => enc(r, base),
    };
    let holder = |numbering: &mut Numbering, out: &mut Rows, r: &Ref| {
        let h = numbering.holder();
        out.nodes.push((h, (domain, HOLDER, h, Vec::new())));
        out.edges.push(((h, 0), encode(r)));
    };
    for (i, item) in s.items.iter().enumerate() {
        let id = base + i as i64;
        match item {
            Item::Op(node) => {
                let (kind, pid, ops, refs): (i64, i64, Vec<i64>, Vec<&Ref>) = match node {
                    Node::Linear { input, ops } => {
                        let ids = ops.iter().map(|op| {
                            let op_id = pay.intern(format!("op {:?}", op));
                            pay.ops.insert(op_id, op.clone());
                            op_id
                        }).collect();
                        (LINEAR, 0, ids, vec![input])
                    }
                    Node::Concat(refs) => (CONCAT, 0, Vec::new(), refs.iter().collect()),
                    Node::Arrange(input) => (ARRANGE, 0, Vec::new(), vec![input]),
                    Node::Join { left, right, projection } => {
                        let pid = pay.intern(format!("join {:?}", projection));
                        pay.projections.insert(pid, projection.clone());
                        (JOIN, pid, Vec::new(), vec![left, right])
                    }
                    Node::Reduce { input, reducer } => {
                        let pid = pay.intern(format!("reduce {:?}", reducer));
                        pay.reducers.insert(pid, reducer.clone());
                        (REDUCE, pid, Vec::new(), vec![input])
                    }
                    Node::Inspect { input, label } => {
                        let pid = pay.intern(format!("inspect {:?}", label));
                        pay.labels.insert(pid, label.clone());
                        (INSPECT, pid, Vec::new(), vec![input])
                    }
                };
                rows.nodes.push((id, (domain, kind, pid, ops)));
                for (pos, r) in refs.into_iter().enumerate() {
                    rows.edges.push(((id, pos as i64), encode(r)));
                }
            }
            Item::Sub(child) => {
                for imp in &child.imports {
                    if let Source::Parent(r) = &imp.from { holder(num, rows, r); }
                }
                export_scope(child, num, pay, rows, None);
            }
        }
    }
    for b in &s.binds { holder(num, rows, &b.value); }
    for e in &s.exports { holder(num, rows, &e.value); }
}

/// The optimizer's output, indexed for read-back.
struct Output {
    nodes: HashMap<i64, (i64, i64, Vec<i64>)>,
    edges: HashMap<i64, BTreeMap<i64, Edge>>,
}

impl Output {
    fn refs(&self, holder: i64, base: i64) -> Vec<Ref> {
        self.edges.get(&holder).map(|m| m.values().map(|e| dec(*e, base)).collect()).unwrap_or_default()
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
                        JOIN => Node::Join { left: refs.next().unwrap(), right: refs.next().unwrap(), projection: pay.projections[pid].clone() },
                        REDUCE => Node::Reduce { input: refs.next().unwrap(), reducer: pay.reducers[pid].clone() },
                        INSPECT => Node::Inspect { input: refs.next().unwrap(), label: pay.labels[pid].clone() },
                        k => panic!("bad kind {k}"),
                    };
                }
            },
            Item::Sub(child) => {
                for imp in child.imports.iter_mut() {
                    if let Source::Parent(r) = &mut imp.from { *r = res.one(num.holder(), base); }
                }
                import_scope(child, num, pay, res);
            }
        }
    }
    for b in s.binds.iter_mut() { b.value = res.one(num.holder(), base); }
    for e in s.exports.iter_mut() { e.value = res.one(num.holder(), base); }
    compact(s, &dead);
}

/// `Scope::compact`, which is private: drop dead items, remap item indices.
fn compact(s: &mut Scope, dead: &[bool]) {
    let mut remap = vec![usize::MAX; s.items.len()];
    let mut next = 0;
    for (i, &d) in dead.iter().enumerate() {
        if !d { remap[i] = next; next += 1; }
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
                Node::Linear { input, .. } | Node::Arrange(input)
                | Node::Reduce { input, .. } | Node::Inspect { input, .. } => fix(input),
                Node::Join { left, right, .. } => { fix(left); fix(right); }
                Node::Concat(refs) => refs.iter_mut().for_each(fix),
            },
            Item::Sub(child) => for imp in child.imports.iter_mut() {
                if let Source::Parent(r) = &mut imp.from { fix(r); }
            },
        }
    }
    s.binds.iter_mut().for_each(|b| fix(&mut b.value));
    s.exports.iter_mut().for_each(|e| fix(&mut e.value));
}

fn count_kinds(s: &Scope, kinds: &mut [usize; 6]) {
    for item in &s.items {
        match item {
            Item::Op(node) => kinds[match node {
                Node::Linear { .. } => 0, Node::Concat(_) => 1, Node::Arrange(_) => 2,
                Node::Join { .. } => 3, Node::Reduce { .. } => 4, Node::Inspect { .. } => 5,
            }] += 1,
            Item::Sub(child) => count_kinds(child, kinds),
        }
    }
}

fn int(n: i64) -> Value { Value::Int(n) }

fn updates(rows: &Rows, diff: i64) -> (Vec<InputUpdate>, Vec<InputUpdate>) {
    let nodes = rows.nodes.iter().map(|(id, (scope, kind, pid, ops))| InputUpdate {
        key: Value::Tuple(vec![int(*id)]),
        val: Value::Tuple(vec![int(*scope), int(*kind), int(*pid), Value::List(ops.iter().map(|o| int(*o)).collect())]),
        diff,
    }).collect();
    let edges = rows.edges.iter().map(|((h, pos), (tag, a, b))| InputUpdate {
        key: Value::Tuple(vec![int(*h), int(*pos)]),
        val: Value::Tuple(vec![int(*tag), int(*a), int(*b)]),
        diff,
    }).collect();
    (nodes, edges)
}

fn ints(v: &Value) -> Vec<i64> {
    match v {
        Value::Tuple(xs) | Value::List(xs) => xs.iter().map(|x| x.as_int()).collect(),
        Value::Int(n) => vec![*n],
        other => panic!("expected ints, got {other:?}"),
    }
}

fn read_result(server: &mut Server, worker: &mut timely::worker::Worker) -> Output {
    let mut nodes = HashMap::new();
    for (k, v, d) in server.snapshot(worker, "nodes").unwrap() {
        assert_eq!(d, 1, "node row with multiplicity {d}: {k:?} {v:?}");
        let Value::Tuple(f) = v else { panic!() };
        nodes.insert(ints(&k)[0], (f[1].as_int(), f[2].as_int(), ints(&f[3])));
    }
    let mut edges: HashMap<i64, BTreeMap<i64, Edge>> = HashMap::new();
    for (k, v, d) in server.snapshot(worker, "edges").unwrap() {
        assert_eq!(d, 1, "edge row with multiplicity {d}: {k:?} {v:?}");
        let k = ints(&k);
        let v = ints(&v);
        let prev = edges.entry(k[0]).or_default().insert(k[1], (v[0], v[1], v[2]));
        assert!(prev.is_none(), "two edges at {k:?}");
    }
    Output { nodes, edges }
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    timely::execute_directly(move |worker| {
        let backend = std::env::var("DDIR_BACKEND").unwrap_or("vec".into()).parse().expect("backend");
        let mut server = Server::with_backend(backend);
        let opt_path = std::env::var("SELF_OPT_DDP").unwrap_or("examples/self_opt/optimize.ddp".into());
        let src = std::fs::read_to_string(&opt_path).expect("run from interactive/");
        let mut optimizer = lower::lower_tree(parse::pipe::parse(&src));
        optimizer.optimize();
        server.install(worker, "opt", &optimizer).unwrap();

        // Shared mode: one domain for every program's root scope, to measure cross-program
        // sharing. Read-back is per program, so it (and the comparison) is skipped.
        let shared = std::env::var("SELF_OPT_SHARED").is_ok();
        let mut separate = [0usize; 6];
        let mut pay = Payloads::default();
        let mut num = Numbering { next_scope: 0, next_holder: HOLDER_BASE };
        let mut loaded: Vec<(String, Rows)> = Vec::new();
        let (mut agree, mut differ) = (0, 0);
        println!("{:<44} {:>6} {:>6} {:>6} {:>9} {:>9}  verdict", "program", "before", "rust", "ddir", "rust µs", "tick ms");
        for path in &paths {
            let Ok(text) = std::fs::read_to_string(path) else { continue };
            // Programs that need a function registry or otherwise fail to lower are skipped.
            let Ok(program) = std::panic::catch_unwind(|| lower::lower_tree(parse::pipe::parse(&text))) else {
                println!("{path:<44} (does not lower; skipped)");
                continue;
            };

            let mut rust = program.clone();
            let optimized = Instant::now();
            rust.optimize();
            let rust_us = optimized.elapsed().as_micros();

            let first = Numbering { next_scope: num.next_scope, next_holder: num.next_holder };
            let mut rows = Rows::default();
            let sources = shared.then(|| program.root.imports.iter().map(|imp| match &imp.from {
                Source::Trace(name) => pay.intern(format!("trace {name}")),
                Source::Input(i) => pay.intern(format!("input {path} {i}")),
                Source::Parent(_) => unreachable!("a root import names an external source"),
            }).collect::<Vec<_>>());
            export_scope(&program.root, &mut num, &mut pay, &mut rows, sources.as_deref());
            let (n, e) = updates(&rows, 1);
            server.feed_batch("opt", 0, n).unwrap();
            server.feed_batch("opt", 1, e).unwrap();
            let ticked = Instant::now();
            server.tick(worker);
            let tick_ms = ticked.elapsed().as_secs_f64() * 1e3;

            if shared {
                let mut kinds = [0usize; 6];
                count_kinds(&rust.root, &mut kinds);
                for (s, k) in separate.iter_mut().zip(kinds) { *s += k; }
                println!("{:<44} {:>6} {:>6} {:>6} {:>9} {:>9.1}  (shared)", path, program.op_count(), rust.op_count(), "", rust_us, tick_ms);
                loaded.push((path.clone(), rows));
                continue;
            }
            let res = read_result(&mut server, worker);
            let mut ddir = program.clone();
            let mut replay = first;
            import_scope(&mut ddir.root, &mut replay, &pay, &res);

            let same = format!("{:?}", rust) == format!("{:?}", ddir);
            if same { agree += 1 } else { differ += 1 }
            println!(
                "{:<44} {:>6} {:>6} {:>6} {:>9} {:>9.1}  {}",
                path, program.op_count(), rust.op_count(), ddir.op_count(), rust_us, tick_ms,
                if same { "same" } else { "DIFFERENT" },
            );
            if std::env::var("SELF_OPT_SHOW").is_ok() { rust.dump(); }
            if !same && std::env::var("SELF_OPT_DUMP").is_ok() {
                println!("--- rust"); rust.dump();
                println!("--- ddir"); ddir.dump();
            }
            loaded.push((path.clone(), rows));
        }
        if shared {
            let res = read_result(&mut server, worker);
            let mut joint = [0usize; 6];
            for (kind, _, _) in res.nodes.values() { if *kind != HOLDER { joint[*kind as usize] += 1; } }
            let names = ["linear", "concat", "arrange", "join", "reduce", "inspect"];
            for (i, name) in names.iter().enumerate() {
                println!("{name:>8}: {:>5} optimized separately, {:>5} optimized jointly", separate[i], joint[i]);
            }
            println!("   total: {:>5} optimized separately, {:>5} optimized jointly",
                separate.iter().sum::<usize>(), joint.iter().sum::<usize>());
        } else {
            println!("{agree} same, {differ} different");
        }

        // Unwind: retract each program; the optimizer's output should empty out.
        let unwound = Instant::now();
        for (_, rows) in &loaded {
            let (n, e) = updates(rows, -1);
            server.feed_batch("opt", 0, n).unwrap();
            server.feed_batch("opt", 1, e).unwrap();
            server.tick(worker);
        }
        let left = read_result(&mut server, worker);
        println!(
            "retracted {} programs in {:.1} ms; {} nodes and {} edges remain",
            loaded.len(), unwound.elapsed().as_secs_f64() * 1e3, left.nodes.len(), left.edges.len(),
        );
    });
}
