//! DD IR vec-backed driver: parse, lower, render (via `interactive::backend::vec`), execute.
//!
//! Usage: `ddir_vec [flags] <program> <arity> <nodes> <edges> [batch] [rounds] [timely args]`
//!
//! Inputs: with `EDGES_FILE` set, rows come from that file — one row per line,
//! whitespace-separated `i64` fields, assigned round-robin to the program's
//! inputs (`line % n_inputs`). Otherwise rows are synthesized by `gen_row`.
//!
//! Flags:
//! - `--diag`: serve timely/DD diagnostics on port 51371.
//!
//! (The `--explain` self-explanation path is disabled on the Value-model port;
//! it returns with the explain stage.)

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;

use interactive::parse;
use interactive::lower;
use interactive::scope_ir as st;
use interactive::ir::{Diff, Value};
use interactive::backend::vec::{render_tree, Row};

#[derive(Clone, Default)]
struct Flags {
    diag: bool,
}

fn run(
    name: &str,
    stmts: Vec<parse::Stmt>,
    n_inputs: usize,
    nodes: u64,
    edges: u64,
    arity: usize,
    batch: u64,
    rounds: Option<u64>,
    flags: Flags,
    timely_args: Vec<String>,
) {
    let mut tree = lower::lower_tree(stmts);
    let ops_before = tree.op_count();
    tree.optimize();
    let tree_export_idx = tree.root.exports.iter().position(|e| e.name == "result").unwrap_or(0);
    println!("{}: {} ops before optimize, {} after; driving export {:?}",
        name, ops_before, tree.op_count(), tree.root.exports[tree_export_idx].name);
    let name = name.to_string();
    let edges_file = std::env::var("EDGES_FILE").ok();

    timely::execute_from_args(timely_args.into_iter(), move |worker| {

        // --diag registers timely/DD logging and serves the diagnostics
        // WebSocket on worker 0 (port 51371) — see diagnostics/README.md.
        let _diag = if flags.diag {
            let state = diagnostics::logging::register(worker, false);
            if worker.index() == 0 {
                Some(diagnostics::server::Server::start(51371, state.sink))
            } else {
                drop(state.sink);
                None
            }
        } else { None };

        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let (h, c) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(h); collections.push(c);
            }
            let mut probe = timely::dataflow::ProbeHandle::new();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = tree.root.imports.iter().map(|imp| match &imp.from {
                    st::Source::Input(n) => entered[*n].clone(),
                    st::Source::Trace(name) => panic!("ddir_vec: Import {:?} not supported in this harness (no trace registry).", name),
                    st::Source::Parent(_) => unreachable!("root scope cannot import from a parent"),
                }).collect();
                let exports = render_tree(&tree.root, inner, 0, root_imports);
                exports[tree_export_idx].clone().leave(scope)
            });
            output.probe_with(&mut probe);
            (handles, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        let timer = std::time::Instant::now();
        let timer_load = std::time::Instant::now();
        // With `EDGES_FILE` set, rows come from the file (one row per line,
        // whitespace-separated i64 fields), assigned round-robin to inputs.
        // Otherwise `gen_row` synthesizes `edges` rows.
        if let Some(path) = &edges_file {
            let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("Cannot read {}: {}", path, e));
            for (e, line) in text.lines().filter(|l| !l.trim().is_empty()).enumerate() {
                if e % peers == index {
                    let input_idx = e % n_inputs;
                    let fields: Vec<Value> = line.split_whitespace().map(|t| Value::Int(t.parse::<i64>().unwrap())).collect();
                    inputs[input_idx].update((Value::Tuple(fields), Value::unit()), 1);
                }
            }
        } else {
            for e in 0..edges {
                if (e as usize) % peers == index {
                    let input_idx = (e as usize) % n_inputs;
                    inputs[input_idx].update(interactive::gen_row(e, nodes, arity), 1);
                }
            }
        }
        for i in inputs.iter_mut() { i.advance_to(1); i.flush(); }
        while probe.less_than(&1u64) { worker.step(); }
        println!("worker {}: {} loaded ({} edges, total {:.2?}, load {:.2?})", index, name, edges, timer.elapsed(), timer_load.elapsed());

        let mut cursor = 0u64;
        let mut round = 0u64;
        let limit = rounds.unwrap_or(u64::MAX);
        while round < limit {
            let timer_round = std::time::Instant::now();
            let time = (round + 2) as u64;
            for _ in 0..batch {
                let remove_idx = cursor;
                let add_idx = edges + cursor;
                if (remove_idx as usize) % peers == index {
                    let input_idx = (remove_idx as usize) % n_inputs;
                    inputs[input_idx].update(interactive::gen_row(remove_idx, nodes, arity), -1);
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % n_inputs;
                    inputs[input_idx].update(interactive::gen_row(add_idx, nodes, arity), 1);
                }
                cursor += 1;
            }
            for i in inputs.iter_mut() { i.advance_to(time); i.flush(); }
            while probe.less_than(&time) { worker.step(); }

            round += 1;
            if round % 100 == 0 {
                println!("worker {}: {} round {} (total {:.2?}, round {:.2?})", index, name, round, timer.elapsed(), timer_round.elapsed());
            }
        }
        println!("worker {}: {} done ({} rounds, batch {}, total {:.2?})", index, name, round, batch, timer.elapsed());
    }).unwrap();
}

fn main() {
    // Strip our flags; everything else stays positional (and the tail is
    // forwarded to timely).
    let raw_args: Vec<String> = std::env::args().collect();
    let (flags, args): (Flags, Vec<String>) = {
        let mut it = raw_args.into_iter();
        let prog = it.next().unwrap();
        let mut flags = Flags::default();
        let mut rest: Vec<String> = Vec::new();
        for a in it {
            if a == "--diag" { flags.diag = true; }
            else { rest.push(a); }
        }
        let mut out = vec![prog]; out.extend(rest);
        (flags, out)
    };
    let program = args.get(1).cloned().unwrap_or_else(|| { std::process::exit(0); });
    let arity: usize = args.get(2).cloned().unwrap_or("2".into()).parse().unwrap();
    let nodes: u64 = args.get(3).cloned().unwrap_or("10".into()).parse().unwrap();
    let edges: u64 = args.get(4).cloned().unwrap_or_else(|| (2 * nodes).to_string()).parse().unwrap();
    let batch: u64 = args.get(5).cloned().unwrap_or("1".into()).parse().unwrap();
    let rounds: Option<u64> = args.get(6).map(|s| s.parse().unwrap());

    let source = interactive::load_program(&program);
    let stmts = if program.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let (n_inputs, imports) = interactive::survey_sources(&stmts);
    if !imports.is_empty() {
        panic!("ddir_vec: program references imports {:?} but this harness has no trace registry.", imports);
    }
    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    let timely_args: Vec<String> = args.iter().skip(4).cloned().collect();
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds, flags, timely_args);
}
