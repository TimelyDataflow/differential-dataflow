//! DD IR vec-backed driver: parse, lower, render (via `interactive::backend::vec`), execute.
//!
//! With `--explain`, applies the explanation rewrite after lowering and treats
//! the last input handle as the query input (seeded from the `QUERY` env var,
//! format `"key_fields:val_fields"`).

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;

use interactive::parse;
use interactive::lower;
use interactive::scope_ir as st;
use interactive::ir::Diff;
use interactive::backend::vec::{render_tree, Row};

fn run(
    name: &str,
    stmts: Vec<parse::Stmt>,
    n_inputs: usize,
    nodes: u64,
    edges: u64,
    arity: usize,
    batch: u64,
    rounds: Option<u64>,
    explain: bool,
) {
    let mut query_shape: Option<(usize, usize)> = None;
    let mut tree = lower::lower_tree(stmts);
    // CLONE_RT=1 routes the program through the explain rewrite's
    // clone-with-lifts as an identity check: outputs must be unchanged.
    if std::env::var("CLONE_RT").is_ok() {
        tree = interactive::explain::clone_identity(&tree);
    }
    // --explain: rewrite for self-explanation before optimization (the
    // rules assume single-op Linears). Sources are the root's imports.
    if explain {
        let source_shapes: Vec<(usize, usize)> = tree.root.imports.iter().map(|imp| match &imp.from {
            interactive::scope_ir::Source::Input(_) => (arity, 0usize),
            other => panic!("ddir_vec --explain: unsupported source {:?}", other),
        }).collect();
        query_shape = Some(interactive::explain::export_shape(&tree, &source_shapes));
        tree = interactive::explain::explain(&tree, &source_shapes);
    }
    let ops_before = tree.op_count();
    tree.optimize();
    let tree_export_idx = tree.root.exports.iter().position(|e| e.name == "result").unwrap_or(0);
    println!("{}: {} ops before optimize, {} after; driving export {:?}",
        name, ops_before, tree.op_count(), tree.root.exports[tree_export_idx].name);
    let name = name.to_string();
    let total_inputs = if explain { n_inputs + 1 } else { n_inputs };
    let query_input_idx = if explain { Some(n_inputs) } else { None };

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        // DIAG=1 registers timely/DD logging and serves the diagnostics
        // WebSocket on worker 0 (port 51371) — see diagnostics/README.md.
        let _diag = if std::env::var("DIAG").is_ok() {
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
            for _ in 0..total_inputs {
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
        // Real inputs are 0..n_inputs. The query input (if any) is at
        // n_inputs and is seeded separately below.
        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % n_inputs;
                inputs[input_idx].update(interactive::gen_row::<Row>(e, nodes, arity), 1);
            }
        }
        // Seed the query input (worker 0 only) from $QUERY = "k:v[,q]".
        if let Some(q_idx) = query_input_idx {
            if index == 0 {
                if let Ok(qstr) = std::env::var("QUERY") {
                    let parse_row = |s: &str| -> Row {
                        if s.is_empty() {
                            Row::new()
                        } else {
                            s.split(',').map(|t| t.trim().parse::<i64>().unwrap()).collect()
                        }
                    };
                    let (k_str, vq_str) = qstr.split_once(':').unwrap_or((qstr.as_str(), ""));
                    let q_key: Row = parse_row(k_str);
                    let mut q_val: Row = parse_row(vq_str);
                    if q_val.is_empty() { q_val.push(0); }
                    if let Some((qk, qv)) = query_shape {
                        assert!(q_key.len() == qk && q_val.len() == qv + 1,
                            "QUERY shape mismatch: export is (k={}, v={}), so QUERY needs {} key field(s) and {} val field(s) + q; got key={:?} val_with_q={:?}",
                            qk, qv, qk, qv, q_key, q_val);
                    }
                    eprintln!("seeding query: key={:?} val_with_q={:?}", q_key, q_val);
                    inputs[q_idx].update((q_key, q_val), 1);
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
                    inputs[input_idx].update(interactive::gen_row::<Row>(remove_idx, nodes, arity), -1);
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % n_inputs;
                    inputs[input_idx].update(interactive::gen_row::<Row>(add_idx, nodes, arity), 1);
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
    // Strip an optional leading --explain flag.
    let raw_args: Vec<String> = std::env::args().collect();
    let (explain, args): (bool, Vec<String>) = {
        let mut it = raw_args.into_iter();
        let prog = it.next().unwrap();
        let mut explain = false;
        let mut rest: Vec<String> = Vec::new();
        for a in it {
            if a == "--explain" { explain = true; } else { rest.push(a); }
        }
        let mut out = vec![prog]; out.extend(rest);
        (explain, out)
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
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds, explain);
}
