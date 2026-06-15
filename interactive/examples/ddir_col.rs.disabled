//! DD IR columnar driver: parse, lower, render (via `interactive::backend::col`), execute.

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use differential_dataflow::dynamic::pointstamp::PointStamp;

use interactive::parse;
use interactive::lower;
use interactive::backend::col::{render_tree, Row, Diff};

type DdirOuterUpdate = (Row, Row, u64, Diff);

fn run(name: &str, stmts: Vec<parse::Stmt>, n_inputs: usize, nodes: u64, edges: u64, arity: usize, batch: u64, rounds: Option<u64>) {
    let mut tree = lower::lower_tree(stmts);
    let ops_before = tree.op_count();
    tree.optimize();
    let tree_export_idx = tree.root.exports.iter().position(|e| e.name == "result").unwrap_or(0);
    println!("{}: {} ops before optimize, {} after; driving export {:?}",
        name, ops_before, tree.op_count(), tree.root.exports[tree_export_idx].name);
    let name = name.to_string();

    timely::execute_from_args(std::env::args().skip(4), move |worker| {
        use timely::dataflow::InputHandle;
        use timely::container::PushInto;
        use differential_dataflow::columnar::ValColBuilder;

        type OuterBuilder = ValColBuilder<DdirOuterUpdate>;

        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let mut h = <InputHandle<_, OuterBuilder>>::new_with_builder();
                let stream = h.to_stream(scope);
                handles.push(h);
                collections.push(differential_dataflow::Collection::new(stream));
            }
            let mut probe = timely::dataflow::ProbeHandle::new();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = tree.root.imports.iter().map(|imp| match &imp.from {
                    interactive::scope_ir::Source::Input(n) => entered[*n].clone(),
                    interactive::scope_ir::Source::Trace(name) => panic!("ddir_col: Import {:?} not supported in this harness (no trace registry).", name),
                    interactive::scope_ir::Source::Parent(_) => unreachable!("root scope cannot import from a parent"),
                }).collect();
                let exports = render_tree(&tree.root, inner, 0, root_imports);
                exports[tree_export_idx].clone().leave(scope)
            });
            output.probe_with(&mut probe);
            (handles, probe)
        });

        let index = worker.index();
        let peers = worker.peers();

        let mut builders: Vec<OuterBuilder> = (0..n_inputs).map(|_| OuterBuilder::default()).collect();

        let timer = std::time::Instant::now();
        let timer_load = std::time::Instant::now();
        for e in 0..edges {
            if (e as usize) % peers == index {
                let input_idx = (e as usize) % inputs.len();
                let (key, val) = interactive::gen_row::<Row>(e, nodes, arity);
                let time = *inputs[input_idx].time();
                builders[input_idx].push_into((key, val, time, 1i64));
            }
        }
        for (i, h) in inputs.iter_mut().enumerate() {
            use timely::container::ContainerBuilder;
            while let Some(container) = builders[i].finish() { h.send_batch(container); }
            h.advance_to(1);
            h.flush();
        }
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
                    let input_idx = (remove_idx as usize) % inputs.len();
                    let (key, val) = interactive::gen_row::<Row>(remove_idx, nodes, arity);
                    builders[input_idx].push_into((key, val, time, -1i64));
                }
                if (add_idx as usize) % peers == index {
                    let input_idx = (add_idx as usize) % inputs.len();
                    let (key, val) = interactive::gen_row::<Row>(add_idx, nodes, arity);
                    builders[input_idx].push_into((key, val, time, 1i64));
                }
                cursor += 1;
            }
            for (i, h) in inputs.iter_mut().enumerate() {
                use timely::container::ContainerBuilder;
                while let Some(container) = builders[i].finish() { h.send_batch(container); }
                h.advance_to(time);
                h.flush();
            }
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
    let program = std::env::args().nth(1).unwrap_or_else(|| { std::process::exit(0); });
    let arity: usize = std::env::args().nth(2).unwrap_or("2".into()).parse().unwrap();
    let nodes: u64 = std::env::args().nth(3).unwrap_or("10".into()).parse().unwrap();
    let edges: u64 = std::env::args().nth(4).unwrap_or_else(|| (2 * nodes).to_string()).parse().unwrap();
    let batch: u64 = std::env::args().nth(5).unwrap_or("1".into()).parse().unwrap();
    let rounds: Option<u64> = std::env::args().nth(6).map(|s| s.parse().unwrap());

    let source = interactive::load_program(&program);
    let stmts = if program.ends_with(".ddp") {
        parse::pipe::parse(&source)
    } else {
        parse::applicative::parse(&source)
    };
    let (n_inputs, imports) = interactive::survey_sources(&stmts);
    if !imports.is_empty() {
        panic!("ddir_col: program references imports {:?} but this harness has no trace registry.", imports);
    }
    let name = std::path::Path::new(&program).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or(program.clone());
    run(&name, stmts, n_inputs, nodes, edges, arity, batch, rounds);
}
