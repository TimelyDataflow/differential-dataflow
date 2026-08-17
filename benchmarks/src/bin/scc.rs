//! Four-way SCC benchmark.
//!
//! This compares optimized compiled differential dataflow, a typed transcription
//! of the DDIR plan, DDIR over Vec rows, and DDIR over Corgi columns.

use std::collections::BTreeMap;
use std::hash::Hash;
use std::io::Write;
use std::sync::mpsc::{channel, Receiver};
use std::time::Instant;

use benchmarks::{consolidate, graph, revision, Context, EdgeResult, Record, Run, Timings};
use differential_dataflow::algorithms::graphs::scc::strongly_connected_at;
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::VecCollection;
use interactive::backend::vec::Row;
use interactive::backend::{corgi, vec};
use interactive::ir::{Diff, Value};
use interactive::{lower, parse, scope_ir};
use mimalloc::MiMalloc;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::{Capture, Probe};
use timely::order::Product;
use timely::progress::Timestamp;

#[global_allocator]
static ALLOCATOR: MiMalloc = MiMalloc;

const SCC: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let trans = edges | key($1 ; $0);

    outer: {
        let scc = edges + trim;

        fwd: {
            let nodes = edges | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }

        let trim_fwd = edges
            | join(fwd::labels, ($1 ; $0, $2))
            | join(fwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);

        bwd: {
            let nodes = trans | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(trim_fwd, ($2 ; $1));
        }

        let trim_bwd = trans
            | join(bwd::labels, ($1 ; $0, $2))
            | join(bwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);

        var trim = trim_bwd - edges;
    }

    export "result" = outer::scc | arrange;
"#;

#[derive(Clone, Copy, Debug)]
enum Implementation {
    Compiled,
    CompiledDdir,
    Vec,
    Corgi,
}

impl Implementation {
    const ALL: [Implementation; 4] = [
        Implementation::Compiled,
        Implementation::CompiledDdir,
        Implementation::Vec,
        Implementation::Corgi,
    ];

    fn run(self, edges: &[(u64, u64)]) -> Run {
        match self {
            Implementation::Compiled => run_compiled(edges, false),
            Implementation::CompiledDdir => run_compiled(edges, true),
            Implementation::Vec => run_ddir(edges, false),
            Implementation::Corgi => run_ddir(edges, true),
        }
    }
}

#[derive(Debug)]
struct Config {
    nodes: u64,
    edges: u64,
    seed: u64,
    warmup: usize,
    runs: usize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut nodes = 1_000;
        let mut edges = None;
        let mut seed = 0xc0ff_ee42;
        let mut warmup = 0;
        let mut runs = 1;
        let mut arguments = std::env::args().skip(1);

        while let Some(arg) = arguments.next() {
            let value = |name: &str, values: &mut std::iter::Skip<std::env::Args>| {
                values
                    .next()
                    .ok_or_else(|| format!("{name} requires a value"))
            };
            match arg.as_str() {
                "--nodes" => nodes = parse_u64("--nodes", &value("--nodes", &mut arguments)?)?,
                "--edges" => {
                    edges = Some(parse_u64("--edges", &value("--edges", &mut arguments)?)?)
                }
                "--seed" => seed = parse_u64("--seed", &value("--seed", &mut arguments)?)?,
                "--warmup" => {
                    warmup = parse_usize("--warmup", &value("--warmup", &mut arguments)?)?
                }
                "--runs" => runs = parse_usize("--runs", &value("--runs", &mut arguments)?)?,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }

        if nodes == 0 {
            return Err("--nodes must be positive".to_owned());
        }
        if nodes > i64::MAX as u64 {
            return Err("--nodes must fit in an i64".to_owned());
        }
        if runs == 0 {
            return Err("--runs must be positive".to_owned());
        }
        let edges = match edges {
            Some(edges) => edges,
            None => nodes
                .checked_mul(2)
                .ok_or_else(|| "the default edge count overflows u64".to_owned())?,
        };

        Ok(Config {
            nodes,
            edges,
            seed,
            warmup,
            runs,
        })
    }
}

fn parse_u64(name: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("invalid {name} value {value:?}"))
}

fn parse_usize(name: &str, value: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("invalid {name} value {value:?}"))
}

fn print_usage() {
    eprintln!("usage: scc [--nodes N] [--edges E] [--seed S] [--warmup N] [--runs N]");
    eprintln!("writes one JSON object per measured implementation run to stdout");
}

/// The typed transcription of the DDIR SCC plan.
fn strongly_connected_ddir<'scope>(
    graph: VecCollection<'scope, u64, (u64, u64), isize>,
) -> VecCollection<'scope, u64, (u64, u64), isize> {
    let parent = graph.scope();
    parent.scoped::<Product<_, usize>, _, _>("StronglyConnectedDdir", |outer| {
        let edges = graph.enter(outer);
        let trans = edges.clone().map(|(src, dst)| (dst, src));
        let (variable, trim) = Variable::new(outer, Product::new(Default::default(), 1usize));
        let scc = edges.clone().concat(trim);

        let trim_fwd = trim_edges_ddir(scc.clone(), edges.clone(), "Forward");
        let trim_bwd = trim_edges_ddir(trim_fwd, trans, "Backward");
        variable.set(trim_bwd.concat(edges.negate()));

        scc.leave(parent)
    })
}

/// One typed transcription of DDIR's forward or backward trim pass.
fn trim_edges_ddir<'scope, T>(
    cycle: VecCollection<'scope, T, (u64, u64), isize>,
    edges: VecCollection<'scope, T, (u64, u64), isize>,
    name: &str,
) -> VecCollection<'scope, T, (u64, u64), isize>
where
    T: Timestamp + Lattice + Hash,
{
    let labels = propagate_ddir(cycle, edges.clone(), name);

    edges
        .join_map(labels.clone(), |src, dst, label| (*dst, (*src, *label)))
        .join_map(labels, |dst, left, right| (*dst, (left.0, left.1, *right)))
        .filter(|(_, (_, left, right))| left == right)
        .map(|(dst, (src, _, _))| (dst, src))
}

/// DDIR's recursive `proposals + nodes | min` label propagation.
fn propagate_ddir<'scope, T>(
    cycle: VecCollection<'scope, T, (u64, u64), isize>,
    edges: VecCollection<'scope, T, (u64, u64), isize>,
    name: &str,
) -> VecCollection<'scope, T, (u64, u64), isize>
where
    T: Timestamp + Lattice + Hash,
{
    let parent = edges.scope();
    parent.scoped::<Product<_, usize>, _, _>(name, |inner| {
        let cycle = cycle.enter(inner);
        let nodes = edges
            .map(|(_, dst)| (dst, dst))
            .enter_at(inner, |(_, node)| priority_round(*node));
        let (variable, proposals) = Variable::new(inner, Product::new(Default::default(), 1usize));
        let labels = proposals
            .concat(nodes)
            .reduce(|_, values, output| output.push((*values[0].0, 1)));
        variable.set(
            labels
                .clone()
                .join_map(cycle, |_, label, dst| (*dst, *label)),
        );
        labels.leave(parent)
    })
}

fn priority_round(node: u64) -> usize {
    256 * (64 - node.leading_zeros() as usize)
}

fn run_compiled(edges: &[(u64, u64)], ddir_plan: bool) -> Run {
    type NativeEvent = Event<u64, Vec<((u64, u64), u64, isize)>>;

    let implementation = if ddir_plan {
        "compiled-ddir"
    } else {
        "compiled"
    };
    let owned_edges = edges.to_vec();
    let (send, recv) = channel::<NativeEvent>();

    let timings = timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::new();

        let build_started = Instant::now();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, graph) = scope.new_collection::<(u64, u64), isize>();
            let output = if ddir_plan {
                strongly_connected_ddir(graph)
            } else {
                // This is the optimized compiled counterpart to DDIR's `enter_at($1[0])`.
                strongly_connected_at(graph, |node| *node)
            };
            output
                .consolidate()
                .inner
                .probe_with(&probe)
                .capture_into(send);
            input
        });
        let build = build_started.elapsed();

        let ingest_started = Instant::now();
        for &edge in &owned_edges {
            input.insert(edge);
        }
        input.advance_to(1);
        input.flush();
        let ingest = ingest_started.elapsed();

        let stabilize_started = Instant::now();
        while probe.less_than(&1) {
            worker.step();
        }
        let stabilize = stabilize_started.elapsed();

        Timings {
            prepare: Default::default(),
            build,
            ingest,
            stabilize,
        }
    });

    Run {
        implementation,
        timings,
        output: collect_native(recv),
    }
}

fn run_ddir(edges: &[(u64, u64)], use_corgi: bool) -> Run {
    type DdirEvent = Event<u64, Vec<((Row, Row), u64, Diff)>>;

    let implementation = if use_corgi { "ddir-corgi" } else { "ddir-vec" };
    let prepare_started = Instant::now();
    let mut program = lower::lower_tree(parse::pipe::parse(SCC));
    program.optimize();
    let prepare = prepare_started.elapsed();
    let export = program
        .root
        .exports
        .iter()
        .position(|item| item.name == "result")
        .unwrap();
    let rows: Vec<(Row, Row)> = edges
        .iter()
        .map(|&(src, dst)| (tuple(&[src, dst]), Value::unit()))
        .collect();
    let (send, recv) = channel::<DdirEvent>();

    let rest = timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::new();

        let build_started = Instant::now();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let imports = program
                    .root
                    .imports
                    .iter()
                    .map(|item| match item.from {
                        scope_ir::Source::Input(0) => collection.clone().enter(inner),
                        ref other => panic!("unexpected SCC source {other:?}"),
                    })
                    .collect();
                let exports = if use_corgi {
                    corgi::render_tree_rows(&program.root, inner, 0, imports)
                } else {
                    vec::render_tree(&program.root, inner, 0, imports)
                };
                exports[export].clone().leave(scope)
            });
            output.inner.probe_with(&probe).capture_into(send);
            input
        });
        let build = build_started.elapsed();

        let ingest_started = Instant::now();
        for row in rows {
            input.update(row, 1);
        }
        input.advance_to(1);
        input.flush();
        let ingest = ingest_started.elapsed();

        let stabilize_started = Instant::now();
        while probe.less_than(&1) {
            worker.step();
        }
        let stabilize = stabilize_started.elapsed();

        Timings {
            prepare: Default::default(),
            build,
            ingest,
            stabilize,
        }
    });

    let timings = Timings { prepare, ..rest };
    Run {
        implementation,
        timings,
        output: collect_ddir(recv),
    }
}

fn collect_native(receiver: Receiver<Event<u64, Vec<((u64, u64), u64, isize)>>>) -> EdgeResult {
    consolidate(receiver.into_iter().flat_map(|event| {
        match event {
            Event::Messages(_, data) => data
                .into_iter()
                .map(|(edge, _, diff)| (edge, i64::try_from(diff).unwrap()))
                .collect(),
            Event::Progress(_) => Vec::new(),
        }
    }))
}

fn collect_ddir(receiver: Receiver<Event<u64, Vec<((Row, Row), u64, Diff)>>>) -> EdgeResult {
    consolidate(receiver.into_iter().flat_map(|event| {
        match event {
            Event::Messages(_, data) => data
                .into_iter()
                .map(|((key, val), _, diff)| ((scalar(&key), scalar(&val)), diff))
                .collect(),
            Event::Progress(_) => Vec::new(),
        }
    }))
}

fn tuple(fields: &[u64]) -> Value {
    Value::Tuple(
        fields
            .iter()
            .map(|field| Value::Int(*field as i64))
            .collect(),
    )
}

fn scalar(value: &Value) -> u64 {
    match value {
        Value::Int(value) => u64::try_from(*value).unwrap(),
        Value::Tuple(fields) if fields.len() == 1 => scalar(&fields[0]),
        other => panic!("expected one integer field, found {other:?}"),
    }
}

fn check(runs: &BTreeMap<&'static str, Run>) -> Result<(), String> {
    let expected = &runs["compiled-ddir"].output;
    for run in runs.values() {
        if run.output != *expected {
            let first = run
                .output
                .iter()
                .zip(expected)
                .find(|(actual, expected)| actual != expected);
            return Err(format!(
                "{} disagrees with compiled-ddir: lengths {} and {}, first paired difference {first:?}",
                run.implementation,
                run.output.len(),
                expected.len(),
            ));
        }
    }
    Ok(())
}

fn run_set(edges: &[(u64, u64)], ordinal: usize) -> Result<BTreeMap<&'static str, Run>, String> {
    let mut implementations = Implementation::ALL;
    let count = implementations.len();
    implementations.rotate_left(ordinal % count);
    let mut runs = BTreeMap::new();
    for implementation in implementations {
        let run = implementation.run(edges);
        runs.insert(run.implementation, run);
    }
    check(&runs)?;
    Ok(runs)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(error) => {
            print_usage();
            return Err(error.into());
        }
    };
    let edges = graph(config.nodes, config.edges, config.seed);
    let revision = revision();

    eprintln!(
        "scc: nodes={} edges={} seed={} warmup={} runs={}",
        config.nodes, config.edges, config.seed, config.warmup, config.runs,
    );

    for warmup in 0..config.warmup {
        let _ = run_set(&edges, warmup)?;
    }

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    for run_number in 0..config.runs {
        let runs = run_set(&edges, config.warmup + run_number)?;
        let context = Context {
            benchmark: "scc",
            revision: &revision,
            run: run_number,
            nodes: config.nodes,
            edges: config.edges,
            seed: config.seed,
        };
        for run in runs.values() {
            let record = Record::from_run(context, run, true);
            serde_json::to_writer(&mut stdout, &record)?;
            writeln!(&mut stdout)?;
        }

        let compiled = runs["compiled"].timings.measured().as_secs_f64();
        let matched = runs["compiled-ddir"].timings.measured().as_secs_f64();
        let vec = runs["ddir-vec"].timings.measured().as_secs_f64();
        let corgi = runs["ddir-corgi"].timings.measured().as_secs_f64();
        eprintln!(
            "run {run_number}: compiled={compiled:.6}s compiled-ddir={matched:.6}s ({:.2}x) vec={vec:.6}s ({:.2}x) corgi={corgi:.6}s ({:.2}x vec, {:.2}x compiled-ddir)",
            matched / compiled,
            vec / matched,
            corgi / vec,
            corgi / matched,
        );
    }

    Ok(())
}
