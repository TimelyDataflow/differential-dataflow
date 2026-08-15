//! Apples-to-apples SCC benchmark: compiled Differential vs DDIR Vec vs DDIR Corgi.
//!
//! All three variants consume the same deterministic graph and update stream. The compiled
//! baseline uses `strongly_connected_at(_, |x| *x as u64)`, matching the logarithmic label staging
//! performed by DDIR's `enter_at($1[0])`. The measured output is only probed; it is not inspected or
//! captured. DDIR parsing, lowering, optimization, and dataflow construction are outside the load
//! and update timers.
//!
//! ```text
//! N=100000 E=200000 BATCH=1000 ROUNDS=20 ITERS=3 \
//!   cargo run --release -p interactive --example scc_compare
//! ```
//!
//! `BACKENDS=compiled,vec,corgi` selects variants. `CHECK=1` first checks Vec/Corgi result parity
//! on a bounded prefix of the graph (default on). Corgi is currently single-worker, so this
//! benchmark intentionally uses `timely::execute_directly` for every variant.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use differential_dataflow::algorithms::graphs::scc::strongly_connected_at;
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;
use interactive::backend::{corgi, vec};
use interactive::ir::{Diff, Value};
use interactive::scope_ir as st;
use interactive::{lower, parse};
use timely::dataflow::operators::probe::Handle;
use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

export "result" = outer::scc | map(;) | arrange;
"#;

#[derive(Clone, Copy, Debug)]
enum Backend {
    Compiled,
    Vec,
    Corgi,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Backend::Compiled => "compiled",
            Backend::Vec => "vec",
            Backend::Corgi => "corgi",
        }
    }
}

#[derive(Debug)]
struct Stats {
    load: Duration,
    rounds: Vec<Duration>,
    operators: Vec<(String, Duration)>,
    reduce_phases: Vec<interactive::corgi::reduce::ReducePhaseStat>,
    proxy_reduce_phases:
        Vec<differential_dataflow::operators::int_proxy::reduce::ProxyReducePhaseStat>,
}

#[derive(Default)]
struct OperatorProfile {
    operators: BTreeMap<usize, (String, Vec<usize>)>,
    elapsed: BTreeMap<usize, Duration>,
}

macro_rules! install_operator_profile {
    ($worker:expr) => {{
        let profile = Rc::new(RefCell::new(OperatorProfile::default()));
        if std::env::var("PROFILE_OPS").is_ok_and(|x| x != "0") {
            let target = Rc::clone(&profile);
            let mut starts = BTreeMap::new();
            $worker
                .log_register()
                .expect("timely logging registry")
                .insert::<TimelyEventBuilder, _>("timely", move |_time, data| {
                    if let Some(events) = data {
                        for (event_time, event) in events.iter() {
                            match event {
                                TimelyEvent::Operates(event) => {
                                    target
                                        .borrow_mut()
                                        .operators
                                        .insert(event.id, (event.name.clone(), event.addr.clone()));
                                }
                                TimelyEvent::Schedule(event) => match event.start_stop {
                                    StartStop::Start => {
                                        starts.insert(event.id, *event_time);
                                    }
                                    StartStop::Stop => {
                                        if let Some(start) = starts.remove(&event.id) {
                                            *target
                                                .borrow_mut()
                                                .elapsed
                                                .entry(event.id)
                                                .or_default() += event_time.saturating_sub(start);
                                        }
                                    }
                                },
                                _ => {}
                            }
                        }
                    }
                });
        }
        profile
    }};
}

fn finish_operator_profile(profile: Rc<RefCell<OperatorProfile>>) -> Vec<(String, Duration)> {
    let profile = profile.borrow();
    let addresses: Vec<&Vec<usize>> = profile
        .operators
        .values()
        .map(|(_, address)| address)
        .collect();
    let mut operators: Vec<_> = profile
        .elapsed
        .iter()
        .filter_map(|(id, elapsed)| {
            let (name, address) = profile
                .operators
                .get(id)
                .cloned()
                .unwrap_or_else(|| (format!("operator-{id}"), Vec::new()));
            let has_child = addresses
                .iter()
                .any(|other| other.len() > address.len() && other.starts_with(&address));
            (!has_child).then_some((name, *elapsed))
        })
        .collect();
    operators.sort_unstable_by_key(|(_, elapsed)| std::cmp::Reverse(*elapsed));
    operators
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(default)
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn edge(index: usize, nodes: usize) -> (usize, usize) {
    let first = mix(index as u64);
    let second = mix(first);
    ((first as usize) % nodes, (second as usize) % nodes)
}

fn row_edge(index: usize, nodes: usize) -> (Value, Value) {
    let (src, dst) = edge(index, nodes);
    (
        Value::Tuple(vec![Value::Int(src as i64), Value::Int(dst as i64)]),
        Value::unit(),
    )
}

fn compiled(nodes: usize, edges: usize, batch: usize, rounds: usize) -> Stats {
    timely::execute_directly(move |worker| {
        let operator_profile = install_operator_profile!(worker);
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            strongly_connected_at(graph, |x| *x as u64).probe_with(&mut probe);
            input
        });

        let timer = Instant::now();
        for index in 0..edges {
            input.insert(edge(index, nodes));
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        let load = timer.elapsed();

        let mut times = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let timer = Instant::now();
            for offset in 0..batch {
                let index = round * batch + offset;
                input.remove(edge(index, nodes));
                input.insert(edge(edges + index, nodes));
            }
            input.advance_to(round + 2);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            times.push(timer.elapsed());
        }
        worker
            .log_register()
            .expect("timely logging registry")
            .flush();
        let operators = finish_operator_profile(operator_profile);
        Stats {
            load,
            rounds: times,
            operators,
            reduce_phases: Vec::new(),
            proxy_reduce_phases: Vec::new(),
        }
    })
}

fn interpreted(
    backend: Backend,
    program: &st::Program,
    nodes: usize,
    edges: usize,
    batch: usize,
    rounds: usize,
) -> Stats {
    let program = program.clone();
    timely::execute_directly(move |worker| {
        if matches!(backend, Backend::Corgi) {
            interactive::corgi::reduce::reset_phase_profile();
            differential_dataflow::operators::int_proxy::reduce::reset_phase_profile();
        }
        let operator_profile = install_operator_profile!(worker);
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Value, Value), Diff>();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered = collection.clone().enter(inner);
                let imports = program
                    .root
                    .imports
                    .iter()
                    .map(|import| match &import.from {
                        st::Source::Input(0) => entered.clone(),
                        ref other => panic!("unexpected SCC import {other:?}"),
                    })
                    .collect();
                let exports = match backend {
                    Backend::Vec => vec::render_tree(&program.root, inner.clone(), 0, imports),
                    Backend::Corgi => {
                        corgi::render_tree_rows(&program.root, inner.clone(), 0, imports)
                    }
                    Backend::Compiled => unreachable!(),
                };
                exports
                    .into_iter()
                    .next()
                    .expect("SCC result export")
                    .leave(scope)
            });
            output.probe_with(&mut probe);
            input
        });

        let timer = Instant::now();
        for index in 0..edges {
            input.update(row_edge(index, nodes), 1);
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        let load = timer.elapsed();

        let mut times = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let timer = Instant::now();
            for offset in 0..batch {
                let index = round * batch + offset;
                input.update(row_edge(index, nodes), -1);
                input.update(row_edge(edges + index, nodes), 1);
            }
            input.advance_to((round + 2) as u64);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            times.push(timer.elapsed());
        }
        worker
            .log_register()
            .expect("timely logging registry")
            .flush();
        let operators = finish_operator_profile(operator_profile);
        let reduce_phases = if matches!(backend, Backend::Corgi) {
            interactive::corgi::reduce::phase_profile()
        } else {
            Vec::new()
        };
        let proxy_reduce_phases = if matches!(backend, Backend::Corgi) {
            differential_dataflow::operators::int_proxy::reduce::phase_profile()
        } else {
            Vec::new()
        };
        Stats {
            load,
            rounds: times,
            operators,
            reduce_phases,
            proxy_reduce_phases,
        }
    })
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn print_operator_profile(backend: Backend, operators: &[(String, Duration)]) {
    if operators.is_empty() {
        return;
    }
    let total: Duration = operators.iter().map(|(_, elapsed)| *elapsed).sum();
    eprintln!(
        "{} operator schedule profile (sum {:?}):",
        backend.name(),
        total
    );
    for (name, elapsed) in operators.iter().take(20) {
        eprintln!(
            "  {:>8.2}%  {:>12?}  {}",
            100.0 * elapsed.as_secs_f64() / total.as_secs_f64(),
            elapsed,
            name,
        );
    }
}

fn print_reduce_profile(stats: &[interactive::corgi::reduce::ReducePhaseStat]) {
    if stats.is_empty() {
        return;
    }
    let measured: Duration = stats.iter().map(|stat| stat.elapsed).sum();
    eprintln!("CorgiReduce exclusive phase profile (sum {measured:?}):");
    for stat in stats {
        eprintln!(
            "  {:>8.2}%  {:>12?}  calls {:>8}  work {:>12}  {}",
            100.0 * stat.elapsed.as_secs_f64() / measured.as_secs_f64(),
            stat.elapsed,
            stat.calls,
            stat.work,
            stat.phase,
        );
    }
}

fn print_proxy_reduce_profile(
    stats: &[differential_dataflow::operators::int_proxy::reduce::ProxyReducePhaseStat],
) {
    if stats.is_empty() {
        return;
    }
    let measured: Duration = stats.iter().map(|stat| stat.elapsed).sum();
    eprintln!("ProxyReduce exclusive driver profile (sum {measured:?}):");
    for stat in stats {
        eprintln!(
            "  {:>8.2}%  {:>12?}  calls {:>8}  work {:>12}  {}",
            100.0 * stat.elapsed.as_secs_f64() / measured.as_secs_f64(),
            stat.elapsed,
            stat.calls,
            stat.work,
            stat.phase,
        );
    }
}

fn main() {
    let nodes = env_usize("N", 100_000);
    let edges = env_usize("E", 2 * nodes);
    let batch = env_usize("BATCH", 1_000);
    let rounds = env_usize("ROUNDS", 20);
    let iters = env_usize("ITERS", 3);
    assert!(nodes > 0, "N must be nonzero");
    assert!(rounds > 0, "ROUNDS must be nonzero");
    assert!(iters > 0, "ITERS must be nonzero");
    assert!(
        batch.saturating_mul(rounds) <= edges,
        "BATCH * ROUNDS must not exceed E"
    );

    let selected = std::env::var("BACKENDS").unwrap_or_else(|_| "compiled,vec,corgi".to_owned());
    let backends: Vec<Backend> = selected
        .split(',')
        .map(|name| match name.trim() {
            "compiled" => Backend::Compiled,
            "vec" => Backend::Vec,
            "corgi" => Backend::Corgi,
            other => panic!("unknown backend {other:?}"),
        })
        .collect();

    let mut program = lower::lower_tree(parse::pipe::parse(SCC));
    program.optimize();

    if std::env::var("CHECK").map_or(true, |x| x != "0") {
        let check_nodes = nodes.min(2_000);
        let check_edges = edges.min(2 * check_nodes);
        let rows: Vec<_> = (0..check_edges).map(|i| row_edge(i, check_nodes)).collect();
        assert_eq!(
            vec::evaluate(&program, std::slice::from_ref(&rows)),
            corgi::evaluate(&program, &[rows]),
            "DDIR Vec and Corgi disagree",
        );
        eprintln!("checked Vec/Corgi parity at n={check_nodes}, e={check_edges}");
    }

    println!("SCC n={nodes} e={edges} batch={batch} rounds={rounds} iterations={iters} workers=1");
    println!("backend\tload_median\tround_median\trounds_total");
    let mut results: BTreeMap<&str, (Duration, Duration, Duration)> = BTreeMap::new();
    for backend in backends {
        let mut loads = Vec::with_capacity(iters);
        let mut round_medians = Vec::with_capacity(iters);
        let mut round_totals = Vec::with_capacity(iters);
        for iteration in 0..iters {
            let stats = match backend {
                Backend::Compiled => compiled(nodes, edges, batch, rounds),
                Backend::Vec | Backend::Corgi => {
                    interpreted(backend, &program, nodes, edges, batch, rounds)
                }
            };
            let round_total: Duration = stats.rounds.iter().sum();
            eprintln!(
                "{} iteration {}: load {:?}, median round {:?}, rounds total {:?}",
                backend.name(),
                iteration + 1,
                stats.load,
                median(stats.rounds.clone()),
                round_total,
            );
            print_operator_profile(backend, &stats.operators);
            print_reduce_profile(&stats.reduce_phases);
            print_proxy_reduce_profile(&stats.proxy_reduce_phases);
            loads.push(stats.load);
            round_medians.push(median(stats.rounds));
            round_totals.push(round_total);
        }
        let result = (median(loads), median(round_medians), median(round_totals));
        println!(
            "{}\t{:?}\t{:?}\t{:?}",
            backend.name(),
            result.0,
            result.1,
            result.2
        );
        results.insert(backend.name(), result);
    }

    if let Some(compiled) = results.get("compiled") {
        for name in ["vec", "corgi"] {
            if let Some(result) = results.get(name) {
                println!(
                    "{name}/compiled\tload {:.2}x\tround {:.2}x\trounds_total {:.2}x",
                    result.0.as_secs_f64() / compiled.0.as_secs_f64(),
                    result.1.as_secs_f64() / compiled.1.as_secs_f64(),
                    result.2.as_secs_f64() / compiled.2.as_secs_f64(),
                );
            }
        }
    }
}
