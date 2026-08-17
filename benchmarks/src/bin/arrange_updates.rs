//! Three-way scorecard for steady-state arrangement maintenance.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::mpsc::{channel, Receiver};
use std::time::{Duration, Instant};

use benchmarks::revision;
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;
use differential_dataflow::AsCollection;
use interactive::backend::vec::Row;
use interactive::backend::{corgi, vec};
use interactive::corgi::container::CorgiContainer;
use interactive::ir::{Diff, Time, Value};
use interactive::{lower, parse, scope_ir};
use mimalloc::MiMalloc;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::{Capture, Event};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::Probe;
use timely::order::Product;

#[global_allocator]
static ALLOCATOR: MiMalloc = MiMalloc;

const ARRANGE: &str = r#"export "result" = input 0 | key($0[0] ; $0[1]) | arrange;"#;

type NativeRow = [i64; 6];
type Canonical = Vec<(i64, i64, i64)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Implementation {
    Compiled,
    Vec,
    Corgi,
}

impl Implementation {
    const ALL: [Implementation; 3] = [
        Implementation::Compiled,
        Implementation::Vec,
        Implementation::Corgi,
    ];

    fn name(self) -> &'static str {
        match self {
            Implementation::Compiled => "compiled",
            Implementation::Vec => "ddir-vec",
            Implementation::Corgi => "ddir-corgi",
        }
    }
}

#[derive(Debug)]
struct Config {
    rows: usize,
    update_rows: usize,
    seed: u64,
    warmup: usize,
    runs: usize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut rows = 100_000;
        let mut update_rows = 1_000;
        let mut seed = 0xc0ff_ee42;
        let mut warmup = 1;
        let mut runs = 5;
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let value = |name: &str, values: &mut std::iter::Skip<std::env::Args>| {
                values
                    .next()
                    .ok_or_else(|| format!("{name} requires a value"))
            };
            match argument.as_str() {
                "--rows" => rows = number("--rows", &value("--rows", &mut arguments)?)?,
                "--update-rows" => {
                    update_rows =
                        number("--update-rows", &value("--update-rows", &mut arguments)?)?;
                }
                "--seed" => seed = number("--seed", &value("--seed", &mut arguments)?)?,
                "--warmup" => warmup = number("--warmup", &value("--warmup", &mut arguments)?)?,
                "--runs" => runs = number("--runs", &value("--runs", &mut arguments)?)?,
                "--help" | "-h" => {
                    usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        if rows == 0 || update_rows == 0 || runs == 0 {
            return Err("--rows, --update-rows, and --runs must be positive".to_owned());
        }
        if update_rows > rows {
            return Err("--update-rows must not exceed --rows".to_owned());
        }
        if rows > (i64::MAX as usize) / 2 {
            return Err("--rows must not exceed i64::MAX / 2".to_owned());
        }
        Ok(Config {
            rows,
            update_rows,
            seed,
            warmup,
            runs,
        })
    }
}

fn number<T: std::str::FromStr>(name: &str, value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("invalid {name} value {value:?}"))
}

fn usage() {
    eprintln!(
        "usage: arrange_updates [--rows N] [--update-rows N] [--seed S] [--warmup N] [--runs N]"
    );
}

#[derive(Clone, Copy, Debug, Default)]
struct Timings {
    prepare: Duration,
    build: Duration,
    initial_ingest: Duration,
    initial_stabilize: Duration,
    update_ingest: Duration,
    update_stabilize: Duration,
}

impl Timings {
    fn measured(self) -> Duration {
        self.update_ingest + self.update_stabilize
    }
}

#[derive(Serialize)]
struct Record<'a> {
    schema: u32,
    benchmark: &'static str,
    case: &'static str,
    implementation: &'static str,
    revision: &'a str,
    run: usize,
    rows: usize,
    update_rows: usize,
    seed: u64,
    prepare_ns: u128,
    build_ns: u128,
    initial_ingest_ns: u128,
    initial_stabilize_ns: u128,
    update_ingest_ns: u128,
    update_stabilize_ns: u128,
    measured_ns: u128,
    correct: bool,
}

struct Dataset {
    base: Vec<NativeRow>,
    removes: Vec<NativeRow>,
    inserts: Vec<NativeRow>,
}

impl Dataset {
    fn new(config: &Config) -> Self {
        let base = native_rows(config.rows, config.seed, 0);
        let removes = base[..config.update_rows].to_vec();
        let inserts = native_rows(config.update_rows, config.seed ^ 0x9e37_79b9, config.rows);
        Self {
            base,
            removes,
            inserts,
        }
    }

    fn expected(&self) -> Canonical {
        consolidate(
            self.base
                .iter()
                .map(|row| (row[0], row[1], 1))
                .chain(self.removes.iter().map(|row| (row[0], row[1], -1)))
                .chain(self.inserts.iter().map(|row| (row[0], row[1], 1))),
        )
    }
}

fn native_rows(rows: usize, seed: u64, offset: usize) -> Vec<NativeRow> {
    let mut state = seed;
    (0..rows)
        .map(|index| {
            let mut row = [0; 6];
            row[0] = i64::try_from(offset + index).unwrap();
            for field in &mut row[1..] {
                *field = (xorshift(&mut state) % 1_000) as i64;
            }
            row
        })
        .collect()
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn dynamic(row: &NativeRow) -> (Row, Row) {
    (
        Value::Tuple(row.iter().copied().map(Value::Int).collect()),
        Value::unit(),
    )
}

fn program() -> scope_ir::Program {
    let mut program = lower::lower_tree(parse::pipe::parse(ARRANGE));
    program.optimize();
    program
}

fn consolidate<I>(updates: I) -> Canonical
where
    I: IntoIterator<Item = (i64, i64, i64)>,
{
    let mut result = BTreeMap::new();
    for (key, value, diff) in updates {
        *result.entry((key, value)).or_insert(0) += diff;
    }
    result
        .into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((key, value), diff)| (key, value, diff))
        .collect()
}

fn scalar(value: &Value) -> i64 {
    match value {
        Value::Int(value) => *value,
        Value::Tuple(fields) if fields.len() == 1 => scalar(&fields[0]),
        other => panic!("expected one integer field, found {other:?}"),
    }
}

fn collect_native(receiver: Receiver<Event<u64, Vec<((i64, i64), u64, isize)>>>) -> Canonical {
    consolidate(receiver.into_iter().flat_map(|event| {
        match event {
            Event::Messages(_, data) => data
                .into_iter()
                .map(|((key, value), _, diff)| (key, value, i64::try_from(diff).unwrap()))
                .collect(),
            Event::Progress(_) => Vec::new(),
        }
    }))
}

fn collect_ddir(receiver: Receiver<Event<u64, Vec<((Row, Row), u64, Diff)>>>) -> Canonical {
    consolidate(receiver.into_iter().flat_map(|event| {
        match event {
            Event::Messages(_, data) => data
                .into_iter()
                .map(|((key, value), _, diff)| (scalar(&key), scalar(&value), diff))
                .collect(),
            Event::Progress(_) => Vec::new(),
        }
    }))
}

fn validate(data: &Dataset) -> Result<(), String> {
    let expected = data.expected();
    let compiled = validate_compiled(data);
    let vec = validate_ddir(data, false);
    let corgi = validate_ddir(data, true);
    for (name, actual) in [
        ("compiled", compiled),
        ("ddir-vec", vec),
        ("ddir-corgi", corgi),
    ] {
        if actual != expected {
            return Err(format!(
                "arrange-update: {name} disagrees with expected output: lengths {} and {}",
                actual.len(),
                expected.len()
            ));
        }
    }
    Ok(())
}

fn validate_compiled(data: &Dataset) -> Canonical {
    type NativeEvent = Event<u64, Vec<((i64, i64), u64, isize)>>;

    let base = data.base.clone();
    let removes = data.removes.clone();
    let inserts = data.inserts.clone();
    let (send, receive) = channel::<NativeEvent>();
    timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<NativeRow, isize>();
            collection
                .map(|row| (row[0], row[1]))
                .arrange_by_key()
                .as_collection(|key, value| (*key, *value))
                .inner
                .probe_with(&probe)
                .capture_into(send);
            input
        });
        for row in base {
            input.insert(row);
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(&1) {
            worker.step();
        }
        for row in removes {
            input.remove(row);
        }
        for row in inserts {
            input.insert(row);
        }
        input.advance_to(2);
        input.flush();
        while probe.less_than(&2) {
            worker.step();
        }
    });
    collect_native(receive)
}

fn validate_ddir(data: &Dataset, use_corgi: bool) -> Canonical {
    type DdirEvent = Event<u64, Vec<((Row, Row), u64, Diff)>>;

    let program = program();
    let export = program
        .root
        .exports
        .iter()
        .position(|item| item.name == "result")
        .unwrap();
    let base: Vec<_> = data.base.iter().map(dynamic).collect();
    let removes: Vec<_> = data.removes.iter().map(dynamic).collect();
    let inserts: Vec<_> = data.inserts.iter().map(dynamic).collect();
    let (send, receive) = channel::<DdirEvent>();
    timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let output = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let imports: Vec<_> = program
                    .root
                    .imports
                    .iter()
                    .map(|item| match item.from {
                        scope_ir::Source::Input(0) => collection.clone().enter(inner),
                        ref other => panic!("unexpected arrangement source {other:?}"),
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
        for row in base {
            input.update(row, 1);
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(&1) {
            worker.step();
        }
        for row in removes {
            input.update(row, -1);
        }
        for row in inserts {
            input.update(row, 1);
        }
        input.advance_to(2);
        input.flush();
        while probe.less_than(&2) {
            worker.step();
        }
    });
    collect_ddir(receive)
}

fn run(implementation: Implementation, data: &Dataset) -> Timings {
    match implementation {
        Implementation::Compiled => run_compiled(data),
        Implementation::Vec => run_ddir(data, false),
        Implementation::Corgi => run_ddir(data, true),
    }
}

fn run_compiled(data: &Dataset) -> Timings {
    let base = data.base.clone();
    let removes = data.removes.clone();
    let inserts = data.inserts.clone();
    timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::new();
        let build_started = Instant::now();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<NativeRow, isize>();
            collection
                .map(|row| (row[0], row[1]))
                .arrange_by_key()
                .as_collection(|key, value| (*key, *value))
                .inner
                .probe_with(&probe);
            input
        });
        let build = build_started.elapsed();

        let initial_ingest_started = Instant::now();
        for row in base {
            input.insert(row);
        }
        input.advance_to(1);
        input.flush();
        let initial_ingest = initial_ingest_started.elapsed();
        let initial_stabilize_started = Instant::now();
        while probe.less_than(&1) {
            worker.step();
        }
        let initial_stabilize = initial_stabilize_started.elapsed();

        let update_ingest_started = Instant::now();
        for row in removes {
            input.remove(row);
        }
        for row in inserts {
            input.insert(row);
        }
        input.advance_to(2);
        input.flush();
        let update_ingest = update_ingest_started.elapsed();
        let update_stabilize_started = Instant::now();
        while probe.less_than(&2) {
            worker.step();
        }
        let update_stabilize = update_stabilize_started.elapsed();
        Timings {
            prepare: Duration::ZERO,
            build,
            initial_ingest,
            initial_stabilize,
            update_ingest,
            update_stabilize,
        }
    })
}

fn run_ddir(data: &Dataset, use_corgi: bool) -> Timings {
    let prepare_started = Instant::now();
    let program = program();
    let prepare = prepare_started.elapsed();
    let export = program
        .root
        .exports
        .iter()
        .position(|item| item.name == "result")
        .unwrap();
    let base: Vec<_> = data.base.iter().map(dynamic).collect();
    let removes: Vec<_> = data.removes.iter().map(dynamic).collect();
    let inserts: Vec<_> = data.inserts.iter().map(dynamic).collect();

    let rest = timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::<Time>::new();
        let build_started = Instant::now();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let imports: Vec<_> = program
                    .root
                    .imports
                    .iter()
                    .map(|item| match item.from {
                        scope_ir::Source::Input(0) => collection.clone().enter(inner),
                        ref other => panic!("unexpected arrangement source {other:?}"),
                    })
                    .collect();
                if use_corgi {
                    let columnar = imports
                        .into_iter()
                        .map(|imported_collection| {
                            imported_collection
                                .inner
                                .unary(Pipeline, "ToCorgi", |_, _| {
                                    |stream_input, output| {
                                        stream_input.for_each(|capability, rows| {
                                            let mut container =
                                                CorgiContainer::from_updates(std::mem::take(rows));
                                            output
                                                .session(&capability)
                                                .give_container(&mut container);
                                        });
                                    }
                                })
                                .as_collection()
                        })
                        .collect();
                    let mut outputs = corgi::render_tree(&program.root, inner, 0, columnar);
                    outputs.swap_remove(export).inner.probe_with(&probe);
                } else {
                    let mut outputs = vec::render_tree(&program.root, inner, 0, imports);
                    outputs.swap_remove(export).inner.probe_with(&probe);
                }
            });
            input
        });
        let build = build_started.elapsed();

        let initial_ingest_started = Instant::now();
        for row in base {
            input.update(row, 1);
        }
        input.advance_to(1);
        input.flush();
        let initial_ingest = initial_ingest_started.elapsed();
        let initial_stabilize_started = Instant::now();
        let initial_target = Product::new(1, PointStamp::default());
        while probe.less_than(&initial_target) {
            worker.step();
        }
        let initial_stabilize = initial_stabilize_started.elapsed();

        let update_ingest_started = Instant::now();
        for row in removes {
            input.update(row, -1);
        }
        for row in inserts {
            input.update(row, 1);
        }
        input.advance_to(2);
        input.flush();
        let update_ingest = update_ingest_started.elapsed();
        let update_stabilize_started = Instant::now();
        let update_target = Product::new(2, PointStamp::default());
        while probe.less_than(&update_target) {
            worker.step();
        }
        let update_stabilize = update_stabilize_started.elapsed();
        Timings {
            prepare: Duration::ZERO,
            build,
            initial_ingest,
            initial_stabilize,
            update_ingest,
            update_stabilize,
        }
    });
    Timings { prepare, ..rest }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(error) => {
            usage();
            return Err(error.into());
        }
    };
    let data = Dataset::new(&config);
    validate(&data)?;
    let revision = revision();
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    for ordinal in 0..(config.warmup + config.runs) {
        let mut implementations = Implementation::ALL;
        let implementation_count = implementations.len();
        implementations.rotate_left(ordinal % implementation_count);
        let measured = ordinal >= config.warmup;
        let run_number = ordinal.saturating_sub(config.warmup);
        let mut times = Vec::new();
        for implementation in implementations {
            let timings = run(implementation, &data);
            times.push((implementation, timings));
            if measured {
                let record = Record {
                    schema: 1,
                    benchmark: "operators",
                    case: "arrange-update",
                    implementation: implementation.name(),
                    revision: &revision,
                    run: run_number,
                    rows: config.rows,
                    update_rows: config.update_rows,
                    seed: config.seed,
                    prepare_ns: timings.prepare.as_nanos(),
                    build_ns: timings.build.as_nanos(),
                    initial_ingest_ns: timings.initial_ingest.as_nanos(),
                    initial_stabilize_ns: timings.initial_stabilize.as_nanos(),
                    update_ingest_ns: timings.update_ingest.as_nanos(),
                    update_stabilize_ns: timings.update_stabilize.as_nanos(),
                    measured_ns: timings.measured().as_nanos(),
                    correct: true,
                };
                serde_json::to_writer(&mut stdout, &record)?;
                writeln!(&mut stdout)?;
            }
        }
        if measured {
            let find = |implementation| {
                times
                    .iter()
                    .find(|(item, _)| *item == implementation)
                    .unwrap()
                    .1
                    .measured()
                    .as_secs_f64()
            };
            let compiled = find(Implementation::Compiled);
            let vec = find(Implementation::Vec);
            let corgi = find(Implementation::Corgi);
            eprintln!(
                "arrange-update run {run_number}: compiled={compiled:.6}s vec={vec:.6}s ({:.2}x) corgi={corgi:.6}s ({:.2}x compiled, {:.2}x vec)",
                vec / compiled,
                corgi / compiled,
                corgi / vec,
            );
        }
    }
    Ok(())
}
