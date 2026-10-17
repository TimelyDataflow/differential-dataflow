//! Four-way scorecard for atomic DDIR operators.
//!
//! Atomic cases have one typed operator plan: there is no reformulation DDIR forces
//! on a map or a join. The two compiled implementations therefore render the same
//! plan and differ only in where they render it. `compiled` uses the root scope at a
//! `u64` timestamp, which is what a typed program would write. `compiled-ddir` uses
//! the iterative scope at [`interactive::ir::Time`], which is where DDIR must render
//! everything it runs. The gap between them is the cost of DDIR's runtime embedding,
//! and holding it apart is what lets the remaining ratios mean interpretation.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::{Duration, Instant};

use benchmarks::{check_rotation, source};
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

const IDENTITY: &str = "export \"result\" = input 0;";
const MAP1: &str = r#"
    let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5], $0[0]);
    export "result" = a | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
"#;
const MAP8: &str = r#"
    let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5], $0[0]);
    let b = a | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    let c = b | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    let d = c | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    let e = d | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    let f = e | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    let g = f | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    let h = g | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
    export "result" = h | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
"#;
const FILTER_NONE: &str = r#"
    let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5]);
    export "result" = a | filter($1[0] < 0);
"#;
const FILTER_HALF: &str = r#"
    let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5]);
    export "result" = a | filter($1[0] < 500);
"#;
const FILTER_ALL: &str = r#"
    let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5]);
    export "result" = a | filter($1[0] < 1000);
"#;
const ARRANGE: &str = r#"export "result" = input 0 | key($0[0] ; $0[1]) | arrange;"#;
const JOIN: &str = r#"
    let left = input 0 | key($0[0] ; $0[1]);
    let right = input 1 | key($0[0] ; $0[1]);
    export "result" = left | join(right, ($1 ; $2));
"#;
const DISTINCT: &str = r#"export "result" = input 0 | key($0[0] ; $0[1]) | distinct;"#;
const COUNT: &str = r#"export "result" = input 0 | key($0[0] ; $0[1]) | count;"#;
const MIN: &str = r#"export "result" = input 0 | key($0[0] ; $0[1]) | min;"#;

type NativeRow = [i64; 6];
type Canonical = Vec<(Vec<i64>, Vec<i64>, i64)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Case {
    Identity,
    Map1,
    Map8,
    FilterNone,
    FilterHalf,
    FilterAll,
    ArrangeSorted,
    ArrangeRandom,
    ArrangeDuplicates,
    JoinOne,
    JoinMiss,
    JoinFanout,
    Distinct,
    Count,
    Min,
}

impl Case {
    const ALL: [Case; 15] = [
        Case::Identity,
        Case::Map1,
        Case::Map8,
        Case::FilterNone,
        Case::FilterHalf,
        Case::FilterAll,
        Case::ArrangeSorted,
        Case::ArrangeRandom,
        Case::ArrangeDuplicates,
        Case::JoinOne,
        Case::JoinMiss,
        Case::JoinFanout,
        Case::Distinct,
        Case::Count,
        Case::Min,
    ];

    fn name(self) -> &'static str {
        match self {
            Case::Identity => "identity",
            Case::Map1 => "map1",
            Case::Map8 => "map8",
            Case::FilterNone => "filter-none",
            Case::FilterHalf => "filter-half",
            Case::FilterAll => "filter-all",
            Case::ArrangeSorted => "arrange-sorted",
            Case::ArrangeRandom => "arrange-random",
            Case::ArrangeDuplicates => "arrange-duplicates",
            Case::JoinOne => "join-one",
            Case::JoinMiss => "join-miss",
            Case::JoinFanout => "join-fanout",
            Case::Distinct => "distinct",
            Case::Count => "count",
            Case::Min => "min",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|case| case.name() == name)
    }

    fn source(self) -> &'static str {
        match self {
            Case::Identity => IDENTITY,
            Case::Map1 => MAP1,
            Case::Map8 => MAP8,
            Case::FilterNone => FILTER_NONE,
            Case::FilterHalf => FILTER_HALF,
            Case::FilterAll => FILTER_ALL,
            Case::ArrangeSorted | Case::ArrangeRandom | Case::ArrangeDuplicates => ARRANGE,
            Case::JoinOne | Case::JoinMiss | Case::JoinFanout => JOIN,
            Case::Distinct => DISTINCT,
            Case::Count => COUNT,
            Case::Min => MIN,
        }
    }

    fn is_join(self) -> bool {
        matches!(self, Case::JoinOne | Case::JoinMiss | Case::JoinFanout)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    fn name(self) -> &'static str {
        match self {
            Implementation::Compiled => "compiled",
            Implementation::CompiledDdir => "compiled-ddir",
            Implementation::Vec => "ddir-vec",
            Implementation::Corgi => "ddir-corgi",
        }
    }
}

#[derive(Debug)]
struct Config {
    case: Option<Case>,
    rows: usize,
    keys: usize,
    fanout: usize,
    seed: u64,
    warmup: usize,
    runs: usize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut case = None;
        let mut rows = 100_000;
        let mut keys = 1_000;
        let mut fanout = 4;
        let mut seed = 0xc0ff_ee42;
        let mut warmup = 2;
        let mut runs = Implementation::ALL.len() * 2;
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let value = |name: &str, values: &mut std::iter::Skip<std::env::Args>| {
                values
                    .next()
                    .ok_or_else(|| format!("{name} requires a value"))
            };
            match argument.as_str() {
                "--case" => {
                    let name = value("--case", &mut arguments)?;
                    case = if name == "all" {
                        None
                    } else {
                        Some(Case::parse(&name).ok_or_else(|| format!("unknown case {name:?}"))?)
                    };
                }
                "--rows" => rows = number("--rows", &value("--rows", &mut arguments)?)?,
                "--keys" => keys = number("--keys", &value("--keys", &mut arguments)?)?,
                "--fanout" => fanout = number("--fanout", &value("--fanout", &mut arguments)?)?,
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
        if rows == 0 || keys == 0 || fanout == 0 || runs == 0 {
            return Err("--rows, --keys, --fanout, and --runs must be positive".to_owned());
        }
        if rows > (i64::MAX as usize) / 2 {
            return Err("--rows must not exceed i64::MAX / 2".to_owned());
        }
        check_rotation(runs, Implementation::ALL.len())?;
        keys = keys.min(rows);
        Ok(Config {
            case,
            rows,
            keys,
            fanout,
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
    eprintln!("usage: operators [--case all|NAME] [--rows N] [--keys N] [--fanout N] [--seed S] [--warmup N] [--runs N]");
}

#[derive(Clone, Copy, Debug, Default)]
struct Timings {
    prepare: Duration,
    build: Duration,
    ingest: Duration,
    stabilize: Duration,
}

impl Timings {
    fn measured(self) -> Duration {
        self.prepare + self.build + self.ingest + self.stabilize
    }
}

#[derive(Serialize)]
struct Record<'a> {
    schema: u32,
    benchmark: &'static str,
    case: &'static str,
    implementation: &'static str,
    revision: &'a str,
    dirty: bool,
    run: usize,
    rows: usize,
    keys: usize,
    fanout: usize,
    seed: u64,
    prepare_ns: u128,
    build_ns: u128,
    ingest_ns: u128,
    stabilize_ns: u128,
    measured_ns: u128,
    checked_against: &'static str,
}

struct Dataset {
    native: Vec<Vec<NativeRow>>,
    dynamic: Vec<Vec<(Row, Row)>>,
}

fn dataset(case: Case, config: &Config) -> Dataset {
    let mut left = native_rows(config.rows, config.seed);
    let mut native = match case {
        Case::ArrangeRandom => {
            shuffle(&mut left, config.seed);
            vec![left]
        }
        Case::ArrangeDuplicates => {
            for (index, row) in left.iter_mut().enumerate() {
                row[0] = (index % config.keys) as i64;
                row[1] = row[0];
            }
            vec![left]
        }
        Case::Distinct | Case::Count | Case::Min => {
            for (index, row) in left.iter_mut().enumerate() {
                row[0] = (index % config.keys) as i64;
            }
            shuffle(&mut left, config.seed);
            vec![left]
        }
        Case::JoinOne | Case::JoinMiss | Case::JoinFanout => {
            let mut right = native_rows(config.rows, config.seed ^ 0x9e37_79b9);
            match case {
                Case::JoinOne => {}
                Case::JoinMiss => {
                    let offset = i64::try_from(config.rows / 2).unwrap();
                    for row in &mut right {
                        row[0] += offset;
                    }
                }
                Case::JoinFanout => {
                    let groups = config.rows.div_ceil(config.fanout).max(1);
                    for (index, row) in left.iter_mut().enumerate() {
                        row[0] = (index % groups) as i64;
                    }
                    for (index, row) in right.iter_mut().enumerate() {
                        row[0] = (index % groups) as i64;
                    }
                }
                _ => unreachable!(),
            }
            shuffle(&mut left, config.seed);
            shuffle(&mut right, config.seed ^ 0x517c_c1b7);
            vec![left, right]
        }
        _ => vec![left],
    };
    if !case.is_join() {
        native.truncate(1);
    }
    let dynamic = native.iter().map(|rows| dynamic_rows(rows)).collect();
    Dataset { native, dynamic }
}

fn native_rows(rows: usize, seed: u64) -> Vec<NativeRow> {
    let mut state = seed;
    (0..rows)
        .map(|index| {
            let mut row = [0; 6];
            row[0] = i64::try_from(index).unwrap();
            for field in &mut row[1..] {
                *field = (xorshift(&mut state) % 1_000) as i64;
            }
            row
        })
        .collect()
}

fn shuffle(rows: &mut [NativeRow], seed: u64) {
    rows.sort_by_key(|row| (mix(row[0] as u64 ^ seed), row[0]));
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn dynamic_rows(rows: &[NativeRow]) -> Vec<(Row, Row)> {
    rows.iter().map(|row| (tuple(row), Value::unit())).collect()
}

fn tuple(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().copied().map(Value::Int).collect())
}

fn program(case: Case) -> scope_ir::Program {
    let mut program = lower::lower_tree(parse::pipe::parse(case.source()));
    program.optimize();
    program
}

/// Check all four implementations against a hand-written oracle, untimed.
///
/// Both compiled implementations are executed here, not merely assumed to agree.
/// `compiled-ddir` renders the same plan at a coarser timestamp lattice, which
/// selects different code paths inside the reduce and join operators, so its
/// agreement is a claim that has to be tested rather than asserted.
fn validate(case: Case, data: &Dataset) -> Result<(), String> {
    let expected = expected(case, &data.native);
    let disagree = |name: &str, actual: Canonical| {
        if actual == expected {
            Ok(())
        } else {
            Err(format!(
                "{}: {name} disagrees with the expected output: lengths {} and {}",
                case.name(),
                actual.len(),
                expected.len(),
            ))
        }
    };

    disagree("compiled", validate_compiled(case, &data.native, false))?;
    disagree("compiled-ddir", validate_compiled(case, &data.native, true))?;

    let program = program(case);
    let vec = vec::evaluate(&program, &data.dynamic);
    let corgi = corgi::evaluate(&program, &data.dynamic);
    if vec != corgi {
        return Err(format!("{}: Vec and Corgi outputs disagree", case.name()));
    }
    disagree("ddir-vec", canonical(&vec["result"]))?;
    disagree("ddir-corgi", canonical(&corgi["result"]))?;
    Ok(())
}

/// Run a compiled implementation once, untimed, capturing its canonical output.
fn validate_compiled(case: Case, inputs: &[Vec<NativeRow>], scoped: bool) -> Canonical {
    let inputs = inputs.to_vec();
    let (root_send, root_receive) = channel::<CompiledCapture<u64>>();
    let (scoped_send, scoped_receive) = channel::<CompiledCapture<Time>>();
    timely::execute_directly(move |worker| {
        let root_probe = timely::dataflow::ProbeHandle::<u64>::new();
        let scoped_probe = timely::dataflow::ProbeHandle::<Time>::new();
        let mut handles = worker.dataflow::<u64, _, _>(|scope| {
            let (left_handle, left) = scope.new_collection::<NativeRow, isize>();
            let (right_handle, right) = scope.new_collection::<NativeRow, isize>();
            if scoped {
                scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                    let (left, right) = (left.enter(inner), right.enter(inner));
                    render_compiled(case, left, right, &scoped_probe, Some(&scoped_send));
                });
            } else {
                render_compiled(case, left, right, &root_probe, Some(&root_send));
            }
            vec![left_handle, right_handle]
        });
        ingest_native(&mut handles, inputs);
        drop(handles);
        while worker.step() {}
    });
    if scoped {
        collect_compiled(scoped_receive)
    } else {
        collect_compiled(root_receive)
    }
}

fn collect_compiled<T: timely::progress::Timestamp>(
    receiver: Receiver<CompiledCapture<T>>,
) -> Canonical {
    consolidate(receiver.into_iter().flat_map(|event| match event {
        Event::Messages(_, data) => data
            .into_iter()
            .map(|((key, value), _, diff)| (key, value, i64::try_from(diff).unwrap()))
            .collect(),
        Event::Progress(_) => Vec::new(),
    }))
}

fn expected(case: Case, inputs: &[Vec<NativeRow>]) -> Canonical {
    let rows = &inputs[0];
    match case {
        Case::Identity => consolidate(rows.iter().map(|row| (row.to_vec(), Vec::new(), 1))),
        Case::Map1 | Case::Map8 => {
            let rounds = if case == Case::Map1 { 1 } else { 8 };
            consolidate(rows.iter().map(|row| {
                let mut value = [row[1], row[2], row[3], row[4], row[5], row[0]];
                for _ in 0..rounds {
                    value = map_value(value);
                }
                (vec![row[0]], value.to_vec(), 1)
            }))
        }
        Case::FilterNone | Case::FilterHalf | Case::FilterAll => {
            let limit = match case {
                Case::FilterNone => 0,
                Case::FilterHalf => 500,
                Case::FilterAll => 1_000,
                _ => unreachable!(),
            };
            consolidate(
                rows.iter()
                    .filter(|row| row[1] < limit)
                    .map(|row| (vec![row[0]], row[1..].to_vec(), 1)),
            )
        }
        Case::ArrangeSorted | Case::ArrangeRandom | Case::ArrangeDuplicates => {
            consolidate(rows.iter().map(|row| (vec![row[0]], vec![row[1]], 1)))
        }
        Case::JoinOne | Case::JoinMiss | Case::JoinFanout => expected_join(inputs),
        Case::Distinct => consolidate(
            rows.iter()
                .map(|row| (vec![row[0]], Vec::new(), 1))
                .collect::<Vec<_>>(),
        )
        .into_iter()
        .map(|(key, value, _)| (key, value, 1))
        .collect(),
        Case::Count => {
            let mut counts = BTreeMap::new();
            for row in rows {
                *counts.entry(row[0]).or_insert(0i64) += 1;
            }
            counts
                .into_iter()
                .map(|(key, count)| (vec![key], vec![count], 1))
                .collect()
        }
        Case::Min => {
            let mut minima = BTreeMap::new();
            for row in rows {
                minima
                    .entry(row[0])
                    .and_modify(|value: &mut i64| *value = (*value).min(row[1]))
                    .or_insert(row[1]);
            }
            minima
                .into_iter()
                .map(|(key, value)| (vec![key], vec![value], 1))
                .collect()
        }
    }
}

fn expected_join(inputs: &[Vec<NativeRow>]) -> Canonical {
    let mut left = BTreeMap::<i64, Vec<i64>>::new();
    let mut right = BTreeMap::<i64, Vec<i64>>::new();
    for row in &inputs[0] {
        left.entry(row[0]).or_default().push(row[1]);
    }
    for row in &inputs[1] {
        right.entry(row[0]).or_default().push(row[1]);
    }
    let mut updates = Vec::new();
    for (key, left_values) in left {
        if let Some(right_values) = right.get(&key) {
            for left_value in left_values {
                for right_value in right_values {
                    updates.push((vec![left_value], vec![*right_value], 1));
                }
            }
        }
    }
    consolidate(updates)
}

fn consolidate<I>(updates: I) -> Canonical
where
    I: IntoIterator<Item = (Vec<i64>, Vec<i64>, i64)>,
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

fn canonical(rows: &[((Row, Row), Diff)]) -> Canonical {
    consolidate(
        rows.iter()
            .map(|((key, value), diff)| (integers(key), integers(value), *diff)),
    )
}

fn integers(value: &Value) -> Vec<i64> {
    match value {
        Value::Int(value) => vec![*value],
        Value::Tuple(fields) => fields.iter().flat_map(integers).collect(),
        other => panic!("expected integer tuple, found {other:?}"),
    }
}

fn map_value(value: [i64; 6]) -> [i64; 6] {
    [
        value[0] + value[1],
        value[1] + value[2],
        value[2] + value[3],
        value[3] + value[4],
        value[4] + value[5],
        value[0],
    ]
}

fn run(implementation: Implementation, case: Case, data: &Dataset) -> Timings {
    match implementation {
        Implementation::Compiled => run_compiled(case, &data.native),
        Implementation::CompiledDdir => run_compiled_ddir(case, &data.native),
        Implementation::Vec => run_ddir(case, &data.dynamic, false),
        Implementation::Corgi => run_ddir(case, &data.dynamic, true),
    }
}

/// The optimized typed plan, rendered where a typed program would put it: the root
/// scope, at the root timestamp.
fn run_compiled(case: Case, inputs: &[Vec<NativeRow>]) -> Timings {
    let inputs = inputs.to_vec();
    timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::new();
        let build_started = Instant::now();
        let mut handles = worker.dataflow::<u64, _, _>(|scope| {
            let (left_handle, left) = scope.new_collection::<NativeRow, isize>();
            let (right_handle, right) = scope.new_collection::<NativeRow, isize>();
            render_compiled(case, left, right, &probe, None);
            vec![left_handle, right_handle]
        });
        let build = build_started.elapsed();
        let ingest = ingest_native(&mut handles, inputs);
        let stabilize_started = Instant::now();
        while probe.less_than(&1) {
            worker.step();
        }
        Timings {
            prepare: Duration::ZERO,
            build,
            ingest,
            stabilize: stabilize_started.elapsed(),
        }
    })
}

/// The same typed plan, rendered where DDIR must put it.
///
/// DDIR renders every program inside an iterative scope, so its operators run at
/// [`interactive::ir::Time`] rather than at `u64`: larger timestamps, vector-valued
/// lattice operations, and an `enter` per input. That cost belongs to DDIR's runtime
/// embedding rather than to interpretation, and this implementation is what separates
/// the two. The operator plan is identical to [`run_compiled`]'s.
fn run_compiled_ddir(case: Case, inputs: &[Vec<NativeRow>]) -> Timings {
    let inputs = inputs.to_vec();
    timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::<Time>::new();
        let build_started = Instant::now();
        let mut handles = worker.dataflow::<u64, _, _>(|scope| {
            let (left_handle, left) = scope.new_collection::<NativeRow, isize>();
            let (right_handle, right) = scope.new_collection::<NativeRow, isize>();
            scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                render_compiled(case, left.enter(inner), right.enter(inner), &probe, None);
            });
            vec![left_handle, right_handle]
        });
        let build = build_started.elapsed();
        let ingest = ingest_native(&mut handles, inputs);
        let stabilize_started = Instant::now();
        let target = Product::new(1, PointStamp::default());
        while probe.less_than(&target) {
            worker.step();
        }
        Timings {
            prepare: Duration::ZERO,
            build,
            ingest,
            stabilize: stabilize_started.elapsed(),
        }
    })
}

/// Submit typed rows and seal the batch, consuming the rows as the DDIR path does.
fn ingest_native(
    handles: &mut [differential_dataflow::input::InputSession<u64, NativeRow, isize>],
    inputs: Vec<Vec<NativeRow>>,
) -> Duration {
    let ingest_started = Instant::now();
    for (handle, rows) in handles.iter_mut().zip(inputs) {
        for row in rows {
            handle.insert(row);
        }
    }
    for handle in handles.iter_mut() {
        handle.advance_to(1);
        handle.flush();
    }
    ingest_started.elapsed()
}

/// A captured compiled output, in the same canonical shape as [`expected`].
type CompiledCapture<T> = Event<T, Vec<((Vec<i64>, Vec<i64>), T, isize)>>;

/// Render the typed plan for `case`, probing its native output.
///
/// `capture` is `None` in every timed run. When it is `Some`, each arm additionally
/// maps its output into the canonical shape and captures it, so that the compiled
/// implementations can be checked against [`expected`] rather than merely assumed
/// to agree with it. The extra operators exist only when capturing, so the timed
/// dataflow is exactly the plan and nothing else.
fn render_compiled<'scope, T>(
    case: Case,
    left: differential_dataflow::VecCollection<'scope, T, NativeRow, isize>,
    right: differential_dataflow::VecCollection<'scope, T, NativeRow, isize>,
    probe: &timely::dataflow::ProbeHandle<T>,
    capture: Option<&Sender<CompiledCapture<T>>>,
) where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + Send,
{
    /// Probe the plan's own output, then optionally capture a canonical copy.
    macro_rules! finish {
        ($output:expr, $canonical:expr) => {{
            let output = $output;
            if let Some(send) = capture {
                output.clone().map($canonical).inner.capture_into(send.clone());
            }
            output.inner.probe_with(probe);
        }};
    }
    match case {
        Case::Identity => finish!(left, |row: NativeRow| (row.to_vec(), Vec::new())),
        Case::Map1 | Case::Map8 => {
            let rounds = if case == Case::Map1 { 1 } else { 8 };
            let mut output = left.map(start_map);
            for _ in 0..rounds {
                output = output.map(|(key, value)| (key, map_value(value)));
            }
            finish!(output, |(key, value): (i64, [i64; 6])| (
                vec![key],
                value.to_vec()
            ))
        }
        Case::FilterNone | Case::FilterHalf | Case::FilterAll => {
            let limit = match case {
                Case::FilterNone => 0,
                Case::FilterHalf => 500,
                Case::FilterAll => 1_000,
                _ => unreachable!(),
            };
            finish!(
                left.map(|row| (row[0], [row[1], row[2], row[3], row[4], row[5]]))
                    .filter(move |(_, value)| value[0] < limit),
                |(key, value): (i64, [i64; 5])| (vec![key], value.to_vec())
            )
        }
        Case::ArrangeSorted | Case::ArrangeRandom | Case::ArrangeDuplicates => finish!(
            left.map(|row| (row[0], row[1]))
                .arrange_by_key()
                .as_collection(|key, value| (*key, *value)),
            |(key, value): (i64, i64)| (vec![key], vec![value])
        ),
        Case::JoinOne | Case::JoinMiss | Case::JoinFanout => finish!(
            left.map(|row| (row[0], row[1]))
                .join_map(right.map(|row| (row[0], row[1])), |_, left, right| {
                    (*left, *right)
                }),
            |(left, right): (i64, i64)| (vec![left], vec![right])
        ),
        Case::Distinct => finish!(left.map(|row| row[0]).distinct(), |key: i64| (
            vec![key],
            Vec::new()
        )),
        Case::Count => finish!(left.map(|row| row[0]).count(), |(key, count): (i64, isize)| (
            vec![key],
            vec![count as i64]
        )),
        Case::Min => finish!(
            left.map(|row| (row[0], row[1]))
                .reduce(|_, values, output| output.push((*values[0].0, 1))),
            |(key, value): (i64, i64)| (vec![key], vec![value])
        ),
    };
}

fn start_map(row: NativeRow) -> (i64, [i64; 6]) {
    (row[0], [row[1], row[2], row[3], row[4], row[5], row[0]])
}

fn run_ddir(case: Case, inputs: &[Vec<(Row, Row)>], use_corgi: bool) -> Timings {
    let prepare_started = Instant::now();
    let program = program(case);
    let prepare = prepare_started.elapsed();
    let export = program
        .root
        .exports
        .iter()
        .position(|item| item.name == "result")
        .unwrap();
    let inputs = inputs.to_vec();
    let rest = timely::execute_directly(move |worker| {
        let probe = timely::dataflow::ProbeHandle::<Time>::new();
        let build_started = Instant::now();
        let mut handles = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..inputs.len() {
                let (handle, collection) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(handle);
                collections.push(collection);
            }
            scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections
                    .iter()
                    .map(|collection| collection.clone().enter(inner))
                    .collect();
                let imports: Vec<_> = program
                    .root
                    .imports
                    .iter()
                    .map(|item| match item.from {
                        scope_ir::Source::Input(index) => entered[index].clone(),
                        ref other => panic!("unexpected operator source {other:?}"),
                    })
                    .collect();
                if use_corgi {
                    let columnar = imports
                        .into_iter()
                        .map(|collection| {
                            collection
                                .inner
                                .unary(Pipeline, "ToCorgi", |_, _| {
                                    |stream_input, output| {
                                        stream_input.for_each(|capability, data| {
                                            let mut container =
                                                CorgiContainer::from_updates(std::mem::take(data));
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
            handles
        });
        let build = build_started.elapsed();
        // Consume the prepared rows. Cloning them here would charge DDIR a deep copy
        // of every `Value` that the typed path, whose rows are `Copy`, never pays.
        let ingest_started = Instant::now();
        for (handle, rows) in handles.iter_mut().zip(inputs) {
            for row in rows {
                handle.update(row, 1);
            }
        }
        for handle in &mut handles {
            handle.advance_to(1);
            handle.flush();
        }
        let ingest = ingest_started.elapsed();
        let stabilize_started = Instant::now();
        let target = Product::new(1, PointStamp::default());
        while probe.less_than(&target) {
            worker.step();
        }
        Timings {
            prepare: Duration::ZERO,
            build,
            ingest,
            stabilize: stabilize_started.elapsed(),
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
    let source = source();
    let cases: Vec<_> = config
        .case
        .map_or_else(|| Case::ALL.to_vec(), |case| vec![case]);
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    for case in cases {
        let data = dataset(case, &config);
        validate(case, &data)?;
        for ordinal in 0..(config.warmup + config.runs) {
            let mut implementations = Implementation::ALL;
            let implementation_count = implementations.len();
            implementations.rotate_left(ordinal % implementation_count);
            let measured = ordinal >= config.warmup;
            let run_number = ordinal.saturating_sub(config.warmup);
            let mut times = Vec::new();
            for implementation in implementations {
                let timings = run(implementation, case, &data);
                times.push((implementation, timings));
                if measured {
                    let record = Record {
                        schema: benchmarks::SCHEMA,
                        benchmark: "operators",
                        case: case.name(),
                        implementation: implementation.name(),
                        revision: &source.revision,
                        dirty: source.dirty,
                        run: run_number,
                        rows: config.rows,
                        keys: config.keys,
                        fanout: config.fanout,
                        seed: config.seed,
                        prepare_ns: timings.prepare.as_nanos(),
                        build_ns: timings.build.as_nanos(),
                        ingest_ns: timings.ingest.as_nanos(),
                        stabilize_ns: timings.stabilize.as_nanos(),
                        measured_ns: timings.measured().as_nanos(),
                        checked_against: "expected",
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
                let scoped = find(Implementation::CompiledDdir);
                let vec = find(Implementation::Vec);
                let corgi = find(Implementation::Corgi);
                eprintln!(
                    "{} run {}: compiled={compiled:.6}s compiled-ddir={scoped:.6}s ({:.2}x) vec={vec:.6}s ({:.2}x) corgi={corgi:.6}s ({:.2}x vec, {:.2}x compiled-ddir)",
                    case.name(), run_number,
                    scoped / compiled, vec / scoped, corgi / vec, corgi / scoped,
                );
            }
        }
    }
    Ok(())
}
