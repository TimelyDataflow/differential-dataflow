//! Operator scorecard — the standing per-operator perf-triage harness for the corgi backend.
//!
//! Each case is a minimal DDIR program that isolates ONE backend operator so its corgi-vs-vec ratio
//! is read directly, instead of reverse-engineered from a whole program. Every case also asserts
//! corgi == vec (correctness gate) before timing.
//!
//! Two modes:
//!   * scorecard (default):  bench every case, print `vec | corgi | ratio | target | verdict`.
//!       ~/.cargo/bin/cargo run --release -p interactive --example corgi_scorecard
//!   * profile one case:     PROG=<name> BACKEND=<corgi|vec> ITERS=<n> loops that case (for samply).
//!       BACKEND=corgi ITERS=400 samply record ... -- .../corgi_scorecard   # PROG=reduce_distinct etc.
//!
//! Targets encode the thesis: parity with vec is the FLOOR (never lose on an operator), beating vec
//! is the GOAL (columnar corgi should avoid the row backend's interpretation + Value pointer-chasing).

// The suite runs on mimalloc (as a real deployment would — ddir_server does): the
// system allocator was 27-28% of both DDIR backends' SCC profiles. One binary per
// benchmark, so every column (native/fair/vec/corgi) shares the same allocator.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse, scope_ir};

type Row = Value;

fn tup(fields: &[i64]) -> Row {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn compile(src: &str) -> scope_ir::Program {
    let mut p = lower::lower_tree(parse::pipe::parse(src));
    p.optimize();
    p
}

/// Median wall-clock over `iters` runs (min 3), after a warmup.
fn bench<F: FnMut()>(iters: usize, mut f: F) -> Duration {
    f();
    let mut ts: Vec<Duration> = (0..iters.max(3))
        .map(|_| { let t = Instant::now(); f(); t.elapsed() })
        .collect();
    ts.sort();
    ts[ts.len() / 2]
}

/// A wide 6-field keyed collection: `(key ; f1..f6)`. Shared input for the compute/arrange cases.
fn wide_input(n: usize, seed: u64) -> Vec<(Row, Row)> {
    let mut s = seed;
    (0..n)
        .map(|i| {
            let f = |s: &mut u64| (xorshift(s) % 1000) as i64;
            (tup(&[i as i64, f(&mut s), f(&mut s), f(&mut s), f(&mut s), f(&mut s)]), Value::unit())
        })
        .collect()
}

/// Edges over `nodes` (key = src, val = dst) — for join / reduce / reach.
fn edges(nodes: usize, fanout: usize, seed: u64) -> Vec<(Row, Row)> {
    let mut s = seed;
    (0..nodes * fanout)
        .map(|_| (tup(&[(xorshift(&mut s) % nodes as u64) as i64, (xorshift(&mut s) % nodes as u64) as i64]), Value::unit()))
        .collect()
}

/// A scorecard case: the operator it isolates, its program, inputs, sizes, and target ratio.
struct Case {
    name: &'static str,
    isolates: &'static str,
    src: String,
    inputs: fn(usize) -> Vec<Vec<(Row, Row)>>,
    sizes: Vec<usize>,
    target: f64, // corgi/vec ratio we're aiming at (<=1 = parity-or-better)
}

fn cases() -> Vec<Case> {
    // 8 wide arithmetic maps, NO arrange — pure columnar compute.
    let mut map8 = String::from("let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5], $0[0]);\n");
    let mut prev = 'a';
    for i in 0..8 {
        let cur = (b'b' + i) as char;
        map8.push_str(&format!(
            "let {cur} = {prev} | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);\n"
        ));
        prev = cur;
    }
    map8.push_str(&format!("export \"r\" = {prev};\n"));

    vec![
        Case {
            name: "map8",
            isolates: "linear (map, columnar eval, no arrange)",
            src: map8,
            inputs: |n| vec![wide_input(n, 0xdead_beef)],
            sizes: vec![20_000, 100_000],
            target: 0.5,
        },
        Case {
            name: "filter",
            isolates: "linear (filter, no arrange)",
            src: "let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5]);\n\
                   let b = a | filter($1[0] != 999999);\n\
                   let c = b | filter($1[1] != 999999);\n\
                   export \"r\" = c | filter($1[2] != 999999);\n".into(),
            inputs: |n| vec![wide_input(n, 0x1234_5678)],
            sizes: vec![20_000, 100_000],
            target: 0.6,
        },
        Case {
            name: "arrange",
            isolates: "arrange (build + as_collection + transcode)",
            src: "export \"r\" = input 0 | key($0[0] ; $0[1]) | arrange;\n".into(),
            inputs: |n| vec![wide_input(n, 0x0a0a_0a0a)],
            sizes: vec![20_000, 100_000],
            target: 1.0,
        },
        Case {
            name: "join",
            isolates: "join (non-recursive, find merge-join)",
            src: "let a = input 0 | key($0[0] ; $0[1]);\n\
                  let b = input 1 | key($0[0] ; $0[1]);\n\
                  export \"r\" = a | join(b, ($1 ; $2)) | arrange;\n".into(),
            inputs: |n| vec![edges(n, 2, 0x11), edges(n, 2, 0x22)],
            sizes: vec![5_000, 20_000],
            target: 1.0,
        },
        Case {
            name: "reduce_distinct",
            isolates: "reduce (distinct, non-recursive)",
            src: "export \"r\" = input 0 | key($0[0] ; $0[1]) | distinct | arrange;\n".into(),
            inputs: |n| vec![edges(n, 4, 0x33)],
            sizes: vec![5_000, 20_000],
            target: 1.0,
        },
        Case {
            name: "reduce_count",
            isolates: "reduce (count, non-recursive)",
            src: "export \"r\" = input 0 | key($0[0] ; $0[1]) | count | arrange;\n".into(),
            inputs: |n| vec![edges(n, 4, 0x44)],
            sizes: vec![5_000, 20_000],
            target: 1.0,
        },
        Case {
            name: "reach",
            isolates: "integration (recursive join + distinct)",
            src: "let edges = input 0 | key($0[0] ; $0[1]);\n\
                  let roots = input 1 | key($0[0] ;);\n\
                  reach: {\n\
                    let proposals = reach | join(edges, ($2 ;));\n\
                    var reach = roots + proposals | distinct;\n\
                  }\n\
                  export \"r\" = reach::reach | arrange;\n".into(),
            inputs: |n| vec![edges(n, 3, 0x9e37), vec![(tup(&[0]), Value::unit())]],
            sizes: vec![1_000, 4_000],
            target: 1.0,
        },
    ]
}

fn verdict(ratio: f64, target: f64) -> &'static str {
    if ratio <= target { "✓ AT TARGET" }
    else if ratio < 1.0 { "win, below target" }
    else if ratio < 1.3 { "~ near parity" }
    else { "✗ SLOW" }
}

fn main() {
    let prog = std::env::var("PROG").ok();

    // Profile mode: loop one case's largest size on one backend (for samply).
    if let Some(name) = prog {
        let case = cases().into_iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no case {name}"));
        let p = compile(&case.src);
        let n = *case.sizes.last().unwrap();
        let inputs = (case.inputs)(n);
        let backend = std::env::var("BACKEND").unwrap_or_else(|_| "corgi".into());
        let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(400);
        let mut acc = 0usize;
        for _ in 0..iters {
            let out = if backend == "vec" { vec::evaluate(&p, &inputs) } else { corgi::evaluate(&p, &inputs) };
            acc += out.values().map(|v| v.len()).sum::<usize>();
        }
        std::hint::black_box(acc);
        eprintln!("done {name} backend={backend} iters={iters} (acc={acc})");
        return;
    }

    // Scorecard mode.
    println!("{:<16} {:<44} {:>8} {:>10} {:>10} {:>7} {:>7}  {}",
             "case", "isolates", "n", "vec", "corgi", "ratio", "target", "verdict");
    println!("{}", "-".repeat(130));
    for case in cases() {
        let p = compile(&case.src);
        for &n in &case.sizes {
            let inputs = (case.inputs)(n);
            assert_eq!(corgi::evaluate(&p, &inputs), vec::evaluate(&p, &inputs), "MISMATCH in {}", case.name);
            let vt = bench(5, || { std::hint::black_box(vec::evaluate(&p, &inputs)); });
            let ct = bench(5, || { std::hint::black_box(corgi::evaluate(&p, &inputs)); });
            let ratio = ct.as_secs_f64() / vt.as_secs_f64();
            println!("{:<16} {:<44} {:>8} {:>9.2?} {:>9.2?} {:>6.2}x {:>6.2}x  {}",
                     case.name, case.isolates, n, vt, ct, ratio, case.target, verdict(ratio, case.target));
        }
    }
    println!("\nfloor = parity (ratio 1.0); goal = beat vec (ratio < 1.0). Profile a case: PROG=<name> BACKEND=corgi samply record ...");
}
