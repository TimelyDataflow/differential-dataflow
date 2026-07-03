//! End-to-end performance: the corgi backend vs the vec backend on whole DDIR programs (NOT the
//! Phase-1 eval-only microbench). Measures the real thing this work produced — `corgi::evaluate`
//! vs `vec::evaluate` — across two shapes of program:
//!   1. LINEAR-HEAVY: a long chain of compilable Project/Filter ops over wide rows — corgi's
//!      columnar strength (eval stays in columns; transcode only at the arrange boundary).
//!   2. REACH: recursive reachability — join + distinct + concat + leave_dynamic; narrow rows,
//!      per-key value slices where the columnar win doesn't apply and row-wise fallbacks + the
//!      (eager, unoptimized) CorgiBatch merge dominate.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_perf

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn tup(fields: &[i64]) -> Row {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

/// Median wall-clock of `iters` runs of `f` (min 3), after one warmup.
fn bench<F: FnMut()>(iters: usize, mut f: F) -> Duration {
    f(); // warmup
    let mut ts: Vec<Duration> = (0..iters.max(3))
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed()
        })
        .collect();
    ts.sort();
    ts[ts.len() / 2]
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn compile(src: &str) -> interactive::scope_ir::Program {
    let mut p = lower::lower_tree(parse::pipe::parse(src));
    p.optimize();
    p
}

fn report(name: &str, n: usize, vec_t: Duration, corgi_t: Duration) {
    let ratio = corgi_t.as_secs_f64() / vec_t.as_secs_f64();
    let faster = if ratio < 1.0 {
        format!("corgi {:.2}x FASTER", 1.0 / ratio)
    } else {
        format!("corgi {ratio:.2}x slower")
    };
    println!("  {name:<22} n={n:<7}  vec {vec_t:>10.2?}   corgi {corgi_t:>10.2?}   → {faster}");
}

fn main() {
    // ---------- 1. LINEAR-HEAVY (corgi's columnar strength) ----------
    // Wide 6-field value; a chain of arithmetic Project maps + a filter, then arrange. All ops are
    // corgi-compilable, so eval stays columnar; only the final arrange transcodes.
    // Flat comma-separated projection fields (the pipe `(K ; V1, V2, ...)` already tuples the list;
    // a `tuple(...)` field would NEST, so we list fields directly for a flat 6-wide value).
    let linear_src = r#"
        let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5], $0[0]);
        let b = a | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
        let c = b | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
        let d = c | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);
        let e = d | filter($1[0] != 999999);
        export "result" = e | arrange;
    "#;
    let linear = compile(linear_src);

    println!("1. LINEAR-HEAVY (3 wide arithmetic maps + filter + arrange; corgi columnar path):");
    for &n in &[2_000usize, 20_000, 100_000] {
        let mut seed = 0x1234_5678u64;
        let input: Vec<(Row, Row)> = (0..n)
            .map(|i| (tup(&[i as i64, (xorshift(&mut seed) % 1000) as i64, (xorshift(&mut seed) % 1000) as i64,
                             (xorshift(&mut seed) % 1000) as i64, (xorshift(&mut seed) % 1000) as i64, (xorshift(&mut seed) % 1000) as i64]),
                      Value::unit()))
            .collect();
        let inputs = vec![input];
        // sanity: identical output
        assert_eq!(corgi::evaluate(&linear, &inputs), vec::evaluate(&linear, &inputs));
        let vt = bench(5, || { std::hint::black_box(vec::evaluate(&linear, &inputs)); });
        let ct = bench(5, || { std::hint::black_box(corgi::evaluate(&linear, &inputs)); });
        report("linear-heavy", n, vt, ct);
    }

    // ---------- 2. REACH (recursive; join + distinct heavy, narrow rows) ----------
    let reach = compile(r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        let roots = input 1 | key($0[0] ;);
        reach: {
            let proposals = reach | join(edges, ($2 ;));
            var reach = roots + proposals | distinct;
        }
        export "result" = reach::reach | arrange;
    "#);

    println!("\n2. REACH (recursive: join + distinct + concat + leave_dynamic; narrow rows):");
    for &nodes in &[200usize, 1_000, 4_000] {
        let mut seed = 0x9e37_79b9u64;
        let n_edges = nodes * 3;
        let edges: Vec<(Row, Row)> = (0..n_edges)
            .map(|_| (tup(&[(xorshift(&mut seed) % nodes as u64) as i64, (xorshift(&mut seed) % nodes as u64) as i64]), Value::unit()))
            .collect();
        let roots = vec![(tup(&[0]), Value::unit())];
        let inputs = vec![edges, roots];
        assert_eq!(corgi::evaluate(&reach, &inputs), vec::evaluate(&reach, &inputs));
        let vt = bench(3, || { std::hint::black_box(vec::evaluate(&reach, &inputs)); });
        let ct = bench(3, || { std::hint::black_box(corgi::evaluate(&reach, &inputs)); });
        report("reach", nodes, vt, ct);
    }

    // ---------- 2b. SCC (nested recursion: fwd/bwd label propagation via `min`) ----------
    let scc = compile(r#"
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
    "#);

    println!("\n2b. SCC (nested recursion; fwd/bwd label propagation via min; the sharp case):");
    for &nodes in &[200usize, 500, 800, 1_000] {
        let mut seed = 0xc0ff_ee42u64;
        let n_edges = nodes * 2;
        let edges: Vec<(Row, Row)> = (0..n_edges)
            .map(|_| (tup(&[(xorshift(&mut seed) % nodes as u64) as i64, (xorshift(&mut seed) % nodes as u64) as i64]), Value::unit()))
            .collect();
        let inputs = vec![edges];
        // NB: corgi diverges from vec on larger random graphs (a corgi-backend bug — the proxy reduce
        // tactic + reference backend pass the scc oracles in `reduce_reference.rs`). Flag, don't panic.
        let (co, ve) = (corgi::evaluate(&scc, &inputs), vec::evaluate(&scc, &inputs));
        if co != ve {
            println!("  scc                    n={nodes:<7}  !! MISMATCH corgi={co:?} vec={ve:?}");
        }
        let vt = bench(3, || { std::hint::black_box(vec::evaluate(&scc, &inputs)); });
        let ct = bench(3, || { std::hint::black_box(corgi::evaluate(&scc, &inputs)); });
        report("scc", nodes, vt, ct);
    }

    // ---------- 3. LINEAR COMPUTE ONLY (deep chain, NO arrange) — isolates the columnar eval ----------
    // 8 wide arithmetic maps, exported without arrange (only ToCorgi in / FromCorgi out transcode
    // bracket the columnar chain). This is where corgi's columnar scalar logic should actually pay.
    let mut deep = String::from("let a = input 0 | key($0[0] ; $0[1], $0[2], $0[3], $0[4], $0[5], $0[0]);\n");
    let mut prev = 'a';
    for i in 0..8 {
        let cur = (b'b' + i) as char;
        deep.push_str(&format!(
            "let {cur} = {prev} | map($0 ; $1[0]+$1[1], $1[1]+$1[2], $1[2]+$1[3], $1[3]+$1[4], $1[4]+$1[5], $1[0]);\n"
        ));
        prev = cur;
    }
    deep.push_str(&format!("export \"result\" = {prev};\n"));
    let deep = compile(&deep);

    println!("\n3. LINEAR COMPUTE ONLY (8 wide maps, NO arrange — isolates the columnar eval):");
    for &n in &[20_000usize, 100_000] {
        let mut seed = 0xdead_beefu64;
        let input: Vec<(Row, Row)> = (0..n)
            .map(|i| (tup(&[i as i64, (xorshift(&mut seed) % 1000) as i64, (xorshift(&mut seed) % 1000) as i64,
                             (xorshift(&mut seed) % 1000) as i64, (xorshift(&mut seed) % 1000) as i64, (xorshift(&mut seed) % 1000) as i64]),
                      Value::unit()))
            .collect();
        let inputs = vec![input];
        assert_eq!(corgi::evaluate(&deep, &inputs), vec::evaluate(&deep, &inputs));
        let vt = bench(5, || { std::hint::black_box(vec::evaluate(&deep, &inputs)); });
        let ct = bench(5, || { std::hint::black_box(corgi::evaluate(&deep, &inputs)); });
        report("linear-compute-8x", n, vt, ct);
    }

    println!("\n(Wall-clock includes a fixed timely single-worker setup per `evaluate` call, equal for both.)");
}
