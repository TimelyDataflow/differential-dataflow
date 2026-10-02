//! What an importer pays for a shared trace: one producer publishes `edges` (keyed by
//! source), then `K` consumers install, each a two-hop join from its own query roots over
//! the imported `edges` (or, with `BENCH_QUERY=reach`, reachability: a join inside a loop).
//! Reports each consumer's install-to-answer time and the process's memory, then the
//! per-tick time under edge churn with all consumers live, and the published trace's size.
//!
//! ```text
//! cargo run --release --example import_bench -- [edges] [consumers] [churn] [ticks] [nodes]
//! DDIR_ARRANGED_IMPORTS=0 cargo run --release --example import_bench   # importers re-arrange
//! DDIR_BACKEND=corgi cargo run --release --example import_bench         # columnar exports
//! BENCH_QUERY=reach cargo run --release --example import_bench          # joins inside a loop
//! ```

use std::time::Instant;

use interactive::ir::Value;
use interactive::server::Server;
use interactive::{lower, parse};

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    program.optimize();
    server.install(worker, name, &program).unwrap();
}

/// This process's memory in MiB: on macOS its `footprint` (which, unlike RSS, counts compressed
/// pages, so it does not shrink under memory pressure), elsewhere its RSS. `None` if unavailable.
fn memory_mib() -> Option<f64> {
    let out = std::process::Command::new("footprint").args(["-p", &std::process::id().to_string()]).output();
    if let Ok(out) = out {
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        let mut words = text.split("Footprint:").nth(1)?.split_whitespace();
        let n: f64 = words.next()?.parse().ok()?;
        return match words.next()? {
            "B" => Some(n / (1024.0 * 1024.0)),
            "KB" => Some(n / 1024.0),
            "MB" => Some(n),
            "GB" => Some(n * 1024.0),
            _ => None,
        };
    }
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib: f64 = status.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024.0)
}

/// [`memory_mib`], formatted.
fn mem() -> String {
    memory_mib().map_or_else(|| "n/a".to_string(), |m| format!("{m:>7.1} MiB"))
}

fn main() {
    let args: Vec<u64> = std::env::args().skip(1).map(|a| a.parse().expect("integer argument")).collect();
    let edges = args.first().copied().unwrap_or(1_000_000);
    let consumers = args.get(1).copied().unwrap_or(8);
    let churn = args.get(2).copied().unwrap_or(1_000);
    let ticks = args.get(3).copied().unwrap_or(20);
    let nodes = args.get(4).copied().unwrap_or(edges / 2);
    let arranged = std::env::var("DDIR_ARRANGED_IMPORTS").map_or(true, |v| v != "0");
    // BENCH_QUERY=reach: each consumer computes reachability, joining `edges` inside a loop.
    let reach = std::env::var("BENCH_QUERY").as_deref() == Ok("reach");
    let roots = if reach { 1 } else { 100 };

    timely::execute_directly(move |worker| {
        let backend = std::env::var("DDIR_BACKEND").unwrap_or("vec".into()).parse().expect("DDIR_BACKEND");
        let mut server = Server::with_backend(backend);
        server.set_arranged_imports(arranged);
        println!("backend {backend:?}, arranged imports: {arranged}; {edges} edges over {nodes} nodes, {consumers} {} consumers, churn {churn}", if reach { "reach" } else { "two-hop" });

        install(&mut server, worker, "prod", r#"export "edges" = input 0 | key($0[0] ; $0[1]);"#);
        server.load(worker, "prod", 0, &format!("random:nodes={nodes},edges={edges},churn={churn}")).unwrap();
        let loaded = Instant::now();
        server.tick(worker);
        println!("producer:    {:>8.1} ms  mem {}", loaded.elapsed().as_secs_f64() * 1e3, mem());

        for c in 0..consumers {
            let name = format!("q{c}");
            let src = if reach {
                // Reachability: the join with `edges` is inside an iterative scope.
                format!(
                    r#"
                    let edges = import "edges";
                    let roots = input 0 | key($0[0] ;);
                    r: {{
                        let step = reach | join(edges, ($2 ;));
                        var reach = roots + step | distinct;
                    }}
                    export "reach{c}" = r::reach | key(;) | count;
                    "#
                )
            } else {
                format!(
                    r#"
                    let edges = import "edges";
                    let roots = input 0 | key($0[0] ;);
                    let hop1 = roots | join(edges, ($2 ; $0));
                    let hop2 = hop1 | join(edges, ($2 ; $1));
                    export "reach{c}" = hop2 | key($1 ;) | count;
                    "#
                )
            };
            let installed = Instant::now();
            install(&mut server, worker, &name, &src);
            for r in 0..roots {
                let root = (interactive::hash_u64(c * 1000 + r) % nodes) as i64;
                server.feed(&name, 0, Value::Tuple(vec![Value::Int(root)]), Value::unit(), None, 1).unwrap();
            }
            server.tick(worker);
            println!("consumer {c:>2}: {:>8.1} ms  mem {}", installed.elapsed().as_secs_f64() * 1e3, mem());
        }

        let churned = Instant::now();
        for _ in 0..ticks { server.tick(worker); }
        println!(
            "churn:       {:>8.2} ms/tick over {ticks} ticks ({churn} edges replaced per tick)  mem {}",
            churned.elapsed().as_secs_f64() * 1e3 / ticks as f64, mem(),
        );
        let answer: i64 = (0..consumers)
            .map(|c| server.snapshot(worker, &format!("reach{c}")).unwrap().iter().map(|(_, v, d)| ints(v) * d).sum::<i64>())
            .sum();
        println!("checksum (sum of consumer answers): {answer}");
        {
            use differential_dataflow::trace::TraceReader;
            use differential_dataflow::trace::implementations::spine_fueled::SpineBatch;
            use differential_dataflow::trace::chunk::Chunk;
            let (mut updates, mut spans) = (0, 0);
            match server.published("edges") {
                Some(interactive::server::Published::Rows(trace)) => {
                    trace.map_spans(|span| { spans += 1; if let Some(b) = &span.inner { updates += b.len(); } });
                }
                Some(interactive::server::Published::Columnar(trace)) => {
                    trace.map_spans(|span| { spans += 1; if let Some(b) = &span.inner { updates += b.chunks.iter().map(Chunk::len).sum::<usize>(); } });
                }
                None => {}
            }
            println!("published edges trace: {updates} updates in {spans} spans");
        }
    });
}

fn ints(v: &Value) -> i64 {
    match v {
        Value::Tuple(xs) => xs.iter().map(ints).sum(),
        Value::Int(n) => *n,
        _ => 0,
    }
}
