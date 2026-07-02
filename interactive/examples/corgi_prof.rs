//! Reach-only profiling harness: runs the corgi backend on recursive reachability in a tight loop so
//! a sampling profiler (samply) isolates the recursive join+distinct hot path.
//!
//!   ~/.cargo/bin/samply record --save-only -o /tmp/reach.json.gz -- \
//!       ~/.cargo/bin/cargo run --release -p interactive --example corgi_prof

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(
        r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        let roots = input 1 | key($0[0] ;);
        reach: {
            let proposals = reach | join(edges, ($2 ;));
            var reach = roots + proposals | distinct;
        }
        export "result" = reach::reach | arrange;
    "#,
    ));
    p.optimize();

    let nodes = 4_000usize;
    let mut seed = 0x9e37_79b9u64;
    let n_edges = nodes * 3;
    let edges: Vec<(Value, Value)> = (0..n_edges)
        .map(|_| (tup(&[(xorshift(&mut seed) % nodes as u64) as i64, (xorshift(&mut seed) % nodes as u64) as i64]), Value::unit()))
        .collect();
    let roots = vec![(tup(&[0]), Value::unit())];
    let inputs = vec![edges, roots];

    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let backend = std::env::var("BACKEND").unwrap_or_else(|_| "corgi".into());
    let mut acc = 0usize;
    for _ in 0..iters {
        let out = if backend == "vec" { vec::evaluate(&p, &inputs) } else { corgi::evaluate(&p, &inputs) };
        acc += out.values().map(|v| v.len()).sum::<usize>();
    }
    std::hint::black_box(acc);
    eprintln!("done {iters} iters backend={backend} (acc={acc})");
}
