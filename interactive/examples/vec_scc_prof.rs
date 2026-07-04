//! Profiling target: loop DDIR-VEC's SCC at a fixed size for samply — the vec-backend twin of
//! `corgi_scc_prof`, so the self-time attribution can be compared apples-to-apples.
//!
//!   ITERS=200 N=800 samply record -- .../vec_scc_prof

use interactive::backend::vec;
use interactive::ir::Value;
use interactive::{lower, parse};

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

const SCC_SRC: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let trans = edges | key($1 ; $0);
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }
        let trim_fwd = edges | join(fwd::labels, ($1 ; $0, $2)) | join(fwd::labels, ($0 ; $1, $2)) | filter($1[1] == $1[2]) | key($0 ; $1[0]);
        bwd: {
            let nodes = trans | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(trim_fwd, ($2 ; $1));
        }
        let trim_bwd = trans | join(bwd::labels, ($1 ; $0, $2)) | join(bwd::labels, ($0 ; $1, $2)) | filter($1[1] == $1[2]) | key($0 ; $1[0]);
        var trim = trim_bwd - edges;
    }
    export "result" = outer::scc | map(;) | arrange;
"#;

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(SCC_SRC));
    p.optimize();
    let nodes: u64 = std::env::var("N").ok().and_then(|s| s.parse().ok()).unwrap_or(800);
    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(200);
    let mut seed = 0xc0ff_ee42u64;
    let edges: Vec<(Value, Value)> = (0..nodes * 2)
        .map(|_| (Value::Tuple(vec![Value::Int((xorshift(&mut seed) % nodes) as i64), Value::Int((xorshift(&mut seed) % nodes) as i64)]), Value::unit()))
        .collect();
    let inputs = vec![edges];
    let mut acc = 0usize;
    for _ in 0..iters {
        acc += std::hint::black_box(vec::evaluate(&p, &inputs)).len();
    }
    eprintln!("done vec scc n={nodes} iters={iters} (acc={acc})");
}
