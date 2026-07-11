//! Profiling target for B2 scc-compound: loop one backend at a fixed size for samply.
//!
//!   N=50000 ITERS=1 BACKEND=corgi samply record --save-only -o /tmp/p.json.gz -- \
//!       target/release/examples/corgi_compound_prof
//!
//! BACKEND=corgi (default) or vec — built identically for apples-to-apples profiles.

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

const SCC_COMPOUND_SRC: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let trans = edges | key($1 ; $0);
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | key($1 ; $1) | enter_at($1[0][0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }
        let trim_fwd = edges | join(fwd::labels, ($1 ; $0, $2)) | join(fwd::labels, ($0 ; $1, $2)) | filter($1[1] == $1[2]) | key($0 ; $1[0]);
        bwd: {
            let nodes = trans | key($1 ; $1) | enter_at($1[0][0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(trim_fwd, ($2 ; $1));
        }
        let trim_bwd = trans | join(bwd::labels, ($1 ; $0, $2)) | join(bwd::labels, ($0 ; $1, $2)) | filter($1[1] == $1[2]) | key($0 ; $1[0]);
        var trim = trim_bwd - edges;
    }
    export "result" = outer::scc | arrange;
"#;

const GROUP_BITS: u64 = 10;

fn node_val(x: u64) -> Value {
    Value::Tuple(vec![Value::Int((x >> GROUP_BITS) as i64), Value::Int((x & ((1 << GROUP_BITS) - 1)) as i64)])
}

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(SCC_COMPOUND_SRC));
    p.optimize();
    let nodes: u64 = std::env::var("N").ok().and_then(|s| s.parse().ok()).unwrap_or(50_000);
    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let backend = std::env::var("BACKEND").unwrap_or_else(|_| "corgi".into());
    let mut seed = 0xc0ff_ee42u64;
    let edges: Vec<(Value, Value)> = (0..nodes * 2)
        .map(|_| (Value::Tuple(vec![node_val(xorshift(&mut seed) % nodes), node_val(xorshift(&mut seed) % nodes)]), Value::unit()))
        .collect();
    let inputs = vec![edges];
    let mut acc = 0usize;
    for _ in 0..iters {
        acc += match backend.as_str() {
            "vec" => std::hint::black_box(vec::evaluate(&p, &inputs)).len(),
            _ => std::hint::black_box(corgi::evaluate(&p, &inputs)).len(),
        };
    }
    eprintln!("done scc-compound backend={backend} n={nodes} iters={iters} (acc={acc})");
}
