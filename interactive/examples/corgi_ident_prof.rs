//! Profiling target for B3 ident-join: loop one backend at a fixed size for samply.
//!
//!   N=1000000 ITERS=1 BACKEND=corgi samply record --save-only -o /tmp/p.json.gz -- \
//!       target/release/examples/corgi_ident_prof

// The suite runs on mimalloc (as a real deployment would — ddir_server does): the
// system allocator was 27-28% of both DDIR backends' SCC profiles. One binary per
// benchmark, so every column (native/fair/vec/corgi) shares the same allocator.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

const IDENT_SRC: &str = r#"
    let parent = input 0 | key($0[0] ; $0[1]);
    let terms  = input 1 | key($0[0] ; $0[1], $0[2]);
    find: {
        let reps = doubled + parent | min;
        var doubled = reps | key($1 ; $0) | join(reps, ($1 ; $2));
    }
    let by_a    = terms | key($1[0] ; $0, $1[1]);
    let with_a  = by_a  | join(find::reps, ($1[0] ; $1[1], $2));
    let by_b    = with_a | key($1[0] ; $0, $1[1]);
    let resolved = by_b | join(find::reps, ($1[0] ; $1[1], $2));
    export "reps" = find::reps | arrange;
    export "resolved" = resolved | arrange;
"#;

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

fn tup(fields: &[i64]) -> Value { Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect()) }

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(IDENT_SRC));
    p.optimize();
    let nodes: u64 = std::env::var("N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let backend = std::env::var("BACKEND").unwrap_or_else(|_| "corgi".into());
    let chain: u64 = std::env::var("CHAIN").ok().and_then(|s| s.parse().ok()).unwrap_or(64);
    let n_terms = nodes * 2;
    let parents: Vec<(Value, Value)> = (0..nodes)
        .map(|x| (tup(&[x as i64, if x % chain == 0 { x as i64 } else { (x - 1) as i64 }]), Value::unit()))
        .collect();
    let mut seed = 0xdead_beef_u64;
    let terms: Vec<(Value, Value)> = (0..n_terms)
        .map(|j| (tup(&[(nodes + j) as i64, (xorshift(&mut seed) % nodes) as i64, (xorshift(&mut seed) % nodes) as i64]), Value::unit()))
        .collect();
    let inputs = vec![parents, terms];
    let mut acc = 0usize;
    for _ in 0..iters {
        acc += match backend.as_str() {
            "vec" => std::hint::black_box(vec::evaluate(&p, &inputs)).len(),
            _ => std::hint::black_box(corgi::evaluate(&p, &inputs)).len(),
        };
    }
    eprintln!("done ident backend={backend} n={nodes} iters={iters} (acc={acc})");
}
