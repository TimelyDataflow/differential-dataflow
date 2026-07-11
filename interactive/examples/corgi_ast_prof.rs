//! Profiling target for B4 ast-compute: loop one backend at a fixed size for samply.
//!
//!   N=1000000 ITERS=1 BACKEND=corgi samply record --save-only -o /tmp/p.json.gz -- \
//!       target/release/examples/corgi_ast_prof

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

const AST_SRC: &str = r#"
    con Fwd(1) = 0;
    con Bwd(1) = 1;

    let rows = input 0 | key($0[0] ; $0[1]);
    let lists = rows | map($0 ; list($0[0], $1[0], $0[0] + $1[0], $0[0] * $1[0], $0[0] - $1[0] + 32768, $1[0] * $1[0], $0[0] * $0[0], $0[0] + $1[0] * $1[0]));
    let exploded = lists | flatmap($1[0]);
    let tagged = exploded
        | map( $1[0] + 8 * if($1[1] < 500000, 0, 1)
             ; case if($1[1] < 250000,
                       Fwd(fold(list($0[0], $1[1], $0[0] * $1[1], $1[1] * $1[1]), 0, ^0 + ^1)),
                       Bwd(fold(list($0[0], $1[1], $0[0] * $1[1], $1[1] * $1[1]), 0, ^0 + ^1)))
               {
                 Fwd(s) => s,
                 Bwd(s) => s,
               } );
    let buckets = tagged | min;
    export "result" = buckets | arrange;
"#;

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(AST_SRC));
    p.optimize();
    let n_rows: u64 = std::env::var("N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let backend = std::env::var("BACKEND").unwrap_or_else(|_| "corgi".into());
    let mut seed = 0xfeed_f00d_u64;
    let rows: Vec<(Value, Value)> = (0..n_rows)
        .map(|_| {
            let a = (xorshift(&mut seed) % 32_768) as i64;
            let b = (xorshift(&mut seed) % 32_768) as i64;
            (Value::Tuple(vec![Value::Int(a), Value::Int(b)]), Value::unit())
        })
        .collect();
    let inputs = vec![rows];
    let mut acc = 0usize;
    for _ in 0..iters {
        acc += match backend.as_str() {
            "vec" => std::hint::black_box(vec::evaluate(&p, &inputs)).len(),
            _ => std::hint::black_box(corgi::evaluate(&p, &inputs)).len(),
        };
    }
    eprintln!("done ast backend={backend} n={n_rows} iters={iters} (acc={acc})");
}
