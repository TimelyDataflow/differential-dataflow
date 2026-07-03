//! Narrow the SCC divergence: is it already in single-level min-label-propagation (the `fwd` block of
//! SCC — one dynamic scope, `min` + `join`), or does it need SCC's nested trimming/negation?
//! Sweep+delta-debug corgi-vs-vec on the standalone forward propagation, comparing the FULL labels.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_cc_min

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn tup(a: i64, b: i64) -> Row {
    Value::Tuple(vec![Value::Int(a), Value::Int(b)])
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

// Standalone forward min-label propagation — SCC's `fwd` block, joining against `edges` directly.
const SRC: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let trans = edges | key($1 ; $0);
    fwd: {
        let nodes = edges | key($1 ; $1) | enter_at($1[0]);
        let labels = proposals + nodes | min;
        var proposals = labels | join(edges, ($2 ; $1));
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
    export "result" = bwd::labels | arrange;
"#;

fn compile() -> interactive::scope_ir::Program {
    let mut p = lower::lower_tree(parse::pipe::parse(SRC));
    p.optimize();
    p
}

fn agrees(prog: &interactive::scope_ir::Program, edges: &[(Row, Row)]) -> bool {
    let inputs = vec![edges.to_vec()];
    corgi::evaluate(prog, &inputs) == vec::evaluate(prog, &inputs)
}

fn main() {
    let prog = compile();

    let mut found: Option<(usize, Vec<(Row, Row)>)> = None;
    'search: for &nodes in &[4usize, 5, 6, 8, 10, 12, 16, 20, 30, 50, 80] {
        for seed_i in 0..6000u64 {
            let mut seed = 0x1000 + seed_i.wrapping_mul(0x9e3779b97f4a7c15);
            let n_edges = nodes * 2;
            let edges: Vec<(Row, Row)> = (0..n_edges)
                .map(|_| (tup((xorshift(&mut seed) % nodes as u64) as i64, (xorshift(&mut seed) % nodes as u64) as i64), Value::unit()))
                .collect();
            if !agrees(&prog, &edges) {
                println!("first mismatch: nodes={nodes} seed_i={seed_i} n_edges={n_edges}");
                found = Some((nodes, edges));
                break 'search;
            }
        }
        println!("  nodes={nodes}: all seeds agree");
    }

    let Some((nodes, mut edges)) = found else {
        println!("\nNO two-pass (fwd->trim->bwd, no outer loop) mismatch found — the bug needs SCC's nesting/negation.");
        return;
    };

    // Delta-debug.
    let mut changed = true;
    while changed {
        changed = false;
        let mut i = 0;
        while i < edges.len() {
            let mut trial = edges.clone();
            trial.remove(i);
            if !trial.is_empty() && !agrees(&prog, &trial) {
                edges = trial;
                changed = true;
            } else {
                i += 1;
            }
        }
    }

    println!("\nMINIMAL failing graph: nodes={nodes}, {} edges:", edges.len());
    let mut es: Vec<(i64, i64)> = edges.iter().map(|(k, _)| match k {
        Value::Tuple(f) => (f[0].as_int(), f[1].as_int()),
        _ => unreachable!(),
    }).collect();
    es.sort();
    println!("  edges = {es:?}");
    let inputs = vec![edges.clone()];
    println!("  corgi = {:?}", corgi::evaluate(&prog, &inputs));
    println!("  vec   = {:?}", vec::evaluate(&prog, &inputs));
}
