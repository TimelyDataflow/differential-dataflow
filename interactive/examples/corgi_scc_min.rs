//! Minimize an SCC corgi-vs-vec divergence: sweep (nodes, seed), find the smallest edge set where
//! `corgi::evaluate != vec::evaluate`, then greedily shrink the edge set (delta-debug) while the
//! mismatch persists. Prints the minimal failing graph for conventional tracing.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_scc_min

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
"#;

fn compile() -> interactive::scope_ir::Program {
    let mut p = lower::lower_tree(parse::pipe::parse(SCC_SRC));
    p.optimize();
    p
}

/// True iff corgi and vec agree on this edge set.
fn agrees(prog: &interactive::scope_ir::Program, edges: &[(Row, Row)]) -> bool {
    let inputs = vec![edges.to_vec()];
    corgi::evaluate(prog, &inputs) == vec::evaluate(prog, &inputs)
}

fn main() {
    let prog = compile();

    // 1. Sweep (nodes, seed) for the smallest failing edge count.
    let mut found: Option<(usize, u64, Vec<(Row, Row)>)> = None;
    'search: for &nodes in &[4usize, 5, 6, 7, 8, 10, 12, 16, 20, 30, 50] {
        for seed_i in 0..4000u64 {
            let mut seed = 0x1000 + seed_i.wrapping_mul(0x9e3779b97f4a7c15);
            let n_edges = nodes * 2;
            let edges: Vec<(Row, Row)> = (0..n_edges)
                .map(|_| (tup((xorshift(&mut seed) % nodes as u64) as i64, (xorshift(&mut seed) % nodes as u64) as i64), Value::unit()))
                .collect();
            if !agrees(&prog, &edges) {
                println!("first mismatch: nodes={nodes} seed_i={seed_i} n_edges={n_edges}");
                found = Some((nodes, seed_i, edges));
                break 'search;
            }
        }
        println!("  nodes={nodes}: all {} seeds agree", 4000);
    }

    let Some((nodes, _seed, mut edges)) = found else {
        println!("NO mismatch found in the sweep.");
        return;
    };

    // 2. Delta-debug: greedily drop edges while the mismatch persists.
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

    // 3. Report the minimal failing graph and the two outputs.
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
