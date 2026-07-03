//! Emit the full (data, time, diff) update STREAM of SCC intermediates on the minimal 4-node repro,
//! via `inspect`, for one backend (BACKEND=corgi|vec). Diff the two streams offline (consolidated by
//! (data, time)) to find the FIRST moment of divergence — the operator whose inputs agree but output
//! does not is the bug.
//!
//!   BACKEND=corgi ~/.cargo/bin/cargo run --release -p interactive --example corgi_scc_stream 2>c.txt
//!   BACKEND=vec   ~/.cargo/bin/cargo run --release -p interactive --example corgi_scc_stream 2>v.txt

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn tup(a: i64, b: i64) -> Row {
    Value::Tuple(vec![Value::Int(a), Value::Int(b)])
}

fn edges() -> Vec<(Row, Row)> {
    [(0, 0), (0, 2), (1, 3), (2, 1), (2, 2)]
        .iter()
        .map(|&(a, b)| (tup(a, b), Value::unit()))
        .collect()
}

// SCC with `inspect` taps on fwd::labels, trim_fwd, bwd::labels (each lifted to the outer scope).
const SRC: &str = r#"
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
            let bin = proposals + nodes | inspect(bwdin);
            let labels = bin | min | inspect(bwdout);
            var proposals = labels | join(trim_fwd, ($2 ; $1));
        }
        let trim_bwd = trans
            | join(bwd::labels, ($1 ; $0, $2))
            | join(bwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);
        let fl = fwd::labels | inspect(fl);
        let tf = trim_fwd | inspect(tf);
        let bl = bwd::labels | inspect(bl);
        var trim = trim_bwd - edges;
    }
    export "result" = outer::scc | arrange;
"#;

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(SRC));
    p.optimize();
    let inputs = vec![edges()];
    match std::env::var("BACKEND").as_deref() {
        Ok("vec") => { vec::evaluate(&p, &inputs); }
        _ => { corgi::evaluate(&p, &inputs); }
    }
}
