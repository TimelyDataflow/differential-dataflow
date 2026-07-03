//! Trace the minimal SCC divergence: on the fixed 4-node repro, export each intermediate of the SCC
//! program (fwd labels, trim_fwd, bwd labels, trim_bwd, scc) and compare corgi vs vec. The first
//! intermediate that diverges localizes the bug within corgi.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_scc_trace

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn tup(a: i64, b: i64) -> Row {
    Value::Tuple(vec![Value::Int(a), Value::Int(b)])
}

// The minimal failing graph found by corgi_scc_min.
fn edges() -> Vec<(Row, Row)> {
    [(0, 0), (0, 2), (1, 3), (2, 1), (2, 2)]
        .iter()
        .map(|&(a, b)| (tup(a, b), Value::unit()))
        .collect()
}

// The SCC body, with a parameterized export target.
fn src(export: &str) -> String {
    format!(r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        let trans = edges | key($1 ; $0);
        outer: {{
            let scc = edges + trim;
            fwd: {{
                let nodes = edges | key($1 ; $1) | enter_at($1[0]);
                let labels = proposals + nodes | min;
                var proposals = labels | join(scc, ($2 ; $1));
            }}
            let trim_fwd = edges
                | join(fwd::labels, ($1 ; $0, $2))
                | join(fwd::labels, ($0 ; $1, $2))
                | filter($1[1] == $1[2])
                | key($0 ; $1[0]);
            bwd: {{
                let nodes = trans | key($1 ; $1) | enter_at($1[0]);
                let labels = proposals + nodes | min;
                var proposals = labels | join(trim_fwd, ($2 ; $1));
            }}
            let trim_bwd = trans
                | join(bwd::labels, ($1 ; $0, $2))
                | join(bwd::labels, ($0 ; $1, $2))
                | filter($1[1] == $1[2])
                | key($0 ; $1[0]);
            let fl = fwd::labels;
            let bl = bwd::labels;
            var trim = trim_bwd - edges;
        }}
        export "result" = {export} | arrange;
    "#)
}

fn compile(export: &str) -> interactive::scope_ir::Program {
    let mut p = lower::lower_tree(parse::pipe::parse(&src(export)));
    p.optimize();
    p
}

fn main() {
    let inputs = vec![edges()];
    // From the inside out: labels feed trims feed the next scope feed scc.
    for target in ["outer::fl", "outer::trim_fwd", "outer::bl", "outer::trim_bwd", "outer::scc"] {
        // Some intermediates may not be exportable across scopes; skip on compile/eval panic.
        let res = std::panic::catch_unwind(|| {
            let prog = compile(target);
            let co = corgi::evaluate(&prog, &inputs);
            let ve = vec::evaluate(&prog, &inputs);
            (co, ve)
        });
        match res {
            Ok((co, ve)) => {
                let tag = if co == ve { "OK  " } else { "DIFF" };
                println!("[{tag}] {target}");
                if co != ve {
                    println!("       corgi = {co:?}");
                    println!("       vec   = {ve:?}");
                }
            }
            Err(_) => println!("[skip] {target} (not exportable)"),
        }
    }
}
