//! B2 "scc-compound": SCC where node identity is a compound value — each node is the
//! nested tuple (group, index) = (x >> 10, x & 1023) — same control structure as B1
//! (corgi_scc_big) but compound keys/labels everywhere: no value-as-id fast path, so
//! hashing, compound compares, and gather of nested columns carry the load.
//!
//!   N=100000,150000 ~/.cargo/bin/cargo run --release --example corgi_scc_compound
//!
//! Fairness: the native twin is `strongly_connected_at` over (i64,i64) nodes with
//! `logic = |n| n.0` — the same delay DDIR's `enter_at($1[0][0])` computes (bucket by
//! group). The encoding x -> (x>>10, x&1023) is order-preserving, so min-label agrees
//! with B1 on the same graph.
//!
//! Correctness (CHECK=1 forces; default smallest size): corgi == vec on the full SCC
//! edge set, and the compound program's output mapped back through the encoding equals
//! the plain-encoding program's output (vec backend) on the same graph.

// The suite runs on mimalloc (as a real deployment would — ddir_server does): the
// system allocator was 27-28% of both DDIR backends' SCC profiles. One binary per
// benchmark, so every column (native/fair/vec/corgi) shares the same allocator.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

use differential_dataflow::algorithms::graphs::scc::strongly_connected_at;
use differential_dataflow::input::Input;
use timely::dataflow::operators::probe::Handle;

/// Compound-node SCC: identical to B1's program except nodes are single fields holding
/// (group, index) tuples, and enter_at buckets by the group (first field of the node).
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

/// Plain-encoding SCC (B1's program with the full edge-set export) — the reference for
/// the encoding cross-check.
const SCC_PLAIN_SRC: &str = r#"
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
    export "result" = outer::scc | arrange;
"#;

const GROUP_BITS: u64 = 10;

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

fn node_val(x: u64) -> Value {
    Value::Tuple(vec![Value::Int((x >> GROUP_BITS) as i64), Value::Int((x & ((1 << GROUP_BITS) - 1)) as i64)])
}

fn decode(v: &Value) -> i64 {
    match v {
        Value::Tuple(fs) => match (&fs[0], &fs[1]) {
            (Value::Int(g), Value::Int(i)) => (g << GROUP_BITS) | i,
            other => panic!("decode: unexpected node fields {other:?}"),
        },
        other => panic!("decode: unexpected node {other:?}"),
    }
}

fn native_scc_compound_once(edges: &[(u64, u64)]) {
    let enc = |x: u64| ((x >> GROUP_BITS) as i64, (x & ((1 << GROUP_BITS) - 1)) as i64);
    let edges: Vec<((i64, i64), (i64, i64))> = edges.iter().map(|&(s, d)| (enc(s), enc(d))).collect();
    timely::execute_directly(move |worker| {
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, graph) = scope.new_collection::<((i64, i64), (i64, i64)), isize>();
            strongly_connected_at(graph, |n| n.0 as u64).probe_with(&mut probe);
            input
        });
        for &(s, d) in &edges { input.insert((s, d)); }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }
    });
}

fn once<F: FnMut()>(mut f: F) -> Duration { let t = Instant::now(); f(); t.elapsed() }

fn consolidated(export: &[((Value, Value), i64)]) -> Vec<((Value, Value), i64)> {
    let mut map: std::collections::BTreeMap<(Value, Value), i64> = Default::default();
    for ((k, v), d) in export { *map.entry((k.clone(), v.clone())).or_insert(0) += d; }
    map.into_iter().filter(|(_, d)| *d != 0).collect()
}

fn main() {
    let mut p_compound = lower::lower_tree(parse::pipe::parse(SCC_COMPOUND_SRC));
    p_compound.optimize();
    let mut p_plain = lower::lower_tree(parse::pipe::parse(SCC_PLAIN_SRC));
    p_plain.optimize();

    let sizes: Vec<u64> = std::env::var("N").ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![100_000, 150_000]);

    println!("scc-compound (B2) — nodes are (group,index) tuples; fair native / DDIR-vec / DDIR-corgi (e = 2n):");
    for (i, &nodes) in sizes.iter().enumerate() {
        let n_edges = nodes * 2;
        let mut seed = 0xc0ff_ee42u64;
        let raw_edges: Vec<(u64, u64)> = (0..n_edges)
            .map(|_| (xorshift(&mut seed) % nodes, xorshift(&mut seed) % nodes))
            .collect();

        let compound_rows: Vec<(Value, Value)> = raw_edges.iter()
            .map(|&(s, d)| (Value::Tuple(vec![node_val(s), node_val(d)]), Value::unit()))
            .collect();
        let inputs = vec![compound_rows];

        let check = std::env::var("CHECK").ok().map(|s| s != "0").unwrap_or(i == 0);
        if check {
            let vec_out = vec::evaluate(&p_compound, &inputs);
            assert_eq!(corgi::evaluate(&p_compound, &inputs), vec_out, "corgi != vec at n={nodes}");

            // Encoding cross-check: compound output mapped back == plain-program output.
            let plain_rows: Vec<(Value, Value)> = raw_edges.iter()
                .map(|&(s, d)| (Value::Tuple(vec![Value::Int(s as i64), Value::Int(d as i64)]), Value::unit()))
                .collect();
            let plain_out = vec::evaluate(&p_plain, &vec![plain_rows]);
            // Export shape: key = (src_node), val = (dst_node), each node one compound field.
            let remap = |t: &Value| match t {
                Value::Tuple(fs) => Value::Tuple(fs.iter().map(|f| Value::Int(decode(f))).collect()),
                other => panic!("unexpected row {other:?}"),
            };
            let mut mapped: Vec<((Value, Value), i64)> = consolidated(&vec_out["result"]).into_iter()
                .map(|((k, v), d)| ((remap(&k), remap(&v)), d))
                .collect();
            mapped.sort();
            assert_eq!(mapped, consolidated(&plain_out["result"]), "compound != plain (mapped) at n={nodes}");
        }

        let ft = once(|| native_scc_compound_once(&raw_edges));
        let vt = once(|| { std::hint::black_box(vec::evaluate(&p_compound, &inputs)); });
        let ct = once(|| { std::hint::black_box(corgi::evaluate(&p_compound, &inputs)); });
        let (ff, vf, cf) = (ft.as_secs_f64(), vt.as_secs_f64(), ct.as_secs_f64());
        println!(
            "  n={nodes:<8} e={n_edges:<9}  native(fair) {ft:>8.2?}   vec-DDIR {vt:>8.2?} ({:.2}x nat)   corgi-DDIR {ct:>8.2?} ({:.2}x nat, {:.2}x vec){}",
            vf / ff, cf / ff, cf / vf, if check { "  [checked]" } else { "" },
        );
    }
}
