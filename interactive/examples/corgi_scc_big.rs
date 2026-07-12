//! Large-N SCC: native DD vs DDIR-vec vs DDIR-corgi at the sizes Frank actually runs
//! (n = 100k–1m, e = 2n). The small corgi_perf sizes (n<=1000) barely touch the spine
//! compaction machinery; this is where the arrangement/merge cost shows its true scaling.
//!
//!   N=100000,250000,500000,1000000 ~/.cargo/bin/cargo run --release -p interactive --example corgi_scc_big
//!
//! One run per backend per size (these are seconds-scale; no median). CHECK=1 asserts corgi==vec
//! (doubles the corgi+vec work at that size) — on by default only for the smallest size.

// The suite runs on mimalloc (as a real deployment would — ddir_server does): the
// system allocator was 27-28% of both DDIR backends' SCC profiles. One binary per
// benchmark, so every column (native/fair/vec/corgi) shares the same allocator.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

use differential_dataflow::algorithms::graphs::scc::{strongly_connected, strongly_connected_at};
use differential_dataflow::input::Input;
use timely::dataflow::operators::probe::Handle;

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

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

fn tup(fields: &[i64]) -> Value { Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect()) }

fn native_scc_once(edges: &[(usize, usize)]) {
    let edges = edges.to_vec();
    timely::execute_directly(move |worker| {
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            strongly_connected(graph).probe_with(&mut probe);
            input
        });
        for &(s, d) in &edges { input.insert((s, d)); }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }
    });
}

/// The FAIR compiled baseline: label introduction log-bucketed exactly as DDIR's
/// `enter_at($1[0])` (propagate_core applies `256 * (64 - leading_zeros(logic(label)))`,
/// the same delay vec.rs/corgi.rs compute), so native and DDIR run the same algorithm.
fn native_scc_at_once(edges: &[(usize, usize)]) {
    let edges = edges.to_vec();
    timely::execute_directly(move |worker| {
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            strongly_connected_at(graph, |x| *x as u64).probe_with(&mut probe);
            input
        });
        for &(s, d) in &edges { input.insert((s, d)); }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }
    });
}

fn once<F: FnMut()>(mut f: F) -> Duration { let t = Instant::now(); f(); t.elapsed() }

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(SCC_SRC));
    p.optimize();
    let sizes: Vec<u64> = std::env::var("N").ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![100_000, 250_000, 500_000, 1_000_000]);

    println!("SCC large-N — native DD / fair native (enter_at) / DDIR-vec / DDIR-corgi (e = 2n):");
    let seed0: u64 = std::env::var("SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0xc0ff_ee42);
    for (i, &nodes) in sizes.iter().enumerate() {
        let n_edges = nodes * 2;
        let mut seed = seed0;
        let edges: Vec<(Value, Value)> = (0..n_edges)
            .map(|_| (tup(&[(xorshift(&mut seed) % nodes) as i64, (xorshift(&mut seed) % nodes) as i64]), Value::unit()))
            .collect();
        let mut seed2 = seed0;
        let native_edges: Vec<(usize, usize)> = (0..n_edges)
            .map(|_| ((xorshift(&mut seed2) % nodes) as usize, (xorshift(&mut seed2) % nodes) as usize))
            .collect();
        let inputs = vec![edges];

        // Correctness check (doubles work): default on for the first/smallest size only.
        let check = std::env::var("CHECK").ok().map(|s| s != "0").unwrap_or(i == 0);
        if check {
            assert_eq!(corgi::evaluate(&p, &inputs), vec::evaluate(&p, &inputs), "corgi != vec at n={nodes}");
        }

        let nt = once(|| native_scc_once(&native_edges));
        let ft = once(|| native_scc_at_once(&native_edges));
        let vt = once(|| { std::hint::black_box(vec::evaluate(&p, &inputs)); });
        let ct = once(|| { std::hint::black_box(corgi::evaluate(&p, &inputs)); });
        let (nf, ff, vf, cf) = (nt.as_secs_f64(), ft.as_secs_f64(), vt.as_secs_f64(), ct.as_secs_f64());
        println!(
            "  n={nodes:<8} e={n_edges:<9}  native {nt:>8.2?}   fair(enter_at) {ft:>8.2?} ({:.2}x nat)   vec-DDIR {vt:>8.2?} ({:.2}x fair)   corgi-DDIR {ct:>8.2?} ({:.2}x fair, {:.2}x vec){}",
            ff / nf, vf / ff, cf / ff, cf / vf, if check { "  [checked]" } else { "" },
        );
    }
}
