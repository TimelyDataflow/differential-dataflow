//! The DDIR benchmark runner: interpreted programs from `examples/programs/*.ddp`,
//! compiled twins and input generators registered here by bench name.
//!
//!   ddir_bench <bench>        bench ∈ { scc, scc-compound, ident, ast }
//!
//! Environment:
//!   N=a,b,..   sizes (per-bench defaults below)
//!   CHECK=0/1  correctness assertions (default: first size only)
//!   SEED=      graph seed (scc)          CHAIN=  chain length (ident)
//!   BACKEND=corgi|vec|native|native-fair with ITERS=k — profiling mode: loop ONE
//!   column at the first size, no table (a samply target; replaces the prof twins).
//!
//! Fairness: the scc twins include `strongly_connected_at` with the same enter_at
//! log-bucketing the DDIR programs apply, so compiled and interpreted run the same
//! algorithm. Correctness: corgi == vec asserted per checked row; ident and ast also
//! check both backends and the compiled twin against closed-form oracles;
//! scc-compound additionally maps its compound encoding back and compares against
//! the plain-encoding program.

// The suite runs on mimalloc (as a real deployment would): one binary, so every
// column shares the allocator.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::scope_ir::Program;
use interactive::{lower, parse};

use differential_dataflow::algorithms::graphs::scc::{strongly_connected, strongly_connected_at};
use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::Variable;
use timely::dataflow::operators::probe::Handle;
use timely::order::Product;

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

fn tup(fields: &[i64]) -> Value { Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect()) }

fn once<F: FnMut()>(mut f: F) -> Duration { let t = Instant::now(); f(); t.elapsed() }

fn program(name: &str) -> Program {
    let path = format!("{}/examples/programs/{name}.ddp", env!("CARGO_MANIFEST_DIR"));
    let mut p = lower::lower_tree(parse::pipe::parse(&interactive::load_program(&path)));
    p.optimize();
    p
}

fn sizes(default: &[u64]) -> Vec<u64> {
    std::env::var("N").ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| default.to_vec())
}

fn checked(i: usize) -> bool {
    std::env::var("CHECK").ok().map(|s| s != "0").unwrap_or(i == 0)
}

/// Profiling mode: `BACKEND=` set — loop that one column `ITERS` times at the first size.
fn prof_mode() -> Option<(String, usize)> {
    std::env::var("BACKEND").ok().map(|b| {
        let iters = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
        (b, iters)
    })
}

fn consolidated(export: &[((Value, Value), i64)]) -> Vec<((Value, Value), i64)> {
    let mut map: std::collections::BTreeMap<(Value, Value), i64> = Default::default();
    for ((k, v), d) in export { *map.entry((k.clone(), v.clone())).or_insert(0) += d; }
    map.into_iter().filter(|(_, d)| *d != 0).collect()
}

// ---------------------------------------------------------------------------------------
// scc — B1: SCC four ways at n = 100k..1m, e = 2n.
// ---------------------------------------------------------------------------------------

fn native_scc_once(edges: &[(usize, usize)], fair: bool) {
    let edges = edges.to_vec();
    timely::execute_directly(move |worker| {
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            if fair {
                strongly_connected_at(graph, |x| *x as u64).probe_with(&mut probe);
            } else {
                strongly_connected(graph).probe_with(&mut probe);
            }
            input
        });
        for &(s, d) in &edges { input.insert((s, d)); }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }
    });
}

fn scc_edges(nodes: u64, seed0: u64) -> (Vec<(Value, Value)>, Vec<(usize, usize)>) {
    let n_edges = nodes * 2;
    let mut seed = seed0;
    let rows: Vec<(Value, Value)> = (0..n_edges)
        .map(|_| (tup(&[(xorshift(&mut seed) % nodes) as i64, (xorshift(&mut seed) % nodes) as i64]), Value::unit()))
        .collect();
    let mut seed = seed0;
    let native: Vec<(usize, usize)> = (0..n_edges)
        .map(|_| ((xorshift(&mut seed) % nodes) as usize, (xorshift(&mut seed) % nodes) as usize))
        .collect();
    (rows, native)
}

fn bench_scc() {
    let p = program("scc");
    let seed0: u64 = std::env::var("SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0xc0ff_ee42);
    let sizes = sizes(&[100_000, 250_000, 500_000, 1_000_000]);

    if let Some((backend, iters)) = prof_mode() {
        let nodes = sizes[0];
        let (rows, native) = scc_edges(nodes, seed0);
        let inputs = vec![rows];
        for i in 0..iters {
            match backend.as_str() {
                "corgi" => { std::hint::black_box(corgi::evaluate(&p, &inputs)); }
                "vec" => { std::hint::black_box(vec::evaluate(&p, &inputs)); }
                "native" => native_scc_once(&native, false),
                "native-fair" => native_scc_once(&native, true),
                other => panic!("scc: unknown BACKEND {other}"),
            }
            println!("done scc backend={backend} n={nodes} iter={i}");
        }
        return;
    }

    println!("SCC large-N — native DD / fair native (enter_at) / DDIR-vec / DDIR-corgi (e = 2n):");
    for (i, &nodes) in sizes.iter().enumerate() {
        let (rows, native) = scc_edges(nodes, seed0);
        let n_edges = nodes * 2;
        let inputs = vec![rows];
        let check = checked(i);
        if check {
            assert_eq!(corgi::evaluate(&p, &inputs), vec::evaluate(&p, &inputs), "corgi != vec at n={nodes}");
        }
        let nt = once(|| native_scc_once(&native, false));
        let ft = once(|| native_scc_once(&native, true));
        let vt = once(|| { std::hint::black_box(vec::evaluate(&p, &inputs)); });
        let ct = once(|| { std::hint::black_box(corgi::evaluate(&p, &inputs)); });
        let (nf, ff, vf, cf) = (nt.as_secs_f64(), ft.as_secs_f64(), vt.as_secs_f64(), ct.as_secs_f64());
        println!(
            "  n={nodes:<8} e={n_edges:<9}  native {nt:>8.2?}   fair(enter_at) {ft:>8.2?} ({:.2}x nat)   vec-DDIR {vt:>8.2?} ({:.2}x fair)   corgi-DDIR {ct:>8.2?} ({:.2}x fair, {:.2}x vec){}",
            ff / nf, vf / ff, cf / ff, cf / vf, if check { "  [checked]" } else { "" },
        );
    }
}

// ---------------------------------------------------------------------------------------
// scc-compound — B2: nodes are (group, index) tuples; compound keys and labels throughout.
// ---------------------------------------------------------------------------------------

const GROUP_BITS: u64 = 10;

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

fn bench_scc_compound() {
    let p_compound = program("scc_compound");
    let p_plain = program("scc_plain");
    let sizes = sizes(&[100_000, 150_000]);

    let gen = |nodes: u64| -> (Vec<(u64, u64)>, Vec<(Value, Value)>) {
        let n_edges = nodes * 2;
        let mut seed = 0xc0ff_ee42u64;
        let raw: Vec<(u64, u64)> = (0..n_edges)
            .map(|_| (xorshift(&mut seed) % nodes, xorshift(&mut seed) % nodes))
            .collect();
        let rows = raw.iter()
            .map(|&(s, d)| (Value::Tuple(vec![node_val(s), node_val(d)]), Value::unit()))
            .collect();
        (raw, rows)
    };

    if let Some((backend, iters)) = prof_mode() {
        let nodes = sizes[0];
        let (raw, rows) = gen(nodes);
        let inputs = vec![rows];
        for i in 0..iters {
            match backend.as_str() {
                "corgi" => { std::hint::black_box(corgi::evaluate(&p_compound, &inputs)); }
                "vec" => { std::hint::black_box(vec::evaluate(&p_compound, &inputs)); }
                "native" | "native-fair" => native_scc_compound_once(&raw),
                other => panic!("scc-compound: unknown BACKEND {other}"),
            }
            println!("done scc-compound backend={backend} n={nodes} iter={i}");
        }
        return;
    }

    println!("scc-compound (B2) — nodes are (group,index) tuples; fair native / DDIR-vec / DDIR-corgi (e = 2n):");
    for (i, &nodes) in sizes.iter().enumerate() {
        let n_edges = nodes * 2;
        let (raw_edges, compound_rows) = gen(nodes);
        let inputs = vec![compound_rows];

        let check = checked(i);
        if check {
            let vec_out = vec::evaluate(&p_compound, &inputs);
            assert_eq!(corgi::evaluate(&p_compound, &inputs), vec_out, "corgi != vec at n={nodes}");

            // Encoding cross-check: compound output mapped back == plain-program output.
            let plain_rows: Vec<(Value, Value)> = raw_edges.iter()
                .map(|&(s, d)| (Value::Tuple(vec![Value::Int(s as i64), Value::Int(d as i64)]), Value::unit()))
                .collect();
            let plain_out = vec::evaluate(&p_plain, &vec![plain_rows]);
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

// ---------------------------------------------------------------------------------------
// ident — B3: pointer-doubling representatives + two term-resolution joins.
// ---------------------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
fn native_ident_once(
    parents: &[(i64, i64)],
    terms: &[(i64, i64, i64)],
    capture: bool,
) -> Option<(Vec<((i64, i64), isize)>, Vec<((i64, (i64, i64)), isize)>)> {
    use timely::dataflow::operators::capture::{Capture, Event, Extract};

    let parents = parents.to_vec();
    let terms = terms.to_vec();
    let (reps_tx, reps_rx) = std::sync::mpsc::channel::<Event<u64, Vec<((i64, i64), u64, isize)>>>();
    let (res_tx, res_rx) = std::sync::mpsc::channel::<Event<u64, Vec<((i64, (i64, i64)), u64, isize)>>>();

    timely::execute_directly(move |worker| {
        let mut probe = Handle::new();
        let (mut p_in, mut t_in) = worker.dataflow::<u64, _, _>(|scope| {
            let (p_in, parent) = scope.new_collection::<(i64, i64), isize>();
            let (t_in, term_rows) = scope.new_collection::<(i64, (i64, i64)), isize>();

            let outer = parent.scope();
            let reps = outer.scoped::<Product<u64, usize>, _, _>("Find", |inner| {
                let parent = parent.enter(inner);
                let (doubled_var, doubled) = Variable::new(inner, Product::new(Default::default(), 1));
                let reps = doubled.concat(parent).reduce(|_k, s, t| t.push((*s[0].0, 1isize)));
                let reps_arr = reps.clone().arrange_by_key();
                let next = reps
                    .clone()
                    .map(|(x, r)| (r, x))
                    .arrange_by_key()
                    .join_core(reps_arr, |_r, &x, &r2| Some((x, r2)));
                doubled_var.set(next);
                reps.leave(outer)
            });

            let reps_arr = reps.clone().arrange_by_key();
            let by_a = term_rows.map(|(id, (a, b))| (a, (id, b))).arrange_by_key();
            let with_a = by_a.join_core(reps_arr.clone(), |_a, &(id, b), &ra| Some((b, (id, ra))));
            let resolved = with_a
                .arrange_by_key()
                .join_core(reps_arr, |_b, &(id, ra), &rb| Some((id, (ra, rb))));

            resolved.clone().probe_with(&mut probe);
            if capture {
                reps.inner.capture_into(reps_tx);
                resolved.inner.capture_into(res_tx);
            }
            (p_in, t_in)
        });

        for &(x, p) in &parents { p_in.insert((x, p)); }
        for &(id, a, b) in &terms { t_in.insert((id, (a, b))); }
        p_in.advance_to(1); p_in.flush();
        t_in.advance_to(1); t_in.flush();
        while probe.less_than(p_in.time()) { worker.step(); }
    });

    if capture {
        let mut reps: std::collections::BTreeMap<(i64, i64), isize> = Default::default();
        for (_, batch) in reps_rx.extract() {
            for (d, _, r) in batch { *reps.entry(d).or_insert(0) += r; }
        }
        let mut res: std::collections::BTreeMap<(i64, (i64, i64)), isize> = Default::default();
        for (_, batch) in res_rx.extract() {
            for (d, _, r) in batch { *res.entry(d).or_insert(0) += r; }
        }
        Some((
            reps.into_iter().filter(|(_, r)| *r != 0).collect(),
            res.into_iter().filter(|(_, r)| *r != 0).collect(),
        ))
    } else {
        None
    }
}

fn bench_ident() {
    let p = program("ident");
    let sizes = sizes(&[100_000, 1_000_000]);
    let chain: u64 = std::env::var("CHAIN").ok().and_then(|s| s.parse().ok()).unwrap_or(64);

    let gen = |nodes: u64| {
        let n_terms = nodes * 2;
        let parents: Vec<(i64, i64)> = (0..nodes)
            .map(|x| (x as i64, if x % chain == 0 { x as i64 } else { (x - 1) as i64 }))
            .collect();
        let mut seed = 0xdead_beef_u64;
        let terms: Vec<(i64, i64, i64)> = (0..n_terms)
            .map(|j| ((nodes + j) as i64, (xorshift(&mut seed) % nodes) as i64, (xorshift(&mut seed) % nodes) as i64))
            .collect();
        let parent_rows: Vec<(Value, Value)> = parents.iter().map(|&(x, p)| (tup(&[x, p]), Value::unit())).collect();
        let term_rows: Vec<(Value, Value)> = terms.iter().map(|&(id, a, b)| (tup(&[id, a, b]), Value::unit())).collect();
        (parents, terms, vec![parent_rows, term_rows])
    };

    if let Some((backend, iters)) = prof_mode() {
        let nodes = sizes[0];
        let (parents, terms, inputs) = gen(nodes);
        for i in 0..iters {
            match backend.as_str() {
                "corgi" => { std::hint::black_box(corgi::evaluate(&p, &inputs)); }
                "vec" => { std::hint::black_box(vec::evaluate(&p, &inputs)); }
                "native" => { native_ident_once(&parents, &terms, false); }
                other => panic!("ident: unknown BACKEND {other}"),
            }
            println!("done ident backend={backend} n={nodes} iter={i}");
        }
        return;
    }

    println!("ident-join (B3) — pointer-doubling reps + 2 term-resolution joins (chains of {chain}, terms = 2n):");
    for (i, &nodes) in sizes.iter().enumerate() {
        let n_terms = nodes * 2;
        let root = |x: u64| (x - x % chain) as i64;
        let (parents, terms, inputs) = gen(nodes);

        let check = checked(i);
        if check {
            let expect_reps: Vec<((Value, Value), i64)> = (0..nodes)
                .map(|x| ((tup(&[x as i64]), tup(&[root(x)])), 1))
                .collect();
            let mut expect_res: Vec<((Value, Value), i64)> = Default::default();
            {
                let mut map: std::collections::BTreeMap<(Value, Value), i64> = Default::default();
                for &(id, a, b) in &terms {
                    *map.entry((tup(&[id]), tup(&[root(a as u64), root(b as u64)]))).or_insert(0) += 1;
                }
                expect_res.extend(map.into_iter());
            }

            let vec_out = vec::evaluate(&p, &inputs);
            assert_eq!(consolidated(&vec_out["reps"]), expect_reps, "vec reps != oracle at n={nodes}");
            assert_eq!(consolidated(&vec_out["resolved"]), expect_res, "vec resolved != oracle at n={nodes}");
            assert_eq!(corgi::evaluate(&p, &inputs), vec_out, "corgi != vec at n={nodes}");

            let (nat_reps, nat_res) = native_ident_once(&parents, &terms, true).unwrap();
            let expect_nat_reps: Vec<((i64, i64), isize)> =
                (0..nodes).map(|x| ((x as i64, root(x)), 1)).collect();
            assert_eq!(nat_reps, expect_nat_reps, "native reps != oracle at n={nodes}");
            let mut expect_nat_res: std::collections::BTreeMap<(i64, (i64, i64)), isize> = Default::default();
            for &(id, a, b) in &terms {
                *expect_nat_res.entry((id, (root(a as u64), root(b as u64)))).or_insert(0) += 1;
            }
            let expect_nat_res: Vec<_> = expect_nat_res.into_iter().collect();
            assert_eq!(nat_res, expect_nat_res, "native resolved != oracle at n={nodes}");
        }

        let nt = once(|| { native_ident_once(&parents, &terms, false); });
        let vt = once(|| { std::hint::black_box(vec::evaluate(&p, &inputs)); });
        let ct = once(|| { std::hint::black_box(corgi::evaluate(&p, &inputs)); });
        let (nf, vf, cf) = (nt.as_secs_f64(), vt.as_secs_f64(), ct.as_secs_f64());
        println!(
            "  n={nodes:<8} terms={n_terms:<9}  native {nt:>8.2?}   vec-DDIR {vt:>8.2?} ({:.2}x nat)   corgi-DDIR {ct:>8.2?} ({:.2}x nat, {:.2}x vec){}",
            vf / nf, cf / nf, cf / vf, if check { "  [checked]" } else { "" },
        );
    }
}

// ---------------------------------------------------------------------------------------
// ast — B4: list build + flatmap + variant/case/fold + min (8 elements per row).
// ---------------------------------------------------------------------------------------

const H: i64 = 500_000;

/// The per-row logic, shared by the oracle and the native twin.
fn explode(a: i64, b: i64) -> impl Iterator<Item = (i64, i64)> {
    let elems = [a, b, a + b, a * b, a - b + 32768, b * b, a * a, a + b * b];
    elems.into_iter().enumerate().map(move |(pos, e)| {
        let bucket = pos as i64 + 8 * if e < H { 0 } else { 1 };
        let payload = a + e + a * e + e * e;
        (bucket, payload)
    })
}

fn native_ast_once(rows: &[(i64, i64)], capture: bool) -> Option<Vec<((i64, i64), isize)>> {
    use timely::dataflow::operators::capture::{Capture, Event, Extract};
    let rows = rows.to_vec();
    let (tx, rx) = std::sync::mpsc::channel::<Event<u64, Vec<((i64, i64), u64, isize)>>>();

    timely::execute_directly(move |worker| {
        let mut probe = Handle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, data) = scope.new_collection::<(i64, i64), isize>();
            let buckets = data
                .flat_map(|(a, b)| explode(a, b))
                .reduce(|_k, s, t| t.push((*s[0].0, 1isize)));
            buckets.clone().probe_with(&mut probe);
            if capture { buckets.inner.capture_into(tx); }
            input
        });
        for &(a, b) in &rows { input.insert((a, b)); }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }
    });

    if capture {
        let mut out: std::collections::BTreeMap<(i64, i64), isize> = Default::default();
        for (_, batch) in rx.extract() {
            for (d, _, r) in batch { *out.entry(d).or_insert(0) += r; }
        }
        Some(out.into_iter().filter(|(_, r)| *r != 0).collect())
    } else {
        None
    }
}

fn bench_ast() {
    let p = program("ast");
    let sizes = sizes(&[1_000_000, 4_000_000]);

    let gen = |n_rows: u64| {
        let mut seed = 0xfeed_f00d_u64;
        let rows: Vec<(i64, i64)> = (0..n_rows)
            .map(|_| ((xorshift(&mut seed) % 32_768) as i64, (xorshift(&mut seed) % 32_768) as i64))
            .collect();
        let ddir: Vec<(Value, Value)> = rows.iter().map(|&(a, b)| (tup(&[a, b]), Value::unit())).collect();
        (rows, vec![ddir])
    };

    if let Some((backend, iters)) = prof_mode() {
        let n_rows = sizes[0];
        let (rows, inputs) = gen(n_rows);
        for i in 0..iters {
            match backend.as_str() {
                "corgi" => { std::hint::black_box(corgi::evaluate(&p, &inputs)); }
                "vec" => { std::hint::black_box(vec::evaluate(&p, &inputs)); }
                "native" => { native_ast_once(&rows, false); }
                other => panic!("ast: unknown BACKEND {other}"),
            }
            println!("done ast backend={backend} n={n_rows} iter={i}");
        }
        return;
    }

    println!("ast-compute (B4) — list build + flatmap + variant/case/fold + min (8 elems/row):");
    for (i, &n_rows) in sizes.iter().enumerate() {
        let (rows, inputs) = gen(n_rows);

        let check = checked(i);
        if check {
            let mut mins: std::collections::BTreeMap<i64, i64> = Default::default();
            for &(a, b) in &rows {
                for (bucket, payload) in explode(a, b) {
                    mins.entry(bucket).and_modify(|m| *m = (*m).min(payload)).or_insert(payload);
                }
            }
            let expect: Vec<((Value, Value), i64)> =
                mins.iter().map(|(&k, &v)| ((tup(&[k]), tup(&[v])), 1)).collect();
            let expect_nat: Vec<((i64, i64), isize)> =
                mins.iter().map(|(&k, &v)| ((k, v), 1)).collect();

            let vec_out = vec::evaluate(&p, &inputs);
            assert_eq!(consolidated(&vec_out["result"]), expect, "vec != oracle at n={n_rows}");
            assert_eq!(corgi::evaluate(&p, &inputs), vec_out, "corgi != vec at n={n_rows}");
            assert_eq!(native_ast_once(&rows, true).unwrap(), expect_nat, "native != oracle at n={n_rows}");
        }

        let nt = once(|| { native_ast_once(&rows, false); });
        let vt = once(|| { std::hint::black_box(vec::evaluate(&p, &inputs)); });
        let ct = once(|| { std::hint::black_box(corgi::evaluate(&p, &inputs)); });
        let (nf, vf, cf) = (nt.as_secs_f64(), vt.as_secs_f64(), ct.as_secs_f64());
        println!(
            "  n={n_rows:<9}  native {nt:>8.2?}   vec-DDIR {vt:>8.2?} ({:.2}x nat)   corgi-DDIR {ct:>8.2?} ({:.2}x nat, {:.2}x vec){}",
            vf / nf, cf / nf, cf / vf, if check { "  [checked]" } else { "" },
        );
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("scc") => bench_scc(),
        Some("scc-compound") => bench_scc_compound(),
        Some("ident") => bench_ident(),
        Some("ast") => bench_ast(),
        other => {
            eprintln!("usage: ddir_bench <scc|scc-compound|ident|ast>  (got {other:?})");
            std::process::exit(1);
        }
    }
}
