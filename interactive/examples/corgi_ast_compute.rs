//! B4 "ast-compute": the compute-heavy AST-style bookend — list build with arithmetic,
//! flatmap explosion, variant tag + case + fold per element, then a tiny min reduce.
//! This is corgi's home turf (wide per-row compute, no joins/recursion); it is labeled
//! as such in the charter — a bookend, not the headline.
//!
//!   N=1000000,4000000 ~/.cargo/bin/cargo run --release --example corgi_ast_compute
//!
//! Per row (a, b < 2^15 — keeps every intermediate non-negative and inside i64; corgi's
//! structural order is unsigned at the integer leaf, so signed values are out of contract
//! for min — see the read-out finding): build list [a, b, a+b, a*b, a-b+2^15, b*b, a*a,
//! a+b*b], explode to
//! (pos, elem), bucket = pos + 8*(elem < H ? 0 : 1), payload = fold over
//! [a, elem, a*elem, elem*elem] wrapped in a Fwd/Bwd variant and matched back out,
//! then min(payload) per bucket (min so the compute cannot be dead-code eliminated,
//! while the reduce itself stays tiny — 16 keys).
//!
//! Correctness: closed-form Rust oracle checked against vec, corgi == vec, and the
//! hand-written native twin checked against the same oracle. CHECK=1 forces.

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

use differential_dataflow::input::Input;
use timely::dataflow::operators::probe::Handle;

const H: i64 = 500_000;

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

fn tup(fields: &[i64]) -> Value { Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect()) }

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

fn once<F: FnMut()>(mut f: F) -> Duration { let t = Instant::now(); f(); t.elapsed() }

fn consolidated(export: &[((Value, Value), i64)]) -> Vec<((Value, Value), i64)> {
    let mut map: std::collections::BTreeMap<(Value, Value), i64> = Default::default();
    for ((k, v), d) in export { *map.entry((k.clone(), v.clone())).or_insert(0) += d; }
    map.into_iter().filter(|(_, d)| *d != 0).collect()
}

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(AST_SRC));
    p.optimize();
    let sizes: Vec<u64> = std::env::var("N").ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1_000_000, 4_000_000]);

    println!("ast-compute (B4) — list build + flatmap + variant/case/fold + min (8 elems/row):");
    for (i, &n_rows) in sizes.iter().enumerate() {
        let mut seed = 0xfeed_f00d_u64;
        let rows: Vec<(i64, i64)> = (0..n_rows)
            .map(|_| ((xorshift(&mut seed) % 32_768) as i64, (xorshift(&mut seed) % 32_768) as i64))
            .collect();
        let ddir_rows: Vec<(Value, Value)> = rows.iter().map(|&(a, b)| (tup(&[a, b]), Value::unit())).collect();
        let inputs = vec![ddir_rows];

        let check = std::env::var("CHECK").ok().map(|s| s != "0").unwrap_or(i == 0);
        if check {
            // Closed-form oracle: min payload per bucket.
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
