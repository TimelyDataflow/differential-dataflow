//! B3 "ident-join": identifier resolution via integer joins — the e-graph-flavored workload.
//!
//! Structure: a parent forest (chains of length CHAIN; roots hold self-loops) resolved to
//! roots by pointer-doubling (iterative min-join fixpoint), then a term table (id, a, b)
//! resolved by two joins mapping child identifiers to their representatives. Everything is
//! integer-keyed joins + a min reduce — "identifier lookup" as the common action, with the
//! joins in DD operators (corgi does per-row logic only).
//!
//!   N=100000,1000000 CHAIN=64 ~/.cargo/bin/cargo run --release --example corgi_ident_big
//!
//! Correctness: closed-form oracle (root(x) = x - x%CHAIN) checked against the vec backend,
//! corgi asserted == vec, and the hand-written native twin captured + checked against the
//! same oracle. CHECK=1 forces checks at every size (default: smallest size only).

use std::time::{Duration, Instant};

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::Variable;
use timely::dataflow::operators::probe::Handle;
use timely::order::Product;

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

/// The hand-written compiled twin, mirroring the DDIR plan: pointer-doubling fixpoint
/// (Variable `doubled`; reps = min(doubled + parent); doubled' = reps∘reps), then the two
/// term-resolution joins against the arranged reps. `capture` returns (reps, resolved).
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

fn once<F: FnMut()>(mut f: F) -> Duration { let t = Instant::now(); f(); t.elapsed() }

/// Consolidate a backend export into a sorted (data, diff) list for oracle comparison.
fn consolidated(export: &[((Value, Value), i64)]) -> Vec<((Value, Value), i64)> {
    let mut map: std::collections::BTreeMap<(Value, Value), i64> = Default::default();
    for ((k, v), d) in export { *map.entry((k.clone(), v.clone())).or_insert(0) += d; }
    map.into_iter().filter(|(_, d)| *d != 0).collect()
}

fn main() {
    let mut p = lower::lower_tree(parse::pipe::parse(IDENT_SRC));
    p.optimize();
    let sizes: Vec<u64> = std::env::var("N").ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![100_000, 1_000_000]);
    let chain: u64 = std::env::var("CHAIN").ok().and_then(|s| s.parse().ok()).unwrap_or(64);

    println!("ident-join (B3) — pointer-doubling reps + 2 term-resolution joins (chains of {chain}, terms = 2n):");
    for (i, &nodes) in sizes.iter().enumerate() {
        let n_terms = nodes * 2;
        let root = |x: u64| (x - x % chain) as i64;

        // Native inputs.
        let parents: Vec<(i64, i64)> = (0..nodes)
            .map(|x| (x as i64, if x % chain == 0 { x as i64 } else { (x - 1) as i64 }))
            .collect();
        let mut seed = 0xdead_beef_u64;
        let terms: Vec<(i64, i64, i64)> = (0..n_terms)
            .map(|j| ((nodes + j) as i64, (xorshift(&mut seed) % nodes) as i64, (xorshift(&mut seed) % nodes) as i64))
            .collect();

        // DDIR inputs: input 0 = (x, parent ;), input 1 = (id, a, b ;).
        let parent_rows: Vec<(Value, Value)> = parents.iter().map(|&(x, p)| (tup(&[x, p]), Value::unit())).collect();
        let term_rows: Vec<(Value, Value)> = terms.iter().map(|&(id, a, b)| (tup(&[id, a, b]), Value::unit())).collect();
        let inputs = vec![parent_rows, term_rows];

        let check = std::env::var("CHECK").ok().map(|s| s != "0").unwrap_or(i == 0);
        if check {
            // Closed-form oracle.
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
