//! `count`, and the worst-case optimal join written with it: a triangle query as three delta
//! paths, each counting both constraining atoms and routing each prefix to the one with fewer
//! updates, is the binary join, as inputs change over several epochs; and routing the other way,
//! or not at all, gives the same rows, since the counts only steer.

use std::collections::BTreeMap;

use interactive::server::{evaluate, Server};
use interactive::{lower, parse};

/// The triangle `R(a, b), S(b, c), T(a, c)` over relations `r`, `s`, `t` of shape `(int, int)`,
/// keyed by their first field, as a worst-case optimal join (`wco`) and a binary join (`join`).
/// `first` and `second` are lambda bodies over `(n1, n2)`, the counts of a path's two atoms in
/// order, choosing which prefixes propose from each: `(n1, n2) le` and `(n1, n2) gt` route each
/// prefix to the atom with fewer updates. Relation order `R < S < T` fixes the strictness.
fn triangle(r: &str, s: &str, t: &str, first: &str, second: &str) -> String {
    format!(r#"
    let r_by_b = {r} | map((a, b) -> (b, a));
    let s_by_c = {s} | map((b, c) -> (c, b));
    let t_by_c = {t} | map((a, c) -> (c, a));
    let r_full = {r} | map((a, b) -> ((a, b), ()));
    let s_full = {s} | map((b, c) -> ((b, c), ()));
    let t_full = {t} | map((a, c) -> ((a, c), ()));

    -- R's changes: prefixes (a, b), extended by c through S(b, c) or T(a, c), both later: strict.
    let dr = {r} | map((a, b) -> (b, a))
        | count({s}, (b, a, n1) -> (a, (b, n1)))
        | count({t}, (a, (b, n1), n2) -> (a, (b, n1, n2)));
    let dr_s = dr | filter((_, (_, n1, n2)) -> {first}) | map((a, (b, _, _)) -> (b, a)) | lift
        | half_join({s}, 1, (b, a, c) -> ((a, c), b))
        | half_join(t_full, 1, ((a, c), b, ()) -> ((a, b, c), ()));
    let dr_t = dr | filter((_, (_, n1, n2)) -> {second}) | map((a, (b, _, _)) -> (a, b)) | lift
        | half_join({t}, 1, (a, b, c) -> ((b, c), a))
        | half_join(s_full, 1, ((b, c), a, ()) -> ((a, b, c), ()));

    -- S's changes: prefixes (b, c), extended by a through R(a, b) (earlier) or T(a, c) (later).
    let ds = {s}
        | count(r_by_b, (b, c, n1) -> (c, (b, n1)))
        | count(t_by_c, (c, (b, n1), n2) -> (c, (b, n1, n2)));
    let ds_r = ds | filter((_, (_, n1, n2)) -> {first}) | map((c, (b, _, _)) -> (b, c)) | lift
        | half_join(r_by_b, 0, (b, c, a) -> ((a, c), b))
        | half_join(t_full, 1, ((a, c), b, ()) -> ((a, b, c), ()));
    let ds_t = ds | filter((_, (_, n1, n2)) -> {second}) | map((c, (b, _, _)) -> (c, b)) | lift
        | half_join(t_by_c, 1, (c, b, a) -> ((a, b), c))
        | half_join(r_full, 0, ((a, b), c, ()) -> ((a, b, c), ()));

    -- T's changes: prefixes (a, c), extended by b through R(a, b) or S(b, c), both earlier.
    let dt = {t}
        | count({r}, (a, c, n1) -> (c, (a, n1)))
        | count(s_by_c, (c, (a, n1), n2) -> (c, (a, n1, n2)));
    let dt_r = dt | filter((_, (_, n1, n2)) -> {first}) | map((c, (a, _, _)) -> (a, c)) | lift
        | half_join({r}, 0, (a, c, b) -> ((b, c), a))
        | half_join(s_full, 0, ((b, c), a, ()) -> ((a, b, c), ()));
    let dt_s = dt | filter((_, (_, n1, n2)) -> {second}) | map((c, (a, _, _)) -> (c, a)) | lift
        | half_join(s_by_c, 0, (c, a, b) -> ((a, b), c))
        | half_join(r_full, 0, ((a, b), c, ()) -> ((a, b, c), ()));

    export "wco" = (dr_s + dr_t + ds_r + ds_t + dt_r + dt_s) | delay((_, (_, t)) -> t) | map((abc, _) -> (abc, ()));
    export "join" = r_by_b | join({s}, (b, a, c) -> ((a, c), b)) | join(t_full, ((a, c), b, ()) -> ((a, b, c), ()));
    "#)
}

/// Three edge inputs as relations keyed by their first field, and the triangle over them.
fn program(first: &str, second: &str) -> String {
    format!(r#"
    let r = input 0 : ((int, int), ()) | map(((a, b), ()) -> (a, b));
    let s = input 1 : ((int, int), ()) | map(((b, c), ()) -> (b, c));
    let t = input 2 : ((int, int), ()) | map(((a, c), ()) -> (a, c));
    {}"#, triangle("r", "s", "t", first, second))
}

const TICKS: u64 = 20;

/// The rows fed at each tick, as `(input, a, b, diff)`: pseudo-random edges over few nodes, so
/// that triangles form, each added and, a few ticks later, retracted. Enough ticks that the
/// arrangements' batches merge.
fn feeds(tick: u64) -> Vec<(usize, i64, i64, i64)> {
    let row = |n: u64| {
        let h = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        ((h % 3) as usize, ((h >> 8) % 5) as i64, ((h >> 16) % 5) as i64)
    };
    let mut rows = Vec::new();
    for i in 0..6 {
        let (input, a, b) = row(6 * tick + i);
        rows.push((input, a, b, 1));
        if tick >= 4 {
            let (old_input, old_a, old_b) = row(6 * (tick - 4) + i);
            rows.push((old_input, old_a, old_b, -1));
        }
    }
    rows
}

/// Both exports after each tick.
fn run(src: &str, workers: usize) -> Vec<[Vec<(String, String, i64)>; 2]> {
    let src = src.to_string();
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let mut program = lower::lower_tree(parse::pipe::parse(&src));
        program.optimize();
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for tick in 0..TICKS {
            if worker.index() == 0 {
                for (input, a, b, d) in feeds(tick) {
                    server.feed("p", input, &format!("{a},{b} diff={d}"), None).unwrap();
                }
            }
            server.tick(worker);
            seen.push(["wco", "join"].map(|name| server.snapshot(worker, name).unwrap().rows()));
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn the_worst_case_optimal_triangle_is_the_join() {
    for workers in [1, 3] {
        let mut any = false;
        for (tick, [wco, join]) in run(&program("(n1, n2) le", "(n1, n2) gt"), workers).into_iter().enumerate() {
            any |= !join.is_empty();
            assert_eq!(wco, join, "tick {tick}, {workers} workers");
        }
        assert!(any, "the join has rows at some tick");
    }
}

/// Routing to the atom with more updates, or every prefix to the first atom, gives the same rows:
/// each route runs the same half joins, and the counts only decide which runs first.
#[test]
fn routing_does_not_change_the_rows() {
    let least = run(&program("(n1, n2) le", "(n1, n2) gt"), 1);
    for (first, second) in [("(n1, n2) gt", "(n1, n2) le"), ("1", "0"), ("0", "1")] {
        let other = run(&program(first, second), 1);
        for (tick, ([a, _], [b, join])) in least.iter().zip(&other).enumerate() {
            assert_eq!(a, b, "tick {tick}, routed by {first}");
            assert_eq!(b, join, "tick {tick}, routed by {first}, against the join");
        }
    }
}

/// `n` for a key fed once is the `count` reducer's: each `(key, val)` once is one update. Rows at
/// keys the arrangement holds nothing for are dropped, and a row keeps its own diff.
#[test]
fn n_is_the_number_of_updates_and_absent_keys_drop() {
    let mut program = lower::lower_tree(parse::pipe::parse(r#"
        let pairs = input 0 : ((int, int), ()) | map(((k, v), ()) -> (k, v));
        let probes = input 1 : ((int, int), ()) | map(((k, p), ()) -> (k, p));
        export "counted" = probes | count(pairs, (k, p, n) -> (k, (p, n)));
        export "reduced" = pairs | count;
    "#));
    program.optimize();
    // Keys 1 and 2 have two and one values; key 3 has none. Probes at 1, 2 (twice) and 3.
    let pairs: Vec<String> = ["1,10", "1,20", "2,30"].map(String::from).to_vec();
    let probes: Vec<String> = ["1,7 diff=2", "2,8", "2,9 diff=-1", "3,5"].map(String::from).to_vec();
    for workers in [1, 2] {
        let got: BTreeMap<_, Vec<_>> = evaluate(timely::Config::process(workers), &program, &[pairs.clone(), probes.clone()])
            .into_iter()
            .map(|(name, updates)| { let mut rows = updates.rows(); rows.sort(); (name, rows) })
            .collect();
        let row = |k: i64, p: i64, n: i64, d: i64| (k.to_string(), format!("tuple({p},{n})"), d);
        assert_eq!(got["counted"], vec![row(1, 7, 2, 2), row(2, 8, 1, 1), row(2, 9, 1, -1)], "{workers} workers");
        assert_eq!(got["reduced"], vec![("1".to_string(), "2".to_string(), 1), ("2".to_string(), "1".to_string(), 1)]);
    }
}
