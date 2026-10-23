//! `half_join`: a delta join, two half joins whose results are delayed to their data times, is the
//! join, at the root and inside an iterative scope, as inputs change over several epochs.

use interactive::server::Server;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let a = input 0 : ((int, int), ()) | map(((k, x), ()) -> (k, x));
    let b = input 1 : ((int, int), ()) | map(((k, y), ()) -> (k, y));

    -- Each of `a`'s updates meets `b`'s at or before it, and each of `b`'s meets `a`'s strictly
    -- before it, so every pair meets once, at the later of its two times.
    let ab = a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y)));
    let ba = b | lift | half_join(a, 1, (k, y, x) -> (k, (x, y)));
    export "delta" = (ab + ba) | delay((_, (_, t)) -> t) | map((k, (xy, _)) -> (k, xy));
    export "join" = a | join(b, (k, x, y) -> (k, (x, y)));

    -- Reachability from `a`'s keys along `b`'s edges, joining by each method.
    let roots = a | map((k, _) -> (k, ()));
    joined: {
        let step = reach | join(b, (_, (), dst) -> (dst, ()));
        var reach = roots + step | distinct;
    }
    halved: {
        let fwd = reach | lift | half_join(b, 0, (_, (), dst) -> (dst, ()));
        let rev = b | lift | half_join(reach, 1, (_, dst, ()) -> (dst, ()));
        let step = (fwd + rev) | delay((_, (_, t)) -> t) | map((n, _) -> (n, ()));
        var reach = roots + step | distinct;
    }
    export "reach_join" = joined::reach;
    export "reach_delta" = halved::reach;
"#;

const TICKS: u64 = 24;

/// The rows fed at each tick, as `(input, key, value, diff)`: a few pseudo-random rows of small
/// keys and values each tick, each added and, a few ticks later, retracted. Enough ticks that the
/// arrangements' batches merge, and so compact.
fn feeds(tick: u64) -> Vec<(usize, i64, i64, i64)> {
    let row = |n: u64| {
        let h = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        ((h % 2) as usize, ((h >> 8) % 6) as i64, ((h >> 16) % 6) as i64)
    };
    let mut rows = Vec::new();
    for i in 0..4 {
        let (input, k, v) = row(4 * tick + i);
        rows.push((input, k, v, 1));
        if tick >= 3 {
            let (input, k, v) = row(4 * (tick - 3) + i);
            rows.push((input, k, v, -1));
        }
    }
    rows
}

/// Every export after each tick, by name.
fn run(workers: usize) -> Vec<[Vec<(String, String, i64)>; 4]> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        program.optimize();
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for tick in 0..TICKS {
            if worker.index() == 0 {
                for (input, k, v, d) in feeds(tick) {
                    server.feed("p", input, &format!("{k},{v} diff={d}"), None).unwrap();
                }
            }
            server.tick(worker);
            seen.push(["delta", "join", "reach_delta", "reach_join"].map(|name| server.snapshot(worker, name).unwrap().rows()));
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn delta_joins_are_joins() {
    for workers in [1, 3] {
        for (tick, [delta, join, reach_delta, reach_join]) in run(workers).into_iter().enumerate() {
            assert!(!join.is_empty() && !reach_join.is_empty(), "tick {tick}: the joins have rows");
            assert_eq!(delta, join, "tick {tick}, {workers} workers: the delta join");
            assert_eq!(reach_delta, reach_join, "tick {tick}, {workers} workers: reachability");
        }
    }
}
