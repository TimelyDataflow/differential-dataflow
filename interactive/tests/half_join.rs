//! `half_join`: a delta join, two half joins whose results are delayed to their data times, is the
//! join, at the root and inside an iterative scope, as inputs change over several epochs.

use interactive::server::host_time;
use timely::progress::Antichain;
use interactive::server::Server;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let a = input 0 : ((int, int), ()) | map(((k, x), ()) -> (k, x));
    let b = input 1 : ((int, int), ()) | map(((k, y), ()) -> (k, y));

    -- Each of `a`'s updates meets `b`'s at or before it, and each of `b`'s meets `a`'s strictly
    -- before it, so every pair meets once, at the later of its two times.
    let ab = a | lift | half_join(b, 0);
    let ba = b | lift | half_join(a, 1, (k, ((y, x), t)) -> (k, ((x, y), t)));
    output "delta" = (ab + ba) | delay((_, (_, t)) -> t) | map((k, (xy, _)) -> (k, xy));
    output "join" = a | join(b);

    -- Reachability from `a`'s keys along `b`'s edges, joining by each method.
    let roots = a | map((k, _) -> (k, ()));
    joined: {
        let step = reach | join(b, (_, ((), dst)) -> (dst, ()));
        set reach = roots + step | distinct;
    }
    halved: {
        let fwd = reach | lift | half_join(b, 0, (_, (((), dst), t)) -> (dst, ((), t)));
        let rev = b | lift | half_join(reach, 1, (_, ((dst, ()), t)) -> (dst, ((), t)));
        let step = (fwd + rev) | delay((_, (_, t)) -> t) | map((n, _) -> (n, ()));
        set reach = roots + step | distinct;
    }
    output "reach_join" = joined::reach;
    output "reach_delta" = halved::reach;
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
        interactive::passes::optimize(&mut program);
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for tick in 0..TICKS {
            if worker.index() == 0 {
                for (input, k, v, d) in feeds(tick) {
                    server.feed("p", &input.into(), &format!("{k},{v} diff={d}"), tick).unwrap();
                }
            }
            for input in [0, 1] {
                server.seal("p", &input.into(), &Antichain::from_elem(host_time(tick + 1))).unwrap();
            }
            server.spin(worker);
            seen.push(["delta", "join", "reach_delta", "reach_join"].map(|name| server.snapshot(worker, &format!("p::{name}")).unwrap().rows()));
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

#[test]
fn projection_sees_joined_data_time_at_the_stream_timestamp() {
    use interactive::server::HostTime;

    timely::execute_directly(|worker| {
        let src = r#"
            let a = input 0 : (int, int);
            let b = input 1 : (int, int);
            let stream = a | lift;
            let match_row = stream | half_join(b, 1);
            output "bare" = match_row;
            output "identity" = stream | half_join(b, 1, row -> row);
            output "mapped" = match_row | map((k, ((l, r), (_, y))) -> (y, (k, (r, l))));
            output "projected" = stream | half_join(b, 1, (k, ((l, r), (_, y))) -> (y, (k, (r, l))));
            output "changed" = stream | half_join(b, 1, (k, ((l, _), (x, y))) -> (k, (l, (y, x))));
            output "dropped" = stream | half_join(b, 1, (_, ((l, r), _)) -> (r, l));
        "#;
        let point = |x, y| HostTime::new(smallvec::smallvec![x, y]);
        let mut server = Server::new();
        server.set_passes(interactive::passes::Config::all());
        let program = lower::lower_tree(parse::pipe::parse(src));
        server.install_with_width(worker, "p", &program, 2).unwrap();
        server.feed_at("p", &0.into(), "7 val=10 diff=2", &point(1, 0)).unwrap();
        server.feed_at("p", &1.into(), "7 val=20 diff=-3", &point(0, 2)).unwrap();
        for input in [0, 1] { server.seal("p", &input.into(), &Antichain::new()).unwrap(); }

        // The arrangement is earlier in the total order. Its second coordinate
        // contributes to the data time (1, 2), even in a snapshot at (1, 0).
        for (name, key, val) in [
            ("bare", "7", "tuple(tuple(10,20),tuple(1,2))"),
            ("identity", "7", "tuple(tuple(10,20),tuple(1,2))"),
            ("mapped", "2", "tuple(7,tuple(20,10))"),
            ("projected", "2", "tuple(7,tuple(20,10))"),
            ("changed", "7", "tuple(10,tuple(2,1))"),
            ("dropped", "20", "10"),
        ] {
            let rows = server.snapshot_as_of(worker, &format!("p::{name}"), &point(1, 0)).unwrap().rows();
            assert_eq!(rows, vec![(key.into(), val.into(), -6)], "{name}");
        }
    });
}
