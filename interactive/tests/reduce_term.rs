//! `reduce(lambda)`: the prefab reducers written as lambdas reduce as the prefabs do, across
//! workers, as inputs are added and retracted.

use interactive::server::Server;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let pairs = input 0 : ((int, int) ; ()) | map(((a, b), ()) -> (a, b));
    export "count" = pairs | count;
    export "count_lambda" = pairs | reduce((_, vals) -> ((0, vals) fold ((acc, (_, d)) -> (acc, d) add), 1) enlist);
    export "distinct" = pairs | distinct;
    export "distinct_lambda" = pairs | reduce(_ -> ((), 1) enlist);
    export "min" = pairs | min;
    export "min_lambda" = pairs | reduce((_, vals) -> ((1000000, vals) fold ((acc, (v, _)) -> ((v, acc) lt, v, acc) select), 1) enlist);
    export "sum" = pairs | reduce((_, vals) -> ((vals map (p -> (p.0, p.1) mul) fold_add), 1) enlist);
"#;

/// The rows fed at each tick, as `(key, val, diff)`: additions, repeats, retractions, a key
/// retracted entirely, and a key brought back.
fn feeds(tick: i64) -> Vec<(i64, i64, i64)> {
    match tick {
        0 => vec![(1, 10, 1), (1, 20, 1), (2, 5, 1), (2, 5, 1), (3, -7, 1), (4, 1, 1)],
        1 => vec![(1, 20, -1), (1, 3, 1), (2, 6, 1), (3, -8, 1), (5, 50, 1)],
        2 => vec![(2, 5, -2), (2, 6, -1), (3, -7, -1), (4, 2, 1)],
        _ => vec![(2, 9, 1), (1, 10, -1), (1, -4, 1)],
    }
}

/// Every export, read after each tick.
fn run(workers: usize) -> Vec<Vec<(String, String, i64)>> {
    let names = ["count", "count_lambda", "distinct", "distinct_lambda", "min", "min_lambda", "sum"];
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        program.optimize();
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for tick in 0..4 {
            if worker.index() == 0 {
                for (k, v, d) in feeds(tick) {
                    server.feed("p", 0, &format!("{k},{v} diff={d}"), None).unwrap();
                }
            }
            server.tick(worker);
            for name in names {
                seen.push(server.snapshot(worker, name).unwrap().rows());
            }
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn lambdas_reduce_as_the_prefabs_do() {
    let want = run(1);
    for tick in want.chunks(7) {
        assert!(tick.iter().all(|rows| !rows.is_empty()), "every export has rows: {tick:?}");
        assert_eq!(tick[0], tick[1], "count");
        assert_eq!(tick[2], tick[3], "distinct");
        assert_eq!(tick[4], tick[5], "min");
    }

    assert_eq!(run(3), want, "3 workers");
}
