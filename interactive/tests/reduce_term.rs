//! `reduce(term)`: the four prefab reducers written as terms reduce as the prefabs do, and a term
//! and the same term as a corgi lambda agree, on both backends, across workers, as inputs are added
//! and retracted.

use interactive::ir::Value;
use interactive::server::{RenderBackend, Server};
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
    export "count" = pairs | count;
    export "count_term" = pairs | reduce(list(tuple(fold($1, 0, ^1 + ^0[1]), 1)));
    export "distinct" = pairs | distinct;
    export "distinct_term" = pairs | reduce(list(tuple(tuple(), 1)));
    export "min" = pairs | min;
    export "min_term" = pairs | reduce(list(tuple(tuple(fold($1, 1000000, if(^0[0][0] < ^1, ^0[0][0], ^1))), 1)));
    export "sum_term" = pairs | reduce(list(tuple(tuple(fold($1, 0, ^1 + ^0[0][0] * ^0[1])), 1)));
    export "sum_lambda" = pairs | reduce((_, vals) -> ((vals map (p -> (p.0.0, p.1) mul) fold_add), 1) enlist);
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
fn run(backend: RenderBackend, workers: usize) -> Vec<Vec<(Value, Value, i64)>> {
    let names = ["count", "count_term", "distinct", "distinct_term", "min", "min_term", "sum_term", "sum_lambda"];
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::with_backend(backend);
        let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        program.optimize();
        server.install(worker, "p", &program).unwrap();
        let mut seen = Vec::new();
        for tick in 0..4 {
            if worker.index() == 0 {
                for (k, v, d) in feeds(tick) {
                    let row = Value::Tuple(vec![Value::Int(k), Value::Int(v)]);
                    server.feed("p", 0, row, Value::unit(), None, d).unwrap();
                }
            }
            server.tick(worker);
            for name in names {
                seen.push(server.snapshot(worker, name).unwrap());
            }
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn terms_reduce_as_the_prefabs_do() {
    let want = run(RenderBackend::Vec, 1);
    for tick in want.chunks(8) {
        assert!(tick.iter().all(|rows| !rows.is_empty()), "every export has rows: {tick:?}");
        assert_eq!(tick[0], tick[1], "count");
        assert_eq!(tick[2], tick[3], "distinct");
        assert_eq!(tick[4], tick[5], "min");
        assert_eq!(tick[6], tick[7], "a term and its lambda");
    }
    for workers in [1, 3] {
        assert_eq!(run(RenderBackend::Vec, workers), want, "vec, {workers} workers");
        assert_eq!(run(RenderBackend::Corgi, workers), want, "corgi, {workers} workers");
    }
}
