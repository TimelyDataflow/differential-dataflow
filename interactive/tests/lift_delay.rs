//! `lift`, `join_function`, and `delay`: each pinned to explicit rows, across workers, as inputs
//! are added and retracted over several epochs.

use interactive::ir::Value;
use interactive::server::Server;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let pairs = input 0 : ((int, int) ; ()) | map(((a, b), ()) -> (a, b));
    export "lift" = pairs | lift;
    export "epoch" = pairs | delay((_, b) -> (b)) | lift;
    export "join_function" = pairs | join_function((a, b) -> (((b, a), (0), 2) enlist, ((a, a), (0), -1) enlist) append);
    stepped: {
        var at = pairs | delay((_, b) -> (0, b)) | lift;
        var below = pairs | delay((_, b) -> (0, (0, b) sub)) | lift;
    }
    export "delay" = stepped::at;
    export "negative" = stepped::below;
"#;

const NAMES: [&str; 5] = ["lift", "epoch", "join_function", "delay", "negative"];

/// The rows fed at each tick, as `(key, val, diff)`: a retraction, and the same row brought back.
fn feeds(tick: i64) -> Vec<(i64, i64, i64)> {
    match tick {
        0 => vec![(1, 2, 1), (3, 4, 1)],
        1 => vec![(5, 1, 1), (1, 2, -1)],
        _ => vec![(1, 2, 1)],
    }
}

/// Every export, read after the last tick.
fn run(workers: usize) -> Vec<Vec<(Value, Value, i64)>> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        program.optimize();
        server.install(worker, "p", &program).unwrap();
        for tick in 0..3 {
            if worker.index() == 0 {
                for (k, v, d) in feeds(tick) {
                    server.feed("p", 0, &format!("{k},{v} diff={d}"), None).unwrap();
                }
            }
            server.tick(worker);
        }
        NAMES.map(|name| server.snapshot(worker, name).unwrap().into_rows()).to_vec()
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

fn int(n: i64) -> Value { Value::Int(n) }
fn ints(ns: &[i64]) -> Value { Value::Tuple(ns.iter().copied().map(int).collect()) }
/// `(key ; (val, time))`, as `lift` writes a row of `pairs`.
fn lifted(k: i64, v: i64, time: &[i64], d: i64) -> (Value, Value, i64) {
    (int(k), Value::Tuple(vec![int(v), ints(time)]), d)
}

/// The rows each export holds after the last tick (epoch 2).
fn expected() -> Vec<Vec<(Value, Value, i64)>> {
    vec![
        // The retraction of `(1, 2)` at epoch 1 lifts to a row of its own: lifted data does not cancel across times.
        vec![lifted(1, 2, &[0], 1), lifted(1, 2, &[1], -1), lifted(1, 2, &[2], 1), lifted(3, 4, &[0], 1), lifted(5, 1, &[1], 1)],
        // Delayed to epoch `b` they do cancel; `(3, 4)` is at epoch 4, not yet complete.
        vec![lifted(1, 2, &[2], 1), lifted(5, 1, &[1], 1)],
        // `(b ; a)` with diff 2 and `(a ; a)` with diff -1, for each of `(1, 2)`, `(3, 4)`, `(5, 1)`.
        vec![
            (int(1), int(1), -1), (int(1), int(5), 2), (int(2), int(1), 2),
            (int(3), int(3), -1), (int(4), int(3), 2), (int(5), int(5), -1),
        ],
        // The scope's coordinate is `b`, read by a `lift` after the delay in the same chain.
        vec![lifted(1, 2, &[0, 2], 1), lifted(1, 2, &[1, 2], -1), lifted(1, 2, &[2, 2], 1), lifted(3, 4, &[0, 4], 1), lifted(5, 1, &[1, 1], 1)],
        // A negative coordinate leaves the time as it was.
        vec![lifted(1, 2, &[0, 0], 1), lifted(1, 2, &[1, 0], -1), lifted(1, 2, &[2, 0], 1), lifted(3, 4, &[0, 0], 1), lifted(5, 1, &[1, 0], 1)],
    ]
}

#[test]
fn rows_are_as_expected() {
    let want = expected();
    for workers in [1, 3] {
        for ((name, got), want) in NAMES.iter().zip(run(workers)).zip(&want) {
            assert_eq!(&got, want, "{name}: {workers} workers");
        }
    }
}

/// A time of the wrong width, or a `join_function` term not of the form `((key, val), time, diff)`, is
/// refused at install.
#[test]
fn ill_shaped_times_are_refused_at_install() {
    timely::execute_directly(|worker| {
        for (src, says) in [
            (r#"export "a" = input 0 : ((int, int) ; ()) | delay((_, _) -> (0, 0));"#, "join_function"),
            (r#"let i = input 0 : ((int, int) ; ()); s: { var a = i | delay((_, _) -> (0)); } export "a" = s::a;"#, "join_function"),
            (r#"export "a" = input 0 : ((int, int) ; ()) | join_function((k, v) -> ((k, v), (0)) enlist);"#, "join_function"),
        ] {
            let mut program = lower::lower_tree(parse::pipe::parse(src));
            program.optimize();
            let error = Server::new().install(worker, "p", &program).unwrap_err();
            assert!(error.contains(says), "{src}: {error}");
        }
    });
}
