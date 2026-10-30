//! Bounded progress experiment for multitemporal delta joins. Frontier steps must release every input-closed cell.
//!
//! CARGO_HOME=~/Projects/dd-hj-steps/.cargo-home CARGO_NET_OFFLINE=true \
//! CARGO_INCREMENTAL=0 cargo test -p interactive --offline \
//!     --test half_join_multitemporal -- --ignored --nocapture
//!
//! Each checkpoint/export is replayed in a subprocess. The parent kills and reaps a
//! blocked wait_for; recovery checkpoints replay the same prefix plus more advances.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use differential_dataflow::trace::TraceReader;
use interactive::server::{HostTime, Server};
use interactive::{lower, parse};
use timely::progress::Antichain;
use timely::PartialOrder;

const PROGRAM: &str = r#"
    let a = input 0 : (int, int);
    let b = input 1 : (int, int);
    let ab = a | lift | half_join(b, 0, (k, x, y) -> (k, (x, y)));
    let ba = b | lift | half_join(a, 1, (k, y, x) -> (k, (x, y)));
    export "delta" = (ab + ba) | delay((_, (_, t)) -> t)
                             | map((k, (xy, _)) -> (k, xy));
    export "join" = a | join(b, (k, x, y) -> (k, (x, y)));
    -- Observe the arrangements' upper frontiers through public trace handles.
    export "a" = a;
    export "b" = b;
"#;

fn time(xs: &[u64]) -> HostTime { HostTime::new(xs.iter().copied().collect()) }
fn lanes(t: &HostTime, width: usize) -> Vec<u64> {
    (0..width).map(|i| t.get(i).copied().unwrap_or(0)).collect()
}

#[derive(Clone)]
enum Action {
    Feed(usize, Vec<u64>, i64, i64, i64),
    Advance(usize, Vec<u64>),
}
#[derive(Clone)]
struct Checkpoint {
    label: &'static str,
    actions: Vec<Action>,
    target: Vec<u64>,
    // join, delta; every wait must complete.
    completes: [bool; 2],
}
#[derive(Clone)]
struct Scenario {
    name: &'static str,
    width: usize,
    shared: bool,
    checkpoints: Vec<Checkpoint>,
}

fn clock_first(width: usize) -> Scenario {
    use Action::*;
    let t = |s, e, z| if width == 3 { vec![s, e, z] } else { vec![s, e] };
    let checkpoint = |label, mut actions: Vec<Action>, next| {
        let target = t(next, 0, 0);
        actions.extend([Advance(0, target.clone()), Advance(1, target.clone())]);
        Checkpoint { label, actions, target, completes: [true, true] }
    };
    Scenario {
        name: if width == 3 { "5-width-three" } else { "1-clock-first" },
        width, shared: false,
        checkpoints: vec![
            checkpoint("first", vec![
                Feed(0, t(0, 2, 3), 7, 10, 1), Feed(1, t(0, 1, 2), 7, 20, 2),
                Feed(0, t(0, 2, 3), 8, 11, 1), Feed(1, t(0, 2, 3), 8, 21, 1),
            ], 1),
            checkpoint("late", vec![
                Feed(0, t(1, 1, 1), 7, 10, -1), Feed(1, t(1, 0, 0), 7, 22, 1),
                Feed(0, t(1, 0, 0), 7, 12, 1), Feed(1, t(1, 3, 2), 7, 20, -1),
            ], 2),
            checkpoint("later", vec![
                Feed(0, t(2, 0, 0), 7, 13, 1), Feed(1, t(2, 1, 1), 7, 22, -1),
            ], 3),
        ],
    }
}

fn scenarios() -> Vec<Scenario> {
    use Action::*;
    let first = clock_first(2);
    let mut reverse = clock_first(2);
    reverse.name = "2-event-first";
    // Same updates, including late events and retractions, with the roles reversed.
    for c in &mut reverse.checkpoints {
        for action in &mut c.actions {
            let t = match action { Feed(_, t, ..) | Advance(_, t) => t };
            t.swap(0, 1);
        }
        c.target.swap(0, 1);
        c.completes = [true, true];
    }
    reverse.checkpoints.extend([
        Checkpoint { label: "partial-event-advance",
            actions: vec![Advance(0, vec![1, 3]), Advance(1, vec![1, 3])],
            target: vec![1, 3], completes: [true, true] },
        Checkpoint { label: "recovered",
            actions: vec![Advance(0, vec![4, 3]), Advance(1, vec![4, 3])],
            target: vec![4, 3], completes: [true, true] },
    ]);
    let independent = Scenario { name: "3-independent", width: 2, shared: false,
        checkpoints: vec![
            Checkpoint { label: "crossed",
                actions: vec![Feed(0, vec![1, 0], 7, 10, 1),
                    Feed(1, vec![0, 2], 7, 20, 2), Feed(1, vec![0, 0], 7, 21, 1),
                    Advance(0, vec![2, 1]), Advance(1, vec![0, 3])],
                target: vec![0, 1], completes: [true, true] },
            Checkpoint { label: "more-inner-progress",
                actions: vec![Feed(1, vec![0, 3], 7, 20, -1), Advance(1, vec![0, 4])],
                target: vec![0, 1], completes: [true, true] },
            Checkpoint { label: "recovered",
                actions: vec![Advance(1, vec![2, 4])],
                target: vec![2, 1], completes: [true, true] },
        ] };
    let shared = Scenario { name: "4-widened", width: 2, shared: true,
        checkpoints: vec![
            Checkpoint { label: "first",
                actions: vec![Feed(0, vec![0, 2], 7, 10, 1), Feed(2, vec![0], 7, 20, 2),
                    Advance(0, vec![1, 0]), Advance(1, vec![1, 0]), Advance(2, vec![1])],
                target: vec![1, 0], completes: [true, true] },
            Checkpoint { label: "late",
                actions: vec![Feed(0, vec![1, 1], 7, 10, -1), Feed(2, vec![1], 7, 21, 1),
                    Advance(0, vec![2, 0]), Advance(1, vec![2, 0]), Advance(2, vec![2])],
                target: vec![2, 0], completes: [true, true] },
            // The producer lags: wait only for its completed prefix, then advance it below.
            // Its open time [2,0] must still hold back both exports.
            Checkpoint { label: "producer-lags",
                actions: vec![Feed(0, vec![2, 0], 7, 12, 1),
                    Advance(0, vec![3, 0]), Advance(1, vec![3, 0])],
                target: vec![2, 0], completes: [true, true] },
            Checkpoint { label: "recovered",
                actions: vec![Advance(2, vec![3])],
                target: vec![3, 0], completes: [true, true] },
        ] };
    vec![first, reverse, independent, shared, clock_first(3)]
}

type Rows = BTreeMap<(i64, i64, i64), i64>;
type Fed = Vec<(usize, HostTime, i64, i64, i64)>;

fn expected(fed: &Fed, point: &HostTime) -> Rows {
    let mut result = Rows::new();
    for (a, ta, k, x, da) in fed {
        for (b, tb, l, y, db) in fed {
            if *a == 0 && *b == 1 && k == l && ta.less_equal(point) && tb.less_equal(point) {
                *result.entry((*k, *x, *y)).or_default() += da * db;
            }
        }
    }
    result.retain(|_, d| *d != 0);
    result
}

fn snapshot(server: &mut Server, worker: &mut timely::worker::Worker,
            name: &str, point: &HostTime) -> Rows {
    server.snapshot_as_of(worker, name, point).unwrap().rows().into_iter().map(|(k, v, d)| {
        let nums: Vec<i64> = v.split(|c: char| !c.is_ascii_digit() && c != '-')
            .filter(|x| !x.is_empty()).map(|x| x.parse().unwrap()).collect();
        assert_eq!(nums.len(), 2);
        ((k.parse().unwrap(), nums[0], nums[1]), d)
    }).collect()
}

fn upper(server: &Server, export: &str) -> Antichain<HostTime> {
    let mut upper = Antichain::new();
    server.published(export).unwrap().0.read_upper(&mut upper);
    upper
}

fn show(frontier: &Antichain<HostTime>, width: usize) -> Vec<Vec<u64>> {
    let mut points: Vec<_> = frontier.elements().iter().map(|t| lanes(t, width)).collect();
    points.sort();
    points
}

fn replay(scenario: &Scenario, checkpoint: usize, export: &str, workers: usize) {
    let scenario = scenario.clone();
    let export = export.to_owned();
    timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let program = if scenario.shared {
            let seed = lower::lower_tree(parse::pipe::parse(r#"export "seed_b" = input 0 : (int, int);"#));
            server.install(worker, "seed", &seed).unwrap();
            PROGRAM.replace("let b = input 1 : (int, int);",
                "let b = (input 1 : (int, int)) + (import \"seed_b\");")
        } else { PROGRAM.to_string() };
        let program = lower::lower_tree(parse::pipe::parse(&program));
        server.install_with_root(worker, "p", &program, scenario.width, time(&[])).unwrap();
        let mut fed = Fed::new();
        for c in &scenario.checkpoints[..=checkpoint] {
            for action in &c.actions {
                let side = match action { Action::Feed(s, ..) | Action::Advance(s, _) => *s };
                let (owner, input) = if side == 2 { ("seed", 0) } else { ("p", side) };
                match action {
                    Action::Feed(_, t, k, v, d) => {
                        if worker.index() == side % workers {
                            server.feed_at(owner, input, &format!("{k} val={v} diff={d}"), &time(t)).unwrap();
                        }
                        fed.push((side.min(1), time(t), *k, *v, *d));
                    }
                    Action::Advance(_, t) => server.advance_input(owner, input, &time(t)).unwrap(),
                }
            }
            // Drain without parking, even at prefixes whose waits would block.
            for _ in 0..2_000 { worker.step(); }
        }
        let width = scenario.width;
        let c = &scenario.checkpoints[checkpoint];
        let mut inputs: Vec<_> = (0..2).map(|i| server.input_frontier("p", i).unwrap().clone()).collect();
        if scenario.shared { inputs.push(server.input_frontier("seed", 0).unwrap().clone()); }
        let producers: Antichain<_> = inputs.iter().cloned().collect();
        println!("WAIT_BEGIN {export}");
        std::io::stdout().flush().unwrap();
        server.wait_for(worker, &export, &time(&c.target)).unwrap();
        // All workers use commanded frontiers for their collective reads. Worker-local
        // observations need not be at the same frontier during an activation.
        for name in ["join", "delta"] { server.wait_for_frontier(worker, name, &producers).unwrap(); }
        let a = upper(&server, "a");
        let b = upper(&server, "b");
        let join = upper(&server, "join");
        let delta = upper(&server, "delta");
        if scenario.shared && c.label == "producer-lags" {
            let ahead = time(&[3, 0]);
            for frontier in [&join, &delta] {
                assert!(frontier.iter().any(|t| !ahead.less_equal(t)), "the lagging import must still hold back [3,0]");
            }
        }
        println!("{} / {}: inputs={:?}, a={:?}, b={:?}, join={:?}, delta={:?}, target={:?}",
            scenario.name, c.label, inputs.iter().map(|t| lanes(t, width)).collect::<Vec<_>>(),
            show(&a, width), show(&b, width), show(&join, width), show(&delta, width), c.target);
        println!("WAIT_OK {export}");
        std::io::stdout().flush().unwrap();

        // A finite, exhaustive partition for these feeds: visibility and closure change
        // only at an input time or frontier coordinate. Include MAX to check the tails.
        // Every point in a cell has the same oracle answer and closure status.
        let mut axes = vec![BTreeSet::from([0, u64::MAX]); width];
        for t in fed.iter().map(|(_, t, ..)| t).chain(inputs.iter()) {
            for (axis, value) in axes.iter_mut().zip(lanes(t, width)) { axis.insert(value); }
        }
        let mut points = vec![Vec::new()];
        for axis in axes {
            points = points.into_iter().flat_map(|p| axis.iter().map(move |v| {
                let mut p = p.clone(); p.push(*v); p
            })).collect();
        }
        let (mut closed, mut compared, mut nonempty) = (0, 0, 0);
        for p in points {
            let point = time(&p);
            if producers.less_equal(&point) { continue; }
            closed += 1;
            assert!(!join.less_equal(&point), "ordinary join must close {p:?}");
            let joined = snapshot(&mut server, worker, "join", &point);
            if worker.index() == 0 { assert_eq!(joined, expected(&fed, &point), "join vs oracle at {p:?}"); }
            assert!(!delta.less_equal(&point), "delta join must close {p:?}");
            let halved = snapshot(&mut server, worker, "delta", &point);
            assert_eq!(halved, joined, "delta vs join at {p:?}");
            nonempty += usize::from(!joined.is_empty());
            compared += 1;
        }
        println!("COMPARE equal={compared}/{closed} closed cells; nonempty={nonempty}; pending={}", closed - compared);
        if c.completes == [true, true] {
            assert_eq!(compared, closed, "a completed checkpoint must make every closed cell readable");
            if worker.index() == 0 { assert!(nonempty > 0, "comparison must include nonempty answers"); }
        }
    }).unwrap().join().into_iter().for_each(|r| r.unwrap());
}

#[test]
#[ignore = "progress regression: subprocess deadlines bound blocked waits"]
fn multitemporal_delta_progress() {
    let cases = scenarios();
    if let Ok(selection) = std::env::var("DDIR_HJ_MT_CHILD") {
        let parts: Vec<_> = selection.split(':').collect();
        replay(&cases[parts[0].parse::<usize>().unwrap()], parts[1].parse().unwrap(), parts[2], parts[3].parse().unwrap());
        return;
    }
    // Redirect to a file, not a pipe: diagnostic output can never fill a pipe and
    // manufacture a timeout. No detached threads or live children survive a trial.
    let path = std::env::temp_dir().join(format!("ddir-hj-mt-{}.log", std::process::id()));
    let mut completed = 0;
    let mut stalled = 0;
    for workers in [1, 3] {
        for (case, scenario) in cases.iter().enumerate() {
            for (checkpoint, c) in scenario.checkpoints.iter().enumerate() {
                for (which, export) in ["join", "delta"].into_iter().enumerate() {
                    let file = std::fs::File::create(&path).unwrap();
                    let mut child = Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "multitemporal_delta_progress", "--ignored", "--nocapture"])
                        .env("DDIR_HJ_MT_CHILD", format!("{case}:{checkpoint}:{export}:{workers}"))
                        .stdout(Stdio::from(file.try_clone().unwrap())).stderr(Stdio::from(file))
                        .spawn().unwrap();
                    let start = Instant::now();
                    let mut wait_start = None;
                    let status = loop {
                        if let Some(status) = child.try_wait().unwrap() { break Some(status); }
                        let output = std::fs::read_to_string(&path).unwrap();
                        let waiting = output.contains("WAIT_BEGIN") && !output.contains("WAIT_OK");
                        if waiting && wait_start.is_none() { wait_start = Some(Instant::now()); }
                        if (waiting && wait_start.is_some_and(|t| t.elapsed() > Duration::from_secs(1)))
                            || start.elapsed() > Duration::from_secs(30) {
                            child.kill().unwrap();
                            child.wait().unwrap();
                            break None;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    };
                    let output = std::fs::read_to_string(&path).unwrap();
                    print!("{output}");
                    let success = status.is_some_and(|s| s.success());
                    if status.is_some() { assert!(success, "child failed: {output}"); }
                    else {
                        assert!(output.contains(&format!("WAIT_BEGIN {export}")) && !output.contains("WAIT_OK"),
                            "timeout outside wait_for: {output}");
                    }
                    assert_eq!(success, c.completes[which], "{} / {} / {export}", scenario.name, c.label);
                    println!("OUTCOME {workers} workers / {} / {} / {export}: {}", scenario.name, c.label,
                        if success { "completes" } else { "stalls (1s wait timeout; child reaped)" });
                    completed += usize::from(success);
                    stalled += usize::from(!success);
                }
            }
        }
    }
    std::fs::remove_file(path).unwrap();
    println!("TOTAL {completed} completed waits, {stalled} timed-out waits; all closed answers equal");
}
