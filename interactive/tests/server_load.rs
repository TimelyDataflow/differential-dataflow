//! `Server::load` from a file is all-or-nothing across the worker group: every
//! worker validates the whole file before applying its shard, so a malformed
//! line fails the load on every worker and no row of the file is fed anywhere.
//! (With per-shard validation, two workers and the lines `1, bad, 3, 4` gave
//! "loaded 4 rows" on worker 0 with only `1` and `3` present.)

use std::collections::BTreeSet;

use interactive::server::Server;
use interactive::{lower, parse};

/// A tuple of integers, as a feed writes it.
fn tup(fields: &[i64]) -> String {
    format!("tuple({})", fields.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
}

/// Run `load` from a file with `body` on `workers` workers; return each
/// worker's result and the rows worker 0 snapshots afterwards.
fn load_on(workers: usize, body: &str) -> (Vec<Result<u64, String>>, Vec<String>) {
    let dir = std::env::temp_dir().join(format!("ddir-server-load-{}-{}", std::process::id(), workers));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.txt", body.len()));
    std::fs::write(&path, body).unwrap();
    let path = path.to_string_lossy().into_owned();

    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(
            "let rows = input 0 : ((int), ()); export \"rows\" = rows;",
        ));
        program.optimize();
        let mut server = Server::new();
        server.install(worker, "world", &program).unwrap();
        let outcome = server.load(worker, "world", 0, &path, 1);
        server.tick(worker);
        let rows = server
            .snapshot(worker, "rows")
            .unwrap().rows()
            .into_iter()
            .map(|(key, _, _)| key)
            .collect::<Vec<_>>();
        (outcome, rows)
    })
    .unwrap();
    let mut results = guards.join().into_iter().map(Result::unwrap);
    let (first_outcome, rows) = results.next().unwrap();
    let mut outcomes = vec![first_outcome];
    outcomes.extend(results.map(|(outcome, _)| outcome));
    (outcomes, rows)
}

#[test]
fn a_malformed_line_fails_the_load_on_every_worker_and_feeds_nothing() {
    let (outcomes, rows) = load_on(2, "1\nbad\n3\n4\n");
    for outcome in &outcomes {
        let error = outcome.as_ref().expect_err("a malformed line must fail the load");
        assert!(error.contains("line 2"), "{error}");
    }
    assert!(rows.is_empty(), "no row of a rejected file may be fed: {rows:?}");
}

#[test]
fn a_well_formed_file_is_fed_exactly_once_across_workers() {
    let (outcomes, rows) = load_on(3, "1\n\n2\n3\n4\n");
    assert!(outcomes.iter().all(|o| *o == Ok(4)), "{outcomes:?}");
    let want: BTreeSet<String> = [tup(&[1]), tup(&[2]), tup(&[3]), tup(&[4])].into_iter().collect();
    assert_eq!(rows.iter().cloned().collect::<BTreeSet<_>>(), want);
    assert_eq!(rows.len(), 4, "each row once: {rows:?}");
}

/// `load` from a published trace on `workers` workers: `world` holds `1..=6`, `gone` holds
/// `2, 4, 6`, and `world` is fed `gone` scaled by `-1`, then `world` scaled by `2`. Returns
/// worker 0's snapshot of `world` as (key, multiplicity).
fn load_trace_on(workers: usize) -> Vec<(String, i64)> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let install = |server: &mut Server, worker: &mut _, name: &str| {
            let text = format!("let rows = input 0 : ((int), ()); export \"{name}.rows\" = rows;");
            let mut program = lower::lower_tree(parse::pipe::parse(&text));
            program.optimize();
            server.install(worker, name, &program).unwrap();
        };
        let mut server = Server::new();
        install(&mut server, worker, "world");
        install(&mut server, worker, "gone");
        if worker.index() == 0 {
            server.feed("world", 0, "1\n2\n3\n4\n5\n6", None).unwrap();
            server.feed("gone", 0, "2\n4\n6", None).unwrap();
        }
        server.tick(worker);
        assert!(server.loads_from_trace("gone.rows"));
        server.load(worker, "world", 0, "gone.rows", -1).unwrap();
        server.tick(worker);
        server.load(worker, "world", 0, "world.rows", 2).unwrap();
        server.tick(worker);
        server
            .snapshot(worker, "world.rows")
            .unwrap()
            .rows()
            .into_iter()
            .map(|(key, _, diff)| (key, diff))
            .collect::<Vec<_>>()
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn a_trace_feeds_its_contents_scaled_from_every_workers_share() {
    for workers in [1, 3] {
        let rows = load_trace_on(workers);
        let want = vec![(tup(&[1]), 3), (tup(&[3]), 3), (tup(&[5]), 3)];
        assert_eq!(rows, want, "at {workers} workers");
    }
}
