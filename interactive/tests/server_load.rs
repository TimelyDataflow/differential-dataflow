//! `Server::load` from a file is all-or-nothing across the worker group: every
//! worker validates the whole file before applying its shard, so a malformed
//! line fails the load on every worker and no row of the file is fed anywhere.
//! (With per-shard validation, two workers and the lines `1, bad, 3, 4` gave
//! "loaded 4 rows" on worker 0 with only `1` and `3` present.)

use std::collections::BTreeSet;

use interactive::ir::Value;
use interactive::server::Server;
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().copied().map(Value::Int).collect())
}

/// Run `load` from a file with `body` on `workers` workers; return each
/// worker's result and the rows worker 0 snapshots afterwards.
fn load_on(workers: usize, body: &str) -> (Vec<Result<u64, String>>, Vec<Value>) {
    let dir = std::env::temp_dir().join(format!("ddir-server-load-{}-{}", std::process::id(), workers));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.txt", body.len()));
    std::fs::write(&path, body).unwrap();
    let path = path.to_string_lossy().into_owned();

    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(
            "let rows = input 0; export \"rows\" = rows;",
        ));
        program.optimize();
        let mut server = Server::new();
        server.install(worker, "world", &program).unwrap();
        let outcome = server.load(worker, "world", 0, &path);
        server.tick(worker);
        let rows = server
            .snapshot(worker, "rows")
            .unwrap()
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
    let (outcomes, rows) = load_on(3, "1\n\n2 20\n3\n4\n");
    assert!(outcomes.iter().all(|o| *o == Ok(4)), "{outcomes:?}");
    let want: BTreeSet<Value> = [tup(&[1]), tup(&[2, 20]), tup(&[3]), tup(&[4])].into_iter().collect();
    assert_eq!(rows.iter().cloned().collect::<BTreeSet<_>>(), want);
    assert_eq!(rows.len(), 4, "each row once: {rows:?}");
}
