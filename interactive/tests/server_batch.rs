//! Atomicity boundary of `Server::feed_batch`.
//!
//! A batch validates every target before staging anything, stages every member
//! at one open host epoch, and leaves visibility to the next `tick`.

use interactive::ir::Value;
use interactive::server::{InputUpdate, Server};
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn update(input: usize, key: i64, value: i64) -> InputUpdate {
    InputUpdate {
        prog: "paired".to_string(),
        input,
        key: tup(&[key]),
        val: tup(&[value]),
        diff: 1,
    }
}

const PAIRED: &str = r#"
    let world = input 0;
    let audit = input 1;
    export "batch_world" = world;
    export "batch_audit" = audit;
"#;

#[test]
fn batch_prevalidates_all_targets_and_becomes_visible_on_tick() {
    timely::execute_directly(move |worker| {
        let statements = parse::pipe::parse(PAIRED);
        let mut program = lower::lower_tree(statements);
        program.optimize();

        let mut server = Server::new();
        server.install(worker, "paired", &program).unwrap();

        // The second target is invalid. The valid first member must not leak
        // into the open epoch, even after that epoch is later closed.
        let err = server
            .feed_batch(vec![update(0, 1, 10), update(7, 2, 20)])
            .unwrap_err();
        assert!(err.contains("has no input 7"), "{err}");
        server.tick(worker);
        assert!(server.snapshot(worker, "batch_world").unwrap().is_empty());
        assert!(server.snapshot(worker, "batch_audit").unwrap().is_empty());

        // A valid batch is staged but remains outside the closed-past snapshot
        // until one tick closes its common epoch.
        server
            .feed_batch(vec![update(0, 1, 10), update(1, 2, 20)])
            .unwrap();
        assert!(server.snapshot(worker, "batch_world").unwrap().is_empty());
        assert!(server.snapshot(worker, "batch_audit").unwrap().is_empty());

        server.tick(worker);
        assert_eq!(
            server.snapshot(worker, "batch_world").unwrap(),
            vec![(tup(&[1]), tup(&[10]), 1)]
        );
        assert_eq!(
            server.snapshot(worker, "batch_audit").unwrap(),
            vec![(tup(&[2]), tup(&[20]), 1)]
        );
    });
}
