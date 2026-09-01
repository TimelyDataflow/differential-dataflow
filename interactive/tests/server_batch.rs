use interactive::ir::Value;
use interactive::server::{InputUpdate, Server};
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().copied().map(Value::Int).collect())
}

#[test]
fn feed_batch_validates_before_staging_one_epoch() {
    timely::execute_directly(|worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(
            "let rows = input 0; export \"rows\" = rows;",
        ));
        program.optimize();

        let mut server = Server::new();
        server.install(worker, "world", &program).unwrap();
        let updates = vec![
            InputUpdate {
                key: tup(&[1]),
                val: tup(&[10]),
                diff: 1,
            },
            InputUpdate {
                key: tup(&[2]),
                val: tup(&[20]),
                diff: 1,
            },
        ];

        assert!(server.feed_batch("world", 1, updates.clone()).is_err());
        server.tick(worker);
        assert!(server.snapshot(worker, "rows").unwrap().is_empty());

        server.feed_batch("world", 0, updates).unwrap();
        assert!(server.snapshot(worker, "rows").unwrap().is_empty());
        server.tick(worker);
        assert_eq!(
            server.snapshot(worker, "rows").unwrap(),
            vec![(tup(&[1]), tup(&[10]), 1), (tup(&[2]), tup(&[20]), 1),]
        );
    });
}
