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
            "let rows = input 0 : ((int) ; (int)); export \"rows\" = rows;",
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

/// Ticks and transient snapshots must wake for delayed remote rows and progress.
#[test]
fn delayed_peers_wake_ticks_and_snapshots() {
    use interactive::server::RenderBackend;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    for backend in [RenderBackend::Vec, RenderBackend::Corgi] {
        let barrier = Arc::new(Barrier::new(3));
        let guards = timely::execute(timely::Config::process(3), move |worker| {
            let mut program = lower::lower_tree(parse::pipe::parse(
                "let rows = input 0 : ((int) ; ()); export \"rows\" = rows;",
            ));
            program.optimize();
            let mut server = Server::with_backend(backend);
            server.install(worker, "world", &program).unwrap();
            for epoch in 0..6 {
                barrier.wait();
                if worker.index() == epoch % 3 {
                    std::thread::sleep(Duration::from_millis(20));
                    let diff = if epoch < 3 { 1 } else { -1 };
                    server.feed_batch("world", 0, vec![InputUpdate {
                        key: tup(&[(epoch % 3) as i64]), val: Value::unit(), diff,
                    }]).unwrap();
                }
                server.tick(worker);
                barrier.wait();
                if worker.index() == (epoch + 1) % 3 {
                    std::thread::sleep(Duration::from_millis(20));
                }
                let rows = server.snapshot(worker, "rows").unwrap();
                let expected: Vec<_> = if worker.index() == 0 {
                    (0..3).filter(|&i| if epoch < 3 { i <= epoch } else { i > epoch - 3 })
                        .map(|i| (tup(&[i as i64]), Value::unit(), 1)).collect()
                } else { Vec::new() };
                assert_eq!(rows, expected);
            }
        }).unwrap();
        for result in guards.join() { result.unwrap(); }
    }
}
