//! Bounded export consumption must preserve inserts/retractions and nested rows.
use interactive::ir::Value;
use interactive::server::{InputUpdate, RenderBackend, Server};
use interactive::{lower, parse};

#[test]
fn bounded_exports_match_full_drain_and_are_consumed_once() {
    timely::execute_directly(|worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(r#"
            type Three = A int | B int | C int;
            let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
            export "plain" = pairs;
            export "lists" = pairs | collect;
            export "tagged" = pairs | map($0 ; variant(Three, 2, $1[0]));
        "#));
        program.optimize();
        let mut full = Server::with_backend(RenderBackend::Corgi);
        let mut bounded = Server::with_backend(RenderBackend::Corgi);
        for server in [&mut full, &mut bounded] {
            server.set_columnar_exports(true);
            server.set_export_taps(true);
        }
        full.install(worker, "full", &program).unwrap();
        bounded.install(worker, "bounded", &program).unwrap();
        for (epoch, limit) in [1, 7, 128, 4096].into_iter().enumerate() {
            let diff = if epoch % 2 == 0 { 1 } else { -1 };
            let input: Vec<_> = (0..257).map(|i| InputUpdate {
                key: Value::Tuple(vec![Value::Int(i % 11), Value::Int(i - 128)]),
                val: Value::unit(), diff,
            }).collect();
            full.feed_batch("full", 0, input.clone()).unwrap();
            bounded.feed_batch("bounded", 0, input).unwrap();
            full.tick(worker);
            bounded.tick(worker);
            for name in ["plain", "lists", "tagged"] {
                let mut expected = full.take_changes(name);
                let mut actual = Vec::new();
                bounded.for_each_change_batch(name, limit, |rows| {
                    assert!(!rows.is_empty() && rows.len() <= limit);
                    actual.extend(rows);
                });
                assert!(!expected.is_empty(), "{name}, epoch {epoch}");
                expected.sort();
                actual.sort();
                assert_eq!(actual, expected, "{name}, epoch {epoch}, limit {limit}");
                bounded.for_each_change_batch(name, limit, |_| panic!("changes drained twice"));
                assert!(bounded.take_changes(name).is_empty());
            }
        }
        bounded.for_each_change_batch("missing", 7, |_| panic!("missing export has changes"));
    });
}
