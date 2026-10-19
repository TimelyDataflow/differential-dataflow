//! Export taps hand over each export's changes as columns, once: summed over every tick, they are
//! the export's contents, inserts and retractions of nested rows alike.
use interactive::corgi::container::Updates;
use interactive::ir::Value;
use interactive::server::{InputUpdate, Server};
use interactive::{lower, parse};

#[test]
fn tapped_changes_sum_to_the_export_and_are_taken_once() {
    timely::execute_directly(|worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(r#"
            type Three = A int | B int | C int;
            let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
            export "plain" = pairs;
            export "lists" = pairs | collect;
            export "tagged" = pairs | map($0 ; variant(Three, 2, $1[0]));
        "#));
        program.optimize();
        let mut server = Server::new();
        server.set_export_taps(true);
        server.install(worker, "taps", &program).unwrap();
        let rows = |shift: i64, diff| (0..257).map(|i| InputUpdate {
            key: Value::Tuple(vec![Value::Int(i % 11), Value::Int(i - 128 + shift)]),
            val: Value::unit(), diff,
        }).collect::<Vec<_>>();
        let mut taken_by_export: Vec<Vec<Updates>> = vec![Vec::new(); 3];
        // Two sets of rows, the first retracted and then added again.
        for (shift, diff) in [(0, 1), (1000, 1), (0, -1), (0, 1)] {
            server.feed_batch("taps", 0, rows(shift, diff)).unwrap();
            server.tick(worker);
            for (name, taken) in ["plain", "lists", "tagged"].into_iter().zip(&mut taken_by_export) {
                let changes = server.take_changes(name);
                assert!(!changes.is_empty(), "{name}, shift {shift}, diff {diff}");
                taken.extend(changes.into_iter().map(|c| Updates { keys: c.keys, vals: c.vals, diffs: c.diffs }));
                assert!(server.take_changes(name).is_empty(), "{name}: changes taken twice");
                let contents = server.snapshot(worker, name).unwrap();
                let shape = (corgi::shape_of_value(&contents.keys), corgi::shape_of_value(&contents.vals));
                assert_eq!(Updates::consolidate(taken.clone(), &shape), contents, "{name}, shift {shift}, diff {diff}");
            }
        }
        assert!(server.take_changes("missing").is_empty());
    });
}
