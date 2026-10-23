//! Export taps hand over each export's changes as columns, once, each at the epoch it happened:
//! summed over every tick, they are the export's contents, inserts and retractions of nested rows
//! alike.
use interactive::corgi::container::Updates;
use interactive::server::Server;
use interactive::{lower, parse};

#[test]
fn tapped_changes_sum_to_the_export_and_are_taken_once() {
    timely::execute_directly(|worker| {
        let mut program = lower::lower_tree(parse::pipe::parse(r#"
            type Three = A int | B int | C int;
            let pairs = input 0 : ((int, int), ()) | map(((a, b), ()) -> (a, b));
            export "plain" = pairs;
            export "lists" = pairs | collect;
            export "tagged" = pairs | map((a, b) -> (a, b inject C));

        "#));
        program.optimize();
        let mut server = Server::new();
        server.set_export_taps(true);
        server.install(worker, "taps", &program).unwrap();
        let rows = |shift: i64, diff: i64| (0..257).map(|i| format!("{},{} diff={diff}", i % 11, i - 128 + shift)).collect::<Vec<_>>().join("\n");
        let mut taken_by_export: Vec<Vec<Updates>> = vec![Vec::new(); 3];
        // Two sets of rows, the first retracted and then added again.
        for (shift, diff) in [(0, 1), (1000, 1), (0, -1), (0, 1)] {
            let epoch = server.epoch();
            server.feed("taps", 0, &rows(shift, diff), None).unwrap();
            server.tick(worker);
            for (name, taken) in ["plain", "lists", "tagged"].into_iter().zip(&mut taken_by_export) {
                let changes = server.take_changes(name);
                assert!(!changes.is_empty(), "{name}, shift {shift}, diff {diff}");
                assert!(changes.iter().all(|c| c.times.to_vec().iter().all(|t| t.outer == epoch && t.inner.is_empty())), "{name}: changes at another epoch than {epoch}");
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
