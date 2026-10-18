//! Ordered proxies must preserve signed, ragged values and retractions through every reducer.
use interactive::ir::Value as V;
use interactive::server::{InputUpdate, RenderBackend, Server};

#[test]
fn ordered_min_collect_and_batched_reconciliation_match_row_backend() {
    timely::execute_directly(|worker| {
        let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
            r#"
            let x = input 0 : ((int, int, List(int)); ())
                | key(((k, n, xs), ()) -> (k, (n, xs)));
            export "minimum" = x | min;
            export "collected" = x | collect;
            export "counted" = x | count;
            export "distinct" = x | distinct;
        "#,
        ));
        program.optimize();
        let mut row = Server::with_backend(RenderBackend::Vec);
        let mut col = Server::with_backend(RenderBackend::Corgi);
        row.install(worker, "row", &program).unwrap();
        col.install(worker, "col", &program).unwrap();
        let update = |k, n, xs: &[i64], diff| InputUpdate {
            key: V::Tuple(vec![
                V::Int(k),
                V::Int(n),
                V::List(xs.iter().copied().map(V::Int).collect()),
            ]),
            val: V::unit(),
            diff,
        };
        // Multiple batches, negative minima, proper prefixes, negative multiplicities, and an
        // empty-list output all matter: min sees negative net values, collect expands positives.
        let first = vec![
            update(0, -5, &[1, 0], 2),
            update(0, -5, &[1], -1),
            update(1, 7, &[4], -2),
            update(2, 0, &[3], 1),
        ];
        let second = vec![
            update(0, -5, &[1, 0], -1),
            update(0, -8, &[2], 1),
            update(1, 7, &[4], 1),
            update(2, 0, &[3], -1),
        ];
        let reverse = |xs: &[InputUpdate]| {
            xs.iter()
                .cloned()
                .map(|mut u| {
                    u.diff = -u.diff;
                    u
                })
                .collect::<Vec<_>>()
        };
        for updates in [
            first.clone(),
            second.clone(),
            reverse(&second),
            reverse(&first),
        ] {
            // Separate admissions exercise presentation across more than one input buffer.
            for chunk in updates.chunks(2) {
                row.feed_batch("row", 0, chunk.to_vec()).unwrap();
                col.feed_batch("col", 0, chunk.to_vec()).unwrap();
            }
            row.tick(worker);
            col.tick(worker);
            for name in ["minimum", "collected", "counted", "distinct"] {
                assert_eq!(
                    col.snapshot(worker, name).unwrap(),
                    row.snapshot(worker, name).unwrap(),
                    "{name}"
                );
            }
        }
        for name in ["minimum", "collected", "counted", "distinct"] {
            assert!(col.snapshot(worker, name).unwrap().is_empty());
        }
    });
}
