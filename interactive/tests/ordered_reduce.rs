//! Ordered proxies must preserve signed, ragged values and retractions through every reducer.
//!
//! The reference is computed here from the keyed input's net contents, by the reducers'
//! definitions: each key's values with non-zero net multiplicity, in value order.
use std::collections::BTreeMap;

use interactive::ir::Value as V;
use interactive::server::{InputUpdate, Server};

type Rows = Vec<(V, V, i64)>;

/// What reducer `name` makes of a collection whose net contents are `keyed`.
fn reference(name: &str, keyed: &Rows) -> Rows {
    let mut groups: BTreeMap<&V, Vec<(&V, i64)>> = BTreeMap::new();
    for (k, v, d) in keyed {
        if *d != 0 { groups.entry(k).or_default().push((v, *d)); }
    }
    let mut out: Rows = groups.into_iter().filter_map(|(k, mut vals)| {
        vals.sort();
        let val = match name {
            "minimum" => vals[0].0.clone(),
            "distinct" => V::unit(),
            "counted" => match vals.iter().map(|(_, d)| d).sum::<i64>() { 0 => return None, n => V::Int(n) },
            "collected" => V::List(vals.iter().flat_map(|(v, d)| std::iter::repeat_n((*v).clone(), (*d).max(0) as usize)).collect()),
            other => panic!("no reference for {other}"),
        };
        Some((k.clone(), val, 1))
    }).collect();
    out.sort();
    out
}

fn sorted(mut rows: Rows) -> Rows { rows.sort(); rows }

#[test]
fn ordered_min_collect_and_batched_reconciliation_match_their_definitions() {
    timely::execute_directly(|worker| {
        let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
            r#"
            let x = input 0 : ((int, int, List(int)); ())
                | key(((k, n, xs), ()) -> (k, (n, xs)));
            export "keyed" = x;
            export "minimum" = x | min;
            export "collected" = x | collect;
            export "counted" = x | count;
            export "distinct" = x | distinct;
        "#,
        ));
        program.optimize();
        let mut col = Server::new();
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
                col.feed_batch("col", 0, chunk.to_vec()).unwrap();
            }
            col.tick(worker);
            let keyed = col.snapshot(worker, "keyed").unwrap().into_rows();
            for name in ["minimum", "collected", "counted", "distinct"] {
                assert_eq!(sorted(col.snapshot(worker, name).unwrap().into_rows()), reference(name, &keyed), "{name}");
            }
        }
        for name in ["minimum", "collected", "counted", "distinct"] {
            assert!(col.snapshot(worker, name).unwrap().is_empty());
        }
    });
}

#[test]
fn batched_equality_spans_windows_and_retracts_compound_keys() {
    timely::execute_directly(|worker| {
        let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(r#"
            let x = input 0 : ((int, List(int)); ())
                | key(((k, xs), ()) -> ((k, (k, 7) mul), xs));
            export "keyed" = x;
            export "minimum" = x | min;
            export "collected" = x | collect;
            export "counted" = x | count;
        "#));
        program.optimize();
        let mut col = Server::new();
        col.install(worker, "col", &program).unwrap();
        for diff in [1, 1, -2] {
            let updates: Vec<_> = (-2500..2500).map(|k| InputUpdate {
                key: V::Tuple(vec![V::Int(k), V::List(vec![V::Int(k), V::Int(-k), V::Int(k + 1)])]),
                val: V::unit(), diff,
            }).collect();
            col.feed_batch("col", 0, updates).unwrap();
            col.tick(worker);
            let keyed = col.snapshot(worker, "keyed").unwrap().into_rows();
            for name in ["minimum", "collected", "counted"] {
                assert_eq!(sorted(col.snapshot(worker, name).unwrap().into_rows()), reference(name, &keyed), "{name}, diff={diff}");
            }
        }
        for name in ["minimum", "collected", "counted"] { assert!(col.snapshot(worker, name).unwrap().is_empty()); }
    });
}
