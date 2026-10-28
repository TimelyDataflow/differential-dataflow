//! Ordered proxies must preserve signed, ragged values and retractions through every reducer.
//!
//! The reference is computed here from the input's net contents, held as Rust values (whose order
//! is corgi's for these shapes), by the reducers' definitions: each key's values with non-zero net
//! multiplicity, in value order. Rows compare as a feed writes them.
use std::collections::BTreeMap;

use interactive::server::Server;

type Rows = Vec<(String, String, i64)>;

fn join<T>(xs: impl IntoIterator<Item = T>, show: impl Fn(T) -> String) -> String {
    xs.into_iter().map(show).collect::<Vec<_>>().join(",")
}

/// What reducer `name` makes of a collection whose net contents are `net`, its keys and values
/// written by `key` and `val`.
fn reference<K: Ord, V: Ord>(name: &str, net: &BTreeMap<(K, V), i64>, key: impl Fn(&K) -> String, val: impl Fn(&V) -> String) -> Rows {
    let mut groups: BTreeMap<&K, Vec<(&V, i64)>> = BTreeMap::new();
    for ((k, v), d) in net {
        if *d != 0 { groups.entry(k).or_default().push((v, *d)); }
    }
    sorted(groups.into_iter().flat_map(|(k, vals)| {
        let outs = match name {
            "minimum" => vec![val(vals[0].0)],
            "distinct" => vals.iter().map(|(v, _)| val(v)).collect(),
            "counted" => match vals.iter().map(|(_, d)| d).sum::<i64>() { 0 => Vec::new(), n => vec![n.to_string()] },
            "collected" => vec![format!("list({})", join(vals.iter().flat_map(|(v, d)| std::iter::repeat_n(*v, (*d).max(0) as usize)), &val))],
            other => panic!("no reference for {other}"),
        };
        let k = key(k);
        outs.into_iter().map(move |out| (k.clone(), out, 1))
    }).collect())
}

/// `net` plus `(row, diff)`, keeping only rows of non-zero multiplicity.
fn apply<R: Ord>(net: &mut BTreeMap<R, i64>, row: R, diff: i64) {
    *net.entry(row).or_default() += diff;
    net.retain(|_, d| *d != 0);
}

/// `net`'s rows, as a snapshot gives them.
fn rows<K, V>(net: &BTreeMap<(K, V), i64>, key: impl Fn(&K) -> String, val: impl Fn(&V) -> String) -> Rows {
    sorted(net.iter().map(|((k, v), d)| (key(k), val(v), *d)).collect())
}

fn sorted(mut rows: Rows) -> Rows { rows.sort(); rows }

#[test]
fn ordered_min_collect_and_batched_reconciliation_match_their_definitions() {
    timely::execute_directly(|worker| {
        let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
            r#"
            let x = input 0 : ((int, int, List(int)), ())
                | map(((k, n, xs), ()) -> (k, (n, xs)));
            export "keyed" = x;
            export "minimum" = x | min;
            export "collected" = x | collect;
            export "counted" = x | count;
            export "distinct" = x | distinct;
        "#,
        ));
        interactive::passes::optimize(&mut program);
        let mut col = Server::new();
        col.install(worker, "col", &program).unwrap();
        let (key, val) = (|k: &i64| k.to_string(), |(n, xs): &(i64, Vec<i64>)| format!("tuple({n},list({}))", join(xs, i64::to_string)));
        let mut net = BTreeMap::new();
        let update = |k: i64, n: i64, xs: &[i64], diff: i64| (k, n, xs.to_vec(), diff);
        // An update as a feed line: `tuple(k,n,list(xs)) diff=d`.
        let line = |(k, n, xs, diff): &(i64, i64, Vec<i64>, i64)| {
            format!("tuple({k},{n},list({})) diff={diff}", xs.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
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
        let reverse = |xs: &[(i64, i64, Vec<i64>, i64)]| {
            xs.iter().map(|(k, n, xs, diff)| (*k, *n, xs.clone(), -diff)).collect::<Vec<_>>()
        };
        for updates in [
            first.clone(),
            second.clone(),
            reverse(&second),
            reverse(&first),
        ] {
            // Separate admissions exercise presentation across more than one input buffer.
            for chunk in updates.chunks(2) {
                col.feed("col", 0, &chunk.iter().map(line).collect::<Vec<_>>().join("\n"), None).unwrap();
            }
            for (k, n, xs, diff) in updates { apply(&mut net, (k, (n, xs)), diff) }
            col.tick(worker);
            assert_eq!(sorted(col.snapshot(worker, "keyed").unwrap().rows()), rows(&net, key, val));
            for name in ["minimum", "collected", "counted", "distinct"] {
                assert_eq!(sorted(col.snapshot(worker, name).unwrap().rows()), reference(name, &net, key, val), "{name}");
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
            let x = input 0 : ((int, List(int)), ())
                | map(((k, xs), ()) -> ((k, (k, 7) mul), xs));
            export "keyed" = x;
            export "minimum" = x | min;
            export "collected" = x | collect;
            export "counted" = x | count;
        "#));
        interactive::passes::optimize(&mut program);
        let mut col = Server::new();
        col.install(worker, "col", &program).unwrap();
        let (key, val) = (|(k, k7): &(i64, i64)| format!("tuple({k},{k7})"), |xs: &Vec<i64>| format!("list({})", join(xs, i64::to_string)));
        let mut net = BTreeMap::new();
        for diff in [1, 1, -2] {
            let updates: Vec<String> = (-2500..2500).map(|k| format!("tuple({k},list({k},{},{})) diff={diff}", -k, k + 1)).collect();
            col.feed("col", 0, &updates.join("\n"), None).unwrap();
            for k in -2500..2500 { apply(&mut net, ((k, k * 7), vec![k, -k, k + 1]), diff) }
            col.tick(worker);
            assert_eq!(sorted(col.snapshot(worker, "keyed").unwrap().rows()), rows(&net, key, val));
            for name in ["minimum", "collected", "counted"] {
                assert_eq!(sorted(col.snapshot(worker, name).unwrap().rows()), reference(name, &net, key, val), "{name}, diff={diff}");
            }
        }
        for name in ["minimum", "collected", "counted"] { assert!(col.snapshot(worker, name).unwrap().is_empty()); }
    });
}

/// A reduce gathers only what its lambda reads: nothing of the key or the values, only the diffs,
/// a field of each value, or a field of the key and one of each value's. Each still sees every
/// value of non-zero net, in order, whatever it reads of them.
#[test]
fn lambdas_reading_part_of_their_input_match_their_definitions() {
    timely::execute_directly(|worker| {
        let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(r#"
            let x = input 0 : ((int, int, List(int)), ())
                | map(((k, n, xs), ()) -> ((k, (k, 3) mul), (n, xs)));
            export "keyed" = x;
            export "lengths" = x | reduce((_, vals) -> (vals len, 1) enlist);
            export "sums" = x | reduce((_, vals) -> let n = vals map ((_, d) -> d) fold_add in (n, (n, 1)) enlist filter);
            export "firsts" = x | reduce((_, vals) -> vals map (((n, _), d) -> (n, d)));
            export "sizes" = x | reduce((k, vals) -> (vals map (((_, xs), d) -> (xs len, d)), (k.1, 1) enlist) append);
        "#));
        interactive::passes::optimize(&mut program);
        let mut col = Server::new();
        col.install(worker, "col", &program).unwrap();
        let mut net: BTreeMap<(i64, (i64, Vec<i64>)), i64> = BTreeMap::new();
        // What each export holds for `net`: per key, its values of non-zero net in order.
        let reference = |name: &str, net: &BTreeMap<(i64, (i64, Vec<i64>)), i64>| -> Rows {
            let mut groups: BTreeMap<i64, Vec<(&(i64, Vec<i64>), i64)>> = BTreeMap::new();
            for ((k, v), d) in net { groups.entry(*k).or_default().push((v, *d)); }
            let mut rows = Vec::new();
            for (k, vals) in groups {
                let mut outs: BTreeMap<String, i64> = BTreeMap::new();
                match name {
                    "lengths" => { outs.insert(vals.len().to_string(), 1); }
                    "sums" => { let n: i64 = vals.iter().map(|(_, d)| d).sum(); if n != 0 { outs.insert(n.to_string(), 1); } }
                    "firsts" => for ((n, _), d) in &vals { *outs.entry(n.to_string()).or_default() += d; },
                    _ => {
                        for ((_, xs), d) in &vals { *outs.entry(xs.len().to_string()).or_default() += d; }
                        *outs.entry((k * 3).to_string()).or_default() += 1;
                    }
                }
                rows.extend(outs.into_iter().filter(|(_, d)| *d != 0).map(|(v, d)| (format!("tuple({k},{})", k * 3), v, d)));
            }
            sorted(rows)
        };
        // Equal first fields under one key, negative nets, and a key whose values all cancel.
        let batches: [&[(i64, i64, &[i64], i64)]; 4] = [
            &[(0, 5, &[1, 2], 2), (0, 5, &[1], 1), (0, -3, &[], -1), (1, 7, &[4], 1), (2, 2, &[3, 3, 3], 1)],
            &[(0, 5, &[1, 2], -2), (0, 9, &[0], 1), (1, 7, &[4], -1), (2, 2, &[3], -1)],
            &[(0, 5, &[1], -1), (1, 1, &[], 3), (2, 2, &[3, 3, 3], -1), (2, 2, &[3], 1)],
            &[(0, -3, &[], 1), (0, 9, &[0], -1), (1, 1, &[], -3)],
        ];
        for batch in batches {
            let lines: Vec<String> = batch.iter().map(|(k, n, xs, d)| format!("tuple({k},{n},list({})) diff={d}", join(xs.iter(), i64::to_string))).collect();
            col.feed("col", 0, &lines.join("\n"), None).unwrap();
            for (k, n, xs, d) in batch { apply(&mut net, (*k, (*n, xs.to_vec())), *d) }
            col.tick(worker);
            for name in ["lengths", "sums", "firsts", "sizes"] {
                assert_eq!(sorted(col.snapshot(worker, name).unwrap().rows()), reference(name, &net), "{name}");
            }
        }
        assert!(net.is_empty());
        for name in ["lengths", "sums", "firsts", "sizes"] { assert!(col.snapshot(worker, name).unwrap().is_empty(), "{name}"); }
    });
}
