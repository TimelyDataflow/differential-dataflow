//! Bracket lists: `[a, -b] | reduce` reduces the signed sum of its elements as one merged reduce.
//!
//! The tests check lowering, the answers against the same programs written as concatenations,
//! shape inference in loops, and the parse errors.

use std::collections::BTreeMap;

use interactive::scope_ir::{Item, Node, ReduceInput, Ref};
use interactive::server::Server;

type Rows = Vec<(String, String, i64)>;

/// Run `src` on `epochs`, each a list of feed lines per input, through `workers` workers, ticking
/// after each epoch. Returns, per epoch, each output's sorted rows.
fn run(workers: usize, src: &str, passes: Option<&str>, epochs: Vec<Vec<Vec<String>>>) -> Vec<BTreeMap<String, Rows>> {
    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(src));
    let passes = passes.map(|p| interactive::passes::Config::parse(p).unwrap());
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        if let Some(passes) = passes { server.set_passes(passes); }
        server.install(worker, "p", &program).expect("install");
        let mut out = Vec::new();
        for epoch in &epochs {
            for (input, rows) in epoch.iter().enumerate() {
                let shard: Vec<&str> = rows.iter().skip(worker.index()).step_by(worker.peers()).map(String::as_str).collect();
                server.feed("p", input, &shard.join("\n"), None).expect("feed");
            }
            server.tick(worker);
            let snapshots = program.root.exports.iter().map(|e| {
                let mut rows = server.snapshot(worker, &format!("p::{}", e.name)).expect("snapshot").rows();
                rows.sort();
                (e.name.clone(), rows)
            }).collect();
            out.push(snapshots);
        }
        out
    }).expect("workers");
    guards.join().into_iter().next().unwrap().unwrap()
}

fn lines(rows: &[(i64, i64, i64)]) -> Vec<String> {
    rows.iter().map(|(k, v, d)| format!("{k} val={v} diff={d}")).collect()
}

/// The panic text of parsing `src`.
fn parse_error(src: &str) -> String {
    let src = src.to_string();
    let payload = std::panic::catch_unwind(move || { interactive::parse::pipe::parse(&src); }).expect_err("the program parses");
    payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string())).expect("a text panic")
}

const LIST_MISUSE: &str = "a list of collections is the input of a reduce";

#[test]
fn list_lowers_to_merged() {
    let src = r#"
        let a = input 0 : (int, int) | arrange;
        let b = input 1 : (int, int) | arrange;
        output "o" = [a, -b] | count;
    "#;
    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(src));
    let items = &program.root.items;
    let arranges: Vec<usize> = (0..items.len()).filter(|i| matches!(&items[*i], Item::Op(Node::Arrange(_)))).collect();
    assert_eq!(arranges.len(), 2, "{items:?}");
    let reduces: Vec<&ReduceInput> = items.iter().filter_map(|i| match i { Item::Op(Node::Reduce { input, .. }) => Some(input), _ => None }).collect();
    assert_eq!(reduces.len(), 1);
    assert_eq!(*reduces[0], ReduceInput::Merged(vec![(Ref::Local(arranges[0]), 1), (Ref::Local(arranges[1]), -1)]));
}

#[test]
fn list_answers_match_concat() {
    let epochs = vec![
        vec![lines(&[(1, 5, 1), (1, 7, 2), (2, 3, 1), (3, 1, 1)]), lines(&[(1, 5, 1), (3, 9, 1), (1, 6, 1)]), lines(&[(1, 7, 1), (2, 3, 1)])],
        vec![lines(&[(1, 5, -1), (4, 4, 1)]), lines(&[(3, 9, -1), (2, 3, 1), (4, 4, 1)]), lines(&[(1, 7, -1), (3, 1, 1)])],
        vec![lines(&[(1, 7, -2), (2, 3, -1)]), lines(&[(1, 5, -1), (1, 6, 1), (2, 3, -1)]), lines(&[(4, 4, 1), (1, 6, 1)])],
    ];
    // The list, then the concatenation it denotes. A lambda inside an element must still lex.
    let cases = [
        ("[a, -b] | count", "a - b | count"),
        ("[a, -b] | min", "a - b | min"),
        ("[a, -b] | collect", "a - b | collect"),
        ("[a, -b] | reduce((k, vals) -> vals)", "a - b | reduce((k, vals) -> vals)"),
        ("[a, -b] | reduce(limit 1, (k, vals) -> vals)", "a - b | reduce(limit 1, (k, vals) -> vals)"),
        ("[-a] | count", "a | negate | count"),
        ("[a, b - c] | distinct", "a + b - c | distinct"),
        ("[a | map((k, v) -> (k, v)), -b] | distinct", "a | map((k, v) -> (k, v)) - b | distinct"),
        ("[a, -b | filter((k, v) -> v)] | distinct", "a - (b | filter((k, v) -> v)) | distinct"),
        ("[a] | distinct", "a | distinct"),
    ];
    for workers in [1, 3] {
        for (list, concat) in cases {
            let program = |expr: &str| format!("let a = input 0 : (int, int); let b = input 1 : (int, int); let c = input 2 : (int, int); output \"o\" = {expr};");
            let expected = run(workers, &program(concat), None, epochs.clone());
            assert_eq!(run(workers, &program(list), None, epochs.clone()), expected, "{list} on {workers} workers");
        }
    }
}

#[test]
fn list_in_loop_infers_shape() {
    // The list's reduce is the left operand of `+`, which pins stage precedence.
    let src = r#"
        let seed = input 0 : (int, int);
        let blocked = input 1 : (int, int);
        s: { set x = [::seed, -::blocked] | distinct + (x | map((k, v) -> (k, v))) | distinct; }
        output "o" = s::x;
    "#;
    let epochs = vec![
        vec![lines(&[(1, 1, 1), (2, 2, 1), (3, 3, 1)]), lines(&[(2, 2, 1)])],
        vec![lines(&[(4, 4, 1)]), lines(&[(1, 1, 1), (2, 2, -1)])],
    ];
    let plain = r#"
        let seed = input 0 : (int, int);
        let blocked = input 1 : (int, int);
        s: { set x = (::seed - ::blocked | distinct) + (x | map((k, v) -> (k, v))) | distinct; }
        output "o" = s::x;
    "#;
    for workers in [1, 2] {
        assert_eq!(run(workers, src, None, epochs.clone()), run(workers, &plain, None, epochs.clone()));
    }
    // No `var` declares x's shape: it comes from the list's elements.
    let src = r#"
        let seed = input 0 : (int, int);
        s: { set x = [::seed, -x] | distinct; }
        output "o" = s::x;
    "#;
    let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(src));
    interactive::passes::optimize(&mut program);
    let (_, problems) = interactive::shapes::infer(&program, &|_| None, 1);
    assert!(problems.conflicts.is_empty(), "{:?}", problems.conflicts);
}

#[test]
fn by_record_distinct_element_is_arranged() {
    let src = r#"
        let a = input 0 : (int, int);
        let b = input 1 : (int, int);
        output "o" = [a | distinct, -b] | distinct;
    "#;
    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(src));
    let items = &program.root.items;
    let mut merged = 0;
    for item in items {
        let Item::Op(Node::Reduce { input: ReduceInput::Merged(inputs), .. }) = item else { continue };
        if inputs.len() < 2 { continue }
        merged += 1;
        for (r, _) in inputs {
            let Ref::Local(i) = r else { panic!("{r:?}") };
            assert!(matches!(&items[*i], Item::Op(Node::Arrange(_) | Node::Reduce { .. })), "{:?}", items[*i]);
        }
    }
    assert_eq!(merged, 1);
    let epochs = vec![
        vec![lines(&[(1, 5, 2), (2, 3, 1), (3, 1, 1)]), lines(&[(1, 5, 1), (3, 9, 1)])],
        vec![lines(&[(1, 5, -1), (4, 4, 1)]), lines(&[(2, 3, 1), (4, 4, 1)])],
        vec![lines(&[(1, 5, -1), (2, 3, -1)]), lines(&[(1, 5, -1), (2, 3, -1)])],
    ];
    let program = |expr: &str| format!("let a = input 0 : (int, int); let b = input 1 : (int, int); output \"o\" = {expr};");
    for workers in [1, 3] {
        assert_eq!(
            run(workers, &program("[a | distinct, -b] | distinct"), None, epochs.clone()),
            run(workers, &program("(a | distinct) - b | distinct"), None, epochs.clone()),
        );
    }
}

#[test]
fn list_misuse_is_rejected() {
    for src in [
        "let a = input 0 : (int, int); let b = input 1 : (int, int); let p = [a, -b];",
        "let a = input 0 : (int, int); output \"o\" = [a];",
        "let a = input 0 : (int, int); let b = input 1 : (int, int); output \"o\" = [a, -b] | map((k, v) -> (k, v));",
        "let a = input 0 : (int, int); let b = input 1 : (int, int); output \"o\" = [a] + b;",
        "let a = input 0 : (int, int); let b = input 1 : (int, int); output \"o\" = a + [b];",
    ] {
        assert_eq!(parse_error(src), LIST_MISUSE, "{src}");
    }
}
