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
    let program = |expr: &str| format!("let a = input 0 : (int, int); let b = input 1 : (int, int); let c = input 2 : (int, int); output \"o\" = {expr};");
    for workers in [1, 3] {
        for passes in [None, Some("none")] {
            for (list, concat) in cases {
                let expected = run(workers, &program(concat), passes, epochs.clone());
                assert_eq!(run(workers, &program(list), passes, epochs.clone()), expected, "{list} on {workers} workers, passes {passes:?}");
            }
        }
    }
    // `-b` exceeds `a` at keys 8 and 9, so their counts are negative. Key 8's value 1 nets to -2
    // and then to 0, and key 9 holds only `b`.
    let negative = vec![
        vec![lines(&[(8, 1, 1), (8, 2, 1), (7, 1, 1)]), lines(&[(8, 1, 3), (9, 4, 2), (7, 1, 1)]), vec![]],
        vec![lines(&[(8, 1, 2), (9, 4, 1)]), lines(&[(8, 2, 2), (7, 1, -1)]), vec![]],
        vec![lines(&[(8, 2, -1)]), lines(&[(9, 4, -3), (8, 1, 1)]), vec![]],
    ];
    for workers in [1, 3] {
        for passes in [None, Some("none")] {
            for (list, concat) in [("[a, -b] | count", "a - b | count"), ("[a, -b] | distinct", "a - b | distinct")] {
                let expected = run(workers, &program(concat), passes, negative.clone());
                assert_eq!(run(workers, &program(list), passes, negative.clone()), expected, "{list} on {workers} workers, passes {passes:?}");
            }
        }
    }
}

/// A merged reduce in a loop, against the same program written as a concatenation, with every pass
/// off. Input 0 holds blocked nodes, input 1 roots, and the loop reaches `n -> n+1` and `n -> n+3`
/// below 200 avoiding blocked nodes. The reduce reads an arrangement built in the loop and one
/// entered from outside, once added and once negated.
#[test]
fn merged_loop_matches_concat() {
    // `cand` and `blocked` are arranged, and a key of `reach` survives when `cand` holds it and
    // `blocked` does not.
    let added = |reach: &str| format!(r#"
        let blocked = input 0 : ((int), ()) | reduce((_, vals) -> ((1), 1) enlist);
        let roots = input 1 : ((int), ()) | distinct;
        r: {{
            let step = (reach | map(((n), ()) -> (((n, 1) add), ()))) + (reach | map(((n), ()) -> (((n, 3) add), ()))) | filter(((m), ()) -> (m, 200) lt);
            let cand = roots + step | reduce((_, vals) -> ((0), 1) enlist);
            set reach = {reach} | reduce((_, vals) -> let h = vals head in (((vals len, 1) eq, (h.0.0, 0) eq) mul, ((), 1)) enlist filter);
        }}
        output "reach" = r::reach;
        output "count" = r::reach | map(((n), ()) -> ((), ())) | count;
    "#);
    let negated = |reach: &str| format!(r#"
        let blocked = input 0 : ((int), ()) | distinct;
        let roots = input 1 : ((int), ()) | distinct;
        r: {{
            let step = (reach | map(((n), ()) -> (((n, 1) add), ()))) + (reach | map(((n), ()) -> (((n, 3) add), ()))) | filter(((m), ()) -> (m, 200) lt);
            let cand = roots + step | distinct;
            set reach = {reach} | reduce((_, vals) -> (((0, vals) fold ((acc, (_, d)) -> (acc, d) add), 0) gt, ((), 1)) enlist filter);
        }}
        output "reach" = r::reach;
        output "count" = r::reach | map(((n), ()) -> ((), ())) | count;
    "#);
    // 25 ticks. Each blocks six nodes and unblocks two blocked earlier. Roots 0 and 50 come first,
    // root 50 is retracted at tick 10 and root 7 added at tick 15.
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move |bound: u64| { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state % bound };
    let mut blocked: Vec<u64> = Vec::new();
    let mut epochs = Vec::new();
    for tick in 0..25 {
        let mut feed = Vec::new();
        for _ in 0..6 {
            let n = next(200);
            blocked.push(n);
            feed.push(format!("{n}"));
        }
        for _ in 0..2 {
            let n = blocked.swap_remove(usize::try_from(next(u64::try_from(blocked.len()).unwrap())).unwrap());
            feed.push(format!("{n} diff=-1"));
        }
        let roots = match tick {
            0 => vec!["0".to_string(), "50".to_string()],
            10 => vec!["50 diff=-1".to_string()],
            15 => vec!["7".to_string()],
            _ => vec![],
        };
        epochs.push(vec![feed, roots]);
    }
    for workers in [1, 4] {
        for (program, list, concat) in [(added as fn(&str) -> String, "[cand, blocked]", "cand + blocked"), (negated, "[cand, -blocked]", "cand - blocked")] {
            let expected = run(workers, &program(concat), Some("none"), epochs.clone());
            assert!(expected.iter().any(|epoch| !epoch["reach"].is_empty()), "the loop reaches nodes");
            assert!(expected.windows(2).filter(|w| w[0]["reach"] != w[1]["reach"]).count() > 10, "the reached nodes change");
            let names = operator_names(&program(list));
            assert_eq!(names.iter().filter(|n| *n == "CorgiReduceMany").count(), 1, "{list}: {names:?}");
            assert_eq!(run(workers, &program(list), Some("none"), epochs.clone()), expected, "{list} on {workers} workers");
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

/// The names of the operators installing `src` builds on one worker.
fn operator_names(src: &str) -> Vec<String> {
    use std::cell::RefCell;
    use std::rc::Rc;
    use timely::logging::{TimelyEvent, TimelyEventBuilder};
    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(src));
    timely::execute_directly(move |worker| {
        let names: Rc<RefCell<Vec<String>>> = Rc::default();
        let sink = Rc::clone(&names);
        worker.log_register().unwrap().insert::<TimelyEventBuilder, _>("timely", move |_, data| {
            if let Some(data) = data {
                for (_, event) in data.iter() {
                    if let TimelyEvent::Operates(op) = event { sink.borrow_mut().push(op.name.clone()); }
                }
            }
        });
        let mut server = Server::new();
        server.install(worker, "p", &program).expect("install");
        server.tick(worker);
        worker.log_register().unwrap().remove("timely");
        let names = names.borrow().clone();
        names
    })
}

#[test]
fn merged_reduce_reads_input_arrangements() {
    let names = operator_names(r#"
        let a = input 0 : (int, int) | arrange;
        let b = input 1 : (int, int) | arrange;
        output "o" = [a, -b] | count;
    "#);
    let count = |name: &str| names.iter().filter(|n| *n == name).count();
    assert_eq!(count("CorgiReduceMany"), 1, "{names:?}");
    assert_eq!(count("CorgiReduce"), 0, "{names:?}");
    // The arrangements of `a` and `b`, and no arrangement of their concatenation.
    assert_eq!(count("CorgiArrange"), 2, "{names:?}");
    assert_eq!(count("Concatenate"), 0, "{names:?}");
}
