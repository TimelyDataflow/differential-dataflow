//! The corgi backend's correctness gate: each canonical `.ddp` program must evaluate
//! identically through the corgi backend and the reference vec backend.
//!
//! Every evaluation goes through the server (`server::evaluate`): install, feed, tick,
//! snapshot — the same path a live install takes.

use interactive::ir::Value;
use interactive::server::{evaluate, RenderBackend};
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Value, Value)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Per-program inputs (arity matches each `.ddp`'s `input N` usage).
fn inputs_for(prog: &str) -> Vec<Vec<(Value, Value)>> {
    let edges = rows(&[&[1, 2], &[2, 3], &[3, 4], &[5, 6], &[4, 2]]);
    match prog {
        "reach" => vec![edges, rows(&[&[1]])],
        "scc" => vec![edges.clone()],
        // kcore: the same edges. Symmetrized they leave a 2-core of {2, 3, 4} and peel
        // 1, 5 and 6 away, so the fixpoint has to retract as well as accumulate.
        "kcore" => vec![edges],
        // stable: edges (l_node, l_pref, r_node, r_pref)
        "stable" => vec![rows(&[&[1, 1, 10, 1], &[1, 2, 11, 1], &[2, 1, 10, 2], &[2, 2, 11, 2]])],
        "unnest" => vec![rows(&[&[1, 2], &[3, 4]])],
        "adt" => vec![edges.clone()],
        // ast: pairs; small and non-negative, per the program's stated input contract.
        "ast" => vec![edges],
        "binders" => vec![rows(&[&[1, 2], &[3, 4]])],
        // join_fallback: two keyed relations with overlapping keys (incl. a key with fanout).
        "join_fallback" => vec![
            rows(&[&[1, 10], &[2, 20], &[2, 21], &[3, 30]]),
            rows(&[&[1, 5], &[2, 6], &[4, 7]]),
        ],
        // scalar_ops: (key, a, b) triples; a values straddle the `> 2` and `= -5` tests.
        "scalar_ops" => vec![rows(&[&[1, 1, 9], &[1, 4, 8], &[2, 3, 7], &[3, -5, 6], &[3, 2, 5]])],
        "sum_ops" => vec![rows(&[&[1, 10], &[2, 20], &[2, 21]])],
        // empty_batch: only key 1 has the two values the filter keeps, so at several workers at
        // least one gets a batch that arrives non-empty and leaves empty.
        "empty_batch" => vec![rows(&[
            &[1, 10], &[1, 20],
            &[2, 1], &[3, 1], &[4, 1], &[5, 1], &[6, 1], &[7, 1], &[8, 1], &[9, 1],
        ])],
        // sum_skew: any keyed pairs — the skew is in the program, not the data.
        "sum_skew" => vec![rows(&[&[1, 10], &[2, 20], &[2, 21], &[3, 30]])],
        "case_ops" => vec![rows(&[&[1, 10], &[2, 20], &[3, 14], &[3, 30]])],
        "if_literal" => vec![rows(&[&[1, 10], &[1, 20], &[2, 30]])],
        // pair_keys: composite keys with overlap, fanout, and one-sided keys on both sides.
        "pair_keys" => vec![
            rows(&[&[1, 1, 10], &[1, 2, 20], &[2, 1, 30], &[2, 1, 31], &[9, 9, 90]]),
            rows(&[&[1, 1, 5], &[2, 1, 6], &[3, 3, 7]]),
        ],
        // spread_values: keys with one and with several values.
        "spread_values" => vec![rows(&[&[1, 10], &[1, 20], &[2, 30]])],
        "signed_min" => vec![rows(&[
            &[1, 0],
            &[1, -1],
            &[1, -3],
            &[2, 5],
            &[2, -2],
        ])],
        // f64_math: (key, a, b) with x = a / 4, y = b / 2: negatives, zero, and a key with
        // two rows; then (key, n) integer exponents for the join, one key unmatched.
        "registered" => vec![rows(&[&[1], &[3], &[-4]])],
        "f64_math" => vec![
            rows(&[&[1, 10, 1], &[2, -6, 3], &[3, 0, -1], &[4, 9, 0], &[5, -1, -4], &[5, 7, 5]]),
            rows(&[&[1, 2], &[2, 3], &[3, -1], &[5, 0]]),
        ],
        // tour: edges (with a cycle and a chord) + roots.
        "tour" => vec![
            rows(&[&[1, 2], &[2, 3], &[3, 1], &[3, 4], &[5, 2]]),
            rows(&[&[1], &[5]]),
        ],
        other => panic!("no inputs configured for {other}"),
    }
}

/// Evaluate `prog` through both backends and assert the outputs match.
fn assert_backends_agree(prog: &str) {
    // Fixtures pinning individual lowerings live with the gate (tests/programs); the
    // algorithm programs double as examples and stay in examples/programs.
    let fixture = format!("{}/tests/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"));
    let path = if std::path::Path::new(&fixture).exists() {
        fixture
    } else {
        format!("{}/examples/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"))
    };
    register_test_functions();
    let src = interactive::load_program(&path);
    let mut tree = lower::lower_tree(parse::pipe::parse(&src));
    tree.optimize();
    let inputs = inputs_for(prog);
    let want = evaluate(RenderBackend::Vec, timely::Config::process(1), &tree, &inputs);
    // At every worker count: the exchange places each key on one worker and every operator is
    // key-local from there, so the answer must not depend on how many workers ran it. 3 is in the
    // list on purpose — it is not a power of two, so it takes the modulus path rather than the
    // mask, and it cannot divide these inputs evenly.
    for workers in [1, 2, 3, 4] {
        assert_eq!(
            evaluate(RenderBackend::Corgi, timely::Config::process(workers), &tree, &inputs),
            want,
            "corgi backend at {workers} worker(s) disagrees with the vec backend on {prog}",
        );
    }
    // The same programs again with serializing channels, so every exchanged container makes the
    // round trip through the wire format. This is the multi-process path: `Config::process` above
    // hands containers between threads as typed values and never encodes a byte.
    assert_eq!(
        evaluate(RenderBackend::Corgi, serializing(3), &tree, &inputs),
        want,
        "corgi backend over serializing channels disagrees with the vec backend on {prog}",
    );
}

/// `n` worker threads whose exchange channels serialize — the wire format in the loop.
fn serializing(n: usize) -> timely::Config {
    timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(n),
        worker: timely::WorkerConfig::default(),
    }
}

#[test] fn reach() { assert_backends_agree("reach"); }
#[test] fn scc() { assert_backends_agree("scc"); }
#[test] fn kcore() { assert_backends_agree("kcore"); }
#[test] fn stable() { assert_backends_agree("stable"); }
#[test] fn unnest() { assert_backends_agree("unnest"); }
#[test] fn adt() { assert_backends_agree("adt"); }
#[test] fn ast() { assert_backends_agree("ast"); }
#[test] fn binders() { assert_backends_agree("binders"); }
#[test] fn join_fallback() { assert_backends_agree("join_fallback"); }
#[test] fn scalar_ops() { assert_backends_agree("scalar_ops"); }
#[test] fn sum_ops() { assert_backends_agree("sum_ops"); }
#[test] fn empty_batch() { assert_backends_agree("empty_batch"); }
#[test] fn sum_skew() { assert_backends_agree("sum_skew"); }
#[test] fn case_ops() { assert_backends_agree("case_ops"); }
#[test] fn if_literal() { assert_backends_agree("if_literal"); }
#[test] fn tour() { assert_backends_agree("tour"); }
#[test] fn pair_keys() { assert_backends_agree("pair_keys"); }
#[test] fn signed_min() { assert_backends_agree("signed_min"); }
#[test] fn spread_values() { assert_backends_agree("spread_values"); }
#[test] fn f64_math() { assert_backends_agree("f64_math"); }
#[test] fn registered() {
    assert_backends_agree("registered");
    // And the program does what it says: the refinement from 1 and 3 reaches every
    // number from 1 to 39 (grow stops at 20, so its last children are 38 and 39), and
    // -4 has no children.
    register_test_functions();
    let src = interactive::load_program(&format!("{}/tests/programs/registered.ddp", env!("CARGO_MANIFEST_DIR")));
    let tree = lower::lower_tree(parse::pipe::parse(&src));
    let out = evaluate(RenderBackend::Corgi, timely::Config::process(2), &tree, &inputs_for("registered"));
    let mut cells: Vec<i64> = out["cells"].iter().map(|((k, _), _)| match k { Value::Tuple(f) => f[0].as_int(), _ => panic!() }).collect();
    cells.sort();
    let mut want: Vec<i64> = (1..40).collect();
    want.insert(0, -4);
    assert_eq!(cells, want);
    assert_eq!(out["joined"].len(), 40);
}

/// The functions `registered.ddp` calls. Registering again replaces (here with the same bodies), so every
/// test may do it.
fn register_test_functions() {
    use corgi::Shape;
    use interactive::ir::{register, Function};
    let int = || Shape::Int;
    let float = || Shape::Sum(vec![Shape::Int]);
    // A cell's children: two, while the cell is small and positive; none after.
    register(Function {
        name: "grow".into(),
        args: vec![int()],
        result: Shape::List(Box::new(Shape::Prod(vec![int(), int()]))),
        body: Box::new(|a| {
            let n = a[0].as_int();
            let kids = if (1..20).contains(&n) { vec![2 * n, 2 * n + 1] } else { vec![] };
            Value::List(kids.into_iter().map(|k| Value::Tuple(vec![Value::Int(k), Value::Int(n)])).collect())
        }),
    });
    // A float from a pair of ints.
    register(Function {
        name: "blend".into(),
        args: vec![Shape::Prod(vec![int(), int()])],
        result: float(),
        body: Box::new(|a| {
            let Value::Tuple(p) = &a[0] else { panic!("blend expects a pair") };
            Value::f64_value((p[0].as_int() as f64).sqrt() - p[1].as_int() as f64 / 3.0)
        }),
    });
    // A nested shape: (n * 3, [divisors of n under 5]).
    register(Function {
        name: "describe".into(),
        args: vec![int()],
        result: Shape::Prod(vec![Shape::Prod(vec![int()]), Shape::List(Box::new(int()))]),
        body: Box::new(|a| {
            let n = a[0].as_int();
            let divisors = (1..5).filter(|d| n % d == 0).map(Value::Int).collect();
            Value::Tuple(vec![Value::Tuple(vec![Value::Int(3 * n)]), Value::List(divisors)])
        }),
    });
    // No arguments: the call passes a `Unit` column, so it still runs once per row.
    register(Function {
        name: "seven".into(),
        args: vec![],
        result: int(),
        body: Box::new(|_| Value::Int(7)),
    });
    // A columnar body: the corgi backend runs `Double` on whole columns, the vec backend the row
    // body; the gate checks they agree.
    register(Function {
        name: "double".into(),
        args: vec![int()],
        result: int(),
        body: Box::new(|a| Value::Int(2 * a[0].as_int())),
    });
    struct Double(Shape, Shape);
    impl corgi::HostKernel for Double {
        fn name(&self) -> &str { "double" }
        fn input(&self) -> &Shape { &self.0 }
        fn output(&self) -> &Shape { &self.1 }
        fn eval(&self, input: corgi::Value) -> Result<corgi::Value, String> {
            let args = input.into_prod("double")?;
            let xs = args[0].as_i64("double")?;
            Ok(corgi::Value::i64(xs.iter().map(|&x| 2 * x).collect()))
        }
    }
    interactive::ir::register_kernel("double", std::sync::Arc::new(Double(Shape::Prod(vec![int()]), int())));
}

/// A filter predicate must be an `Int`: both backends reject a tuple rather than one of them
/// keeping nothing.
#[test]
fn filter_requires_an_int_predicate() {
    let mut tree = lower::lower_tree(parse::pipe::parse(r#"export "result" = input 0 | filter($0);"#));
    tree.optimize();
    let inputs = vec![rows(&[&[1, 10], &[0, 20]])];
    for backend in [RenderBackend::Vec, RenderBackend::Corgi] {
        let (tree, inputs) = (tree.clone(), inputs.clone());
        let result = std::panic::catch_unwind(move || evaluate(backend, timely::Config::process(1), &tree, &inputs));
        assert!(result.is_err(), "{backend:?} accepted a tuple filter predicate");
    }
}

/// A call whose argument does not have the declared shape is rejected by both backends: corgi
/// when it types the program, the row backend when it makes the call.
#[test]
fn call_argument_shapes_are_checked_by_both_backends() {
    register_test_functions();
    // `blend` takes a pair; pass it an Int.
    let mut tree = lower::lower_tree(parse::pipe::parse(r#"export "result" = input 0 | map($0 ; blend($0[0]));"#));
    tree.optimize();
    let inputs = vec![rows(&[&[1], &[2]])];
    for backend in [RenderBackend::Vec, RenderBackend::Corgi] {
        let (tree, inputs) = (tree.clone(), inputs.clone());
        let result = std::panic::catch_unwind(move || evaluate(backend, timely::Config::process(1), &tree, &inputs));
        assert!(result.is_err(), "{backend:?} ran a call with a mis-shaped argument");
    }
}

/// Registering a name again replaces its kernel too: the row adapter for the old body, or a
/// columnar kernel registered for it, no longer runs.
#[test]
fn reregistering_a_function_drops_its_old_kernel() {
    use corgi::Shape;
    use interactive::ir::{kernel_of, register, register_kernel, Function};
    let one = |k: i64| Function { name: "reregistered".into(), args: vec![Shape::Int], result: Shape::Int, body: Box::new(move |_| Value::Int(k)) };
    register(one(1));
    let first = kernel_of("reregistered").unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &kernel_of("reregistered").unwrap()), "one kernel per registration");
    register(one(2));
    let second = kernel_of("reregistered").unwrap();
    assert!(!std::sync::Arc::ptr_eq(&first, &second), "re-registration kept the old kernel");
    register_kernel("reregistered", first);
    register(one(3));
    assert!(!std::sync::Arc::ptr_eq(&second, &kernel_of("reregistered").unwrap()));
    // A keyword can never be called, so it cannot be registered.
    assert!(std::panic::catch_unwind(|| register(Function { name: "min".into(), args: vec![], result: Shape::Int, body: Box::new(|_| Value::Int(0)) })).is_err());
}
