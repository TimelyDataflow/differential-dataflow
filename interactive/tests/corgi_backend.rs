//! The corgi backend's correctness gate: each canonical `.ddp` program must evaluate to its
//! recorded answer (`<prog>.expected`, beside it), at every worker count and over serializing
//! channels. The answers were recorded from the row backend while it lived, or by hand where
//! that backend could not run the program.
//!
//! Every evaluation goes through the server (`server::evaluate`): install, feed, tick,
//! snapshot — the same path a live install takes.

use interactive::ir::Value;
use interactive::server::evaluate;
use interactive::{lower, parse};

/// Rows of integer tuples and unit values, as feed lines (`1,2`).
fn rows(rs: &[&[i64]]) -> Vec<String> {
    rs.iter().map(|f| f.iter().map(i64::to_string).collect::<Vec<_>>().join(",")).collect()
}

/// Per-program inputs (arity matches each `.ddp`'s `input N` usage).
fn inputs_for(prog: &str) -> Vec<Vec<String>> {
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
        // ast: any pairs.
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
        // reduce_term: keys with one value, several, and a repeated one.
        "reduce_term" => vec![rows(&[&[1, 10], &[2, 20], &[2, -21], &[2, 20], &[3, 30], &[3, 5]])],
        // sum_skew: any keyed pairs — the skew is in the program, not the data.
        "sum_skew" => vec![rows(&[&[1, 10], &[2, 20], &[2, 21], &[3, 30]])],
        "case_ops" => vec![rows(&[&[1, 10], &[2, 20], &[3, 14], &[3, 30]])],
        // pair_keys: composite keys with overlap, fanout, and one-sided keys on both sides.
        "pair_keys" => vec![
            rows(&[&[1, 1, 10], &[1, 2, 20], &[2, 1, 30], &[2, 1, 31], &[9, 9, 90]]),
            rows(&[&[1, 1, 5], &[2, 1, 6], &[3, 3, 7]]),
        ],
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

/// Evaluate `prog` and assert the output is its recorded answer.
fn assert_matches_expected(prog: &str) {
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
    let recorded = path.replace(".ddp", ".expected");
    let want = std::fs::read_to_string(&recorded)
        .unwrap_or_else(|e| panic!("{prog} has no recorded answer at {recorded}: {e}"))
        .trim_end()
        .to_string();
    // The recorded answer, up to the last bits of floats: libm's functions differ there across
    // platforms, and the answers were recorded on one.
    let got = render(&evaluate(timely::Config::process(1), &tree, &inputs));
    assert!(same_answer(&got, &want), "{prog} disagrees with its recorded answer\n got: {got}\nwant: {want}");
    // At every worker count, exactly: the exchange places each key on one worker and every
    // operator is key-local from there, so the answer must not depend on how many workers ran it.
    // 3 is in the list on purpose — it is not a power of two, so it takes the modulus path rather
    // than the mask, and it cannot divide these inputs evenly.
    for workers in [2, 3, 4] {
        assert_eq!(
            render(&evaluate(timely::Config::process(workers), &tree, &inputs)),
            got,
            "{prog} at {workers} worker(s) disagrees with one worker",
        );
    }
    // The same programs again with serializing channels, so every exchanged container makes the
    // round trip through the wire format. This is the multi-process path: `Config::process` above
    // hands containers between threads as typed values and never encodes a byte.
    assert_eq!(
        render(&evaluate(serializing(3), &tree, &inputs)),
        got,
        "{prog} over serializing channels disagrees with one worker",
    );
}

/// Whether rendered answers agree, each float to within four units in the last place.
fn same_answer(got: &str, want: &str) -> bool {
    /// A line's text between floats, and its floats.
    fn split(line: &str) -> (Vec<&str>, Vec<f64>) {
        let (mut text, mut floats, mut rest) = (Vec::new(), Vec::new(), line);
        while let Some(at) = rest.find("Float(") {
            let end = at + rest[at..].find(')').expect("a closed Float");
            text.push(&rest[..at]);
            floats.push(rest[at + "Float(".len()..end].parse::<f64>().expect("a float"));
            rest = &rest[end + 1..];
        }
        text.push(rest);
        (text, floats)
    }
    let close = |a: f64, b: f64| a == b || (a.is_nan() && b.is_nan()) || (a.signum() == b.signum() && (a.to_bits() as i64 - b.to_bits() as i64).abs() <= 4);
    got.lines().count() == want.lines().count() && got.lines().zip(want.lines()).all(|(g, w)| {
        let ((gt, gf), (wt, wf)) = (split(g), split(w));
        gt == wt && gf.len() == wf.len() && gf.iter().zip(&wf).all(|(&a, &b)| close(a, b))
    })
}

/// A program's output as text, one row per line: the export's name, then the row and its diff.
fn render(out: &std::collections::BTreeMap<String, interactive::corgi::container::Updates>) -> String {
    out.iter()
        .flat_map(|(name, updates)| updates.clone().into_rows().into_iter().map(move |(k, v, d)| format!("{name} {:?}", ((k, v), d))))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `n` worker threads whose exchange channels serialize — the wire format in the loop.
fn serializing(n: usize) -> timely::Config {
    timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(n),
        worker: timely::WorkerConfig::default(),
    }
}

#[test] fn reach() { assert_matches_expected("reach"); }
#[test] fn scc() { assert_matches_expected("scc"); }
#[test] fn kcore() { assert_matches_expected("kcore"); }
#[test] fn stable() { assert_matches_expected("stable"); }
#[test] fn unnest() { assert_matches_expected("unnest"); }
#[test] fn adt() { assert_matches_expected("adt"); }
#[test] fn ast() { assert_matches_expected("ast"); }
#[test] fn binders() { assert_matches_expected("binders"); }
#[test] fn join_fallback() { assert_matches_expected("join_fallback"); }
#[test] fn scalar_ops() { assert_matches_expected("scalar_ops"); }
#[test] fn sum_ops() { assert_matches_expected("sum_ops"); }
#[test] fn sum_skew() { assert_matches_expected("sum_skew"); }
#[test] fn case_ops() { assert_matches_expected("case_ops"); }
#[test] fn tour() { assert_matches_expected("tour"); }
#[test] fn pair_keys() { assert_matches_expected("pair_keys"); }
#[test] fn signed_min() { assert_matches_expected("signed_min"); }
#[test] fn f64_math() { assert_matches_expected("f64_math"); }
#[test] fn reduce_term() { assert_matches_expected("reduce_term"); }
#[test] fn registered() {
    assert_matches_expected("registered");
    // And the program does what it says: the refinement from 1 and 3 reaches every
    // number from 1 to 39 (grow stops at 20, so its last children are 38 and 39), and
    // -4 has no children.
    register_test_functions();
    let src = interactive::load_program(&format!("{}/tests/programs/registered.ddp", env!("CARGO_MANIFEST_DIR")));
    let tree = lower::lower_tree(parse::pipe::parse(&src));
    let out = evaluate(timely::Config::process(2), &tree, &inputs_for("registered"));
    let mut cells: Vec<i64> = out["cells"].clone().into_rows().iter().map(|(k, _, _)| k.as_int()).collect();
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
    let float = || Shape::Float;
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
    // A columnar body: `Double` runs on whole columns, and the row body is its reference.
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

/// A filter predicate must be an `Int`: a tuple is rejected rather than keeping nothing.
#[test]
fn filter_requires_an_int_predicate() {
    let mut tree = lower::lower_tree(parse::pipe::parse(r#"export "result" = input 0 : ((int, int) ; ()) | filter($0);"#));
    tree.optimize();
    let inputs = vec![rows(&[&[1, 10], &[0, 20]])];
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || evaluate(timely::Config::process(1), &tree, &inputs)));
    assert!(result.is_err(), "accepted a tuple filter predicate");
}

/// A call whose argument does not have the declared shape is rejected when the program is typed.
#[test]
fn call_argument_shapes_are_checked() {
    register_test_functions();
    // `blend` takes a pair; pass it an Int.
    let mut tree = lower::lower_tree(parse::pipe::parse(r#"export "result" = input 0 : ((int) ; ()) | map($0 ; blend($0[0]));"#));
    tree.optimize();
    let inputs = vec![rows(&[&[1], &[2]])];
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || evaluate(timely::Config::process(1), &tree, &inputs)));
    assert!(result.is_err(), "ran a call with a mis-shaped argument");
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
