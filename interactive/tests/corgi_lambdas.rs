//! Slots written as corgi lambdas, `pat -> body`, compute what the same slots written as terms do,
//! on both backends.

use interactive::ir::Value;
use interactive::server::{evaluate, RenderBackend};
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Value, Value)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Evaluate both programs on both backends and assert all four agree, and have output.
fn assert_same(terms: &str, lambdas: &str, inputs: &[Vec<(Value, Value)>]) {
    let tree = |src: &str| {
        let mut tree = lower::lower_tree(parse::pipe::parse(src));
        tree.optimize();
        tree
    };
    let (terms, lambdas) = (tree(terms), tree(lambdas));
    let want = evaluate(RenderBackend::Vec, timely::Config::process(1), &terms, inputs);
    assert!(want.values().all(|rows| !rows.is_empty()), "every export has rows: {want:?}");
    assert_eq!(evaluate(RenderBackend::Vec, timely::Config::process(1), &lambdas, inputs), want, "vec backend");
    for workers in [1, 3] {
        let config = || timely::Config::process(workers);
        assert_eq!(evaluate(RenderBackend::Corgi, config(), &terms, inputs), want, "corgi backend, terms, {workers} workers");
        assert_eq!(evaluate(RenderBackend::Corgi, config(), &lambdas, inputs), want, "corgi backend, lambdas, {workers} workers");
    }
}

fn edges() -> Vec<(Value, Value)> {
    rows(&[&[1, 2], &[2, 3], &[3, 4], &[5, 6], &[4, 2], &[6, 5]])
}

#[test]
fn reach() {
    let terms = r#"
        let edges = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        let roots = input 1 : ((int) ; ()) | key($0[0] ;);
        reach: {
            let proposals = reach | join(edges, ($2 ;));
            var reach = roots + proposals | distinct;
        }
        export "result" = reach::reach;
    "#;
    let lambdas = r#"
        let edges = input 0 : ((int, int) ; ()) | key(((src, dst), ()) -> ((src), (dst)));
        let roots = input 1 : ((int) ; ()) | key(((n), ()) -> ((n), ()));
        reach: {
            let proposals = reach | join(edges, (_, _, dst) -> (dst, ()));
            var reach = roots + proposals | distinct;
        }
        export "result" = reach::reach;
    "#;
    assert_same(terms, lambdas, &[edges(), rows(&[&[1]])]);
}

#[test]
fn scc() {
    let terms = &interactive::load_program(&format!("{}/examples/programs/scc.ddp", env!("CARGO_MANIFEST_DIR")));
    let lambdas = r#"
        let edges = input 0 : ((int, int) ; ()) | key(((src, dst), ()) -> ((src), (dst)));
        let trans = edges | key((src, dst) -> (dst, src));

        outer: {
            let scc = edges + trim;

            fwd: {
                let nodes = edges | key((_, dst) -> (dst, dst)) | enter_at((_, (n)) -> n);
                let labels = proposals + nodes | min;
                var proposals = labels | join(scc, (_, label, dst) -> (dst, label));
            }

            let trim_fwd = edges
                | join(fwd::labels, ((src), (dst), (l)) -> ((dst), (src, l)))
                | join(fwd::labels, (dst, (src, l), (m)) -> (dst, (src, l, m)))
                | filter((_, (_, l, m)) -> (l, m) eq)
                | key((dst, (src, _, _)) -> (dst, (src)));

            bwd: {
                let nodes = trans | key((_, dst) -> (dst, dst)) | enter_at((_, (n)) -> n);
                let labels = proposals + nodes | min;
                var proposals = labels | join(trim_fwd, (_, label, dst) -> (dst, label));
            }

            let trim_bwd = trans
                | join(bwd::labels, ((src), (dst), (l)) -> ((dst), (src, l)))
                | join(bwd::labels, (dst, (src, l), (m)) -> (dst, (src, l, m)))
                | filter((_, (_, l, m)) -> (l, m) eq)
                | key((dst, (src, _, _)) -> (dst, (src)));

            var trim = trim_bwd - edges;
        }

        export "result" = outer::scc | map(_ -> ((), ())) | arrange | inspect(total);
    "#;
    assert_same(terms, lambdas, &[edges()]);
}

/// The slots that take one term: `value`, `filter`, `flatmap`, `enter_at`.
#[test]
fn term_slots() {
    let terms = r#"
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        export "value" = pairs | value($1[0] * 10 + $0[0]);
        export "filter" = pairs | filter($0[0] < $1[0]);
        export "flatmap" = pairs | flatmap(list($0[0], $1[0], $0[0] + $1[0]));
        stepped: {
            let delayed = pairs | enter_at($1[0]);
            var counted = delayed | count;
        }
        export "enter_at" = stepped::counted;
    "#;
    let lambdas = r#"
        let pairs = input 0 : ((int, int) ; ()) | key(((a, b), ()) -> ((a), (b)));
        export "value" = pairs | value(((a), (b)) -> ((b, 10) mul, a) add);
        export "filter" = pairs | filter(((a), (b)) -> (a, b) lt);
        export "flatmap" = pairs | flatmap(((a), (b)) -> ((a enlist, b enlist) append, (a, b) add enlist) append);
        stepped: {
            let delayed = pairs | enter_at((_, (b)) -> b);
            var counted = delayed | count;
        }
        export "enter_at" = stepped::counted;
    "#;
    assert_same(terms, lambdas, &[edges()]);
}

/// A lambda whose pattern does not fit its slot's rows, or whose body does not parse, is refused
/// at install with corgi's message.
#[test]
fn mismatched_lambdas_are_refused_at_install() {
    timely::execute_directly(|worker| {
        for (src, says) in [
            (r#"export "a" = input 0 : ((int, int) ; ()) | key(((s), ()) -> ((s), ()));"#, "takes apart a product of 1 field"),
            (r#"export "a" = input 0 : ((int, int) ; ()) | filter(((s, d), ()) -> (s, d));"#, "not an integer"),
            (r#"export "a" = input 0 : ((int, int) ; ()) | key(((s, d), ()) -> (s, d, ) add);"#, "expected an expression"),
        ] {
            let mut program = lower::lower_tree(parse::pipe::parse(src));
            program.optimize();
            let error = interactive::server::Server::with_backend(RenderBackend::Corgi).install(worker, "p", &program).unwrap_err();
            assert!(error.contains(says), "{src}: {error}");
        }
    });
}

/// Words a lambda `call`s: registered functions, whose input is their argument or the tuple of
/// their arguments, and the Float functions.
#[test]
fn calls() {
    use corgi::Shape;
    use interactive::ir::{register, Function};
    register(Function { name: "triple".into(), args: vec![Shape::Int], result: Shape::Int, body: Box::new(|a| Value::Int(3 * a[0].as_int())) });
    register(Function { name: "plus".into(), args: vec![Shape::Int, Shape::Int], result: Shape::Int, body: Box::new(|a| Value::Int(a[0].as_int() + a[1].as_int())) });
    register(Function { name: "eleven".into(), args: vec![], result: Shape::Int, body: Box::new(|_| Value::Int(11)) });
    let terms = r#"
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        export "one" = pairs | value(triple($1[0]));
        export "two" = pairs | value(plus($0[0], $1[0]));
        export "none" = pairs | value(eleven() + $0[0]);
        export "floats" = pairs | value(tuple(fint(fsqrt(float($1[0]))), fint(fpowi(float($0[0]), 3)), flt(float($0[0]), float($1[0]))));
    "#;
    let lambdas = r#"
        let pairs = input 0 : ((int, int) ; ()) | key(((a, b), ()) -> ((a), (b)));
        export "one" = pairs | value((_, (b)) -> b call triple);
        export "two" = pairs | value(((a), (b)) -> (a, b) call plus);
        export "none" = pairs | value(((a), _) -> (() call eleven, a) add);
        export "floats" = pairs | value(((a), (b)) -> (b to_float call fsqrt call fint, (a to_float, 3) call fpowi call fint, (a to_float, b to_float) call flt));
    "#;
    assert_same(terms, lambdas, &[edges()]);
}

/// A program's `type`s, and the built-in `Option`, are enums its lambdas name. A lambda builds a
/// value a term takes apart, and a term builds one a lambda takes apart.
#[test]
fn types() {
    let terms = r#"
        type Size = Small int | Big (int, int);
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        let sized = pairs | value(if($1[0] > 3, Big($1[0], $0[0]), Small($1[0])));
        export "built" = sized | value(case $1 { Small(x) => x, Big(a, b) => a + b });
        let maybe = pairs | value(if($1[0] > 3, Some($1[0]), None));
        export "option" = maybe | value(case $1 { Some(x) => x, None => 0 });
    "#;
    let lambdas = r#"
        type Size = Small int | Big (int, int);
        let pairs = input 0 : ((int, int) ; ()) | key(((a, b), ()) -> ((a), (b)));
        let built = pairs | value(((a), (b)) -> ((b, 3) gt, (b, a) inject Big, b inject Small) select);
        export "built" = built | value(case $1 { Small(x) => x, Big(a, b) => a + b });
        let maybe = pairs | value((_, (b)) -> (b, (b, 3) gt) branch Option map_variant None (_ -> ()));
        export "option" = maybe | value(case $1 { Some(x) => x, None => 0 });
    "#;
    assert_same(terms, lambdas, &[edges()]);
}

/// A lambda takes apart a value of a declared type. Only the corgi backend runs it: the vec backend
/// types a lambda's input from the row alone, and a variant does not say which type it is.
#[test]
fn types_matched() {
    let tree = |src: &str| {
        let mut tree = lower::lower_tree(parse::pipe::parse(src));
        tree.optimize();
        tree
    };
    let terms = tree(r#"
        type Size = Small int | Big (int, int);
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        let sized = pairs | value(if($1[0] > 3, Big($1[0], $0[0]), Small($1[0])));
        export "matched" = sized | value(case $1 { Small(x) => x, Big(a, b) => a * b });
    "#);
    let lambdas = tree(r#"
        type Size = Small int | Big (int, int);
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        let sized = pairs | value(if($1[0] > 3, Big($1[0], $0[0]), Small($1[0])));
        export "matched" = sized | value((_, s) -> s match (Small (x -> x), Big ((a, b) -> (a, b) mul)));
    "#);
    let want = evaluate(RenderBackend::Vec, timely::Config::process(1), &terms, &[edges()]);
    assert!(!want["matched"].is_empty());
    for workers in [1, 3] {
        assert_eq!(evaluate(RenderBackend::Corgi, timely::Config::process(workers), &lambdas, &[edges()]), want, "{workers} workers");
    }
}

/// A type whose payloads are other sums: a declared type, and an `Option`, which has no name of its
/// own in a lambda's scope.
#[test]
fn nested_types() {
    let terms = r#"
        type Inner = A int | B;
        type Outer = Wrap Inner | Opt Option(int) | Nums List(int);
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        let outer = pairs | value(Wrap(A($1[0])));
        export "outer" = outer | value(case $1 { Wrap(i) => case i { A(x) => x, _ => 0 }, _ => 0 - 1 });
    "#;
    let lambdas = r#"
        type Inner = A int | B;
        type Outer = Wrap Inner | Opt Option(int) | Nums List(int);
        let pairs = input 0 : ((int, int) ; ()) | key(((a, b), ()) -> ((a), (b)));
        let outer = pairs | value((_, (b)) -> b inject A inject Wrap);
        export "outer" = outer | value(case $1 { Wrap(i) => case i { A(x) => x, _ => 0 }, _ => 0 - 1 });
    "#;
    assert_same(terms, lambdas, &[edges()]);
}

/// A DDIR `--` comment may sit inside a lambda written over several lines.
#[test]
fn comments_in_lambdas() {
    let terms = r#"
        let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        export "sum" = pairs | value($0[0] + $1[0]);
    "#;
    let lambdas = r#"
        let pairs = input 0 : ((int, int) ; ()) | key(((a, b), ()) -> ((a), (b)));
        export "sum" = pairs | value(((a), (b)) ->
            -- the key and the value, added
            (a, b) add  -- one Int
        );
    "#;
    assert_same(terms, lambdas, &[edges()]);
}
