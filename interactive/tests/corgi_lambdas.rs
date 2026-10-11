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
