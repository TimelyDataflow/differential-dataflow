//! A slot written as a corgi lambda, `pat -> body`, whose pattern does not fit its slot's rows, or
//! whose body does not parse, is refused at install with corgi's message. What lambdas compute is
//! checked against recorded answers, in `tests/programs` (`lambda_slots.ddp` and the rest).

use interactive::{lower, parse};

#[test]
fn aliases_inside_lambdas_explain_the_limit() {
    timely::execute_directly(|worker| {
        let src = r#"type Text = List(int); output "out" = input 0 : (int, Text) | map((k, x : Text) -> (k, x));"#;
        let program = lower::lower_tree(parse::pipe::parse(src));
        let error = interactive::server::Server::new().install(worker, "p", &program).unwrap_err();
        assert!(error.contains("alias `Text`: aliases are not yet available inside lambdas"), "{error}");
    });
}

#[test]
fn inline_enums_inside_lambdas_still_work() {
    timely::execute_directly(|worker| {
        let src = r#"output "out" = input 0 : (int, int) | map((k, v) -> enum Local = One int in (k, v inject One));"#;
        let program = lower::lower_tree(parse::pipe::parse(src));
        interactive::server::Server::new().install(worker, "p", &program).unwrap();
    });
}

#[test]
fn mismatched_lambdas_are_refused_at_install() {
    timely::execute_directly(|worker| {
        for (src, says) in [
            (r#"output "a" = input 0 : ((int, int), ()) | map(((s), ()) -> ((s), ()));"#, "wants a product of 1 field"),
            (r#"output "a" = input 0 : ((int, int), ()) | filter(((s, d), ()) -> (s, d));"#, "not an integer"),
            (r#"output "a" = input 0 : ((int, int), ()) | map(((s, d), ()) -> (s, d, ) add);"#, "expected an expression"),
            // Type errors the optimizer would trip on, or drop as unused.
            (r#"output "a" = input 0 : (int, ()) | map((k, v) -> ((k, v).2, ()));"#, "Field(2) expects a product with > 2 fields"),
            (r#"output "a" = input 0 : (int, ()) | map((k, v) -> let (a, b, c) = (k, v) in (k, v));"#, "wants a product of 3 fields"),
        ] {
            let mut program = lower::lower_tree(parse::pipe::parse(src));
            interactive::passes::optimize(&mut program);
            let error = interactive::server::Server::new().install(worker, "p", &program).unwrap_err();
            assert!(error.contains(says), "{src}: {error}");
        }
    });
}
