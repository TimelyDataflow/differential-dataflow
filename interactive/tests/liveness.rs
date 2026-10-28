//! Unread computations are removed while inputs and Inspect remain observable.
use interactive::{lower, parse, scope_ir::{Item, Program, Source}, server::Server};

fn program(src: &str) -> Program { lower::lower_tree(parse::pipe::parse(src)) }

#[test]
fn unread_operators_and_recursive_scopes_are_removed() {
    let original = program(r#"
        let x = input 0 : (int, int);
        let used = x | map((k, v) -> (v, k));
        let dead = x | map((k, v) -> (k, (v, 1) add)) | arrange;
        let dead2 = dead | reduce((k, vs) -> vs);
        loop: {
            var r;
            let step = r | map((k, v) -> (v, k));
            let unread = step | arrange;
            set r = (::x + step) | distinct;
        }
        export "out" = used;
    "#);
    let mut optimized = original.clone();
    optimized.optimize();
    assert_eq!(optimized.root.imports.len(), 1);
    assert_eq!(optimized.root.imports[0].from, Source::Input(0));
    assert_eq!(optimized.root.items.len(), 1);
    assert_eq!(optimized.root.labels, ["used"]);
    let inputs = vec![vec!["1 val=2".to_string(), "2 val=3".to_string()]];
    let run = |p: &Program| {
        let mut rows = interactive::server::evaluate(timely::Config::process(1), p, &inputs)["out"].rows();
        rows.sort(); rows
    };
    assert_eq!(run(&optimized), run(&original));
}

#[test]
fn inspect_keeps_its_input_and_enclosing_scopes() {
    let mut p = program(r#"
        let unused = input 0 : (int, int);
        let x = input 1 : (int, int);
        values: {
            let dead = unused | map((k, v) -> (v, k));
            let used = x | map((k, v) -> (v, k));
        }
        let unread = values::dead | arrange;
        observer: {
            inner: { let seen = values::used | inspect(seen); }
        }
    "#);
    p.optimize();
    assert!(p.root.exports.is_empty());
    assert_eq!(p.root.imports.len(), 2);
    assert_eq!(p.root.imports[0].from, Source::Input(0));
    assert_eq!(p.root.imports[1].from, Source::Input(1));
    assert_eq!(p.op_count(), 2);
    let Item::Sub(values) = &p.root.items[0] else { panic!() };
    assert_eq!(values.exports.len(), 1);
    assert_eq!(values.exports[0].name, "used");
    assert_eq!(values.imports.len(), 1);
    // Installing checks the remapped child exports and imports all the way in.
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "p", &p).unwrap();
        // The unread root input is still part of the client interface.
        server.feed("p", 0, "3 val=4", None).unwrap();
        server.feed("p", 1, "1 val=2", None).unwrap();
        server.tick(worker);
    });
}
