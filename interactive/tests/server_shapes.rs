//! `Server::install` knows the shapes of the traces it publishes, and rejects programs whose
//! shapes conflict before installing anything for them.

use corgi::Shape;
use interactive::scope_ir::Program;
use interactive::server::Server;
use interactive::{lower, parse};

fn program(src: &str) -> Program {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    program.optimize();
    program
}

fn ints(n: usize) -> Shape { Shape::Prod(vec![Shape::Int; n]) }

const PRODUCER: &str = r#"export "edges" = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));"#;


#[test]
fn exports_publish_their_shapes() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "producer", &program(PRODUCER)).unwrap();
        assert_eq!(server.trace_shape("edges"), Some((ints(1), ints(1))));
        // An undeclared import takes the shape its trace is published at.
        server.install(worker, "consumer", &program(r#"export "flipped" = import "edges" | map(((src), (dst)) -> ((dst), (src, 0)));"#)).unwrap();
        assert_eq!(server.trace_shape("flipped"), Some((ints(1), ints(2))));
        // An input must declare its shape.
        let err = server.install(worker, "loose", &program(r#"export "loose" = input 0;"#)).unwrap_err();
        assert!(err.contains("without declaring its shape"), "{err}");
        // Dropping a producer forgets its shapes.
        server.drop_program(worker, "consumer").unwrap();
        assert_eq!(server.trace_shape("flipped"), None);
    });
}

#[test]
fn a_declared_import_must_match_its_trace() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "producer", &program(PRODUCER)).unwrap();
        server.install(worker, "agrees", &program(r#"export "a" = import "edges" : ((int), (int));"#)).unwrap();
        let err = server.install(worker, "disagrees", &program(r#"export "b" = import "edges" : ((int, int), ());"#)).unwrap_err();
        assert!(err.contains("published as"), "{err}");
        assert!(server.snapshot(worker, "b").is_err(), "a rejected program publishes nothing");
    });
}

#[test]
fn conflicting_shapes_reject_the_install() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        // Join keys of different shapes.
        let join = r#"let a = input 0 : ((int), ()); let b = input 1 : ((int, int), ()); export "j" = a | join(b, (k, _, _) -> (k, ()));"#;
        let err = server.install(worker, "join", &program(join)).unwrap_err();
        assert!(err.contains("join keys"), "{err}");
        // A field of an integer, through an import whose shape is published.
        server.install(worker, "producer", &program(PRODUCER)).unwrap();
        let err = server.install(worker, "proj", &program(r#"export "p" = import "edges" | map(((src), _) -> ((src .1), ()));"#
)).unwrap_err();
        assert!(err.contains("conflicting shapes"), "{err}");
        assert!(server.snapshot(worker, "p").is_err());
    });
}

#[test]
fn generated_sources_have_shapes() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let src = r#"let r = import "random:nodes=8,edges=12,arity=3"; let i = import "iota:4"; let c = import "clock"; export "out" = r + i + c;"#;
        let err = server.install(worker, "mixed", &program(src)).unwrap_err();
        assert!(err.contains("concat"), "{err}");
        let src = r#"export "out" = import "iota:4" + import "clock";"#;
        server.install(worker, "fine", &program(src)).unwrap();
        assert_eq!(server.trace_shape("iota:4"), Some((ints(1), Shape::Unit)));
        assert_eq!(server.trace_shape("clock"), Some((ints(1), Shape::Unit)));
        assert_eq!(server.trace_shape("out"), Some((ints(1), Shape::Unit)));
    });
}

#[test]
fn bindings_and_recipe_loads_must_match_declared_inputs() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "producer", &program(PRODUCER)).unwrap();
        server.install(worker, "target", &program(r#"export "t" = input 0 : ((int, int), ());"#)).unwrap();
        let err = server.bind(worker, "edges", "target", 0).unwrap_err();
        assert!(err.contains("published as"), "{err}");
        let err = server.load(worker, "target", 0, "random:nodes=8,edges=12,arity=3", 1).unwrap_err();
        assert!(err.contains("generates"), "{err}");
        server.load(worker, "target", 0, "random:nodes=8,edges=12", 1).unwrap();
    });
}

#[test]
fn declared_exports_publish_their_shapes() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        // An export may declare the shape it is inferred to have.
        server.install(worker, "declared", &program(r#"export "d" : ((int), ()) = input 0 : ((int), ());"#)).unwrap();
        assert_eq!(server.trace_shape("d"), Some((ints(1), Shape::Unit)));
        // A declaration that disagrees with the inferred shape rejects the install.
        let err = server.install(worker, "wrong", &program(&PRODUCER.replace("export \"edges\" =", "export \"w\" : ((int), ()) ="))).unwrap_err();
        assert!(err.contains("export `w`"), "{err}");
    });
}

/// A collection whose shape nothing determines rejects the install: here a variable bound only
/// to itself, which a declared shape would settle.
#[test]
fn collections_without_a_shape_reject_the_install() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let alone = r#"s: { var v = v | distinct; } export "out" = s::v;"#;
        let err = server.install(worker, "alone", &program(alone)).unwrap_err();
        assert!(err.contains("variable `v` has no shape"), "{err}");
        let declared = alone.replace("var v =", "var v : ((int), ()) =");
        server.install(worker, "declared", &program(&declared)).unwrap();
    });
}

/// Rows fed or loaded in another shape than their input declares are refused, and none of
/// their batch is staged.
#[test]
fn rows_of_another_shape_are_refused() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.install(worker, "producer", &program(PRODUCER)).unwrap();
        let fed = server.feed("producer", 0, "1", None).unwrap_err();
        assert!(fed.contains("row 1") && fed.contains("expected 2 comma-separated integers"), "{fed}");
        let batch = server.feed("producer", 0, "1,2\n3", None).unwrap_err();
        assert!(batch.contains("row 2"), "{batch}");
        let file = std::env::temp_dir().join(format!("ddir-rows-{}.txt", std::process::id()));
        std::fs::write(&file, "1 2\n3 4 5\n").unwrap();
        let loaded = server.load(worker, "producer", 0, file.to_str().unwrap(), 1).unwrap_err();
        assert!(loaded.contains("line 2"), "{loaded}");
        server.tick(worker);
        assert!(server.snapshot(worker, "edges").unwrap().is_empty(), "nothing refused was staged");
        server.feed("producer", 0, "1,2\n3,4", None).unwrap();
        // Integers read as Rust reads them, a leading `+` included.
        std::fs::write(&file, "5 6\n+7 -8\n").unwrap();
        server.load(worker, "producer", 0, file.to_str().unwrap(), 1).unwrap();
        server.tick(worker);
        assert_eq!(server.snapshot(worker, "edges").unwrap().rows().len(), 4);
        std::fs::remove_file(&file).unwrap();
    });
}
