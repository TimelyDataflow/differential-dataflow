//! `Server::install` checks a program before installing anything for it.

use interactive::scope_ir::Program;
use interactive::server::Server;
use interactive::{lower, parse};

fn program(src: &str) -> Program {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    program.optimize();
    program
}

#[test]
fn a_rejected_install_leaves_no_generated_sources() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let recipe = "random:nodes=8,edges=12";
        server.install(worker, "first", &program(r#"export "taken" = input 0;"#)).unwrap();

        // The recipe would be generated, but the export is taken.
        let clash = program(&format!(r#"let e = import "{recipe}"; export "taken" = e;"#));
        assert!(server.install(worker, "second", &clash).is_err());
        assert!(server.snapshot(worker, recipe).is_err());

        // The recipe would be generated, but the other import is unknown.
        let unknown = program(&format!(r#"let e = import "{recipe}"; let f = import "nope"; export "x" = e + f;"#));
        assert!(server.install(worker, "third", &unknown).is_err());
        assert!(server.snapshot(worker, recipe).is_err());

        // Accepted, the program installs the recipe as a source.
        let fine = program(&format!(r#"let e = import "{recipe}"; export "fresh" = e;"#));
        server.install(worker, "fourth", &fine).unwrap();
        assert!(server.snapshot(worker, recipe).is_ok());
    });
}
