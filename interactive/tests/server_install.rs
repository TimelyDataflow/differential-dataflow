//! `Server::install` checks a program before installing anything for it.

use interactive::server::host_time;
use timely::progress::Antichain;
use interactive::scope_ir::Program;
use interactive::server::Server;
use interactive::{lower, parse};

fn program(src: &str) -> Program {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    interactive::passes::optimize(&mut program);
    program
}

#[test]
fn a_rejected_install_leaves_no_generated_sources() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        let valid = program(r#"output "taken" = input 0 : ((int), ());"#);
        server.install(worker, "first", &valid).unwrap();
        assert!(server.install(worker, "first", &valid).is_err());
        let mut invalid = program(r#"output "x" = input "taken" : ((int), ()) + input "nope" : ((int), ());"#);
        invalid.bind([("taken", "first::taken"), ("nope", "nope")]).unwrap();
        assert!(server.install(worker, "second", &invalid).is_err());
        assert!(server.trace_shape("second::x").is_none());
        server.drop_program(worker, "first").unwrap();
    });
}

#[test]
fn unread_imports_are_validated_before_passes() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        for passes in [interactive::passes::Config::default(), interactive::passes::Config::none()] {
            server.set_passes(passes);
            let mut invalid = lower::lower_tree(parse::pipe::parse(r#"let dead = input "nope" : ((int), ());"#));
            invalid.bind([("nope", "nope")]).unwrap();
            let error = server.install(worker, "invalid", &invalid).unwrap_err();
            assert!(error.contains("to unknown trace \"nope\""), "{error}");


        }
    });
}

#[test]
fn emitted_strict_join_keeps_updates_after_a_fresh_servers_as_of() {
    let text = r#"let a = input 0 : (int, int); let b = input 1 : (int, int);
        output "out" = a | lift | half_join(b, 1, (k, ((v, _), t)) -> (k, (v, t)));"#;
    let path = std::env::temp_dir().join(format!("ddir-as-of-{}.ddp", std::process::id()));
    std::fs::write(&path, text).unwrap();
    let emit = |args: &[&str]| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_ddir-plan"))
            .arg("--emit-ddp").args(args).arg(&path).output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        String::from_utf8(output.stdout).unwrap()
    };
    let emitted = emit(&[]);
    assert!(emitted.contains("(t0, 0) ne"));
    assert!(emit(&["--as-of", "7"]).contains("(t0, 7) ne"));
    std::fs::remove_file(path).unwrap();
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.set_passes(interactive::passes::Config::none());
        for (name, text) in [("original", text), ("emitted", emitted.as_str())] {
            let text = text.replace("\"out\"", &format!("{name:?}"));
            let program = lower::lower_tree(parse::pipe::parse(&text));
            server.install(worker, name, &program).unwrap();
            server.feed(name, 1, "1 val=20", 0).unwrap();
        }
        for name in ["original", "emitted"] {
            for input in [0, 1] {
                server.seal(name, input, &Antichain::from_elem(host_time(1))).unwrap();
            }
        }
        server.spin(worker);
        for name in ["original", "emitted"] { server.feed(name, 0, "1 val=10", 1).unwrap(); }
        for name in ["original", "emitted"] {
            for input in [0, 1] {
                server.seal(name, input, &Antichain::from_elem(host_time(2))).unwrap();
            }
        }
        server.spin(worker);
        let original = server.snapshot(worker, "original::original").unwrap().rows();
        assert_eq!(original, vec![("1".into(), "tuple(10,tuple(1))".into(), 1)]);
        assert_eq!(server.snapshot(worker, "emitted::emitted").unwrap().rows(), original);
    });
}

/// A number finds only the input written with it.
#[test]
fn numbers_find_only_numbered_inputs() {
    use interactive::server::InputId;
    timely::execute_directly(|worker| {
        let mut server = Server::new();
        server.install(worker, "p", &program(r#"output "out" = input 0 : (int, ()) + input "b" : (int, ());"#)).unwrap();
        assert_eq!(server.has_input("p", &InputId::Number(0)), Ok(()));
        assert_eq!(server.has_input("p", &InputId::Name("b".into())), Ok(()));
        assert_eq!(server.has_input("p", &InputId::Number(1)), Err("program \"p\" has no input 1".into()));
    });
}
