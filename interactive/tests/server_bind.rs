//! Semantics of `Server::bind` — export→input feedback, one epoch per tick.
//!
//! The counter is the canonical case: a program whose next state is a pure
//! function of its current state, advanced by the server alone. The client's
//! only acts are one seed row and one `bind`; every subsequent step happens
//! because `tick` drains the bound export's changes back into the feedback
//! input. See the `Binding` docs for the `f(state) + (seed | negate)` idiom.

use interactive::ir::Value;
use interactive::server::Server;
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let statements = parse::pipe::parse(src);
    let mut program = lower::lower_tree(statements);
    program.optimize();
    server.install(worker, name, &program).unwrap();
}

const COUNTER: &str = r#"
    let seed = input 0;
    let feedback = input 1;
    let state = seed + feedback;
    export "count" = state;
    export "next" = (state | map($0[0] + 1 ;)) + (seed | negate);
"#;

#[test]
fn bind_advances_a_counter_without_a_client() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        install(&mut server, worker, "counter", COUNTER);
        server
            .feed("counter", 0, tup(&[0]), Value::unit(), None, 1)
            .unwrap();
        server.bind(worker, "next", "counter", 1).unwrap();

        // state(t) = f(state(t-1)) = state(t-1) + 1, one step per tick. After
        // N ticks the snapshot (which reads the closed past) shows N - 1.
        for expected in 0..5 {
            server.tick(worker);
            let rows = server.snapshot(worker, "count").unwrap();
            assert_eq!(rows, vec![(tup(&[expected]), Value::unit(), 1)]);
        }

        // A later seed change injects as a perturbation: adding a second
        // token forks the counter into two independent tracks.
        server
            .feed("counter", 0, tup(&[100]), Value::unit(), None, 1)
            .unwrap();
        server.tick(worker);
        server.tick(worker);
        let rows = server.snapshot(worker, "count").unwrap();
        assert_eq!(
            rows,
            vec![
                (tup(&[6]), Value::unit(), 1),
                (tup(&[101]), Value::unit(), 1)
            ]
        );
    });
}

#[test]
fn bind_lifecycle_guards() {
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        install(&mut server, worker, "counter", COUNTER);

        // Unknown pieces are rejected.
        assert!(server.bind(worker, "nope", "counter", 1).is_err());
        assert!(server.bind(worker, "next", "nope", 1).is_err());
        assert!(server.bind(worker, "next", "counter", 7).is_err());

        server.bind(worker, "next", "counter", 1).unwrap();
        // Identical duplicate is rejected; the binding is listed.
        assert!(server.bind(worker, "next", "counter", 1).is_err());
        assert_eq!(
            server.binding_info(),
            vec![("next".to_string(), "counter".to_string(), 1)]
        );

        // While bound, the program can be dropped from neither side: it is
        // its own source's exporter (importer refcount) and the binding's
        // target (explicit guard).
        assert!(server.drop_program(worker, "counter").is_err());

        server.unbind(worker, "next", "counter", 1).unwrap();
        assert!(server.unbind(worker, "next", "counter", 1).is_err());
        server.drop_program(worker, "counter").unwrap();
    });
}
