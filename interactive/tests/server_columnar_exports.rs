//! Columnar exports (`Server::set_columnar_exports`) read the same as row exports:
//! through `snapshot`, through another program's `import`, and through `bind`.

use interactive::ir::Value;
use interactive::server::{RenderBackend, Server};
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    program.optimize();
    server.install(worker, name, &program).unwrap();
}

const PRODUCER: &str = r#"
    type Three = A int | B int | C int;
    let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
    export "plain" = pairs;
    export "lists" = pairs | collect;
    export "tagged" = pairs | map($0 ; variant(Three, 2, $1[0]));
"#;

const CONSUMER: &str = r#"
    let p = import "plain";
    export "shifted" = p | map($0 ; $1[0] + 1000);
"#;

const COUNTER: &str = r#"
    let seed = input 0 : ((int) ; ());
    let feedback = input 1 : ((int) ; ());
    let state = seed + feedback;
    export "count" = state;
    export "next" = (state | map($0[0] + 1 ;)) + (seed | negate);
"#;

/// Everything the server can read back, after the same inputs and ticks.
fn run(worker: &mut timely::worker::Worker, columnar: bool) -> Vec<Vec<(Value, Value, i64)>> {
    let mut server = Server::with_backend(RenderBackend::Corgi);
    server.set_columnar_exports(columnar);
    install(&mut server, worker, "producer", PRODUCER);
    install(&mut server, worker, "consumer", CONSUMER);
    install(&mut server, worker, "counter", COUNTER);
    server.feed("counter", 0, tup(&[0]), Value::unit(), None, 1).unwrap();
    server.bind(worker, "next", "counter", 1).unwrap();
    let mut seen = Vec::new();
    for epoch in 0..4i64 {
        for i in 0..40i64 {
            let diff = if (i + epoch) % 3 == 0 { -1 } else { 1 };
            server.feed("producer", 0, tup(&[i % 7, i * epoch]), Value::unit(), None, diff).unwrap();
        }
        server.tick(worker);
        for name in ["plain", "lists", "tagged", "shifted", "count"] {
            seen.push(server.snapshot(worker, name).unwrap());
        }
    }
    seen
}

#[test]
fn columnar_exports_read_like_row_exports() {
    timely::execute_directly(|worker| {
        let rows = run(worker, false);
        let columnar = run(worker, true);
        assert!(rows.iter().any(|s| !s.is_empty()));
        // The counter advanced through the binding in both.
        assert_eq!(rows.last(), Some(&vec![(tup(&[3]), Value::unit(), 1)]));
        assert_eq!(rows, columnar);
    });
}
