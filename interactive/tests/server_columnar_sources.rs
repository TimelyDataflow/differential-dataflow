//! Generated sources, recipe loads, and fed inputs read the same whether the server keeps them
//! as rows or, with columnar exports on the corgi backend, as corgi columns: through `snapshot`,
//! through a program's `import`, and through `bind`, across ticks of churn, and across workers.

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

const RANDOM: &str = "random:nodes=20,edges=60,churn=7";

const SOURCES: &str = r#"
    let edges = import "random:nodes=20,edges=60,churn=7" | key($0[0] ; $0[1]);
    let iota = import "iota:12";
    let clock = import "clock";
    export "degrees" = edges | count;
    export "small" = iota | filter($0[0] < 5);
    export "now" = clock;
"#;

const LOADED: &str = r#"
    let triples = input 0 : ((int, int, int) ; ());
    export "sums" = triples | map($0[0] + $0[1] + $0[2] ;) | count;
"#;

/// An undeclared input, so its export's shape is unknown at install.
const UNDECLARED: &str = r#"
    export "loose" = input 0;
"#;

/// A declared import of a trace whose shape is unknown at install.
const DECLARING: &str = r#"
    let l = import "loose" : ((int) ; ());
    export "doubled" = l | map($0[0] * 2 ;);
"#;

/// An input bound to "doubled", so it mirrors it one tick behind.
const MIRROR: &str = r#"
    export "mirror" = input 0 : ((int) ; ());
"#;

/// Everything the server can read back, after the same installs, loads, feeds, and ticks.
fn run(workers: usize, backend: RenderBackend, columnar: bool) -> Vec<Vec<(Value, Value, i64)>> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::with_backend(backend);
        server.set_columnar_exports(columnar);
        install(&mut server, worker, "sources", SOURCES);
        install(&mut server, worker, "loaded", LOADED);
        install(&mut server, worker, "undeclared", UNDECLARED);
        install(&mut server, worker, "declaring", DECLARING);
        install(&mut server, worker, "mirror", MIRROR);
        server.bind(worker, "doubled", "mirror", 0).unwrap();
        server.load(worker, "loaded", 0, "random:nodes=5,edges=40,arity=3,churn=4").unwrap();
        let mut seen = Vec::new();
        for epoch in 1..5i64 {
            if worker.index() == 0 {
                for i in 0..10 {
                    server.feed("undeclared", 0, tup(&[i * epoch]), Value::unit(), None, 1).unwrap();
                }
            }
            server.tick(worker);
            for name in [RANDOM, "iota:12", "clock", "degrees", "small", "now", "sums", "doubled", "mirror"] {
                seen.push(server.snapshot(worker, name).unwrap());
            }
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn columnar_sources_read_like_row_sources() {
    for workers in [1, 3] {
        let vec = run(workers, RenderBackend::Vec, false);
        assert!(vec[vec.len() - 9..].iter().all(|s| !s.is_empty()), "every read has rows by the last tick");
        assert_eq!(vec, run(workers, RenderBackend::Corgi, false), "corgi over rows, {workers} workers");
        assert_eq!(vec, run(workers, RenderBackend::Corgi, true), "corgi over columns, {workers} workers");
    }
}
