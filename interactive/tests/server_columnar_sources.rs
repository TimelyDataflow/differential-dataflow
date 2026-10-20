//! Generated sources, recipe loads, and fed inputs, held as corgi columns, read the same at any
//! number of workers: through `snapshot`, through a program's `import`, and through `bind`, across
//! ticks of churn.

use interactive::ir::Value;
use interactive::server::Server;
use interactive::{lower, parse};

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    program.optimize();
    server.install(worker, name, &program).unwrap();
}

const RANDOM: &str = "random:nodes=20,edges=60,churn=7";

const SOURCES: &str = r#"
    let edges = import "random:nodes=20,edges=60,churn=7" | key(((src, dst), ()) -> (src, dst));
    let iota = import "iota:12";
    let clock = import "clock";
    export "degrees" = edges | count;
    export "small" = iota | filter(((n), ()) -> (n, 5) lt);
    export "now" = clock;
"#;

const LOADED: &str = r#"
    let triples = input 0 : ((int, int, int) ; ());
    export "sums" = triples | map(((a, b, c), ()) -> (((a, b) add, c) add, ())) | count;
"#;

/// A fed input, read through another program's import.
const LOOSE: &str = r#"
    export "loose" = input 0 : ((int) ; ());
"#;

/// A declared import of that input's export, checked against its shape at install.
const DECLARING: &str = r#"
    let l = import "loose" : ((int) ; ());
    export "doubled" = l | map(((n), ()) -> (((n, 2) mul), ()));

"#;

/// An input bound to "doubled", so it mirrors it one tick behind.
const MIRROR: &str = r#"
    export "mirror" = input 0 : ((int) ; ());
"#;

/// Everything the server can read back, after the same installs, loads, feeds, and ticks.
fn run(workers: usize) -> Vec<Vec<(Value, Value, i64)>> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        install(&mut server, worker, "sources", SOURCES);
        install(&mut server, worker, "loaded", LOADED);
        install(&mut server, worker, "undeclared", LOOSE);
        install(&mut server, worker, "declaring", DECLARING);
        install(&mut server, worker, "mirror", MIRROR);
        server.bind(worker, "doubled", "mirror", 0).unwrap();
        server.load(worker, "loaded", 0, "random:nodes=5,edges=40,arity=3,churn=4").unwrap();
        let mut seen = Vec::new();
        for epoch in 1..5i64 {
            if worker.index() == 0 {
                for i in 0..10 {
                    server.feed("undeclared", 0, &(i * epoch).to_string(), None).unwrap();
                }
            }
            server.tick(worker);
            for name in [RANDOM, "iota:12", "clock", "degrees", "small", "now", "sums", "doubled", "mirror"] {
                seen.push(server.snapshot(worker, name).unwrap().into_rows());
            }
        }
        seen
    })
    .unwrap();
    guards.join().into_iter().next().unwrap().unwrap()
}

#[test]
fn sources_read_alike_across_workers() {
    let want = run(1);
    assert!(want[want.len() - 9..].iter().all(|s| !s.is_empty()), "every read has rows by the last tick");
    assert_eq!(run(3), want, "3 workers");
}
