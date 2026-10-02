//! Imports that arrive as the producer's arrangement (`Server::set_arranged_imports`) answer
//! exactly as imports that arrive as rows, on both backends: a join and a reduce of an import
//! at the root, a join inside an iterative scope, and the import read as rows, through
//! additions and retractions of the shared trace, including for an importer installed late.

use interactive::ir::Value;
use interactive::server::{RenderBackend, Server};
use interactive::{lower, parse};

type Snapshot = Vec<(Value, Value, i64)>;

const PRODUCER: &str = r#"export "edges" = input 0 | key($0[0] ; $0[1]);"#;

/// A consumer of `edges`, its exports suffixed by `tag`.
fn consumer(tag: &str) -> String {
    format!(
        r#"
        let edges = import "edges";
        let roots = input 0 | key($0[0] ;);
        r: {{
            let step = reach | join(edges, ($2 ;));
            var reach = roots + step | distinct;
        }}
        export "hop{tag}" = roots | join(edges, ($2 ; $0));
        export "degree{tag}" = edges | count;
        export "reach{tag}" = r::reach;
        export "flipped{tag}" = edges | map($1 ; $0);
        "#
    )
}

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    program.optimize();
    server.install(worker, name, &program).unwrap();
}

fn edge(server: &mut Server, src: i64, dst: i64, diff: i64) {
    let row = Value::Tuple(vec![Value::Int(src), Value::Int(dst)]);
    server.feed("prod", 0, row, Value::unit(), None, diff).unwrap();
}

fn root(server: &mut Server, name: &str, node: i64) {
    server.feed(name, 0, Value::Tuple(vec![Value::Int(node)]), Value::unit(), None, 1).unwrap();
}

/// Snapshot each named export, each expected to be non-empty.
fn take(server: &mut Server, worker: &mut timely::worker::Worker, names: &[String], snapshots: &mut Vec<Snapshot>) {
    for name in names {
        let rows = server.snapshot(worker, name).unwrap();
        assert!(!rows.is_empty(), "export {name} is empty");
        snapshots.push(rows);
    }
}

/// Run the scenario, snapshotting every consumer export after each tick.
fn scenario(backend: RenderBackend, arranged: bool) -> Vec<Snapshot> {
    timely::execute_directly(move |worker| {
        let mut snapshots = Vec::new();
        let mut server = Server::with_backend(backend);
        server.set_arranged_imports(arranged);
        install(&mut server, worker, "prod", PRODUCER);
        for (s, d) in [(1, 2), (2, 3), (3, 4), (5, 6), (2, 7)] { edge(&mut server, s, d, 1); }
        server.tick(worker);

        let mut exports: Vec<String> = Vec::new();

        // An importer installed over an existing trace.
        install(&mut server, worker, "early", &consumer("_early"));
        exports.extend(["hop_early", "degree_early", "reach_early", "flipped_early"].map(String::from));
        root(&mut server, "early", 1);
        server.tick(worker);
        take(&mut server, worker, &exports, &mut snapshots);

        // The shared trace changes: an addition, and a retraction that cuts reachability.
        edge(&mut server, 4, 5, 1);
        edge(&mut server, 2, 3, -1);
        server.tick(worker);
        take(&mut server, worker, &exports, &mut snapshots);

        // An importer installed after the trace has history.
        install(&mut server, worker, "late", &consumer("_late"));
        exports.extend(["hop_late", "degree_late", "reach_late", "flipped_late"].map(String::from));
        root(&mut server, "late", 2);
        edge(&mut server, 2, 3, 1);
        server.tick(worker);
        take(&mut server, worker, &exports, &mut snapshots);

        for _ in 0..3 {
            edge(&mut server, 6, 1, 1);
            edge(&mut server, 6, 1, -1);
            server.tick(worker);
        }
        take(&mut server, worker, &exports, &mut snapshots);
        snapshots
    })
}

#[test]
fn arranged_imports_answer_as_rows_do_vec() {
    assert_eq!(scenario(RenderBackend::Vec, true), scenario(RenderBackend::Vec, false));
}

#[test]
fn arranged_imports_answer_as_rows_do_corgi() {
    assert_eq!(scenario(RenderBackend::Corgi, true), scenario(RenderBackend::Corgi, false));
}

#[test]
fn backends_agree_with_arranged_imports() {
    assert_eq!(scenario(RenderBackend::Vec, true), scenario(RenderBackend::Corgi, true));
}
