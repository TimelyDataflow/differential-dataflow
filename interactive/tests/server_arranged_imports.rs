//! Imports that arrive as the producer's arrangement (`Server::set_arranged_imports`) answer
//! exactly as imports that arrive as containers: a join and a reduce of an import at the root,
//! a join inside an iterative scope, and the import read as rows, through additions and
//! retractions of the shared trace, including for an importer installed late.

use interactive::server::Server;
use interactive::{lower, parse};

type Snapshot = Vec<(String, String, i64)>;

const PRODUCER: &str = r#"output "edges" = input 0 : ((int, int), ()) | map(((src, dst), ()) -> (src, dst));"#;

/// A consumer of `edges`. Each install of it publishes its outputs under its own program name.
fn consumer() -> String {
    format!(
        r#"
        let edges = import "prod::edges";
        let roots = input 0 : ((int), ()) | map(((n), ()) -> (n, ()));
        r: {{
            let step = reach | join(edges, (_, _, dst) -> (dst, ()));
            set reach = roots + step | distinct;
        }}
        output "hop" = roots | join(edges, (n, _, dst) -> (dst, n));
        output "degree" = edges | count;
        output "reach" = r::reach;
        output "flipped" = edges | map((src, dst) -> (dst, src));

        "#
    )
}

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    interactive::passes::optimize(&mut program);
    server.install(worker, name, &program).unwrap();
}

fn edge(server: &mut Server, src: i64, dst: i64, diff: i64) {
    server.feed("prod", 0, &format!("{src},{dst} diff={diff}"), None).unwrap();
}

fn root(server: &mut Server, name: &str, node: i64) {
    server.feed(name, 0, &node.to_string(), None).unwrap();
}

/// Snapshot each named export, each expected to be non-empty.
fn take(server: &mut Server, worker: &mut timely::worker::Worker, names: &[String], snapshots: &mut Vec<Snapshot>) {
    for name in names {
        let rows = server.snapshot(worker, name).unwrap().rows();
        assert!(!rows.is_empty(), "export {name} is empty");
        snapshots.push(rows);
    }
}

/// Run the scenario, snapshotting every consumer export after each tick.
fn scenario(arranged: bool) -> Vec<Snapshot> {
    timely::execute_directly(move |worker| {
        let mut snapshots = Vec::new();
        let mut server = Server::new();
        server.set_arranged_imports(arranged);
        install(&mut server, worker, "prod", PRODUCER);
        for (s, d) in [(1, 2), (2, 3), (3, 4), (5, 6), (2, 7)] { edge(&mut server, s, d, 1); }
        server.tick(worker);

        let mut exports: Vec<String> = Vec::new();

        // An importer installed over an existing trace.
        install(&mut server, worker, "early", &consumer());
        exports.extend(["early::hop", "early::degree", "early::reach", "early::flipped"].map(String::from));
        root(&mut server, "early", 1);
        server.tick(worker);
        take(&mut server, worker, &exports, &mut snapshots);

        // The shared trace changes: an addition, and a retraction that cuts reachability.
        edge(&mut server, 4, 5, 1);
        edge(&mut server, 2, 3, -1);
        server.tick(worker);
        take(&mut server, worker, &exports, &mut snapshots);

        // An importer installed after the trace has history.
        install(&mut server, worker, "late", &consumer());
        exports.extend(["late::hop", "late::degree", "late::reach", "late::flipped"].map(String::from));
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
fn arranged_imports_answer_as_unarranged_imports_do() {
    assert_eq!(scenario(true), scenario(false));
}

/// How many arrange operators installing a consumer whose two joins read one trace import builds,
/// with arranged imports `arranged`.
fn import_arranges(arranged: bool) -> usize {
    use std::cell::RefCell;
    use std::rc::Rc;
    use timely::logging::{TimelyEvent, TimelyEventBuilder};
    const READER: &str = r#"
        let edges = import "prod::edges";
        let roots = input 0 : ((int), ()) | map(((n), ()) -> (n, ()));
        output "out" = roots | join(edges, (n, _, dst) -> (dst, n));
        output "in" = roots | join(edges, (n, _, dst) -> (n, dst));
    "#;
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.set_arranged_imports(arranged);
        install(&mut server, worker, "prod", PRODUCER);
        server.tick(worker);
        let names: Rc<RefCell<Vec<String>>> = Rc::default();
        let sink = Rc::clone(&names);
        worker.log_register().unwrap().insert::<TimelyEventBuilder, _>("timely", move |_, data| {
            if let Some(data) = data {
                for (_, event) in data.iter() {
                    if let TimelyEvent::Operates(op) = event { sink.borrow_mut().push(op.name.clone()); }
                }
            }
        });
        install(&mut server, worker, "reader", READER);
        server.tick(worker);
        worker.log_register().unwrap().remove("timely");
        let count = names.borrow().iter().filter(|n| *n == "CorgiArrange").count();
        count
    })
}

/// A trace import that arrives as rows is arranged once, and both joins read that arrangement.
#[test]
fn unarranged_import_is_arranged_once() {
    assert_eq!(import_arranges(false), import_arranges(true) + 1);
}
