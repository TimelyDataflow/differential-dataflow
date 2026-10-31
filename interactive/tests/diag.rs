//! Diagnostics: the `diag.*` sources and the shipped views see the programs a server runs, and
//! their state stays bounded while programs are installed and dropped.
use std::collections::BTreeMap;

use interactive::scope_ir::Program;
use interactive::server::Server;
use interactive::{diag, lower, parse};

const DIAG: &str = include_str!("../server/diag.ddp");

const PAIRS: &str = "
let edges = input 0 : ((int, int), ());
let forward = edges | map(((a, b), ()) -> (a, b));
let pairs = forward | join(forward, (_, x, y) -> ((x, y), ()));
export \"NAME\" = pairs | distinct;
";

fn program(src: &str) -> Program {
    let mut p = lower::lower_tree(parse::pipe::parse(src));
    interactive::passes::optimize(&mut p);
    p
}

/// Printed text with each `list(..)` of integers written as the string of those bytes.
fn decode(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find("list(") {
        out.push_str(&rest[..at]);
        let after = &rest[at + 5..];
        let end = after.find(')').expect("a closed list");
        let bytes: Option<Vec<u8>> = after[..end].split(',').filter(|s| !s.is_empty()).map(|s| s.parse().ok()).collect();
        match bytes.and_then(|b| String::from_utf8(b).ok()) {
            Some(s) => out.push_str(&format!("{s:?}")),
            None => out.push_str(&rest[at..at + 5 + end + 1]),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

/// A `count` view's rows: each key, decoded, and its count.
fn view(server: &mut Server, worker: &mut timely::worker::Worker, name: &str) -> BTreeMap<String, i64> {
    let rows = server.snapshot(worker, name).expect("a published view").rows();
    rows.into_iter().map(|(key, val, diff)| {
        assert_eq!(diff, 1, "a count row");
        (decode(&key), val.parse().expect("a count"))
    }).collect()
}

/// A view of `(updates, bytes)` pairs: each key, decoded, and its pair.
fn sizes(server: &mut Server, worker: &mut timely::worker::Worker, name: &str) -> BTreeMap<String, (i64, i64)> {
    let rows = server.snapshot(worker, name).expect("a published view").rows();
    rows.into_iter().map(|(key, val, diff)| {
        assert_eq!(diff, 1, "a joined count row");
        let pair = val.strip_prefix("tuple(").and_then(|v| v.strip_suffix(')')).expect("a pair");
        let (n, b) = pair.split_once(',').expect("two counts");
        (decode(&key), (n.parse().expect("a count"), b.parse().expect("a count")))
    }).collect()
}

#[test]
fn views_see_programs_and_state_stays_bounded() {
    timely::execute_directly(move |worker| {
        let diagnostics = diag::register(worker);
        let mut server = Server::new();
        server.set_diagnostics(std::rc::Rc::clone(&diagnostics));
        server.install(worker, "diag", &program(DIAG)).unwrap();
        server.install(worker, "p", &program(&PAIRS.replace("NAME", "p.pairs"))).unwrap();
        server.feed("p", 0, "1,2\n2,3\n3,1\n2,1", None).unwrap();
        server.tick(worker);
        server.tick(worker);

        let time = view(&mut server, worker, "diag.time_by_statement");
        assert!(time.iter().any(|(k, &ns)| k.contains("\"p\"") && k.contains("\"pairs\"") && ns > 0), "{time:#?}");
        assert!(time.keys().all(|k| !k.contains("\"diag\"")), "the diagnostics program is left out: {time:#?}");
        let records = view(&mut server, worker, "diag.records_by_statement");
        assert!(records.iter().any(|(k, &n)| k.contains("\"p\"") && n > 0), "{records:#?}");
        let operators = view(&mut server, worker, "diag.operators_by_program");
        assert!(operators.get("\"p\"").is_some_and(|&n| n > 0), "{operators:#?}");
        // Each arrangement's updates take bytes: at least a diff, a time and a key column apiece.
        let arrangements = sizes(&mut server, worker, "diag.arrangements");
        assert!(arrangements.iter().any(|(k, &(n, b))| k.contains("\"p\"") && n > 0 && b >= 24 * n), "{arrangements:#?}");
        let by_program = sizes(&mut server, worker, "diag.arrangements_by_program");
        let summed = arrangements.iter().filter(|(k, _)| k.contains("\"p\"")).fold((0, 0), |(n, b), (_, s)| (n + s.0, b + s.1));
        assert_eq!(by_program.get("\"p\""), Some(&summed), "{by_program:#?}");

        // A program installed and dropped every epoch: its time still adds up, and nothing the
        // diagnostics hold grows with the number of installs.
        let mut held = Vec::new();
        for round in 0..30 {
            server.install(worker, "q", &program(&PAIRS.replace("NAME", "q.pairs"))).unwrap();
            server.feed("q", 0, "1,2\n2,3", None).unwrap();
            server.tick(worker);
            server.drop_program(worker, "q").unwrap();
            server.tick(worker);
            if round == 9 || round == 29 {
                let (ops, triples) = diagnostics.borrow().size();
                let diag_records = diagnostics.borrow().records_by_program().get("diag").copied().unwrap_or(0);
                held.push((ops, triples, diag_records));
            }
        }
        let by_program = view(&mut server, worker, "diag.time_by_program");
        assert!(by_program.get("\"q\"").is_some_and(|&ns| ns > 0), "q's time survives its drops: {by_program:#?}");
        let live = view(&mut server, worker, "diag.operators_by_program");
        assert!(!live.contains_key("\"q\""), "q has no live operators: {live:#?}");
        assert_eq!(held[0].0, held[1].0, "live operators after 10 and 30 cycles: {held:?}");
        assert_eq!(held[0].1, held[1].1, "retired triples after 10 and 30 cycles: {held:?}");
        assert!(held[1].2 <= held[0].2 + held[0].2 / 4 + 64, "the diagnostics program's records stay flat: {held:?}");
        assert_eq!(diagnostics.borrow().unbalanced, 0, "DD's drop events retract every record and byte");
    });
}
