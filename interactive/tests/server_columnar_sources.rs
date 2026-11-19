use interactive::server::Server;
use interactive::{lower, parse};

fn install(server: &mut Server, worker: &mut timely::worker::Worker, name: &str, src: &str) {
    let mut program = lower::lower_tree(parse::pipe::parse(src));
    interactive::passes::optimize(&mut program);
    server.install(worker, name, &program).unwrap();
}

/// A fed input, read through another program's import.
const LOOSE: &str = r#"
    output "loose" = input 0 : ((int), ());
"#;

/// A declared import of that input's export, checked against its shape at install.
const DECLARING: &str = r#"
    let l = import "undeclared::loose" : ((int), ());
    output "doubled" = l | map(((n), ()) -> (((n, 2) mul), ()));

"#;

/// Everything the server can read back, after the same installs, loads, feeds, and ticks.
fn run(workers: usize) -> Vec<Vec<(String, String, i64)>> {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        install(&mut server, worker, "undeclared", LOOSE);
        install(&mut server, worker, "declaring", DECLARING);
        let mut seen = Vec::new();
        for epoch in 1..5i64 {
            if worker.index() == 0 {
                for i in 0..10 {
                    server.feed("undeclared", 0, &(i * epoch).to_string(), None).unwrap();
                }
            }
            server.tick(worker);
            for name in ["undeclared::loose", "declaring::doubled"] {
                seen.push(server.snapshot(worker, name).unwrap().rows());
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
    assert!(want[want.len() - 2..].iter().all(|s| !s.is_empty()), "every read has rows by the last tick");
    assert_eq!(run(3), want, "3 workers");
}
