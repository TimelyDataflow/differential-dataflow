//! `evaluate`: a program on explicit inputs through a throwaway server, the entry point the
//! test suites build on.

use super::*;

/// Evaluate `program` on explicit inputs through a throwaway server: every
/// export's consolidated final contents (rows with non-zero net multiplicity),
/// by name.
///
/// This is the data-in/data-out entry point the test suites build on, and it
/// takes the same path a live install does — `install`, one `feed` of each
/// positional input (`inputs[i]` holds input `i`'s rows, one feed line each,
/// `<key> [val=<v>] [diff=<d>]`, dealt round-robin across the workers), one
/// `tick`, then a `snapshot` of each export. `config` picks the worker group: `Config::process(n)` hands
/// exchanged containers between threads as typed values, while
/// `CommunicationConfig::ProcessBinary(n)` sends every one through the wire
/// format. The answer must not depend on either choice.
pub fn evaluate(
    config: timely::Config,
    program: &st::Program,
    inputs: &[Vec<String>],
) -> std::collections::BTreeMap<String, Updates> {
    let program = program.clone();
    let inputs = inputs.to_vec();
    let guards = timely::execute(config, move |worker| {
        let mut server = Server::new();
        server.install(worker, "evaluate", &program).expect("evaluate: install");
        let has_input = |i: usize| program.root.imports.iter().any(|imp| matches!(imp.from, st::Source::Input(n) if n == i));
        for (input, rows) in inputs.iter().enumerate().filter(|(i, _)| has_input(*i)) {
            let shard: Vec<&str> = rows.iter().skip(worker.index()).step_by(worker.peers()).map(String::as_str).collect();
            server.feed("evaluate", input, &shard.join("\n"), None).expect("evaluate: feed");
        }
        server.tick(worker);
        // `snapshot` gathers to worker 0; the other workers' results are empty.
        program
            .root
            .exports
            .iter()
            .map(|e| (e.name.clone(), server.snapshot(worker, &e.name).expect("evaluate: snapshot")))
            .collect()
    })
    .expect("evaluate: worker startup");
    guards
        .join()
        .into_iter()
        .next()
        .expect("evaluate: worker 0")
        .expect("evaluate: worker 0 returned")
}
