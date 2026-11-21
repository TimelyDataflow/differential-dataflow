//! `evaluate`: a program on explicit inputs through a throwaway server, the entry point the
//! test suites build on.

use super::*;

/// Evaluate `program` on explicit inputs through a throwaway server: every
/// export's consolidated final contents (rows with non-zero net multiplicity),
/// by name.
///
/// This is the data-in/data-out entry point the test suites build on, and it
/// takes the same path a live install does — `install`, one `feed` of each
/// numbered input (`inputs[i]` holds `input i`'s rows, one feed line each,
/// `<key> [val=<v>] [diff=<d>]`, dealt round-robin across the workers), then each input closes. Each export is read at zero.
/// `config` picks the worker group: `Config::process(n)` hands
/// exchanged containers between threads as typed values, while
/// `CommunicationConfig::ProcessBinary(n)` sends every one through the wire
/// format. The answer must not depend on either choice.
pub fn evaluate(
    config: timely::Config,
    program: &st::Program,
    inputs: &[Vec<String>],
) -> std::collections::BTreeMap<String, Updates> {
    let inputs = inputs.iter().enumerate().map(|(i, rows)| (InputId::Number(i), rows.clone())).collect();
    evaluate_labelled(config, program, &inputs)
}

/// [`evaluate`], with each input's rows by its label, for named inputs too.
pub fn evaluate_labelled(
    config: timely::Config,
    program: &st::Program,
    inputs: &std::collections::BTreeMap<InputId, Vec<String>>,
) -> std::collections::BTreeMap<String, Updates> {
    let program = program.clone();
    let inputs = inputs.clone();
    let guards = timely::execute(config, move |worker| {
        let mut server = Server::new();
        server.install(worker, "evaluate", &program).expect("evaluate: install");
        let labels: std::collections::BTreeSet<InputId> = program.root.imports.iter().filter_map(|imp| match &imp.from {
            st::Source::Input(input) => Some(input.clone()),
            _ => None,
        }).collect();
        for input in labels {
            if let Some(rows) = inputs.get(&input) {
                let shard: Vec<&str> = rows.iter().skip(worker.index()).step_by(worker.peers()).map(String::as_str).collect();
                server.feed("evaluate", input.clone(), &shard.join("\n"), 0).expect("evaluate: feed");
            }
            server.seal("evaluate", input, &Antichain::new()).expect("evaluate: seal");
        }
        // `snapshot` gathers to worker 0; the other workers' results are empty.
        program
            .root
            .exports
            .iter()
            .map(|e| (e.name.clone(), server.snapshot_as_of(worker, &output_name("evaluate", &e.name), &host_time(0)).expect("evaluate: snapshot")))
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
