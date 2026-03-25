//! Smoke test: run a small DD computation with diagnostics and a WS server.
//!
//! Start this, then open a browser console and connect:
//!   let ws = new WebSocket("ws://localhost:51371");
//!   ws.onmessage = e => console.log(JSON.parse(e.data));
//!
//! You should see operator, channel, and stat updates flowing.

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::time::Duration;

use timely::dataflow::operators::probe::Handle;
use differential_dataflow::input::Input;

use diagnostics::logging;
use diagnostics::server::Server;

fn hash_to_u64<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn edge_for(index: usize, nodes: usize) -> (usize, usize) {
    let h1 = hash_to_u64(&index);
    let h2 = hash_to_u64(&h1);
    ((h1 as usize) % nodes, (h2 as usize) % nodes)
}

fn main() {
    let timer = std::time::Instant::now();

    timely::execute(timely::Config::thread(), move |worker| {
        // Register diagnostics (log_logging = true to see the diagnostics dataflow itself).
        let state = logging::register(worker, true);

        // Start the WebSocket server on worker 0 only.
        // Non-server workers drop the SinkHandle so the client input
        // Replay can advance its frontier.
        let _server = if worker.index() == 0 {
            Some(Server::start(51371, state.sink))
        } else {
            drop(state.sink);
            None
        };

        // Build a user dataflow.
        let nodes = 1000;
        let edges = 2000;
        let batch = 100;

        let mut probe = Handle::new();
        let mut input = worker.dataflow(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            graph
                .map(|(src, _dst)| src)
                .probe_with(&mut probe);
            input
        });

        // Load initial edges.
        for i in 0..edges {
            input.insert(edge_for(i, nodes));
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        if worker.index() == 0 {
            eprintln!("{:?}\tloaded {edges} edges", timer.elapsed());
        }

        // Run rounds of changes, keeping the server alive for browsers to connect.
        for round in 0..usize::MAX {
            let round_timer = std::time::Instant::now();
            for i in 0..batch {
                input.remove(edge_for(round * batch + i, nodes));
                input.insert(edge_for(edges + round * batch + i, nodes));
            }
            input.advance_to(round + 2);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            if worker.index() == 0 && round % 100 == 0 {
                eprintln!("{:?}\t{:?}\tround {round}", timer.elapsed(), round_timer.elapsed());
            }

            // Slow down so there's time to connect a browser.
            std::thread::sleep(Duration::from_millis(10));
        }
    })
    .unwrap();
}
