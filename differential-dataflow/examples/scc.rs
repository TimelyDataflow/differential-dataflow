//! Minimal SCC benchmark, no diagnostics or logging.
//!
//! Usage: scc [timely args] [nodes [edges [batch [rounds]]]]
//!
//! The COMPILED counterpart of `interactive/examples/ddir`'s `scc.ddp`: the edge stream is
//! byte-identical to ddir's synthetic generator (`interactive::gen_row` with arity 2, seed 0),
//! and label introduction is staged by `priority_round(label)` — the same log-bucket scheme the
//! interpreted backends give `enter_at($1[0])`, so rounds match exactly. Comparing either interpreted
//! backend against this example is then a same-input, same-algorithm comparison; the earlier
//! form of this example ran the unprioritized default, which is several times slower and not
//! what the interpreted programs compute.
//!
//! Run single-worker for benchmarking: prioritized propagation inside SCC's nested scopes has
//! been observed to trip a timely progress defect under >= 3 workers with CPU contention (see
//! `strongly_connected_at`).

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::algorithms::graphs::propagate::priority_round;
use differential_dataflow::algorithms::graphs::scc::strongly_connected_at;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// `interactive`'s row hash (splitmix-style), replicated so the edge stream is byte-identical
/// to `ddir`'s `gen_row(edge_index, nodes, 2)` without a dependency on that crate.
fn hash_u64(index: u64) -> u64 {
    let mut x = index.wrapping_mul(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn edge_for(index: usize, nodes: usize) -> (usize, usize) {
    let base = (index as u64).wrapping_mul(31);
    (
        (hash_u64(base) % nodes as u64) as usize,
        (hash_u64(base.wrapping_add(1)) % nodes as u64) as usize,
    )
}

fn main() {
    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let positional: Vec<String> = std::env::args()
            .skip(1)
            .filter(|a| !a.starts_with('-'))
            .collect();
        let nodes: usize = positional.get(0).and_then(|s| s.parse().ok()).unwrap_or(100_000);
        let edges: usize = positional.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
        let batch: usize = positional.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000);
        let rounds: usize = positional.get(3).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

        if worker.index() == 0 {
            println!("nodes: {nodes}, edges: {edges}, batch: {batch}, rounds: {}, workers: {}",
                if rounds == usize::MAX { "∞".to_string() } else { rounds.to_string() },
                worker.peers());
        }

        let mut probe = Handle::new();
        let mut input = worker.dataflow(|scope| {
            let (input, graph) = scope.new_collection::<(usize, usize), isize>();
            let _scc = strongly_connected_at(graph, |n| priority_round(*n as u64)).probe_with(&mut probe);
            input
        });

        let index = worker.index();
        let peers = worker.peers();

        // Load initial edges (partitioned across workers).
        let timer_load = std::time::Instant::now();
        for i in (0..edges).filter(|i| i % peers == index) {
            input.insert(edge_for(i, nodes));
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        if index == 0 {
            println!("{:?}\t{:?}\tloaded {edges} edges", timer.elapsed(), timer_load.elapsed());
        }

        // Apply changes in rounds.
        for round in 0..rounds {
            let timer_round = std::time::Instant::now();
            for i in (0..batch).filter(|i| i % peers == index) {
                input.remove(edge_for(round * batch + i, nodes));
                input.insert(edge_for(edges + round * batch + i, nodes));
            }
            input.advance_to(round + 2);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            if index == 0 {
                println!("{:?}\t{:?}\tround {round} ({} changes)",
                    timer.elapsed(), timer_round.elapsed(), batch * 2);
            }
        }
    }).unwrap();

    println!("{:?}\tshut down", timer.elapsed());
}
