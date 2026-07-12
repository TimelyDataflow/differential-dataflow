//! Profiling target: loop FAIR compiled SCC (`strongly_connected_at`, the same enter_at
//! log-bucketing DDIR uses) at a fixed size for samply — the compiled twin of
//! `vec_scc_prof`/`corgi_scc_prof`, for decomposing the DDIR-over-compiled framework tax.
//!
//!   ITERS=5 N=100000 samply record -- .../native_scc_prof

// The suite runs on mimalloc (as a real deployment would — ddir_server does): the
// system allocator was 27-28% of both DDIR backends' SCC profiles. One binary per
// benchmark, so every column (native/fair/vec/corgi) shares the same allocator.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use differential_dataflow::algorithms::graphs::scc::strongly_connected_at;
use differential_dataflow::input::Input;
use timely::dataflow::operators::probe::Handle;

fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

fn main() {
    let nodes: u64 = std::env::var("N").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let iters: usize = std::env::var("ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let n_edges = nodes * 2;
    let mut seed = 0xc0ff_ee42u64;
    let edges: Vec<(usize, usize)> = (0..n_edges)
        .map(|_| ((xorshift(&mut seed) % nodes) as usize, (xorshift(&mut seed) % nodes) as usize))
        .collect();

    for i in 0..iters {
        let edges = edges.clone();
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let mut input = worker.dataflow::<u64, _, _>(|scope| {
                let (input, graph) = scope.new_collection::<(usize, usize), isize>();
                strongly_connected_at(graph, |x| *x as u64).probe_with(&mut probe);
                input
            });
            for &(s, d) in &edges { input.insert((s, d)); }
            input.advance_to(1);
            input.flush();
            while probe.less_than(input.time()) { worker.step(); }
        });
        println!("done native-fair scc n={nodes} iter={i}");
    }
}
