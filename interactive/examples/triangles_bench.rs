//! Triangles in a random graph, by binary joins or by a delta query of half joins. The binary
//! joins arrange the two-paths; the delta query arranges only the edges, by source, by
//! destination, and by themselves. Reports the time and memory to load the graph, and then the
//! time per tick as edges are replaced.
//!
//! ```text
//! cargo run --release --example triangles_bench -- [join|delta] [edges] [nodes] [churn] [ticks] [workers]
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Counts live heap bytes and their peak, to tell the heap's size from the process's.
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(live, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

fn heap() -> String {
    format!("heap {:.1} MiB (peak {:.1} MiB)", LIVE.load(Ordering::Relaxed) as f64 / 1048576.0, PEAK.load(Ordering::Relaxed) as f64 / 1048576.0)
}

use interactive::server::Server;
use interactive::{lower, parse};

const EDGES: &str = r#"
    let e = input 0 : ((int, int) ; ()) | key(((x, y), ()) -> (x, y));
    let r = e | key((x, y) -> (y, x));
    let s = e | key((x, y) -> ((x, y), ()));
"#;

/// Each triangle `(a, b, c)` has edges `(a, b)`, `(b, c)`, and `(a, c)`.
const JOIN: &str = r#"
    let paths = r | join(e, (b, a, c) -> ((a, c), b));
    let tri = paths | join(s, ((a, c), b, ()) -> ((a, b, c), ()));
    export "count" = tri | key((_, _) -> ((), ())) | count;
"#;

/// One path of half joins per edge of the triangle, each meeting the earlier edges' changes at or
/// before its own time and the later edges' strictly before.
const DELTA: &str = r#"
    let d1 = r | lift | half_join(e, 1, (b, a, c) -> ((a, c), b)) | half_join(s, 1, ((a, c), b, ()) -> ((a, b, c), ()));
    let d2 = e | lift | half_join(r, 0, (b, c, a) -> ((a, c), b)) | half_join(s, 1, ((a, c), b, ()) -> ((a, b, c), ()));
    let d3 = e | lift | half_join(e, 0, (a, c, b) -> ((b, c), a)) | half_join(s, 0, ((b, c), a, ()) -> ((a, b, c), ()));
    let tri = (d1 + d2 + d3) | delay((_, (_, t)) -> t) | key((abc, (v, _)) -> (abc, v));
    export "count" = tri | key((_, _) -> ((), ())) | count;
"#;

/// This process's memory in MiB: on macOS its `footprint`, elsewhere its RSS.
fn memory_mib() -> Option<f64> {
    let out = std::process::Command::new("footprint").args(["-p", &std::process::id().to_string()]).output();
    if let Ok(out) = out {
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        let mut words = text.split("Footprint:").nth(1)?.split_whitespace();
        let n: f64 = words.next()?.parse().ok()?;
        return match words.next()? {
            "B" => Some(n / (1024.0 * 1024.0)),
            "KB" => Some(n / 1024.0),
            "MB" => Some(n),
            "GB" => Some(n * 1024.0),
            _ => None,
        };
    }
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib: f64 = status.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024.0)
}

fn mem() -> String {
    memory_mib().map_or_else(|| "n/a".to_string(), |m| format!("{m:>7.1} MiB"))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let method = args.next().unwrap_or("delta".into());
    let nums: Vec<u64> = args.map(|a| a.parse().expect("integer argument")).collect();
    let edges = nums.first().copied().unwrap_or(1_000_000);
    let nodes = nums.get(1).copied().unwrap_or(100_000);
    let churn = nums.get(2).copied().unwrap_or(1_000);
    let ticks = nums.get(3).copied().unwrap_or(20);
    let workers = nums.get(4).copied().unwrap_or(1) as usize;
    let query = match method.as_str() {
        "join" => JOIN,
        "delta" => DELTA,
        other => panic!("unknown method {other:?}: join or delta"),
    };
    let source = format!("{EDGES}{query}");

    timely::execute(timely::Config::process(workers), move |worker| {
        let index = worker.index();
        let mut server = Server::new();
        let mut program = lower::lower_tree(parse::pipe::parse(&source));
        program.optimize();
        server.install(worker, "p", &program).unwrap();
        server.load(worker, "p", 0, &format!("random:nodes={nodes},edges={edges},churn={churn}")).unwrap();
        let loaded = Instant::now();
        server.tick(worker);
        let count = |server: &mut Server, worker: &mut timely::worker::Worker| -> i64 {
            server.snapshot(worker, "count").unwrap().iter().map(|(_, v, d)| match v { interactive::ir::Value::Int(n) => n * d, _ => 0 }).sum()
        };
        // A snapshot is a dataflow, so every worker takes part; worker 0 receives the rows.
        let elapsed = loaded.elapsed();
        let n = count(&mut server, worker);
        if index == 0 {
            println!("{method}: {edges} edges over {nodes} nodes, {workers} workers");
            println!("load:  {:>9.1} ms  mem {}  {}  count {n}", elapsed.as_secs_f64() * 1e3, mem(), heap());
        }
        let churned = Instant::now();
        for _ in 0..ticks { server.tick(worker); }
        let elapsed = churned.elapsed();
        let n = count(&mut server, worker);
        if index == 0 {
            println!(
                "churn: {:>9.2} ms/tick over {ticks} ticks ({churn} edges replaced per tick)  mem {}  count {n}",
                elapsed.as_secs_f64() * 1e3 / ticks as f64, mem(),
            );
        }
    })
    .unwrap();
}
