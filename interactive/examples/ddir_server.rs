//! A live DDIR server: install interpreted dataflows into one running timely
//! computation and let them share results by name. Multi-worker capable.
//!
//! Usage:
//!   cargo run --release --example ddir_server                      # stdin, 1 worker
//!   cargo run --release --example ddir_server -- <script>          # script, 1 worker
//!   cargo run --release --example ddir_server -- <script> -w4      # script, 4 workers
//!   cargo run --release --example ddir_server -- -w4               # stdin, 4 workers
//!
//! The optional script path is the first positional argument; everything else
//! is forwarded to timely (e.g. `-w N` for worker count).
//!
//! # How commands reach the workers
//!
//! The main thread reads command lines and hands them to worker 0 over an
//! `mpsc` channel. Worker 0 injects each into a timely [`Sequencer`], which
//! circulates a single *total order* of commands to every worker. So all
//! workers `install`/`tick` the same programs in the same order (collective
//! dataflow construction stays in lockstep), while `feed` is applied on worker
//! 0 only — the arrangement's exchange pact then routes data to the worker that
//! owns each key. This is the legacy `dd_server` pattern, retargeted from
//! `dlopen` to the DDIR interpreter.
//!
//! Commands (one per line; `#` or `--` starts a comment):
//!   install <name> <file>
//!   feed <prog> <in#> <csv> [val=<csv>] [diff=<int>]
//!   tick
//!   drop <name>
//!   list
//!   help
//!   exit | quit

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, TryRecvError};
use std::time::{Duration, Instant};

use timely::synchronization::Sequencer;
use timely::worker::Worker;

use interactive::parse;
use interactive::lower;
use interactive::scope_ir as st;
use interactive::ir::Value;
use interactive::server::Server;

/// Parse, lower, and optimize a program file (`.ddp` = pipe syntax, else applicative).
fn load(path: &str) -> st::Program {
    let src = interactive::load_program(path);
    let stmts = if path.ends_with(".ddp") {
        parse::pipe::parse(&src)
    } else {
        parse::applicative::parse(&src)
    };
    let mut prog = lower::lower_tree(stmts);
    prog.optimize();
    prog
}

/// A comma-separated row of integers as a `Value::Tuple` (`_`/empty = unit).
fn csv(s: &str) -> Value {
    if s.is_empty() || s == "_" {
        Value::unit()
    } else {
        Value::Tuple(s.split(',').map(|t| Value::Int(t.trim().parse::<i64>()
            .unwrap_or_else(|_| panic!("not an integer: {:?}", t)))).collect())
    }
}

/// Apply one sequenced command on this worker. Collective commands
/// (`install`/`tick`) run on every worker; `feed` and all printing happen on
/// worker 0 only. Returns `false` for `exit`/`quit`.
fn dispatch(toks: &[String], server: &mut Server, worker: &mut Worker) -> bool {
    let w0 = worker.index() == 0;
    match toks[0].as_str() {
        "install" if toks.len() == 3 => {
            // Every worker loads + builds collectively; only worker 0 reports.
            let prog = load(&toks[2]);
            match server.install(worker, &toks[1], &prog) {
                Ok(()) => if w0 { println!("installed {:?} from {}", toks[1], toks[2]); },
                Err(e) => if w0 { println!("error: {}", e); },
            }
        }
        "feed" if toks.len() >= 4 => {
            // Apply on worker 0 only; the dataflow exchanges to the key owner.
            if w0 {
                let input: usize = toks[2].parse().expect("feed: <in#> must be a number");
                let key = csv(&toks[3]);
                let mut val = Value::unit();
                let mut diff = 1i64;
                for t in &toks[4..] {
                    if let Some(v) = t.strip_prefix("val=") { val = csv(v); }
                    else if let Some(d) = t.strip_prefix("diff=") { diff = d.parse().expect("feed: diff= must be an integer"); }
                    else { println!("feed: ignoring unrecognized argument {:?}", t); }
                }
                if let Err(e) = server.feed(&toks[1], input, key, val, diff) {
                    println!("error: {}", e);
                }
            }
        }
        "tick" => {
            server.tick(worker);
            if w0 { println!("tick -> epoch {}", server.epoch()); }
        }
        "drop" if toks.len() == 2 => {
            // Collective: every worker tears down its shard.
            match server.drop_program(worker, &toks[1]) {
                Ok(()) => if w0 { println!("dropped {:?}", toks[1]); },
                Err(e) => if w0 { println!("error: {}", e); },
            }
        }
        "list" => if w0 { server.list(); },
        "help" => if w0 {
            println!("commands:");
            println!("  install <name> <file>");
            println!("  feed <prog> <in#> <csv> [val=<csv>] [diff=<int>]");
            println!("  tick");
            println!("  drop <name>");
            println!("  list");
            println!("  exit");
        },
        "exit" | "quit" => return false,
        other => if w0 { println!("unknown command {:?} (try `help`)", other); },
    }
    true
}

fn main() {
    // First positional arg (if it isn't a flag) is the script; the rest go to
    // timely. We always pass a leading dummy so getopts has an argv[0] to skip.
    let mut it = std::env::args();
    let _bin = it.next();
    let mut script: Option<String> = None;
    let mut timely_args: Vec<String> = vec!["ddir_server".to_string()];
    let mut saw_positional = false;
    for a in it {
        if !saw_positional && !a.starts_with('-') {
            script = Some(a);
            saw_positional = true;
        } else {
            saw_positional = true;
            timely_args.push(a);
        }
    }

    // Main thread -> worker 0 command channel.
    let (send, recv) = channel::<Vec<String>>();
    let recv = Arc::new(Mutex::new(recv));

    let guards = timely::execute_from_args(timely_args.into_iter(), move |worker| {
        let recv = recv.clone();
        let me_zero = worker.index() == 0;
        let mut server = Server::new();
        let mut sequencer: Sequencer<Vec<String>> = Sequencer::new(worker, Instant::now());

        let mut done = false;
        let mut sealed = false; // worker 0 has observed the channel closing
        while !done {
            // Worker 0 feeds the shared command log; others only consume it.
            if me_zero && !sealed {
                match recv.lock().expect("command channel poisoned").try_recv() {
                    Ok(line) => sequencer.push(line),
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        // Broadcast a final `exit` so every worker stops together.
                        sequencer.push(vec!["exit".to_string()]);
                        sealed = true;
                    }
                }
            }

            let mut worked = false;
            while let Some(cmd) = sequencer.next() {
                worked = true;
                if !cmd.is_empty() && !dispatch(&cmd, &mut server, worker) {
                    done = true;
                }
            }

            worker.step();
            // Idle backoff: nothing to do but keep the command log circulating.
            if !worked {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }).expect("timely execute failed");

    // Main thread: stream command lines to worker 0.
    let lines: Box<dyn Iterator<Item = String>> = match script {
        Some(path) => {
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read script {:?}: {}", path, e));
            Box::new(body.lines().map(|s| s.to_string()).collect::<Vec<_>>().into_iter())
        }
        None => {
            use std::io::BufRead;
            println!("ddir server — type `help`, or `exit` to quit.");
            Box::new(std::io::stdin().lock().lines().map(|l| l.unwrap()))
        }
    };
    for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("--") {
            continue;
        }
        let toks: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        let is_exit = matches!(toks[0].as_str(), "exit" | "quit");
        if send.send(toks).is_err() { break; } // workers gone
        if is_exit { break; }
    }
    // Closing the channel makes worker 0 broadcast `exit`.
    drop(send);
    guards.join();
}
