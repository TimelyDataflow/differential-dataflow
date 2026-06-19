//! A live DDIR server: install interpreted dataflows into one running timely
//! computation and let them share results by name. Multi-worker capable.
//!
//! Usage:
//!   cargo run --release --example ddir_server                      # stdin, 1 worker
//!   cargo run --release --example ddir_server -- <script>          # script, 1 worker
//!   cargo run --release --example ddir_server -- <script> -w4      # script, 4 workers
//!   cargo run --release --example ddir_server -- -w4               # stdin, 4 workers
//!
//! # Where parsing happens
//!
//! Commands are parsed, lowered, and validated **on the main (intake) thread**,
//! not on the workers. A malformed program or value is reported to the prompt
//! and the server keeps running — bad input can never panic a worker. Only a
//! well-typed [`Command`] is handed to worker 0, which injects it into a timely
//! `Sequencer`; the resulting total order is replayed on every worker, so
//! install/tick/drop stay collective while `feed` is applied on worker 0 only
//! (the arrangement's exchange pact routes data to key owners).
//!
//! Commands (one per line; `#` or `--` starts a comment):
//!   install <name> <file>
//!   feed <prog> <in#> <value> [val=<value>] [time=<t>] [diff=<int>]
//!   tick
//!   drop <name>
//!   peek <trace> [key]
//!   list
//!   help
//!   exit | quit
//!
//! A `<value>` is either a comma-separated row of integers (becoming a
//! `Tuple` of `Int`s; `_`/empty = unit), or — for structured/ADT data such as
//! ASTs — any closed scalar term: `tuple(1,2)`, `list(1,2,3)`,
//! `inject(2,tuple(3,4))`, etc. A value is one whitespace-delimited token, so
//! write terms without spaces. `feed`'s value defaults to unit, `time` to the
//! current epoch (use a future `time=` to schedule ahead), and `diff` to +1.

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, TryRecvError};
use std::time::{Duration, Instant};

use timely::synchronization::Sequencer;
use timely::worker::Worker;

use interactive::parse;
use interactive::lower;
use interactive::scope_ir as st;
use interactive::ir::{eval, Value};
use interactive::server::{Command, Server};

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

/// A `<value>`: a comma-separated integer row -> `Tuple`, or a closed scalar
/// term evaluated to a constant `Value` (for ADT-shaped input).
fn parse_value(s: &str) -> Value {
    let s = s.trim();
    if s.is_empty() || s == "_" {
        return Value::unit();
    }
    // Fast path: a plain `a,b,c` integer row.
    if s.chars().all(|c| c.is_ascii_digit() || c == ',' || c == '-' || c.is_whitespace()) {
        if let Ok(ints) = s.split(',').map(|t| t.trim().parse::<i64>()).collect::<Result<Vec<_>, _>>() {
            return Value::Tuple(ints.into_iter().map(Value::Int).collect());
        }
    }
    // Otherwise: a closed scalar term (no `$n` input references).
    eval(&parse::pipe::parse_term(s), &mut Vec::new())
}

/// Render a caught panic payload as a message.
fn panic_msg(e: Box<dyn Any + Send>) -> String {
    if let Some(s) = e.downcast_ref::<&str>() { (*s).to_string() }
    else if let Some(s) = e.downcast_ref::<String>() { s.clone() }
    else { "panic".to_string() }
}

/// Parse one command line into a typed `Command`, on the intake thread.
/// Parsing/lowering that would `panic!` (malformed program or value) is caught
/// and returned as an error, so the prompt survives bad input.
fn parse_command(line: &str) -> Result<Command, String> {
    let toks: Vec<&str> = line.split_whitespace().collect();
    match toks[0] {
        "install" if toks.len() == 3 => {
            let name = toks[1].to_string();
            let file = toks[2].to_string();
            let program = catch_unwind(AssertUnwindSafe(|| load(&file))).map_err(panic_msg)?;
            Ok(Command::Install { name, program })
        }
        "feed" if toks.len() >= 4 => {
            let prog = toks[1].to_string();
            let input: usize = toks[2].parse().map_err(|_| format!("feed: <in#> must be a number, got {:?}", toks[2]))?;
            let key = catch_unwind(AssertUnwindSafe(|| parse_value(toks[3]))).map_err(panic_msg)?;
            let mut val = Value::unit();
            let mut time = None;
            let mut diff = 1i64;
            for t in &toks[4..] {
                if let Some(v) = t.strip_prefix("val=") {
                    val = catch_unwind(AssertUnwindSafe(|| parse_value(v))).map_err(panic_msg)?;
                } else if let Some(s) = t.strip_prefix("time=") {
                    time = Some(s.parse().map_err(|_| format!("feed: time= must be a number, got {:?}", s))?);
                } else if let Some(s) = t.strip_prefix("diff=") {
                    diff = s.parse().map_err(|_| format!("feed: diff= must be an integer, got {:?}", s))?;
                } else {
                    return Err(format!("feed: unrecognized argument {:?}", t));
                }
            }
            Ok(Command::Feed { prog, input, key, val, time, diff })
        }
        "tick" => Ok(Command::Tick),
        "drop" if toks.len() == 2 => Ok(Command::Drop { name: toks[1].to_string() }),
        "peek" if toks.len() == 2 || toks.len() == 3 => {
            let trace = toks[1].to_string();
            let key = match toks.get(2) {
                Some(k) => Some(catch_unwind(AssertUnwindSafe(|| parse_value(k))).map_err(panic_msg)?),
                None => None,
            };
            Ok(Command::Peek { trace, key })
        }
        "list" => Ok(Command::List),
        "help" => Ok(Command::Help),
        "exit" | "quit" => Ok(Command::Exit),
        other => Err(format!("unknown or malformed command {:?} (try `help`)", other)),
    }
}

fn print_help() {
    println!("commands:");
    println!("  install <name> <file>");
    println!("  feed <prog> <in#> <value> [val=<value>] [time=<t>] [diff=<int>]");
    println!("  tick");
    println!("  drop <name>");
    println!("  peek <trace> [key]");
    println!("  list");
    println!("  exit");
}

/// Execute one sequenced command on this worker. Collective commands
/// (`install`/`tick`/`drop`) run on every worker; `feed` and all printing
/// happen on worker 0. Returns `false` for `exit`.
fn dispatch(cmd: &Command, server: &mut Server, worker: &mut Worker) -> bool {
    let w0 = worker.index() == 0;
    match cmd {
        Command::Install { name, program } => match server.install(worker, name, program) {
            Ok(()) => if w0 { println!("installed {:?}", name); },
            Err(e) => if w0 { println!("error: {}", e); },
        },
        Command::Feed { prog, input, key, val, time, diff } => {
            if w0 {
                if let Err(e) = server.feed(prog, *input, key.clone(), val.clone(), *time, *diff) {
                    println!("error: {}", e);
                }
            }
        }
        Command::Tick => {
            server.tick(worker);
            if w0 { println!("tick -> epoch {}", server.epoch()); }
        }
        Command::Drop { name } => match server.drop_program(worker, name) {
            Ok(()) => if w0 { println!("dropped {:?}", name); },
            Err(e) => if w0 { println!("error: {}", e); },
        },
        // Collective: every worker imports its shard; `peek` gathers to worker 0
        // (which prints) and reports an error there if the trace is unknown.
        Command::Peek { trace, key } => {
            if let Err(e) = server.peek(worker, trace, key.clone()) {
                if w0 { println!("error: {}", e); }
            }
        }
        Command::List => if w0 { server.list(); },
        Command::Help => if w0 { print_help(); },
        Command::Exit => return false,
    }
    true
}

fn main() {
    // Quiet the default panic printer on the main (intake) thread, where parser
    // panics are caught and reported as clean `error:` lines; worker-thread
    // panics still print normally.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if std::thread::current().name() != Some("main") {
            default_hook(info);
        }
    }));

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

    // Main thread -> worker 0 command channel, carrying typed commands.
    let (send, recv) = channel::<Command>();
    let recv = Arc::new(Mutex::new(recv));

    let guards = timely::execute_from_args(timely_args.into_iter(), move |worker| {
        let recv = recv.clone();
        let me_zero = worker.index() == 0;
        let mut server = Server::new();
        let mut sequencer: Sequencer<Command> = Sequencer::new(worker, Instant::now());

        let mut done = false;
        let mut sealed = false; // worker 0 has observed the channel closing
        while !done {
            if me_zero && !sealed {
                match recv.lock().expect("command channel poisoned").try_recv() {
                    Ok(cmd) => sequencer.push(cmd),
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        sequencer.push(Command::Exit); // stop every worker together
                        sealed = true;
                    }
                }
            }

            let mut worked = false;
            while let Some(cmd) = sequencer.next() {
                worked = true;
                if !dispatch(&cmd, &mut server, worker) {
                    done = true;
                }
            }

            worker.step();
            if !worked {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }).expect("timely execute failed");

    // Main thread: parse command lines and stream typed commands to worker 0.
    let lines: Box<dyn Iterator<Item = String>> = match script {
        Some(path) => match std::fs::read_to_string(&path) {
            Ok(body) => Box::new(body.lines().map(|s| s.to_string()).collect::<Vec<_>>().into_iter()),
            Err(e) => { eprintln!("cannot read script {:?}: {}", path, e); Box::new(std::iter::empty()) }
        },
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
        match parse_command(line) {
            Ok(cmd) => {
                let is_exit = matches!(cmd, Command::Exit);
                if send.send(cmd).is_err() { break; } // workers gone
                if is_exit { break; }
            }
            Err(e) => println!("error: {}", e), // the prompt survives bad input
        }
    }
    // Closing the channel makes worker 0 broadcast `Exit`.
    drop(send);
    guards.join();
}
