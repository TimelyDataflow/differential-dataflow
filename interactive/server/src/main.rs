//! ddir_server entry point.
//!
//! One binary hosts a configurable Timely worker group and three transports:
//!   - stdin/stdout (always on, process-wide)
//!   - raw TCP line-protocol on `DDIR_BIND` (default 127.0.0.1:7777)
//!   - WebSocket on `DDIR_WS_BIND` (default 127.0.0.1:7778) — one WS
//!     text message per protocol line; same protocol otherwise.
//!
//! Each client (stdin, TCP, WS) is its own "session" with its own
//! outbound channel, so a `tail` issued by one client streams updates
//! only to that client while other clients continue to operate
//! independently.
//!
//! Threads:
//!   - main: spawns the worker, the TCP listener, and the stdin session;
//!     then waits for the worker to finish.
//!   - worker 0: admits the transport FIFO to the distributed control stream.
//!   - all workers: replay the same registry + dispatch operations.
//!   - per-session reader: parses lines into commands, tags each with this
//!     session's response sender, and wakes the worker through the shared
//!     control handle.
//!   - per-session writer: drains the response channel onto the wire.

mod cmd;
#[path = "loop_.rs"]
mod control_loop;

use mimalloc::MiMalloc;

/// The allocator the retired example driver ran on; the arrangement-heavy paths lean on it.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, SendError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cmd::{prepare, ConnectionId, LineParser, Request};
use interactive::server::RenderBackend;
use timely::scheduling::activate::SyncActivations;

enum ControlEvent {
    Request(Request),
    SessionEnded(ConnectionId),
}

/// Sends one event to the worker and wakes it from Timely's scheduler park.
#[derive(Clone)]
struct ControlHandle {
    events: Sender<ControlEvent>,
    activations: SyncActivations,
}

impl ControlHandle {
    fn send(&self, event: ControlEvent) -> Result<(), SendError<ControlEvent>> {
        self.events.send(event)?;
        // The empty path schedules no operator. It exists only to make
        // `step_or_park` return so the outer control loop can drain `events`.
        let _ = self.activations.activate(Vec::new());
        Ok(())
    }
}

/// Process-wide allocator for per-session ids. Stdin uses 0;
/// subsequent connections get 1, 2, 3, ....
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn alloc_connection_id() -> ConnectionId {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

fn main() {
    let bind_addr = std::env::var("DDIR_BIND").unwrap_or_else(|_| "127.0.0.1:7777".to_string());
    let ws_bind = std::env::var("DDIR_WS_BIND").unwrap_or_else(|_| "127.0.0.1:7778".to_string());
    let workers = std::env::var("DDIR_WORKERS")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("DDIR_WORKERS must be a positive integer")
        })
        .unwrap_or(1);
    assert!(workers > 0, "DDIR_WORKERS must be positive");
    let backend = std::env::var("DDIR_BACKEND")
        .unwrap_or_else(|_| "vec".to_string())
        .parse::<RenderBackend>()
        .unwrap_or_else(|error| panic!("DDIR_BACKEND: {error}"));

    let (event_tx, event_rx) = channel::<ControlEvent>();
    let (activation_tx, activation_rx) = sync_channel(1);
    // `execute` invokes one shared closure on every worker. Only worker 0
    // takes the control receiver; the mutex preserves that unique ownership.
    let event_rx = Arc::new(Mutex::new(Some(event_rx)));
    let guards = {
        let event_rx = event_rx.clone();
        timely::execute(timely::Config::process(workers), move |worker| {
            let events = if worker.index() == 0 {
                activation_tx
                    .send(worker.activations().borrow().sync())
                    .expect("main dropped before worker initialization");
                Some(
                    event_rx
                        .lock()
                        .unwrap()
                        .take()
                        .expect("control receiver taken twice"),
                )
            } else {
                None
            };
            control_loop::run_worker(worker, events, backend);

            // The live server intentionally keeps installed dataflows and
            // diagnostics around. Once every worker observes shutdown, remove
            // them so Timely's post-closure drain can finish normally.
            for id in worker.installed_dataflows() {
                worker.drop_dataflow(id);
            }
        })
        .expect("failed to start Timely workers")
    };
    let control = ControlHandle {
        events: event_tx,
        activations: activation_rx
            .recv()
            .expect("worker exited before publishing its activator"),
    };

    // TCP listener: each accepted connection spawns its own reader and
    // writer pair. Failures to bind are non-fatal — the stdin transport
    // still works.
    let tcp_handle = {
        spawn_listener(
            &bind_addr,
            "tcp",
            control.clone(),
            move |stream, control| run_tcp_session(stream, control).map_err(|e| e.to_string()),
        )
    };

    // WebSocket listener on a separate port. Per-connection session uses
    // a single-thread cooperative read/write loop so the WebSocket isn't
    // shared across threads (tungstenite::WebSocket isn't easily split).
    let ws_handle = {
        spawn_listener(&ws_bind, "ws", control.clone(), move |stream, control| {
            run_ws_session(stream, control).map_err(|e| e.to_string())
        })
    };

    // Stdin session: shares the same control handle; responses go to stdout
    // via this session's per-connection channel.
    let stdin_done = std::thread::spawn({
        let control = control.clone();
        move || run_stdin_session(control)
    });
    drop(control);

    let _ = stdin_done.join();
    let worker_failed = guards.join().into_iter().any(|result| {
        if let Err(error) = result {
            eprintln!("ddir_server: worker failed: {error}");
            true
        } else {
            false
        }
    });
    // The listener threads are parked on accept(); without an explicit
    // shutdown signal, the cleanest exit is to let the OS reap them.
    let _ = tcp_handle;
    let _ = ws_handle;
    std::process::exit(i32::from(worker_failed));
}

/// Spawn a TcpListener accept loop that hands each accepted stream to a
/// per-connection session function. Returns `None` if the bind itself
/// failed (logged, but non-fatal — other transports still work).
fn spawn_listener<F>(
    bind: &str,
    label: &'static str,
    control: ControlHandle,
    session: F,
) -> Option<std::thread::JoinHandle<()>>
where
    F: Fn(TcpStream, ControlHandle) -> Result<(), String> + Send + Sync + 'static,
{
    match TcpListener::bind(bind) {
        Ok(listener) => {
            eprintln!("ddir_server: {} listening on {}", label, bind);
            let session = Arc::new(session);
            Some(std::thread::spawn(move || {
                for incoming in listener.incoming() {
                    match incoming {
                        Ok(stream) => {
                            let control = control.clone();
                            let session = session.clone();
                            std::thread::spawn(move || {
                                if let Err(e) = session(stream, control) {
                                    eprintln!("ddir_server: {} session ended: {}", label, e);
                                }
                            });
                        }
                        Err(e) => eprintln!("ddir_server: {} accept failed: {}", label, e),
                    }
                }
            }))
        }
        Err(e) => {
            eprintln!(
                "ddir_server: {} bind {} failed: {} (other transports still work)",
                label, bind, e
            );
            None
        }
    }
}

/// Run one session against a `BufRead` source and a `Write` sink. Spawns
/// the writer pump, then loops on lines, parses each, tags it with this
/// session's `connection_id`, and forwards to the worker. On return,
/// announces the session's end through the same control FIFO so the workers
/// can tear down any tails this session initiated.
fn run_session<R: BufRead, W: Write + Send + 'static>(
    input: R,
    output: W,
    control: ControlHandle,
    connection_id: ConnectionId,
    on_exit: impl FnOnce() + Send + 'static,
) -> std::io::Result<()> {
    let (resp_tx, resp_rx) = channel::<String>();
    let writer_thread = std::thread::spawn(move || {
        let mut out = output;
        while let Ok(line) = resp_rx.recv() {
            if out.write_all(line.as_bytes()).is_err() {
                break;
            }
            let _ = out.flush();
        }
        on_exit();
    });

    let mut parser = LineParser::new();
    for line in input.lines() {
        let Ok(line) = line else {
            break;
        };
        if let Some((reqid, kind)) = parser.feed(&line) {
            let is_exit = matches!(kind, Ok(cmd::Cmd::Exit));
            let kind = kind.and_then(prepare);
            let req = Request {
                reqid,
                kind,
                resp: resp_tx.clone(),
                connection_id,
            };
            if control.send(ControlEvent::Request(req)).is_err() {
                break;
            }
            // For stdin, `exit` terminates the whole server; for TCP, it
            // terminates only this session. Either way the reader stops.
            if is_exit {
                break;
            }
        }
    }
    // The reader loop is done — the client is gone (or asked to exit).
    // Notify the worker BEFORE joining the writer thread: any live tail
    // operator holds a clone of `resp_tx` inside its inspect closure,
    // which would otherwise keep `resp_rx` open indefinitely. The worker
    // sees the session-ended event, auto-stops those tails, which drops
    // the closure (and its resp_tx clone), letting the writer pump exit.
    drop(resp_tx);
    let _ = control.send(ControlEvent::SessionEnded(connection_id));
    let _ = writer_thread.join();
    Ok(())
}

fn run_stdin_session(control: ControlHandle) {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    // stdin always gets connection_id 0.
    let _ = run_session(stdin.lock(), stdout, control, 0, || {});
}

fn run_tcp_session(stream: TcpStream, control: ControlHandle) -> std::io::Result<()> {
    let connection_id = alloc_connection_id();
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".into());
    eprintln!(
        "ddir_server: tcp client {} (conn={}) connected",
        peer, connection_id
    );
    let reader_stream = stream.try_clone()?;
    let writer_stream = stream;
    let peer_for_exit = peer.clone();
    run_session(
        BufReader::new(reader_stream),
        writer_stream,
        control,
        connection_id,
        move || {
            eprintln!(
                "ddir_server: tcp client {} (conn={}) disconnected",
                peer_for_exit, connection_id
            )
        },
    )
}

/// Run one WebSocket session. Single thread per connection: a short
/// read timeout on the underlying TCP socket lets us interleave reads
/// and writes (draining the per-connection outbound channel between
/// read attempts). tungstenite's `WebSocket` isn't easily split across
/// threads, so this is the cleanest shape.
fn run_ws_session(stream: TcpStream, control: ControlHandle) -> Result<(), tungstenite::Error> {
    let connection_id = alloc_connection_id();
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".into());
    eprintln!(
        "ddir_server: ws client {} (conn={}) connecting",
        peer, connection_id
    );
    let mut ws = match tungstenite::accept(stream) {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!(
                "ddir_server: ws client {} (conn={}) handshake failed: {}",
                peer, connection_id, e
            );
            let _ = control.send(ControlEvent::SessionEnded(connection_id));
            return Ok(());
        }
    };
    // Apply the read timeout to the underlying TCP socket so .read()
    // returns WouldBlock periodically, letting us interleave writes.
    ws.get_ref()
        .set_read_timeout(Some(Duration::from_millis(20)))
        .ok();
    eprintln!(
        "ddir_server: ws client {} (conn={}) connected",
        peer, connection_id
    );

    let (resp_tx, resp_rx) = channel::<String>();
    let mut parser = LineParser::new();
    let mut should_exit = false;

    loop {
        // Drain any pending outbound first.
        while let Ok(line) = resp_rx.try_recv() {
            // WS frames don't carry a trailing newline by convention;
            // strip the one our handlers append.
            let payload = line.trim_end_matches('\n').to_string();
            ws.send(tungstenite::Message::Text(payload.into()))?;
        }

        match ws.read() {
            Ok(tungstenite::Message::Text(text)) => {
                // One WS message may carry multiple lines (e.g., a
                // multi-line load body sent as a single message). Split
                // on '\n' and feed each through the parser.
                for line in text.lines() {
                    if line.trim().is_empty() && !parser.awaiting_body() {
                        continue;
                    }
                    if let Some((reqid, kind)) = parser.feed(line) {
                        let is_exit = matches!(kind, Ok(cmd::Cmd::Exit));
                        let kind = kind.and_then(prepare);
                        let req = Request {
                            reqid,
                            kind,
                            resp: resp_tx.clone(),
                            connection_id,
                        };
                        if control.send(ControlEvent::Request(req)).is_err() {
                            should_exit = true;
                            break;
                        }
                        if is_exit {
                            should_exit = true;
                        }
                    }
                }
            }
            Ok(tungstenite::Message::Close(_)) => break,
            Ok(_) => {} // ping/pong/binary/frame — ignore for v0
            Err(tungstenite::Error::Io(e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // Read timed out; loop back to drain outbound.
            }
            Err(e) => {
                eprintln!(
                    "ddir_server: ws client {} (conn={}) read error: {}",
                    peer, connection_id, e
                );
                break;
            }
        }

        if should_exit {
            break;
        }
    }

    // Final outbound drain before close.
    while let Ok(line) = resp_rx.try_recv() {
        let payload = line.trim_end_matches('\n').to_string();
        let _ = ws.send(tungstenite::Message::Text(payload.into()));
    }
    let _ = ws.close(None);
    eprintln!(
        "ddir_server: ws client {} (conn={}) disconnected",
        peer, connection_id
    );
    let _ = control.send(ControlEvent::SessionEnded(connection_id));
    Ok(())
}
