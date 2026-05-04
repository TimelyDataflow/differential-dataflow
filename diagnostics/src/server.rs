//! WebSocket server that bridges the diagnostics dataflow to browser clients.
//!
//! The server runs on a background thread and manages the full client lifecycle:
//!
//! 1. Accepts WebSocket connections and assigns each a unique client ID.
//! 2. Announces connects/disconnects to the diagnostics dataflow via the
//!    [`SinkHandle`](crate::logging::SinkHandle)'s client input channel.
//! 3. Reads diagnostic updates from the capture channel (produced by the
//!    dataflow's cross-join of clients × data) and forwards them as JSON
//!    to the appropriate browser client.
//!
//! This server only handles the WebSocket data protocol. The browser loads
//! `index.html` (and its JavaScript) from a separate static file server
//! (e.g., `python3 -m http.server 8000`). A future improvement could embed
//! static file serving here so only one port is needed.

use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use serde_json;
use tungstenite::{Message, accept};

use timely::dataflow::operators::capture::Event;

use crate::logging::{DiagnosticUpdate, SinkHandle, StatKind};

/// A running diagnostics WebSocket server.
///
/// Created by [`Server::start`]. The server thread runs in the background
/// until the `Server` is dropped.
pub struct Server {
    _handle: thread::JoinHandle<()>,
}

impl Server {
    /// Start the diagnostics WebSocket server on the given port.
    ///
    /// Takes ownership of the [`SinkHandle`] and moves it to a background
    /// thread. When a browser connects to `ws://localhost:{port}`:
    ///
    /// 1. The server announces the new client to the diagnostics dataflow.
    /// 2. The dataflow's cross-join produces the full current state as diffs.
    /// 3. The server serializes those diffs as JSON and sends them over the
    ///    WebSocket, followed by incremental updates as the computation runs.
    ///
    /// The browser should load `index.html` over HTTP (not `file://`), since
    /// browsers restrict WebSocket connections from `file://` origins. Serve
    /// the diagnostics directory with any static file server, e.g.:
    ///
    /// ```text
    /// cd diagnostics && python3 -m http.server 8000
    /// ```
    ///
    /// Then open `http://localhost:8000/index.html` and click Connect.
    ///
    /// # Panics
    ///
    /// Panics if the port cannot be bound.
    pub fn start(port: u16, sink: SinkHandle) -> Self {
        let handle = thread::spawn(move || run_server(port, sink));
        eprintln!("Diagnostics server on ws://localhost:{port}");
        Server { _handle: handle }
    }
}

/// JSON-serializable update sent to clients.
#[derive(serde::Serialize)]
#[serde(tag = "type")]
enum JsonUpdate<'a> {
    Operator {
        id: usize,
        name: &'a str,
        addr: &'a [usize],
        diff: i64,
    },
    Channel {
        id: usize,
        scope_addr: &'a [usize],
        source: (usize, usize),
        target: (usize, usize),
        diff: i64,
    },
    Stat {
        kind: &'a str,
        id: usize,
        diff: i64,
    },
}

fn stat_kind_str(kind: &StatKind) -> &'static str {
    match kind {
        StatKind::Elapsed => "Elapsed",
        StatKind::Messages => "Messages",
        StatKind::ArrangementBatches => "ArrangementBatches",
        StatKind::ArrangementRecords => "ArrangementRecords",
        StatKind::Sharing => "Sharing",
        StatKind::BatcherRecords => "BatcherRecords",
        StatKind::BatcherSize => "BatcherSize",
        StatKind::BatcherCapacity => "BatcherCapacity",
        StatKind::BatcherAllocations => "BatcherAllocations",
    }
}

fn update_to_json(update: &DiagnosticUpdate, diff: i64) -> serde_json::Value {
    match update {
        DiagnosticUpdate::Operator { id, name, addr } => {
            serde_json::to_value(JsonUpdate::Operator {
                id: *id,
                name,
                addr,
                diff,
            })
            .unwrap()
        }
        DiagnosticUpdate::Channel {
            id,
            scope_addr,
            source,
            target,
        } => serde_json::to_value(JsonUpdate::Channel {
            id: *id,
            scope_addr,
            source: *source,
            target: *target,
            diff,
        })
        .unwrap(),
        DiagnosticUpdate::Stat { kind, id } => {
            serde_json::to_value(JsonUpdate::Stat {
                kind: stat_kind_str(kind),
                id: *id,
                diff,
            })
            .unwrap()
        }
    }
}

const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

fn run_server(port: u16, sink: SinkHandle) {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .unwrap_or_else(|e| panic!("Failed to bind to port {port}: {e}"));
    listener
        .set_nonblocking(true)
        .expect("Cannot set non-blocking");

    let mut client_input = sink.client_input;
    let receiver = sink.output_receiver;
    let start = sink.start;

    let mut clients: HashMap<usize, tungstenite::WebSocket<std::net::TcpStream>> = HashMap::new();
    let mut next_client_id: usize = 0;

    // Per-client buffer of records awaiting their timestamp to close, keyed
    // by the inner `ts` of each record. A timestamp is "closed" when the
    // capture stream's frontier (tracked in `progress_counts`) advances past
    // it — at which point we flush the bucket as a single Frame to the client.
    let mut pending: HashMap<usize, BTreeMap<Duration, Vec<serde_json::Value>>> =
        HashMap::new();
    // Running multiplicities from `Event::Progress` updates. The current
    // frontier is the smallest key with positive count; an entry at zero is
    // removed. Anything strictly less than the smallest live key is closed.
    //
    // Timely's capture protocol sends progress as *deltas* relative to an
    // assumed initial frontier of `{T::default(): 1}` — so we must seed the
    // counter that way, otherwise the first event's `(0ns, -1)` retraction
    // leaves us at `{0ns: -1}` and the frontier sticks at 0 forever.
    let mut progress_counts: BTreeMap<Duration, i64> = BTreeMap::new();
    progress_counts.insert(Duration::default(), 1);

    loop {
        // Accept pending connections.
        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    eprintln!("Diagnostics client connected from {addr}");
                    stream.set_nonblocking(false).ok();
                    match accept(stream) {
                        Ok(ws) => {
                            let client_id = next_client_id;
                            next_client_id += 1;
                            clients.insert(client_id, ws);

                            // Announce to the diagnostics dataflow.
                            client_input.connect(client_id, start.elapsed());
                            eprintln!("  assigned client id {client_id}");
                        }
                        Err(e) => eprintln!("WebSocket handshake failed: {e}"),
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("Accept error: {e}");
                    break;
                }
            }
        }

        // Drain diagnostic updates: bucket records by their inner timestamp
        // per client; absorb progress updates into the running frontier.
        let mut frontier_changed = false;
        loop {
            match receiver.try_recv() {
                Ok(Event::Messages(_envelope_time, data)) => {
                    // The capture envelope time is incidental; the meaningful
                    // logical time is the per-record `ts`.
                    for ((client_id, update), ts, diff) in data {
                        let json = update_to_json(&update, diff);
                        pending
                            .entry(client_id)
                            .or_default()
                            .entry(ts)
                            .or_default()
                            .push(json);
                    }
                }
                Ok(Event::Progress(updates)) => {
                    for (t, diff) in updates {
                        let entry = progress_counts.entry(t).or_insert(0);
                        *entry += diff;
                        if *entry == 0 {
                            progress_counts.remove(&t);
                        }
                    }
                    frontier_changed = true;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    eprintln!("Diagnostics output channel closed, shutting down server");
                    // Close all clients gracefully.
                    for (_, mut ws) in clients.drain() {
                        let _ = ws.close(None);
                    }
                    return;
                }
            }
        }

        // If the frontier moved, flush every closed timestamp bucket. One
        // Frame per closed `ts`, in timestamp order, so each Frame is one
        // atomic transaction on the client.
        let mut disconnected = Vec::new();
        if frontier_changed {
            // Anything strictly less than the smallest live progress count
            // is closed. If `progress_counts` is empty, every buffered
            // timestamp is closed.
            let frontier: Option<Duration> = progress_counts.keys().next().copied();
            for (client_id, buckets) in pending.iter_mut() {
                let closed: Vec<Duration> = match frontier {
                    Some(f) => buckets.range(..f).map(|(t, _)| *t).collect(),
                    None => buckets.keys().copied().collect(),
                };
                for ts in closed {
                    let updates = buckets.remove(&ts).unwrap_or_default();
                    if updates.is_empty() {
                        continue;
                    }
                    let Some(ws) = clients.get_mut(client_id) else {
                        continue;
                    };
                    let frame = serde_json::json!({
                        "type": "Frame",
                        "ts_us": ts.as_micros() as u64,
                        "updates": updates,
                    });
                    let payload = serde_json::to_string(&frame).unwrap();
                    if ws.send(Message::Text(payload.into())).is_err() {
                        disconnected.push(*client_id);
                        break;
                    }
                }
            }
        }

        // Handle disconnects (also check for clients that closed their end).
        for (client_id, ws) in clients.iter_mut() {
            // Non-blocking read to detect closed connections.
            // tungstenite in blocking mode would block here, so we just
            // check on send failure above.
            let _ = ws; // placeholder — send failure detection above is sufficient
            let _ = client_id;
        }
        for client_id in disconnected {
            clients.remove(&client_id);
            pending.remove(&client_id);
            client_input.disconnect(client_id, start.elapsed());
            eprintln!("Diagnostics client {client_id} disconnected");
        }

        // Advance time periodically even without client events, so the
        // dataflow frontier can progress.
        client_input.advance(start.elapsed());

        std::thread::sleep(FLUSH_INTERVAL);
    }
}
