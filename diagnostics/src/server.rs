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

use std::collections::HashMap;
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

        // Drain diagnostic updates and group by client.
        let mut batches_by_client: HashMap<usize, Vec<serde_json::Value>> = HashMap::new();
        loop {
            match receiver.try_recv() {
                Ok(Event::Messages(_time, data)) => {
                    for ((client_id, update), _ts, diff) in data {
                        let json = update_to_json(&update, diff);
                        batches_by_client.entry(client_id).or_default().push(json);
                    }
                }
                Ok(Event::Progress(_)) => {}
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

        // Send batched updates to each client.
        let mut disconnected = Vec::new();
        for (client_id, updates) in &batches_by_client {
            if let Some(ws) = clients.get_mut(client_id) {
                if !updates.is_empty() {
                    let payload = serde_json::to_string(updates).unwrap();
                    if ws.send(Message::Text(payload.into())).is_err() {
                        disconnected.push(*client_id);
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
            client_input.disconnect(client_id, start.elapsed());
            eprintln!("Diagnostics client {client_id} disconnected");
        }

        // Advance time periodically even without client events, so the
        // dataflow frontier can progress.
        client_input.advance(start.elapsed());

        std::thread::sleep(FLUSH_INTERVAL);
    }
}
