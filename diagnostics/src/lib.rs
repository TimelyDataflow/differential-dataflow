//! Diagnostics and visualization for timely and differential dataflow.
//!
//! This crate provides a live diagnostics console for timely and differential
//! dataflow computations. It captures logging events, maintains them as
//! differential dataflow collections with indexed arrangements, and serves
//! them to browser clients over WebSocket.
//!
//! # Quick start
//!
//! ```ignore
//! use diagnostics::{logging, server::Server};
//!
//! timely::execute(config, |worker| {
//!     // Register diagnostics logging on each worker.
//!     let state = logging::register(worker, false);
//!
//!     // Start the WebSocket server on worker 0 only.
//!     // Other workers drop their sink handles (diagnostics collections
//!     // are still maintained, but not served).
//!     let _server = if worker.index() == 0 {
//!         Some(Server::start(51371, state.sink))
//!     } else {
//!         None
//!     };
//!
//!     // Build your dataflow as usual...
//!     // worker.dataflow(|scope| { ... });
//! });
//! ```
//!
//! To view the diagnostics, serve the included `index.html` over HTTP
//! (browsers restrict WebSocket connections from `file://` URLs):
//!
//! ```text
//! cd diagnostics && python3 -m http.server 8000
//! ```
//!
//! Then open `http://localhost:8000/index.html` and click Connect.
//! The browser connects to `ws://localhost:51371` for live data.
//!
//! With multiple workers (`-w N`), all workers' diagnostics are exchanged
//! to worker 0 via the DD arrangements, so the browser sees everything.
//!
//! # Architecture
//!
//! [`logging::register`] builds a dataflow that:
//! 1. Captures timely and differential logging events via [`EventLink`](timely::dataflow::operators::capture::EventLink) pairs.
//! 2. Demuxes them into typed DD collections (operators, channels, schedule elapsed, message counts, arrangement stats).
//! 3. Arranges them into indexed traces for persistence.
//! 4. Cross-joins all collections with a client input: when a browser connects, the join naturally produces the full current state as a batch of diffs, followed by incremental updates.
//! 5. Captures the output into an `mpsc` channel for the WebSocket thread.
//!
//! [`server::Server`] runs a WebSocket server that:
//! - Accepts browser connections and assigns client IDs.
//! - Announces connect/disconnect to the dataflow via the client input channel.
//! - Reads diagnostic updates from the capture channel and routes them as JSON to the appropriate browser client.
//!
//! The browser-side `index.html` simply applies the diffs to maintain local
//! state and renders the dataflow graph — no client-side aggregation needed.

pub mod logging;
pub mod server;
