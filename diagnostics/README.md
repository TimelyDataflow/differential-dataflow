# diagnostics

A diagnostics crate for differential / timely dataflow programs.
Captures live operator, channel, and arrangement state into a DD
computation, exposes it over a WebSocket, and ships two browser
frontends.

## Wiring it into a program

```rust
use diagnostics::{logging, server::Server};

timely::execute_from_args(std::env::args(), move |worker| {
    let state = logging::register(worker, /* log_logging */ false);
    // Worker 0 owns the WebSocket server; others drop their sink so
    // the dataflow's input frontiers can advance.
    let _server = if worker.index() == 0 {
        Some(Server::start(51371, state.sink))
    } else {
        drop(state.sink);
        None
    };

    // ... your computation ...
})
```

See `examples/scc-bench.rs` for a complete instrumented program.

## Viewing diagnostics

The server prints a hint on startup. The two options:

### Single-file (no build step)

```
cd diagnostics
python3 -m http.server 8000
```

Open `http://localhost:8000/index.html`, click Connect.

This is the path of least resistance — `index.html` is one file you
can scp anywhere. No tooling required.

### Console (React + TanStack DB)

```
cd diagnostics/console
npm install      # first time
npm run dev
```

Open the URL Vite prints, click Connect. The console adds incremental
filtering, a per-frame transactional commit boundary, and a
forward-looking architecture; see `console/README.md` for details.
The default port `5173` of Vite means simultaneous use with the
single-file UI on port 8000 is fine.

Both UIs consume the same wire format and can connect to the same
running server side by side.

## Wire format

Each WebSocket message is one `Frame` envelope:

```json
{ "type": "Frame", "ts_us": <u64>, "updates": [...] }
```

The server emits a Frame only after the dataflow's frontier has
advanced past `ts_us`, so each Frame is a transactionally complete
view at one closed logical timestamp. Update variants inside
`updates` are tagged unions for `Operator`, `Channel`, and `Stat`;
see `src/server.rs::JsonUpdate` for the schema.
