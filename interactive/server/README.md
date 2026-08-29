# Live DDIR server

One long-running timely worker hosts interpreted DDIR dataflows through a
load-run-drop lifecycle. Programs share results by name — each may import
collections that others export — and clients follow along over TCP,
WebSocket, or stdin.

Run `cargo run -p ddir-server`, then open `interactive/server/console.html` or
connect a line-oriented client to TCP port 7777. The same protocol is available
over WebSocket on port 7778. Set `DDIR_BIND`, `DDIR_WS_BIND`, or
`DDIR_TICK_MS` to change those defaults; `DDIR_TICK_MS=0` disables automatic
progress while subscriptions are active. The current `diagnostics` crate is
connected on `DDIR_DIAG_PORT` (default 51371).

Every request can begin with an arbitrary request id. If omitted, the server
generates one. Responses are `<id> data ...`, followed by `<id> ok ...` or
`<id> err ...`. A `tail` remains active after its `ok` and ends when stopped.
Between commands, blank lines and `#` comment lines are skipped, so command
scripts can be piped to stdin (see `demo/`).

The useful commands are `load`, `drop`, `list`, `feed`, `peek`, `tail`,
`stop`, `tick`, and `exit`. `load` accepts an inline pipe-syntax program:

    load graph begin
    let edges = import "random:nodes=8,edges=12,seed=1,churn=1";
    export "graph.edges" = edges;
    graph end-load
    tail graph.edges

A binding may also be spelled as a call, so
`edges=random(seed=1,arity=2,range=8,count=12,churn=1)` redirects the local
import named `edges` to the same content-addressed source as the
`random:...` form. Such a source is deterministic: it begins with a
fixed-size window into an infinite hash-derived sequence and replaces
`churn` rows on every tick.

Automatic ticking happens only while at least one tail is active. This makes a
live demonstration move without assigning input durability semantics to DDIR.
Explicit `tick [n]` remains available for reproducible sessions. Treat
auto-tick as demo furniture rather than a design commitment: as specified,
observation advances time (an observer effect), and the alternative — that a
watcher must be present to move things along, by ticking or by running a
metronome client whose ticks are ordinary logged commands — may be the better
design once the server has real tenants.

## Writes: `feed`

    feed <prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]

pushes one update into a loaded program's positional input, exactly as in the
`ddir_server` example (`1,2` → a tuple; `_` → unit; a closed scalar term such
as `inject(2,tuple(3,4))` for ADT-shaped rows).

The stance on contention: **writes are open; policy lives in the dataflow**.
The server does not decide who may write what. Cooperating clients follow a
simple protocol — include your id and an ordering epoch in the data — and
programs resolve races over those facts (first-claim-wins is a `min` over
`(epoch, id)`, see `demo/claims.txt`; full optimistic transactions are a
recursive view, see `demo/txn.txt`). Racing writes settle identically on
every replay. Identity is convention, not enforcement: we are not defending
against adversarial clients yet, and server-side attribution is deliberately
deferred until a deployment needs it.

## One gate

Loads are cheap to request and costly to render, so intake is bounded:
`DDIR_MAX_PROGRAM_BYTES` (default 65536) — a larger `load` body is swallowed
and rejected with one error, before parsing. This is transport self-defense,
not semantics. There are no ownership or quota gates: sessions are trusted,
and admission policy (auth, quotas, rate limits) belongs in a fronting proxy
if a deployment ever needs one.

## Demos

    cargo run -p ddir-server --release
    # then, or piped straight to stdin:
    ./target/release/ddir_server < interactive/server/demo/claims.txt
    ./target/release/ddir_server < interactive/server/demo/txn.txt
    python3 interactive/server/demo/two_sessions.py   # races + size gate over TCP

`load --explain` and `query` are reserved but unimplemented: explanation
support belongs on the scope-tree explanation machinery, and until that lands
the server reports an error rather than giving those commands an improvised
meaning.
