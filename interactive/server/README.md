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
Between commands, blank lines and `#` comment lines are skipped, so annotated
command scripts can be piped straight to stdin.

The useful commands are `load`, `drop`, `list`, `peek`, `tail`, `stop`, `tick`,
and `exit`. `load` accepts an inline pipe-syntax program:

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

`load --explain` and `query` are reserved but unimplemented: explanation
support belongs on the scope-tree explanation machinery, and until that lands
the server reports an error rather than giving those commands an improvised
meaning.
