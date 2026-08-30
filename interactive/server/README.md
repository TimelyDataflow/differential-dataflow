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

The useful commands are `load`, `drop`, `list`, `feed`, `batch`, `bind`,
`unbind`, `peek`, `tail`, `stop`, `tick`, and `exit`. `load` accepts an inline
pipe-syntax program:

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

Several coupled updates can be staged as one feed batch:

    r7 batch begin
    feed world 0 3,4 val=9
    feed audit 0 42 val=3,4,9
    r7 end-batch

The body accepts only bare `feed` commands. `time=` is rejected: every member
uses the current open epoch. The server parses the complete body off-worker,
then the worker validates every program, input number, and write policy before
changing any input handle. A validation error stages nothing. A successful
batch is one worker command, so a command from another session — particularly
`tick` — cannot interleave between its members.

This is atomic staging, not a durable transaction or an implicit commit. The
batch's rows remain invisible in closed-past snapshots until a later `tick`
closes their common epoch; that tick may be issued by any session. A server
failure before the tick loses ordinary in-memory inputs just as it does for a
standalone `feed`. Other sessions may execute commands while the multi-line
body is still being uploaded; the atomic boundary begins only once
`end-batch` turns the body into a worker command.

The stance on contention: **writes are open; policy lives in the dataflow**.
The server does not decide who may write what. Cooperating clients follow a
simple protocol — include your id and an ordering epoch in the data — and
programs resolve races over those facts (first-claim-wins is a `min` over
`(epoch, id)`, see `demo/claims.txt`; full optimistic transactions are a
recursive view, see `demo/txn.txt`). Racing writes settle identically on
every replay. Identity is convention, not enforcement: we are not defending
against adversarial clients yet, and server-side attribution is deliberately
deferred until a deployment needs it.

## Feedback: `bind`

    bind <trace> <prog> <in#>        unbind <trace> <prog> <in#>

From then on, every `tick` delivers the trace's *changes* into that input at
the next epoch, so the input mirrors the trace one epoch delayed. This is the
write path for *programs*: an installed dataflow can act on the world — or on
itself — with no client in the loop, one well-founded recursion step per tick.

The state-machine idiom (see `demo/counter.txt` and the `server_bind` tests):
give the program a seed input and a dedicated feedback input,

    let state = seed + feedback;

and bind the export `f(state) + (seed | negate)` to the feedback input; then
`state(t) = f(state(t-1))`, while later seed feeds still inject as
perturbations. A bound source cannot be dropped (it holds an importer), nor
can the bound target (unbind first).

## Intake gates

Loads are cheap to request and costly to render, so intake is bounded:
`DDIR_MAX_PROGRAM_BYTES` (default 65536) — a larger `load` body is swallowed
and rejected with one error, before parsing. `DDIR_MAX_BATCH_BYTES` (also
default 65536) similarly bounds an accumulated `batch` body; an oversized or
malformed body is swallowed through `end-batch` and produces one error, never
a partial worker command. These are transport self-defense, not semantics.
There are no ownership or quota gates: sessions are trusted, and admission
policy (auth, quotas, rate limits) belongs in a fronting proxy if a deployment
ever needs one.

## Demos

    cargo run -p ddir-server --release
    # then, or piped straight to stdin:
    ./target/release/ddir_server < interactive/server/demo/counter.txt
    ./target/release/ddir_server < interactive/server/demo/claims.txt
    ./target/release/ddir_server < interactive/server/demo/txn.txt
    python3 interactive/server/demo/two_sessions.py   # races + batches + size gate over TCP
    python3 interactive/server/demo/dem/run_dem.py    # equilibrium water on a real Swiss DEM

`load --explain` and `query` are reserved but unimplemented: explanation
support belongs on the scope-tree explanation machinery, and until that lands
the server reports an error rather than giving those commands an improvised
meaning.
