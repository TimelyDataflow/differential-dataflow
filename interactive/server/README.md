# Live DDIR server

A long-running Timely worker group hosts interpreted DDIR dataflows through a
load-run-drop lifecycle. Programs share results by name — each may import
collections that others export — and clients follow along over TCP, WebSocket,
or stdin.

Run `cargo run -p ddir-server`, then open `interactive/server/console.html` or
connect a line-oriented client to TCP port 7777. The same protocol is available
over WebSocket on port 7778. Set `DDIR_BIND` or `DDIR_WS_BIND` to change those
defaults. Diagnostics are disabled by default so an idle server can park;
`DDIR_DIAGNOSTICS=1` enables the diagnostics dataflow and its listener on
`DDIR_DIAG_PORT` (default 51371). `DDIR_WORKERS` selects the number of worker
threads (default 1), and `DDIR_BACKEND=vec|corgi` selects the renderer for installed
programs (default `vec`).

One backend is selected for the whole server. The current registry is a
transitional row-speaking bridge: inputs and imports convert from `Value` rows
to Corgi columns at a program boundary, and exports convert back before they
become shareable traces. A Corgi program stays columnar between those
boundaries, but a production Corgi server should replace the bridge with native
columnar inputs and traces.

Worker 0 admits one FIFO control stream and routes one ordered record to every
worker. Small commands are replicated; framed input batches are partitioned by
body position before exchange, so each worker receives only its local typed
shard. Because the one source already defines a total order, command
coordination uses no wall clock or distributed sequencer. Workers execute
commands serially in that order without a physical rendezvous between commands;
Timely progress and probes establish logical completion where it is required.
Response channels remain local to worker 0.

Transport threads wake worker 0 when they enqueue a control event, and Timely
wakes the other workers when their control record arrives. When neither Timely
nor the control plane has work, workers park through the scheduler; the server
does not poll requests with a periodic sleep.

Every request can begin with an arbitrary request id. If omitted, the server
generates one. Responses are `<id> data ...`, followed by `<id> ok ...` or
`<id> err ...`. A `tail` remains active after its `ok` and ends when stopped.
Between commands, blank lines and `#` comment lines are skipped, so command
scripts can be piped to stdin (see `demo/`).

The useful commands are `load`, `drop`, `list`, `feed`, `bind`, `unbind`,
`peek`, `tail`, `stop`, `tick`, and `exit`. `load <name> from <path>` installs
a program file (`.ddp` pipe syntax, anything else applicative); `load <name>
from <path> explain=<arity>[,debug]` applies the explanation rewrite first,
every source taken to have `arity` key fields and no value, after which the
query input is the one after the program's own and the demand sets are
`peek`able exports. `feed <prog> <in#> from <source>` fills an input from a
source the server reads itself — a recipe such as `random:nodes=N,edges=E,churn=C`
or `iota:N`, or a file of integer rows — with each worker taking its shard, so
no row crosses the wire; a `churn=C` recipe then replaces `C` rows on every
`tick`. `peek <trace> [key]` snapshots a trace or one key of it, and `tick [n]`
reports the epoch reached and the wall-clock time it took. `load` also accepts
an inline pipe-syntax program:

    load graph begin
    let edges = import "random:nodes=8,edges=12,seed=1,churn=1";
    export "graph.edges" = edges;
    graph end-load
    tail graph.edges
    tick

A binding may also be spelled as a call, so
`edges=random(seed=1,arity=2,range=8,count=12,churn=1)` redirects the local
import named `edges` to the same content-addressed source as the
`random:...` form. Such a source is deterministic: it begins with a
fixed-size window into an infinite hash-derived sequence and replaces
`churn` rows on every tick.

Only an explicit, ordered `tick [n]` closes epochs. Tails observe progress but
do not cause it, and an idle server has no deadline to service. A future
queue-driven commit policy can seal an epoch as soon as the preceding epoch
retires and intents are waiting; that decision should come from logical queue
state, not elapsed wall-clock time.

## Writes: `feed`

    feed <prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]

pushes one update into a loaded program's positional input, exactly as in the
`interactive::server` tests (`1,2` → a tuple; `_` → unit; a closed scalar term such
as `inject(2,tuple(3,4))` for ADT-shaped rows).

For many updates to one target at the current epoch, frame them as one feed:

    feed world 0 begin
    7 val=9
    7 val=-3
    8 val=10 diff=-1
    end-feed

Each body row is `<key> [val=<v>] [diff=<d>]`; `time=` is intentionally absent
because the enclosing command supplies one epoch. The complete body is admitted
atomically, and workers divide its rows by body position before introducing
them to the dataflow. Partitioning happens before worker transport, avoiding a
full `Value` batch clone on every worker. Text parsing and the row-to-column
boundary remain visible optimization opportunities rather than hidden protocol
behavior.

The batch's `ok` means that the complete request was admitted to the ordered
worker stream; it does not wait for every worker to stage its shard. A later
`tick` is the visibility and completion boundary for those rows.

This text-to-`Value` framing is a convenience and compatibility path, not the
intended representation for high-volume ingestion. Large or already-columnar
payloads should be acquired and partitioned through the data plane, with only a
small descriptor entering the ordered control stream.

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

Multi-line intake is bounded: `DDIR_MAX_PROGRAM_BYTES` (default 65536) caps a
`load`, and `DDIR_MAX_FEED_BYTES` (default 16 MiB) caps a framed `feed`. An
oversized or malformed body is swallowed through its terminator and rejected
with one error; no partial command reaches a worker. This is transport
self-defense, not semantics. There are no ownership or quota gates: sessions
are trusted, and admission policy (auth, quotas, rate limits) belongs in a
fronting proxy if a deployment ever needs one.

## Demos

    cargo run -p ddir-server --release
    # then, or piped straight to stdin:
    ./target/release/ddir_server < interactive/server/demo/counter.txt
    ./target/release/ddir_server < interactive/server/demo/claims.txt
    ./target/release/ddir_server < interactive/server/demo/txn.txt
    python3 interactive/server/demo/two_sessions.py   # races + size gate over TCP

`load --explain` and `query` are reserved but unimplemented: explanation
support belongs on the scope-tree explanation machinery, and until that lands
the server reports an error rather than giving those commands an improvised
meaning.
