# Live DDIR server

A long-running Timely worker group hosts interpreted DDIR dataflows through a
load-run-drop lifecycle. Programs share results by name — each may import
collections that others export — and clients follow along over TCP, WebSocket,
or stdin.

Run `cargo run -p ddir-server`, then connect a line-oriented client to TCP port
7777. The same protocol is available
over WebSocket on port 7778. Set `DDIR_BIND` or `DDIR_WS_BIND` to change those
defaults. `DDIR_DIAGNOSTICS=1` turns on diagnostics: the server's own logging
becomes the `diag.*` collections, fed at each `tick` (see `interactive/src/diag.rs`), and
the views in [`diag.ddp`](diag.ddp) are installed as program `diag`, so
`peek diag.time_by_statement` reads time by program statement like any other
trace. `DDIR_WORKERS` selects the number of worker
threads (default 1).

Installed programs render on the Corgi backend, and their data stays in Corgi
columns: fed text is read into columns at each input's declared shape, exports are
kept as traces of Corgi chunks, and imports, binds, `peek` and `tail` read the
chunks as containers.

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
a program file (`.ddp`, pipe syntax); `load <name> from <path>
explain` applies the explanation rewrite first (`explain=debug` also taps
every demand collection with an inspect), after which the query input is
the one after the program's own, its rows `(key, (val, q))`, and the demand
sets are `peek`able exports. `feed <prog> <in#> from <source>` fills an input from a
source the server reads itself — a recipe such as `random:nodes=N,edges=E,churn=C`
or `iota:N`, or a file of integer rows — with each worker taking its shard, so
no row crosses the wire; a `churn=C` recipe then replaces `C` rows on every
`tick`. The source may also be a published trace, read as its contents before
the current epoch, each worker reading its own share of the arrangement, and
`diff=<d>` scales every row: `feed bank 0 from retire diff=-1` deletes from an
input the rows a program exports as no longer needed (see
[`bench/txn`](../bench/txn/README.md)). Unlike `bind`, this is one delivery of the
trace's contents, not a standing mirror of its changes. `peek <trace> [key]` snapshots a trace or one key of it (the key is read
at the trace's key shape, as a feed reads one), `tail <trace>` follows its changes,
and `tick [n]` reports the epoch reached and the wall-clock time it took. `load` also accepts
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

## Reads: text, or binary frames

By default, `peek` answers with one `data` line per consolidated row, written as
`<key> [val=<v>] [diff=<d>]`. `tail` sends one `data` line per change, written as
`<key> [val=<v>] time=<epoch> [diff=<d>]`. The key and value use the same
`tuple(…)`, `list(…)`, `inject(lane,payload)`, integer, and float spelling as a
text feed. Unit values omit `val=`, and a diff of 1 omits `diff=`. For example,
`_5 data tuple(inject(0,3),100) val=inject(1,tuple())` is a row that can be
copied into `feed <prog> <in#> tuple(inject(0,3),100) val=inject(1,tuple())`
when the input has the same shape. A tail row's `time=` is also accepted by a
single-row feed. The server refuses writes to a closed epoch, so when feeding a
tail row later, remove or update its `time=`. To use several rows in
`feed … begin`, remove their `time=` fields; the batch takes one epoch.

`peek <trace> [key] as columns` and `tail <trace> as columns` keep the earlier
text dump: one `data` line of Corgi `Debug` columns for a peek
(`keys=… vals=… diffs=[…]`) or each tail batch
(`times=… diffs=… keys=… vals=…`). Column storage can differ between runs and
worker counts for the same answer.

With `as corgi` (`peek <trace> [key] as corgi`, `tail <trace> as corgi`), they
answer with binary frames instead. A frame is a header line
`<id> frame <kind> <nbytes>` followed by exactly `nbytes` bytes on TCP and stdin;
on WebSocket it is one binary message holding both. The kinds:

| Kind | When | Payload |
|---|---|---|
| `schema` | first | text: `trace=<name> key=<shape> val=<shape>` |
| `data` | a `peek`'s answer; a `tail`'s batch of changes | updates as columns, below |
| `progress` | a `tail`'s frontier advances | `u64 upper`: every change before `upper` has been sent |

A `data` payload is `u64 rows | u64 lanes | keys | vals | i64*rows diffs |
u64*rows*lanes times`, all little-endian. `keys` and `vals` are each one column in
Corgi's self-describing byte encoding (`corgi::bytes`), so a client reads their
shapes from the bytes. A `peek` frame has no times (`lanes` is 0); a `tail` frame
has one lane, each change's epoch. `interactive::runtime::frame` decodes a payload,
and `../bench/ldbc/client.py` has a Python decoder. A `tail` whose client falls more
than 256 MiB of frames behind gets `err client too slow` and `end`, and stops
sending.

Only an explicit, ordered `tick [n]` closes epochs. Tails observe progress but
do not cause it, and an idle server has no deadline to service. A future
queue-driven commit policy can seal an epoch as soon as the preceding epoch
retires and intents are waiting; that decision should come from logical queue
state, not elapsed wall-clock time.

## Writes: `feed`

    feed <prog> <in#> <key> [val=<v>] [time=<t>] [diff=<d>]

pushes one update into a loaded program's positional input. The input declares
its shapes (`input 0 : ((int, int), ())`), and each value is read at them:
an integer, a decimal number for a float, `tuple(a,b)`, `list(a,b)`, and
`inject(lane,payload)` for a sum. A whole key or value may drop its wrapper:
`1,2` for a tuple of integers, and `_` or nothing for unit. The session reads
the text into columns before the workers see it, and refuses text that is not
of the input's shapes.

For many updates to one target at the current epoch, frame them as one feed:

    feed world 0 begin
    7 val=9
    7 val=-3
    8 val=10 diff=-1
    end-feed

Each body row is `<key> [val=<v>] [diff=<d>]`; `time=` is intentionally absent
because the enclosing command supplies one epoch. The complete body is admitted
atomically, and workers divide its rows by body position before introducing
them to the dataflow. The body is read into columns at the input's shapes as it
arrives, never held row by row; one row not of those shapes refuses the
whole body, naming the row.

A program that already has the updates as columns can skip the text:

    feed <prog> <in#> frame <nbytes>

is followed by exactly `nbytes` bytes, a `data` frame in the format `peek … as
corgi` answers with, with no time lanes (`lanes` is 0): `rows | 0 | keys | vals |
diffs`. On WebSocket the header line and its bytes are one binary message. The
session checks that the keys and values have the input's declared shapes (any
integer storage will do) and admits the columns as one feed, as above. A frame
of other shapes, not a whole frame, or larger than `DDIR_MAX_FEED_BYTES` is
refused, and its bytes are read past. `../bench/ldbc/client.py` has an encoder,
`encode_frame`. The bench clients (`run.py`, `suite.py`, `update_stream.py`) feed
frames by default, so a benchmark measures the server rather than the client's
text; `--feed text` restores text.

The batch's `ok` means that the complete request was admitted to the ordered
worker stream; it does not wait for every worker to stage its shard. A later
`tick` is the visibility and completion boundary for those rows.

This text framing is a convenience and compatibility path, not the
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

## Requests as input rows

Request parameters can arrive as ordinary input rows. For example, a row
`(request_id, node)` can join a named graph export by `node`, retaining
`request_id` in the output.
Insert the request, `tick`, `peek` its answer, then retract the request and
`tick` again. An empty result is complete when the tick completes; it does not
need an output row as a completion marker. Keeping the binding instead gives
a standing query whose answer follows later graph changes.

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

## Benchmarking

The [LDBC-derived workload](../bench/ldbc/README.md) runs parameterized interactive
queries and maintained BI queries together through this server. It includes a
tiny correctness fixture and can read an existing SNB CSV snapshot for timing.
