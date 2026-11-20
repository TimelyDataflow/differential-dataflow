# Live DDIR server

A long-running Timely worker group hosts interpreted DDIR dataflows through a
load-run-drop lifecycle. Programs share results by name — each may import
collections that others output, a program installed as `p` publishing its
`output "x"` as `p::x` — and clients follow along over TCP, WebSocket, or stdin.

## Server command reference

For a first session, load a program, feed timestamped updates, seal the contributing inputs, then peek at a closed time. Trim an output when its older snapshots can be released. [Runnable two-input example](#worked-example-a-union-of-two-inputs)

### Write, drive progress, and read

| Command | Intended behavior |
| --- | --- |
| `feed P I KEY [val=V] time=T [diff=D]` | Supply a signed update at T. Text batches and binary frames accept per-row times or one shared header time. Feeding leaves the write frontier in place, so updates may arrive out of time order. |
| `seal P I F` | Advance one input’s write frontier. Every new frontier element must be reachable from the old frontier. A scalar seal to 8 still permits writes at 8. `[]` finishes the input permanently. The reply acknowledges the promise immediately. |
| `spin` | Wait for each output and active tail to reach or pass the meet of its program’s input and import write frontiers. An open input can limit this target. Use this to flush active tails, measure completion time, or sequence work across outputs. A peek waits for its own read. |
| `peek P::OUT [KEY] [as-of T]` | Return a consolidated snapshot of the updates through T, optionally filtered to one key. T must be complete and retained. A successful read replies `ok as-of=T`, even for zero rows. A default scalar peek selects the latest complete retained time, or reports `err` if none exists. After all contributing inputs finish, a default scalar peek reads at `18446744073709551615` (the largest scalar timestamp). The returned point can be supplied to another peek. Multidimensional outputs require a point. |
| `tail P::OUT` | Subscribe to a scalar output. Replay starts at its retained read floor and continues with timestamped changes, including retractions. Use `as corgi` for binary data and progress frames. |
| `stop ID` | Cancel the tail started under request ID on this connection. |
| `trim P::OUT T` | The floor T must be at or before every point of the output’s commanded write frontier. Seal the contributing inputs first. While a narrower import is open, the wider program sees zeros in the coordinates absent from that import, so the floor can advance only in coordinates the import shares. The server waits for this output to reach T, then releases reads before T. Existing importers retain independent holds. `[]` releases all future reads through this handle. |
| `feed P I time=T from SOURCE [diff=D]` | Stamp the file rows or latest complete scalar trace snapshot with destination time T. T can be a product point. |
| `feed P I time=now …` | Stage an untimed row, batch, zero-lane frame, or `from SOURCE` load at the input’s sole frontier element. With zero or multiple capabilities there is no unique write time and the command fails. |

**A keyed peek pays for the snapshot.** The current implementation gathers the output across workers, consolidates it on worker 0, and then filters by key. Returning one row, or even zero rows, can cost work proportional to the whole snapshot. For occasional inspection this can be convenient. For frequent key requests, express the request keys as an input and join them to the data in a maintained dataflow. The server currently exposes no indexed point-read command. [Read implementation](../src/server/read.rs) · [Requests as input rows](#requests-as-input-rows)

Each row carries **one update time**. That time determines which snapshots include its diff. A container carries a separate **Timely message stamp**, the full set of capabilities held by the input when it sends the container. For example, a message stamped with `[[2,0],[0,2]]` can carry one row at `[2,0]` and another at `[0,2]`. The stamp cannot tell a reader which time belongs to which row. `feed` takes that information from the caller.

A write frontier is the set of minimal times at which future writes are possible. Each row’s time must be at or beyond at least one frontier element. Scalars use unsigned 64-bit times. For product time, write coordinates as `[x,y]` and an antichain as `[[x1,y1],[x2,y2]]`, without spaces. Coordinates compare component by component. An explicit peek at T is complete once no contributing frontier element is at or before T. A complete, retained time can yield an empty snapshot. An open or discarded point yields an error.

### Programs and sessions

| Command | Intended behavior |
| --- | --- |
| `load P from PATH` or `load P [width=N] begin` | Install a program and publish its outputs under `P::OUT`. Both forms accept options. Width defaults to one. Inputs start at zero or the join of their imports’ read floors. `I=TRACE` at load time wires an input directly to a published trace and preserves its times. |
| `plan P from PATH` or `plan P begin` | Inspect the preparation stages using the live catalog. The program name labels the report only. |
| `list` | Show installed programs, imports and exports. |
| `diag` | Sample the worker logs and advance the diagnostic inputs. Requires diagnostics to be enabled. |
| `drop P` | Remove a program and its outputs. Release dependent imports and tails first. |
| `exit` | Close the client session. On stdin this also shuts down the server. |

### Replacing global tick

| What the caller used `tick` for | Operation to use |
| --- | --- |
| Close writes through scalar time T | `seal` each contributing input past T. |
| Wait for observable results | Use `spin` to wait for all outputs and active tails. A `peek` waits for its own output. |
| Release old snapshots | `trim` each output to its desired read floor. |
| Sample diagnostics | With diagnostics enabled, `diag` samples worker logs and advances only diagnostic inputs. Peek at the resulting diagnostic views directly. |

Every feed supplies its update times, either in the rows or on the header. Write `time=now` to use the input’s sole frontier element.

## Running the server

Run `cargo run -p ddir-server`, then connect a line-oriented client to TCP port
7777. The same protocol is available
over WebSocket on port 7778. Set `DDIR_BIND` or `DDIR_WS_BIND` to change those
defaults. `DDIR_DIAGNOSTICS=1` turns on diagnostics: the server's own logging
becomes the `diag.*` collections, sampled by `diag` (see `interactive/src/diag.rs`), and
the views in [`diag.ddp`](diag.ddp) are installed as program `diag`, so
`peek diag::time_by_statement` reads time by program statement like any other
trace. `DDIR_WORKERS` selects the number of worker
threads (default 1). `DDIR_PASSES` chooses the passes install runs on each program
(default `all,-projection`; for example `none` or `all,-projection`, with names `dead-code`, `structural`,
`collapse-parent-arranges`, `hoist-invariant`, `compose-maps`,
`projection` and `as-of-drop`; `hoist-arranges` is an alias for `hoist-invariant`).
`dead-code` is on by default and runs before structural fusion in every optimizer
round, removing unread computations and bindings while keeping root inputs.
`all,-projection,-dead-code` disables that cleanup. None is needed to run a program; the server's `plan` command reports with the same choice.

Installed programs render on the Corgi backend, and their data stays in Corgi
columns: fed text is read into columns at each input's declared shape, exports are
kept as traces of Corgi chunks, and imports, `peek` and `tail` read the
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

`load <name> from <path>` installs
a program file (`.ddp`, pipe syntax); `load <name> from <path>
explain` applies the explanation rewrite first (`explain=debug` also taps
every demand collection with an inspect), after which the query input is
the one after the program's own, its rows `(key, (val, q))`, and the demand
sets are `peek`able exports. `feed <prog> <in> time=T from <source>` fills an input from a
file of integer rows. Each worker feeds its shard. The source may also be a published trace, read at its latest complete scalar time, each worker reading its own share of the arrangement, and
`diff=<d>` scales every row: `feed bank 0 time=now from bank::retire diff=-1` deletes from an
input the rows a program outputs as no longer needed (see
[`bench/txn`](../bench/txn/README.md)). `peek <trace> [key]` snapshots a trace or one key of it (the key is read
at the trace's key shape, as a feed reads one), `tail <trace>` follows its changes,
and `seal` advances a single input. `load` also accepts
an inline pipe-syntax program:

    load graph begin
    let edges = input 0 : ((int, int), ());
    output "edges" = edges;
    graph end-load
    feed graph 0 1,2 time=0
    seal graph 0 1
    peek graph::edges as-of 0

A load may bind its program's inputs and imports, as `load <name> from <path> <param>=<trace> ...`.
A parameter is an input, by its name (`edges` in `input "edges" : …`, or `input<N>` if it has none), or an import, by the name it imports.
A bound input reads the trace as an import does, checked against the shape the input declares, and is no longer fed.
For example, `load reach from reach_import.ddp edges=prod::edges` reads `prod`'s output `edges` as `reach`'s input `edges`.
`plan <name> from <path>` prints the same four preparation stages as
`ddir-plan`, using the server's currently published import shapes. The inline
form is `plan <name> begin`, DDP lines, then `<name> end-plan`. Each report line
is an `<id> data` reply, followed by `<id> ok planned ...`. The name labels the
reply; it does not reserve a name or install anything. Unresolved imports and
shape problems appear in the report and mark the reply incomplete.

Add `--timings` as `plan <name> from <path> --timings` or
`plan <name> --timings begin` to include per-stage and per-pass wall times,
call/change counts, and fixed-point rounds. Timings are a display option;
ordinary plan replies remain deterministic.

Planning uses the configured passes, assumes arranged imports, and derives its scalar
as-of point from the read floors of the program’s imports, just as a scalar install does. Reports are not yet
parseable DDP. Command-line load bindings and the explain transform are not
options of `plan`. The report prints compiled lambda graphs.
The command does not run the optional generated-row checks.

## Reads: text, or binary frames

By default, `peek` answers with one `data` line per consolidated row, written as
`<key> [val=<v>] [diff=<d>]`. `tail` sends one `data` line per change, written as
`<key> [val=<v>] time=<epoch> [diff=<d>]`. The key and value use the same
`tuple(…)`, `list(…)`, `inject(lane,payload)`, integer, and float spelling as a
text feed. Unit values omit `val=`, and a diff of 1 omits `diff=`. For example,
`_5 data tuple(inject(0,3),100) val=inject(1,tuple())` is a row that can be
copied into `feed <prog> <in> tuple(inject(0,3),100) val=inject(1,tuple()) time=7`
when the input has the same shape and accepts time 7. Text tail rows already carry
their time. For a batch with one shared time, put `time=T` on the batch header.

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
| `progress` | a `tail`'s frontier advances | little-endian `u64 upper` after all changes before that time, or zero bytes for a completed trace (empty frontier) |

A `data` payload is `u64 rows | u64 lanes | keys | vals | i64*rows diffs |
u64*rows*lanes times`, all little-endian. `keys` and `vals` are each one column in
Corgi's self-describing byte encoding (`corgi::bytes`), so a client reads their
shapes from the bytes. A `peek` frame has no times (`lanes` is 0); a `tail` frame
has one lane containing each change's time. `interactive::runtime::frame` decodes a payload,
and `../bench/ldbc/client.py` has a Python decoder. A `tail` whose client falls more
than 256 MiB of frames behind gets `err client too slow` and `end`, and stops
sending.

## Writes: `feed`

    feed <prog> <in> <key> [val=<v>] time=<t> [diff=<d>]

pushes one update into an input of a loaded program. The input declares
its shapes (`input 0 : ((int, int), ())`), and each value is read at them:
an integer, a decimal number for a float, `tuple(a,b)`, `list(a,b)`, and
`inject(lane,payload)` for a sum. A whole key or value may drop its wrapper:
`1,2` for a tuple of integers, and `_` or nothing for unit. The session reads
the text into columns before the workers see it, and refuses text that is not
of the input's shapes.

An input `<in>` of a program `<prog>` is its position or the name it is written with, as `rows` in `input "rows" : …`, and `<prog> <in>` may also be written `<prog>::<in>`.
The workers resolve the name against the program installed.

For many updates at a single time, put the time on the batch header.

    feed world 0 time=7 begin
    7 val=9
    7 val=-3
    8 val=10 diff=-1
    end-feed

For per-row times, leave the header time out and specify a point on every row.

    feed world 0 begin
    7 val=9 time=8
    8 val=10 time=6 diff=-1
    end-feed

Both forms admit the whole batch atomically. A malformed row names its position
in the error. A row outside the input frontier rejects every shard.

A program that already has the updates as columns can skip the text:

    feed <prog> <in> frame <nbytes>

is followed by exactly `nbytes` bytes. Each row has its own timestamp. The layout is `rows | lanes | keys | vals | diffs | times`,
with one column of unsigned 64-bit integers per coordinate. Wire loads and frames
support up to 64 coordinates. A frame without time lanes needs a header time, either `time=T` or `time=now`. On WebSocket the header line and its bytes are one binary message. The
session checks that the keys and values have the input's declared shapes (any
integer storage will do) and admits the columns as one feed, as above. A frame
of other shapes, not a whole frame, or larger than `DDIR_MAX_FEED_BYTES` is
refused, and its bytes are read past. `../bench/ldbc/client.py` has an encoder,
`encode_frame`. The bench clients (`run.py`, `suite.py`, `update_stream.py`) feed
frames by default, so a benchmark measures the server rather than the client's
text; `--feed text` restores text.

The batch's `ok` means that the complete request was admitted to the ordered
worker stream; it does not wait for every worker to stage its shard. Seal the contributing inputs to close a time. A subsequent peek drains its own read.

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

## Worked example: a union of two inputs

```text
load u begin
let a = input "a" : (int, ());
let b = input "b" : (int, ());
output "both" = a + b;
u end-load
feed u a 42 time=7
seal u a 10
spin
seal u b 5
peek u::both as-of 4
seal u b 8
peek u::both as-of 7
trim u::both 4
```

The first spin returns with `b` still at zero because the union can still receive an update at zero. Sealing `b` to 5 makes the read at 4 complete and empty. The read at 7 returns 42. After trimming, a read at 2 is
refused and time 4 is still available. [TCP regression test](tests/multiworker.rs)

For two-dimensional inputs, `load p width=2 begin` sets the root timestamp width.
`seal p 0 [[2,0],[0,2]]` permits writes at either point and at times reachable
from either, such as `[3,0]` and `[2,2]`. A row at `[1,1]` is already closed.
A binary batch can carry rows from both branches in one container. DDIR checks
the entire batch before admission and stamps each shard’s message with all capabilities
in the input frontier. Sealing to `[[3,0],[0,3]]` closes both earlier points.
`peek p::rows as-of [2,0]` and `peek p::rows as-of [0,2]` can then read them
independently. [Multiworker protocol tests](tests/multiworker.rs)

## Requests as input rows

Request parameters can arrive as ordinary input rows. For example, a row
`(request_id, node)` can join a named graph export by `node`, retaining
`request_id` in the output.
Feed the request at T, seal the contributing inputs past T, and peek at T.
Retract it at a later time. An empty result at a closed time is complete; it does not
need an output row as a completion marker. Keeping the binding instead gives
a standing query whose answer follows later graph changes.

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
    ./target/release/ddir_server < interactive/server/demo/claims.txt
    ./target/release/ddir_server < interactive/server/demo/txn.txt
    python3 interactive/server/demo/two_sessions.py   # races + size gate over TCP

## Benchmarking

The [LDBC-derived workload](../bench/ldbc/README.md) runs parameterized interactive
queries and maintained BI queries together through this server. It includes a
tiny correctness fixture and can read an existing SNB CSV snapshot for timing.
