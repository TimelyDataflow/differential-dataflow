# The DDIR server

[Server command reference](../../server/README.md#server-command-reference)


A live instance that hosts interpreted DDIR dataflows in one running timely
computation and lets them **share results by name**. You install dataflows one
at a time; each may `import` traces other dataflows `output`, so a
consumer maintains a computation incrementally over a producer's output. It is
the interpreter-driven successor to the old `dd_server` crate (which hot-loaded
compiled `.so`s); here "install" means render a DDIR program, not `dlopen`.

The implementation is `interactive::server` (the registry + lifecycle), driven
by the `ddir-server` crate in `interactive/server/` — a long-running process
that takes commands over stdin, TCP and WebSocket (its README has the protocol
and the configuration). The session scripts here are piped into its stdin.

## Two kinds of file (don't mix them up)

- **`programs/*.ddp`** — DDIR *programs*: dataflow definitions you `load`.
  The ones here are server-oriented (they use `import`/`output`); the programs
  in `../programs/` read positional `input`s instead, which you fill with
  `feed` (see "Running a program in batch" below).
- **`sessions/*.txt`** — *command scripts*: a stream of server commands
  (`load`/`feed`/`seal`/…) you hand to the server. You do **not** `load` a
  session; you run the server *on* it.

## Running

```
cd interactive
# Run a session script (paths inside the scripts are relative to here):
cargo run --release -p ddir-server < examples/server/sessions/shared_trace.txt

# Four workers (default: one):
DDIR_WORKERS=4 cargo run --release -p ddir-server < examples/server/sessions/shared_trace.txt

# Or interactively: type commands; `exit` (or end of input) stops the server.
cargo run --release -p ddir-server
```

Every response line starts with the request's id (`_7 ok …`, `_7 data …`,
`_7 err …`); the server numbers requests that do not carry one.

## Commands

| command | effect |
|---|---|
| `load <name> from <file> [<in>=<trace> ...] [explain=<arity>[,debug]]` | parse + lower + install a program file under `<name>`, its named inputs (or imports) bound to published traces; optionally after the explanation rewrite (`load <name> begin … end-load` takes the program inline) |
| `feed <prog> <in> <value> [val=<value>] time=<t> [diff=<int>]` | stage an input update |
| `feed <prog> <in> time=now from <file-or-trace>` | fill an input from a published trace or a file of integer rows, sharded across workers |
| `seal <prog> <in> <frontier>` | advance an input’s write frontier; a scalar frontier of 1 closes time 0 |
| `spin` | wait for the commanded progress of outputs and tails |
| `trim <trace> <frontier>` | release older snapshots of the output |
| `open` … `shut` | run the `feed`, `seal`, `trim` and `spin` commands between as one request, with one answer |
| `drop <name>` | evict a program (refused if a live program still imports its trace) |
| `peek <trace> [key] [as-of <t>]` | print a closed, retained snapshot (consolidated across workers), one `data` line per row |
| `tail <trace>` / `stop <reqid>` | follow timestamped changes as the contributing inputs advance |
| `list` | show programs, traces (+ importer counts) |

A program installed as `<name>` publishes each `output "x"` as the trace `<name>::x`, so two programs may name their outputs alike, and one file may be loaded several times under different names.

An input `<in>` is its position or the name it is written with, as `edges` in `input "edges" : …`, and `<prog> <in>` may be written `<prog>::<in>`.

A `<value>` is read at the shape its input declares, **written without spaces**:
an integer, a decimal number for a float, `tuple(a,b)`, `list(1,2,3)`, and
`inject(lane,payload)` for a sum, as in ADT-shaped data such as ASTs. A whole
key or value may drop its wrapper: `1,2` for a tuple of integers, and `_` or
nothing for unit. A feed defaults to a unit value and a diff of +1. Supply the update time explicitly, or use `time=now` when the input frontier has one element.

## Running a program in batch

The programs in `../programs/` (reachability, SCC, stable matching, …) read
positional inputs. A session fills them in bulk and closes epochs; that is the
whole of the old single-program harness, so there is no separate binary:

```
load scc from examples/programs/scc.ddp
feed scc 0 1,2 time=0
feed scc 0 2,1 time=0
seal scc 0 1
peek scc::result as-of 0
exit
```

`feed … time=now from` deals the rows across the workers (each feeds its shard, and
the exchange places every row on its key's owner). A file source is one row per line of
whitespace-separated integers, each becoming `(Tuple[ints], ())`; the
`aoc2023/run.sh` suite drives every AoC program this way. A row `feed` still
serves the small inputs (roots, queries).

`load … explain=<arity>` applies the explanation rewrite before
optimization, treating every source without a declared shape as `arity` key
fields with no value; the
rewritten program has one extra input after its own — the query input, fed as
`(key, val ++ q)` — and outputs one `demand:inputN` trace per source, which
you `peek` as `<name>::demand:inputN`. `,debug` taps every demand collection with an inspect.

## The programs

- **`producer.ddp`** — republishes input 0 as the output `edges`
  (keyed by source), published as `prod::edges` when it is installed as `prod`. A producer whose arrangement others import.
- **`reach_import.ddp`** — input `edges`, bound at load as `edges=prod::edges`, and input `roots`; computes
  reachability in an iterative scope, `output "result"`. A consumer of a shared
  trace.
- **`echo.ddp`** — passes input straight to output via `inspect`; used to show
  values flowing in.
- **`arrange_idem.ddp`** — a minimal e-graph *in DDIR*: congruence closure plus
  one rewrite rule, arrange-idempotence (`arrange(x) ≡ x` when `x` is already
  arranged). Reads a node graph `(id, (op, [children]))` and maintains trace
  `classes` `(id, class-leader)`. Optimizing DDIR by feeding it DDIR's own
  e-nodes — eat-your-own-dogfood.

## The sessions — what to look for

- **`shared_trace.txt`** — the headline: `prod` publishes `prod::edges`, `reach`
  imports it. Feed edges and roots, seal the inputs, and watch `reach` update incrementally;
  add and retract an edge to see `+1`/`-1` changes propagate **across two
  separately-installed dataflows**.
- **`drop.txt`** — the teardown gate: dropping `prod` is refused while `reach`
  reads `prod::edges`; drop the consumer first, then the producer drops cleanly and
  its name frees for reuse.
- **`values.txt`** — structured ADT input (`inject`/`tuple`/`list`), an update
  **scheduled for a future logical time** (`time=3`, visible only once the
  frontier passes it), and a malformed value reported without crashing the server.
- **`peek.txt`** — reading results back out: `peek` a whole trace and a single
  key, and the clean error for an unknown trace.
- **`arrange_idem.txt`** — the e-graph rewrite rule firing **incrementally**:
  build a base graph, then add `arrange(reduce(…))` and watch the rule fold it
  (`+1`), add another arrange and watch saturation carry the fold one hop
  further, then retract and watch the fold undo (`-1`). Equality saturation as a
  differential dataflow.
