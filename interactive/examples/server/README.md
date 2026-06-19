# The DDIR server

A live instance that hosts interpreted DDIR dataflows in one running timely
computation and lets them **share results by name**. You install dataflows one
at a time; each may `import` traces other dataflows have `export`ed, so a
consumer maintains a computation incrementally over a producer's output. It is
the interpreter-driven successor to the old `dd_server` crate (which hot-loaded
compiled `.so`s); here "install" means render a DDIR program, not `dlopen`.

The implementation is `interactive::server` (the registry + lifecycle) driven by
the `ddir_server` example (a command loop). It is multi-worker: commands are
totally ordered across workers by a timely `Sequencer`.

## Two kinds of file (don't mix them up)

- **`programs/*.ddp`** — DDIR *programs*: dataflow definitions you `install`.
  The ones here are server-oriented (they use `import`/`export`), so unlike the
  programs in `../programs/` they are not runnable by the batch `ddir_vec`
  harness.
- **`sessions/*.txt`** — *command scripts*: a stream of server commands
  (`install`/`feed`/`tick`/…) you hand to the server. You do **not** `install` a
  session; you run the server *on* it.

## Running

```
# Run a session script (add -w4 for four workers):
cargo run --release --example ddir_server -- interactive/examples/server/sessions/shared_trace.txt

# Or interactively (no script arg): type `help`, or `exit`.
cargo run --release --example ddir_server
```

Paths inside the session scripts are relative to the `interactive/` crate
directory, so run from there (as the examples above assume `cargo` is invoked at
the repo root with `--example`; adjust if you `cd interactive` first).

## Commands

| command | effect |
|---|---|
| `install <name> <file>` | parse + lower + install a program under `<name>` |
| `feed <prog> <in#> <value> [val=<value>] [time=<t>] [diff=<int>]` | stage an input update |
| `tick` | close the epoch and run to quiescence |
| `drop <name>` | evict a program (refused if a live program still imports its trace) |
| `peek <trace> [key]` | print a trace's current contents (consolidated across workers) |
| `list` | show traces (+ importer counts) and installed programs |

A `<value>` is a comma-separated integer row (`1,2` → a tuple; `_`/empty → unit)
or any **closed scalar term, written without spaces** (`inject(2,tuple(3,4))`,
`list(1,2,3)`) for ADT-shaped data such as ASTs. `feed` defaults to value=unit,
`time`=the current epoch (use a future `time=` to schedule ahead), `diff`=+1.

## Generated (named) sources

A program can `import` a *recipe* name instead of another program's export:

```
let edges = import "random:nodes=8,edges=12";
```

`random:nodes=N,edges=E[,arity=A][,seed=S]` is a deterministic random graph
(rows `(Tuple[a,b] ; ())`, like a raw input). The first import **installs the
generator on demand** — no producer to set up first — and the source is
*content-addressed*: two programs importing the same recipe (in any key order)
share one generated source, generated once. It shows up in `list` tagged
`[generated]`, is not writable (`feed` is refused), and is dropped like any
program once nothing imports it. This is a first step toward unifying `input`
and `import`: a generated source is just an `import` whose data is computed.

## The programs

- **`producer.ddp`** — republishes input 0 as the named trace `edges`
  (keyed by source). A producer whose arrangement others import.
- **`reach_import.ddp`** — `import "edges"` + roots on input 0, computes
  reachability in an iterative scope, `export "result"`. A consumer of a shared
  trace.
- **`echo.ddp`** — passes input straight to output via `inspect`; used to show
  values flowing in.
- **`reach_gen.ddp`** / **`count_gen.ddp`** — two consumers of the *generated*
  source `random:nodes=8,edges=12` (reachability and an edge count); they share
  the one on-demand source.

## The sessions — what to look for

- **`shared_trace.txt`** — the headline: `prod` publishes `edges`, `reach`
  imports it. Feed edges/roots, `tick`, and watch `reach` update incrementally;
  add and retract an edge to see `+1`/`-1` changes propagate **across two
  separately-installed dataflows**.
- **`drop.txt`** — the teardown gate: dropping `prod` is refused while `reach`
  imports `edges`; drop the consumer first, then the producer drops cleanly and
  its name frees for reuse.
- **`values.txt`** — structured ADT input (`inject`/`tuple`/`list`), an update
  **scheduled for a future logical time** (`time=3`, visible only once the
  frontier passes it), and a malformed value reported without crashing the server.
- **`peek.txt`** — reading results back out: `peek` a whole trace and a single
  key, and the clean error for an unknown trace.
- **`generated.txt`** — a random graph imported by recipe, installed on demand
  and shared by two programs (`importers: 2`, one source), then GC'd on drop.
