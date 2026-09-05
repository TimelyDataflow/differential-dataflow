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
  The ones here are server-oriented (they use `import`/`export`); the programs
  in `../programs/` read positional `input`s instead, which you fill with
  `feed` (see "Running a program in batch" below).
- **`sessions/*.txt`** — *command scripts*: a stream of server commands
  (`install`/`feed`/`tick`/…) you hand to the server. You do **not** `install` a
  session; you run the server *on* it.

## Running

```
# Run a session script (add -w4 for four workers):
cargo run --release --example ddir_server -- interactive/examples/server/sessions/shared_trace.txt

# Or interactively (no script arg): type `help`, or `exit`.
cargo run --release --example ddir_server

# Render every installed program on the Corgi columnar backend (default: vec):
cargo run --release --example ddir_server -- --backend=corgi -w4
```

Paths inside the session scripts are relative to the `interactive/` crate
directory, so run from there (as the examples above assume `cargo` is invoked at
the repo root with `--example`; adjust if you `cd interactive` first).

## Commands

| command | effect |
|---|---|
| `install <name> <file> [explain=<arity>[,debug]]` | parse + lower + install a program under `<name>`; optionally after the explanation rewrite |
| `feed <prog> <in#> <value> [val=<value>] [time=<t>] [diff=<int>]` | stage an input update |
| `feed <prog> <in#> from <recipe-or-file>` | fill an input from a recipe (`random:…`, `iota:N`) or a file of integer rows, sharded across workers |
| `tick [n]` | close `n` epochs (default 1), running to quiescence after each; reports the wall-clock time |
| `bind <trace> <prog> <in#>` / `unbind …` | feed a trace's changes back into an input at every tick (one-epoch-delayed feedback) |
| `drop <name>` | evict a program (refused if a live program still imports its trace) |
| `peek <trace> [key]` | print a trace's current contents (consolidated across workers) |
| `list` | show traces (+ importer counts) and installed programs |

A `<value>` is a comma-separated integer row (`1,2` → a tuple; `_`/empty → unit)
or any **closed scalar term, written without spaces** (`inject(2,tuple(3,4))`,
`list(1,2,3)`) for ADT-shaped data such as ASTs. `feed` defaults to value=unit,
`time`=the current epoch (use a future `time=` to schedule ahead), `diff`=+1.

## Running a program in batch

The programs in `../programs/` (reachability, SCC, stable matching, …) read
positional inputs. A session fills them in bulk and closes epochs; that is the
whole of the old single-program harness, so there is no separate binary:

```
install scc ../programs/scc.ddp
feed scc 0 from random:nodes=100000,edges=200000,churn=100
tick          # the initial load: reported as one epoch's time
tick 100      # 100 epochs of 100 replaced edges each, timed together
peek result
exit
```

`feed … from` deals the rows across the workers (each feeds its shard, and
the exchange places every row on its key's owner). A `random:` recipe with
`churn=C` keeps the input changing: every later `tick` retracts the next `C`
rows of its window and adds `C` fresh ones, which is the standing-change regime
programs are benchmarked under. A file source is one row per line of
whitespace-separated integers, each becoming `(Tuple[ints] ; ())`; the
`aoc2023/run.sh` suite drives every AoC program this way. A row `feed` still
serves the small inputs (roots, queries).

`install … explain=<arity>` applies the explanation rewrite before
optimization, treating every source as `arity` key fields with no value; the
rewritten program has one extra input after its own — the query input, fed as
`(key ; val ++ q)` — and exports one `demand:inputN` trace per source, which
you `peek`. `,debug` taps every demand collection with an inspect.

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

Two more primitives let you derive sources *in the language* rather than baking
them in:

- **`iota:N`** — the rows `(0) .. (N-1)`. The minimal index source.
- **`clock`** — a single row holding the current epoch, advanced by one each
  `tick` (an O(1) change, tagged `[clock]` in `list`).
- **`hash(bound, keys…)`** — a scalar builtin: a deterministic draw in
  `[0, bound)` from the key `Int`s (raw non-negative hash if `bound <= 0`).

So `random:…` is really sugar — the composable form is `iota` + `hash`:

```
let edges = import "iota:12" | map( hash(8,$0[0],0) ; hash(8,$0[0],1) );
```

(A `clock`-driven *changing* generator is possible too, but a naive
`iota × clock` spends its time reconciling the input diff; the efficient,
log-work form is left for later.)

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
- **`rand_reach.ddp`** — reachability over a graph derived from `iota` via
  `hash` (the composable form of `random:`).
- **`clock_watch.ddp`** — `inspect`s the `clock` source ticking.
- **`arrange_idem.ddp`** — a minimal e-graph *in DDIR*: congruence closure plus
  one rewrite rule, arrange-idempotence (`arrange(x) ≡ x` when `x` is already
  arranged). Reads a node graph `(id ; op, [children])` and maintains trace
  `classes` `(id ; class-leader)`. Optimizing DDIR by feeding it DDIR's own
  e-nodes — eat-your-own-dogfood.

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
- **`derived.txt`** — a random graph derived in-language from `iota` via `hash`.
- **`clock.txt`** — the `clock` source advancing one step per tick (O(1) deltas).
- **`arrange_idem.txt`** — the e-graph rewrite rule firing **incrementally**:
  build a base graph, then add `arrange(reduce(…))` and watch the rule fold it
  (`+1`), add another arrange and watch saturation carry the fold one hop
  further, then retract and watch the fold undo (`-1`). Equality saturation as a
  differential dataflow.
