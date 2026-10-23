# DDIR

An intermediate representation of differential dataflow suitable for interpretation.

## Background

Differential dataflow is a framework that supports declarative computation over continually changing data.
One of its primary irritants is its compile times, due to an over-indulgence in monomorphization.
This project looks at isolating an interpretable core, which lays the groundwork for experimentation in different concrete syntaxes, optimization, and back-ends.

## An example: reachability

Here's an example that performs graph reachability:

```
let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> (src, dst));
let roots = input 1 : ((int), ()) | map(((n), ()) -> (n, ()));

reach: {
    let label = reach | join(edges, (_, _, dst) -> (dst, ()));
    var reach = roots + label | distinct;
}

export "result" = reach::reach | map(_ -> ((), ())) | arrange | inspect(total);

```

Informally, this computation starts from graph `edges` and nodes `roots`, and repeatedly expands a set of nodes that can be reached from each root.
The details of this example are somewhat arbitrary, but the `let`, `var`, and `{ .. }` bits are quite important.
Everything else does have a role, but what they do and how they are expressed is intentionally fluid.
If you are familiar with differential dataflow, they correspond to various of its transformation operators.

## Syntax and Semantics

Programs in DDIR are structured as a tree of nested "iterative scopes", below a root that does not iterate.
Within each named scope one can

1. let-bind names to expressions,
2. name and bind iteration variables,
3. create further nested scopes.

The root can do the same except bind iteration variables: a `var` belongs in a named scope, so what a program exports is what it computes at each time, not a round of an iteration.

The expression language is on collections of data, and involves differential's relatively few operators: join, reduce, concat, and a surprisingly general flatmap operator.
The particular details of the expression language are evolving, and the intent is to let anyone pick their favorite concrete syntax.

The intended semantics assign values to the names in each scope through an iterative process:

1. Initially, each variable is the empty collection.
2. Repeatedly, all variables synchronously update to new values, as a function of their prior values.
3. Eventually, the value of each named variable is its fixed point under this process.

References between scopes are resolved at their least common ancestor.
A reference sees the converged value as observed from the ancestor scope.

## Design goals

The IR is meant to be amenable to reasoning and manipulation, and has some properties that support this.

1.  **Declarative**: the statement order does not affect the semantics.
2.  **Functional**: each value is determined from a pure function of its inputs.
3.  **Unambiguous**: the name-to-value map is fixed within each iteration.
4.  **Equational**: all statement equalities within a scope hold outside the scope.

The language is meant to be referentially transparent, and support equational substitution.
I don't know much about designing languages, so I may have gotten this wrong.

## Architecture

The architecture is fairly standard, and where it isn't it probably should become more standard.
A program moves through these parts:

1. `parse/` is the concrete syntax: one front-end, the pipe parser for `.ddp` files.
2. `lower.rs` translates its AST into the scope-tree IR.
3. `scope_ir.rs` is that IR — scopes, items, exports — and its optimizations; `ir.rs` is the
   vocabulary they are written over: corgi lambdas, operator steps, and registered functions.
4. `backend/` renders a program against a substrate — `vec` (the row reference) or `corgi`
   (columnar), whose kernels live in `corgi/`.
5. `explain/` rewrites a program into one that explains its own outputs.
6. `server.rs` is the in-process registry and lifecycle; the `ddir-server` crate in `server/`
   is the executable that drives it.

The `examples/programs/` directory contains example programs, intentionally simple at the moment.
The one executable is the server (the `ddir-server` crate in `server/`, documented in
`server/README.md`; the session scripts in `examples/server/` show it at work); a program runs by
loading it, feeding its inputs, and closing epochs. For example, on a random graph of 100 nodes and
200 edges, 10 of which change each epoch, for 100 epochs, on four workers of the Corgi backend:
```
cd interactive
printf 'load reach from examples/programs/reach.ddp
feed reach 0 from random:nodes=100,edges=200,churn=10
feed reach 1 0
tick 100
exit
' | DDIR_WORKERS=4 cargo run --release -p ddir-server
```

For the full command vocabulary — `load`, `feed`, `tick`, `peek`, `tail`, and the rest — see
[`examples/server/README.md`](examples/server/README.md), and `server/README.md` for the
protocol and configuration.

## Differential testing

`tests/difftest.rs` generates random lambda programs over random streams of inserts and
retractions, and checks each against other ways of computing the same answers: more workers,
evaluation from scratch, the unoptimized program, delta joins, a producer and consumer sharing
traces, and explanations. It is ignored by default; run a sweep with

```
DIFFTEST_SEED=0 DIFFTEST_CASES=1000 cargo test --release -p interactive --test difftest -- --ignored --nocapture
```

Its module documentation lists the oracles, and how to pick them and replay a failure.

## Status

This is a research project, primarily for personal learning at this point.
Various bits of what is written above are not yet entirely true.
I would not recommend relying on any of this yet.
