# DDIR

An intermediate representation of differential dataflow suitable for interpretation.

## Background

Differential dataflow is a framework that supports declarative computation over continually changing data.
One of its primary irritants is its compile times, due to an over-indulgence in monomorphization.
This project looks at isolating an interpretable core, which lays the groundwork for experimentation in different concrete syntaxes, optimization, and back-ends.

## An example: reachability

Here's an example that performs graph reachability:

```
let edges = input 0 | key($0[0] ; $0[1]);
let roots = input 1 | key($0[0] ;);

reach: {
    let label = reach | join(edges, ($2 ;));
    var reach = roots + label | distinct;
}

result reach::reach | key(;) | arrange | inspect(total);
```

Informally, this computation starts from graph `edges` and nodes `roots`, and repeatedly expands a set of nodes that can be reached from each root.
The details of this example are somewhat arbitrary, but the `let`, `var`, and `{ .. }` bits are quite important.
Everything else does have a role, but what they do and how they are expressed is intentionally fluid.
If you are familiar with differential dataflow, they correspond to various of its transformation operators.

## Syntax and Semantics

Programs in DDIR are structured as a tree of nested "iterative scopes".
Within each scope one can

1. let-bind names to expressions,
2. name and bind iteration variables,
3. create further nested scopes.

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

The architecture has one execution path: the server parses and installs DDIR
programs, feeds their inputs, and advances logical time. The library is split
into the following parts:

1. `parse/` contains the concrete syntax parsers.
2. `lower.rs` translates their AST into the scope-tree IR.
3. `scope_ir.rs` and `ir.rs` define the program and row-level IRs.
4. `backend/` contains the vector and Corgi renderers.
5. `server.rs` owns the in-process registry and lifecycle; the `server/` crate
   provides the `ddir-server` executable and its stdin/TCP/WebSocket protocol.

The `examples/programs/` directory contains small example programs. The
`examples/server/` directory contains command sessions, and `server/demo/`
contains protocol demos. Both use the same server executable; a program runs by
loading it, feeding its inputs, and closing epochs. For example, on a random graph of 100 nodes and
200 edges, 10 of which change each epoch, for 100 epochs, on four workers of the Corgi backend:
```
cd interactive
printf 'load reach from examples/programs/reach.ddp
feed reach 0 from random:nodes=100,edges=200,churn=10
feed reach 1 0
tick 100
exit
' | DDIR_BACKEND=corgi DDIR_WORKERS=4 cargo run --release -p ddir-server
```

For the general command vocabulary, see [`examples/server/README.md`](examples/server/README.md).
The server accepts both pipe-form `.ddp` programs and applicative `.ddir`
programs; the extension selects the parser for `load ... from ...`.

## Status

This is a research project, primarily for personal learning at this point.
Various bits of what is written above are not yet entirely true.
I would not recommend relying on any of this yet.
