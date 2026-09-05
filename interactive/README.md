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

The architecture is fairly standard, and where it isn't it probably should become more standard.
The flow moves through four steps:

1. The `parse/` directory contains any number of concrete syntax parsers.
2. The `lower/` directory contains lowering from the AST to the IR.
3. The `ir/` directory is the IR itself, with optimizations.
4. The `examples/` directory contains back-ends that execute programs.

The `examples/programs/` directory contains example programs, intentionally simple at the moment.
The one executable is the server (`examples/ddir_server.rs`, documented in `examples/server/README.md`);
a program runs by installing it, loading its inputs, and closing epochs. For example, on a random
graph of 100 nodes and 200 edges, 10 of which change each epoch, for 100 epochs:
```
cd interactive
printf 'install reach examples/programs/reach.ddp
feed reach 0 from random:nodes=100,edges=200,churn=10
feed reach 1 0
tick 100
exit
' | cargo run --release --example ddir_server -- --backend=corgi -w4
```

More generally, you can run
```
cargo run --release --example ddir_vec -- <program> <arity> <range> <count> <batch> [<rounds>]
```
where
* `<program>` is a path to your program file,
* `<arity>` is the number of columns expected by your program,
* `<range>` is the range of values from zero for each column,
* `<count>` is the number of records the harness will maintain,
* `<batch>` is the number of records the harness will change in each round,
* `<rounds>` is the number of rounds the harness will perform.

You can leave off the rounds, or any suffix really, to watch it just run for a while.

## Status

This is a research project, primarily for personal learning at this point.
Various bits of what is written above are not yet entirely true.
I would not recommend relying on any of this yet.
