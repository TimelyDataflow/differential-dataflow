# DDIR

An interpreted language (and runtime) for computation over continually changing collections.

## Motivation

This is a small language intended primarily for writing differential dataflows clearly.
What is written should translate directly to the dataflow that is built.
You should be able to write most dataflows that you could construct in the library itself.

There are secondary goals for the language, mostly about removing incidental complexity, and increasing clarity.
The language is:

* functional: each defined collection is a pure function of its inputs.
* declarative: within scopes, the order of statements does not matter.
* equational: things that are said to be equal are in fact equal.

Other than non-termination, the language is total.
I am currently iterating on increasing its legibility.

## An example: fine grained access control

Folks often have rules to determine who should be allowed to access resources, like documents.
One approach organizes users into groups, and allows groups to contain other groups themselves.
A user is allowed to read a document if:
* it is a (transitive) member of a group that has been given access, and
* it is not a (transitive) member of a group that has been denied access.

You can write this as (it's one file, but broken up for exposition):

<details>
<summary><b>Shapes</b>: defining the types and inputs the dataflow uses</summary>
<p>

Although at the beginning, this can technically go anywhere in the root scope.
Here we just define some types, and the number and shapes of input collections.
The language is structurally typed, with collections containing non-recursive algebraic data types (sums, products, lists).

</details>
<p>

```
-- Access control: documents are shared with users and with groups,
-- groups contain users and other groups, and a block on a member
-- reaches everyone inside it.

type Access = Owner | Edit | View;           -- decreasing levels of access
type Member = User int | Group int;          -- a user, or group of members

let grants = input 0 : (int, (Member, Access));   -- doc -> (member, access)
let groups = input 1 : (int, Member);             -- group id -> member
let blocks = input 2 : (int, Member);             -- doc -> blocked member
```

<details>
<summary><b>Expressions</b>: manipulating records themselves</summary>
<p>

The shape of records is load bearing when we perform joins and other data-parallel operations.
The pair structure `(key, val)` calls out first how the data are organized, and then any associated values.

A currently unfortunate amount of symbolic load is in shuffling around fields of collection elements.
The `map` operator applies to each record, and the `->` fragment is an expression converting the input to the output.
The language is a small interpreted columnar language with ADTs, and is at the moment a bit esoteric (the `inject` verb puts data within an enum variant).
</details>
<p>

```
-- Wrap group ids with the Group variant, to not match users.
let members = groups | map((grp, who) -> (grp inject Group, who));
```

<details>
<summary><b>Recursion</b>: defining collections in terms of themselves</summary>
<p>

Scopes of the form `name: { .. }` set up regions that can be potentially recursive.

* The `var` keyword announces a collection that can change round to round; its use is optional.
* The `let` keyword defines an equality that always holds.
* The `set` keyword assigns the value of a variable in the next iteration.

Within a scope, the values of variables are established like so:

1. Initially, each variable is the empty collection.
2. Repeatedly, all variables synchronously update to new values, as a function of their prior values.
3. Eventually, the value of each named variable is its fixed point under this process.

Should termination occur, all names bound in the scope reach a limit, and all `set` bindings become equalities.
From outside the scope, a path `scope::name` references the fixed point of the collection bound to `name`.
If any recursive definitions fail to reach a fixed point the computation diverges (please avoid doing this).
All cycles of definitions must include at least one variable.

</details>
<p>

```
-- Barrings spread from `blocks` through `groups` to their members.
let barred = barring::barred | map((who, doc) -> ((who, doc), ()));
barring: {
   var barred : (Member, int);     -- member -> doc: expanded blocked list.
   let beyond = barred | join(members, (_grp, doc, who) -> (who, doc));
   set barred = blocks | map((doc, who) -> (who, doc)) + beyond | distinct;
}
```


<details>
<summary><b>Unrestricted recursion</b>: negation, aggregation, and nested recursion</summary>
<p>

A recursive scope can contain arbitrary logic, including negation, aggregation, and other nested scopes.
The only rule is that each cyclic definition must pass through a `set` binding for it to be well defined.

```
-- Access spreads from `grants` through `groups`, except where `barred`.
export "access" = sharing::allowed;
sharing: {
   var allowed : ((Member, int), Access);  -- (member, doc) -> strongest
   set allowed = offered
               - (offered | join(barred, (pair, acc, ()) -> (pair, acc)))
               | min;

   let offered = grants | map((doc, (who, acc)) -> ((who, doc), acc))
               + inherited;

   let inherited = allowed
         | map(((who, doc), acc) -> (who, (doc, acc)))
         | join(members, (_grp, (doc, acc), who) -> ((who, doc), acc));
}
```

The `-` sigil subtracts one multiset from another.
The `min` operation produces for each key its smallest associated value.
Were there further iteration to perform we could do that as well, with a distinct `name: { .. }` and references through `name::`, supplying the value of an inner fixed point to each iteration of this outer fixed point.

Also notice that in this scope we put the `set` binding statement up top, as a top-down explanation.
The previous example used a bottom-up order, which can be more legible for smaller blocks.
You get to choose!

</details>
<p>

```
-- Access spreads from `grants` through `groups`, except where `barred`.
export "access" = sharing::allowed;
sharing: {
   var allowed : ((Member, int), Access);  -- (member, doc) -> strongest
   set allowed = offered
               - (offered | join(barred, (pair, acc, ()) -> (pair, acc)))
               | min;

   let offered = grants | map((doc, (who, acc)) -> ((who, doc), acc))
               + inherited;

   let inherited = allowed
         | map(((who, doc), acc) -> (who, (doc, acc)))
         | join(members, (_grp, (doc, acc), who) -> ((who, doc), acc));
}
```

This program can be run as a differential dataflow, and any changes to the three inputs will be reflected in the published output, exactly as if recomputed from scratch.

### Operation

Let's demo the current lifecycle of working with computations and collections.
This part is largely demo-ware, meant to exercise loading and running dataflows more than delightful ergonomics.
The intent is that the operation of the system is conducted less by humans and keyboards, and more by integrations and agents.

The examples will feed and read data with textual, row-oriented versions.
This is better done through binary columnar forms, which exist but aren't easy to present.

```
# Install access.ddp under the name `demo`.
load demo from access.ddp
```
This binds the name `demo` to the root scope of `access.ddp`.
There are now three inputs we can reference, and the output `access`.
The dataflow is up and running, but has nothing to do yet.

```
# Variants are structural, and written by position:
# inject(0,x) is User x, inject(1,x) is Group x,
# inject(0,_), inject(1,_), inject(2,_) are Owner, Edit, View.

# Input 1, `groups`: group 10 holds user 2 and group 11; group 11 holds user 3.
feed demo 1 begin
10 val=inject(0,2)
10 val=inject(1,11)
11 val=inject(0,3)
end-feed
```
Data are updated with the `feed` command.
This is the textual line-oriented feed.
There is also a binary columnar `feed` as well, but it is harder to read.


```
# Input 0, `grants`: user 1 owns doc 100, group 10 can view doc 100 and edit
# doc 200, and user 3 can edit doc 100.
feed demo 0 begin
100 val=tuple(inject(0,1),inject(0,tuple()))
100 val=tuple(inject(1,10),inject(2,tuple()))
200 val=tuple(inject(1,10),inject(1,tuple()))
100 val=tuple(inject(0,3),inject(1,tuple()))
end-feed
```
Further updates for the grants, putting us in a place where we could now expect real output.
We have not introduced any bans yet.

```
# Apply the changes, then read every (member, doc) and its access.
tick
peek access
```

The `tick` advances all inputs to the next timestamp.
The `peek` asks for the contents of `access`, in the row-oriented textual form.
There is also a binary columnar version of `peek`, but it's also hard to read.
```
_4 ok t=1 elapsed=446.33µs
_5 data tuple(inject(0,1),100) val=inject(0,tuple())
_5 data tuple(inject(0,2),100) val=inject(2,tuple())
_5 data tuple(inject(0,2),200) val=inject(1,tuple())
_5 data tuple(inject(0,3),100) val=inject(1,tuple())
_5 data tuple(inject(0,3),200) val=inject(1,tuple())
_5 data tuple(inject(1,10),100) val=inject(2,tuple())
_5 data tuple(inject(1,10),200) val=inject(1,tuple())
_5 data tuple(inject(1,11),100) val=inject(2,tuple())
_5 data tuple(inject(1,11),200) val=inject(1,tuple())
_5 ok t=1
```
The responses are prefixed with a command identifier (the `_x`), telling us the `tick` took ~0.5ms.
We then get the data we asked for, which is first pairs of users or groups and documents, and then the level of access.
The first row says that user 1 can read document 100 with the highest level of access.

Now let's add a block to mess things up a bit.
```
# Input 2, `blocks`: block group 10 on doc 100. It reaches users 2 and 3 through
# groups 10 and 11, even past user 3's own grant.
feed demo 2 100 val=inject(1,10)
tick
peek access
```
```
_7 ok t=2 elapsed=481.25µs
_8 data tuple(inject(0,1),100) val=inject(0,tuple())
_8 data tuple(inject(0,2),200) val=inject(1,tuple())
_8 data tuple(inject(0,3),200) val=inject(1,tuple())
_8 data tuple(inject(1,10),200) val=inject(1,tuple())
_8 data tuple(inject(1,11),200) val=inject(1,tuple())
_8 ok t=2
```
There are fewer rows now.

```
# Read one key: user 3 on doc 100. Nothing comes back while the block holds.
peek access tuple(inject(0,3),100)
```
```
_9 ok t=2
```
No row, but confirmation that the read is complete.

```
# Remove the block by retracting the same row, and read the key again.
feed demo 2 100 val=inject(1,10) diff=-1
tick
peek access tuple(inject(0,3),100)
exit
```
Retract the block and confirm that everything works again.
```
_11 ok t=3 elapsed=488.42µs
_12 data tuple(inject(0,3),100) val=inject(1,tuple())
_12 ok t=3
_13 ok bye
```
I didn't write it, but I find `ok bye` hilarious.

## Architecture

A program moves through four parts, each using only the ones before it.

1. **The language.** `parse/` reads `.ddp` text. `lower.rs` turns it into the scope-tree IR of
   `scope_ir.rs`: scopes, items, exports, and the IR's own optimizations. `shapes.rs` infers the
   shape of every collection. `ir.rs` is the vocabulary all of these are written in: lambdas,
   operator steps, reducers, and registered functions. `lambda.rs` is where the expression
   language plugs in: it parses, types, compiles and composes the lambdas in a program's slots.
2. **Analyses** rewrite a program into another program. `explain/` produces one that explains its
   own outputs. `projection.rs`, on with `DDIR_PROJECTION`, replaces fields that nothing reads
   by `()`.
3. **The runtime** renders a program to differential dataflow. `backend/` holds the `Backend`
   trait and `render_tree`, which turns the scope tree into operators. `runtime/` is today's
   backend: columnar containers and chunks, joins, reductions, half joins, exchange, and the feed
   and frame formats.
4. **The host.** `server/` is the in-process registry: install, feed, tick, publish and peek,
   one file per concern.
   `diag.rs` turns the server's own logging into the `diag.*` collections. The `ddir-server`
   crate in `server/` is the executable that drives it all.

The stack aims to stay generic over two things: the expression language and the backend.
- **The expression language** is corgi today. Slots are corgi lambdas, typed and compiled to corgi
  graphs in `lambda.rs`.
- **The backend** is the columnar runtime today, plugged in through `backend/`'s `Backend` trait.

Another expression language or backend should plug in at those two places, without the rest of
the stack depending on it.

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
