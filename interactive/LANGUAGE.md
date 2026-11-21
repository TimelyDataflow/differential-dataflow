# The DDIR language

DDIR programs describe collections that change over time, and the server keeps every collection up to date as its inputs change.
This page is the reference: what a program is made of, every collection operator, the scalar language inside operators, time, iteration, and the positions the language takes on purpose.
For programs at work in the server, see the [examples](examples/README.md).
For the commands that load programs, feed them and read them, see the [server reference](server/README.md#server-command-reference).

## A first program

```
-- Total sales per store, kept current as sales and refunds arrive.
let sales = input 0 : (int, int);    -- (store, amount)

output "total" = sales | sum;
```

A session loads it, feeds rows at times, closes those times, and reads the result:

```
load sales from sales.ddp
feed sales 0 1 val=5 time=0
feed sales 0 2 val=40 time=0
seal sales 0 1
peek sales::total
```

Later feeds at later times update `total`; nothing is recomputed from scratch.
The [tour](examples/tour/tour.txt) walks through the server's commands, and [`tables/sum.ddp`](examples/tables/sum.ddp) is this program over a shop's orders.

## Collections

A collection is a multiset of rows, each a `(key, value)` pair, that changes over time.
Each change is an update: a row, the time it happens, and a signed integer diff that adds to the row's count.
A row is present at a time when the sum of its diffs up to that time is not zero.

Operators that group rows, such as `join`, `reduce` and `min`, group them by key.
When the whole row should be the key, use the unit value: `((a, b), ())` is a row keyed by the pair `(a, b)`.

### Shapes

Every collection has a shape, written `(key_shape, val_shape)`.

| Shape | Meaning |
|---|---|
| `int` | a signed 64-bit integer |
| `float` | a 64-bit float |
| `()` | unit, the tuple of no fields |
| `(a, b, ...)` | a tuple |
| `List(a)` | a list; a string is `List(int)` of its UTF-8 bytes |
| `Option(a)`, `Result(a, b)` | the built-in sums |
| a declared name | an `enum` or `type` declared in the program |

`(int, int)` is keyed by the first integer with the second as its value.
`((int, int), ())` is keyed by both, with a unit value.

Parentheses always build a tuple: `(int)` is a tuple of one `int`, and `(x)` is a tuple of one value.
Write `int` and `x` where you mean the value itself; the two are different shapes, and mixing them is a shape error.

### Declarations

```
enum Name = Known List(int) | Unknown;     -- a sum type; a variant's payload is () when omitted
type Pair = (int, int);                    -- an alias, erased before planning
```

Types are global to the program, may appear in any order, and cannot be recursive.
Aliases are not yet available inside lambdas.

## Statements

| Statement | Meaning |
|---|---|
| `let x = expr;` | name a collection |
| `output "x" = expr;` | publish a collection as `PROGRAM::x`; root scope only, and each name once per scope |
| `name: { ... }` | a nested scope, which iterates to a fixed point (see [Iteration](#iteration)) |
| `var x;` or `var x : (k, v);` | declare a feedback variable, optionally with its shape |
| `set x = expr;` | bind a feedback variable |
| `enum ...;`, `type ...;` | declare types |

A `--` starts a comment to the end of the line, inside lambdas too.
Within a scope the order of statements does not matter.

### Sources

| Source | Meaning |
|---|---|
| `input N : (k, v)` | the input labelled `N`, with its shape |
| `input "name" : (k, v)` | the input labelled `name`, an identifier, with its shape |
| `name`, `scope::name` | a binding in this scope, an enclosing one, or a child scope's result |

An input's label is its identity, and the order inputs are written in never matters.
Writing a label again reads the same input.
An input must declare its shape.

A program does not name the traces it reads.
The client feeds an input by its label, or the load binds it to another program's output instead, as `load p from p.ddp edges=graph::edges`.
A bound input's shape must match the shape the output is published at.

## Operators

An expression is a source followed by operator stages: `source | op | op`.
`+ b` and `- b` are stages too, and each stage applies to everything to its left: `a | map(f) + b | distinct` is `((a | map(f)) + b) | distinct`.
To add a whole pipeline, parenthesize it.

Each operator's per-row logic is a lambda, written `(pattern -> body)`; see [Lambdas](#lambdas).

Each group of operators below has one reference form, and the other operators in the group are sugar for it.
A sugar's expansion is shown next to it, and writing the expansion gives the same result.

### Linear operators

The reference form is `join_function((k, v) -> list)`.
Each row becomes the lambda's `List` of `((key, value), time, diff)`, at the row's time joined with `time`, with the two diffs multiplied.
The `time` is in the shape of the scope's time (see [Time](#time-and-scopes)), and a coordinate of `0` leaves that coordinate as it is.

| Sugar | Expansion |
|---|---|
| `map((k, v) -> (k2, v2))` | `join_function((k, v) -> ((k2, v2), 0, 1) enlist)` |
| `filter((k, v) -> mask)` | `join_function((k, v) -> (mask, ((k, v), 0, 1)) enlist filter)` |
| `flatmap((k, v) -> rows)` | `join_function((k, v) -> rows map (r -> (r, 0, 1)))` |
| `negate` | `join_function((k, v) -> ((k, v), 0, -1) enlist)` |
| `delay((k, v) -> t)` | `join_function((k, v) -> ((k, v), t, 1) enlist)` |

A `filter` keeps the rows where `mask` is a nonzero `int`.
Inside a nested scope each `0` above is a time of every coordinate, such as `(0, 0)` one level down.

`delay` retracts rows after a fixed time ([`retention`](examples/tables/retention.ddp)) and orders work within a loop ([`components`](examples/graphs/components.ddp), [`shortest_paths`](examples/graphs/shortest_paths.ddp)).

`a + b` is the union of two collections, adding diffs.
It is its own operator, as it reads two inputs, and `a - b` is sugar for `a + (b | negate)`.

The operators in this group are functions of data: a result at time `t` depends only on the input's rows at `t`, not on when they arrived.

### Joins

The reference form is `join(other)`.
For each key it pairs every row `(k, v1)` on the left with every row `(k, v2)` on the right, giving `(k, (v1, v2))`, and multiplies their diffs.

| Sugar | Expansion |
|---|---|
| `join(other, (k, (v1, v2)) -> (k2, v3))` | `join(other) \| map((k, (v1, v2)) -> (k2, v3))`, without building the joined rows |

[Join fragments](#join-fragments) are sugar for a chain of `join`s.

#### Join fragments

A join fragment writes a multiway join as a rule.
Each atom names a collection and its key and value patterns, and `*` joins the atoms left to right:

```
let revenue = orders(o, (c, _)) * customer(c, (_, n)) * nation(n, (name, r))
            | map(((o, c, n, name, r), ()) -> (name, o));
```

A fragment's rows are keyed by its variables in order of first appearance, with a unit value; one variable is the key itself, and no variables are `()`.
A repeated variable tests equality, and a literal in a pattern tests that value.
Every later atom's key must use a variable bound by an earlier atom; the join order is the order written, so put selective atoms first.
Fragments are experimental: the parser expands them into plain joins, and they may change.

### Reductions

The reference form is `reduce((k, vals) -> out)`, or `reduce(limit n, (k, vals) -> out)`.
It groups rows by key.
`vals` is the key's `List` of `(value, diff)` in value order, the values whose diffs do not sum to zero, and only the first `n` of them with a limit.
`out` is a `List` of `(value, diff)`, the key's result, and a key whose `out` is empty has no row.

| Sugar | Result per key | Expansion |
|---|---|---|
| `min` | the least value | `reduce(limit 1, (k, vals) -> (vals head .0, 1) enlist)` |
| `count` | the sum of the diffs | `map((k, _) -> (k, ())) \| reduce((k, vals) -> (vals head .1, 1) enlist)` |
| `sum` | the sum of the integer values, each weighted by its diff | `lift \| join_function((k, (v, t)) -> ((k, ()), t, v) enlist) \| count` |
| `distinct` | each present value once | `reduce((k, vals) -> vals map ((v, _) -> (v, 1)))` |
| `collect` | a `List` of the values, each repeated by its count | `reduce((k, vals) -> (vals map ((v, d) -> (v, (0, d) range) cap_list map ((v, _) -> v)) flatten .1, 1) enlist)` |

When the value is not `()`, `count` drops it first and `distinct` keys by the whole record, so neither reduce reads a key's values together.

`sum` moves each value into its diff and counts, so it costs what `count` costs. Its `lift` only hands `join_function` each row's own time, which leaves the time as it is at any depth ([`sum`](examples/tables/sum.ddp)).
A key whose diffs sum to zero has no row, so a total of zero is absent.
There is no `max` or top-k operator; negate the value and use `min` or `reduce(limit n, ...)`, through a tree of groups for large inputs ([`top_k`](examples/tables/top_k.ddp)).

### Operators that read time

These operators are not functions of data alone: their results depend on the times at which rows arrived, and not only on the rows present at each time.
None of them is sugar.

| Operator | Result |
|---|---|
| `lift` | each value paired with its row's time: `(k, (v, t))` |
| `half_join(other, strict)` | for a lifted stream `(k, (v1, t))`: each update meets `other`'s rows `(k, v2)` whose times come before its own (strictly when `strict`, a bool written `0` or `1`, is `1`), giving `(k, ((v1, v2), t2))` at the update's time, where `t2` is the later of the two times, coordinate by coordinate; `other`'s own changes produce nothing |
| `half_join(other, strict, f)` | `half_join(other, strict) \| map(f)`, without building the joined rows |

`half_join` is the building block of delta joins, which never build the intermediate results of a multiway join; see the [`triangles`](examples/graphs/triangles.ddp) example.
A loop's round is the last coordinate of `lift`'s time.
`lift` with `delay` makes an input row count at its own time only ([`upsert`](examples/tables/upsert.ddp)).

### Arrangements and inspection

| Operator | Result |
|---|---|
| `arrange` | the collection, indexed by key; joins and reductions arrange their inputs themselves, and share an arrangement when they can |
| `inspect(label)` | the collection unchanged, with its updates printed to the server's stdout |

## Lambdas

A lambda runs `let pattern = input in body`, where `input` is the tuple of the operator's rows: `(key, value)` for a row-by-row operator, `(key, (left value, right value))` for a join, and `(key, vals)` for `reduce`.
The language is corgi's.

Patterns are names, `_`, and tuples of patterns, each optionally with a shape: `((src, dst), ())`, `(k, v : int)`.
Operators follow their operands: `(a, b) add`, `xs len`, `(mask, a, b) select`.
Stages chain by juxtaposition, `xs map (x -> (x, 1) add) fold_add`, so nothing needs parentheses for grouping.

| Form | Meaning |
|---|---|
| `x.0` | a tuple's field |
| `(a, b)`, `(a)`, `()` | a tuple, a one-field tuple, unit |
| `let p = e in body` | bind a value |
| `7`, `-3`, `2.5`, `"text"` | literals; a string is its `List(int)` of bytes |
| `[1, 2]`, `[] : List(int)` | a constant list |
| `xs map (x -> e)` | apply to each element |
| `(seed, xs) fold ((acc, x) -> e)`, `scan` | a fold, and its running form |
| `x inject Variant` | build a sum |
| `s match (A (x -> e1), B (y -> e2))` | take a sum apart |
| `s map_variant A (x -> e)` | change one variant |
| `(x, c) branch Option map_variant None (_ -> ())` | `Some(x)` when `c` is nonzero, else `None` |
| `x call name` | a host function, such as `fsqrt` or `bit_length` |
| `e : shape` | an ascription: `e` has this shape |

A lambda reads only its own input and literals; to use a list beside another value inside `map`, pair them first with `cap_list`.

### Scalar operations

Integers are signed 64-bit; arithmetic takes its kind from its operands, so `(1, 2.5) add` is a shape error.

| Group | Operations |
|---|---|
| arithmetic | `add sub mul div rem neg min max`; `to_float` |
| comparison | `eq ne lt le gt ge` give `int` 0 or 1, comparing any shapes in structural order, strings included |
| logic | `(a, b) and`, `or`, `xor` bitwise; `x and 255` masks; `(a, b, c) all`, `any` |
| choice | `(mask, then, else) select` |
| 64-bit words | `add_b64 sub_b64 mul_b64`; `shl_b64 k shr_b64 k rotl_b64 k rotr_b64 k` |
| lists | `len enlist append flatten range zip head get gather slices chunk k find cut adjacent filter sort dedup group sort_by sort_limit k transpose` |
| list reductions | `fold_add fold_mul fold_min fold_max fold_all fold_any`, and `scan_` of each |
| capture | `cap_list` pairs a context with every list element; `cap_sum` distributes one into every sum lane |
| checked forms | `try_get try_gather try_zip try_chunk` give a two-lane sum, whose second lane holds the rows the plain form would have lost something on |
| text | `split ","`, `parse_int` |
| other | `hash` (a stable 64-bit content hash), `unit` |
| host functions, by `call` | `fabs fsqrt fexp fln ffloor fceil fround fsin fcos ftan fpow fpowi fint fmin fmax`; `feq fne flt fle fgt fge` (IEEE comparisons); `bit_length` |

`zip`, `get` and `gather` read the zero of the element's shape past the end of a list, and `chunk` drops a short last piece; the `try_` forms report those cases.

## Time and scopes

A program's root time is one integer by default, the time the client gives each feed.
A program loaded with `width=N` has root times of `N` coordinates, written `[x,y]` in commands, compared coordinate by coordinate.
The client advances an input's time with `seal`; a read at time `t` is complete once every input it depends on, fed or bound, is past `t`.

Inside a nested scope the time gains one coordinate per level, the loop's round: `(epoch, round)` one level down.
A `delay` or `join_function` there names every coordinate, as in `delay((_, d) -> (0, d))`.
`lift` gives the whole time, a tuple even at the root: `(k, (v, (t)))` for a one-coordinate time.

## Iteration

A nested scope, `name: { ... }`, computes a fixed point.

1. Each feedback variable starts empty.
2. Each round, every variable becomes its `set` value, computed from the previous round's values.
3. The scope's result is the value each variable stops changing at.

From outside, `name::x` is the fixed point of `x`.
A scope can hold negation, reductions and further scopes.
A recursion that never stops changing never finishes, and holds up every program on the server.

Prioritized iteration uses `delay` to hold some rows back to later rounds, so that work likely to be undone waits ([`components`](examples/graphs/components.ddp)).
It can make the first computation much faster, and some later edits slower.

At the root, `var` and `set` carry state from one time to the next instead.
A root variable is empty at first, and at each time `t` holds what its `set` held at `t - 1`.
Nothing iterates, and anything may feed the `set`, as the cycle passes through the step ([`upsert`](examples/tables/upsert.ddp)).
Root variables need a one-coordinate root time.

## Design positions

These are deliberate, and differ from what SQL users expect.

- **Inputs are multisets.** Feeding a row twice gives it a count of 2, and retracting an absent row gives it a count of -1. `distinct` keeps any row whose count is not zero. Replacing a key's value is a program's job ([`upsert`](examples/tables/upsert.ddp)).
- **Arithmetic does not fail.** Integers wrap: `(9223372036854775807, 1) add` is `-9223372036854775808`. `(x, 0) div` is 0 and `(x, 0) rem` is `x`; division rounds toward zero, so `(-7, 2) div` is -3. A program that needs errors checks its operands and builds a `Result`.
- **There is no NULL.** A missing value is a variant. Declare an enum for it ([`left_join`](examples/tables/left_join.ddp)); `() inject None` is refused, because `Option`'s `Some` lane has no shape until a value supplies one.
- **Nothing is durable.** A server's programs and data last as long as its process.
- **One server is one trust domain.** Programs share one command queue and one set of workers: a slow program delays the others, and a program that never finishes or crashes a worker stops them all.
- **Inputs belong to their program.** Another program cannot read an input, only an output. Changing a program's schema is a migration; a stable program that only publishes its inputs, read by programs whose inputs are bound to them, keeps views replaceable.
- **There are no dates or times of day.** Use integers, such as days or seconds since an epoch.
- **Joins match keys exactly.** A range or approximate join is a re-keying into buckets first.
- **Strings are byte lists.** A feed and a peek spell `"ann"` as `list(97,110,110)`.

## Seeing what a program does

`ddir-plan file.ddp` prints a program's plan and its inferred shapes without running it, and exits non-zero when a program has a problem.
`peek` reads an output at a time, `tail` follows its changes, and `inspect(label)` prints a collection's updates to the server's stdout.
Only outputs can be read, so to see an intermediate collection, publish it as an output.
