# Language gaps, from the AoC 2023 survey

Thirteen recurring gaps hit while expressing the 33 parts in this suite,
ranked by how many days each would have unlocked or simplified. They are
recorded together here rather than as thirteen issues: most are one-line
asks whose real content is the workaround they forced, and they rank
against each other better than they read alone. Each entry names what is
missing, how the programs coped, and the smallest change that would have
sufficed.

1. **Integer division family: `/`, `%`, shifts.** Worked around on four
   days — enumeration fixpoints (day 6), parity folds (10p1), relational
   modulo via a k-table cross join (15p1), a 40-step binary-halving gadget
   (18) — and a named blocker for days 8, 23, 24. Minimal fix: `/` and `%`
   BinOps. The highest value per implementation line in the survey.
2. **No `sum`/`max` reducers.** Every solved day paid this tax: sum is
   `collect` + `fold` (materializing a list to add numbers), max is `min`
   over `BIG - x` or a negate — the latter interacting badly with corgi's
   encoding-order contract for negatives. Minimal fix: `sum` and `max`
   beside `min` and `count`.
3. **`distinct` erases the value to unit**, so every iterative loop's state
   must be packed into keys and re-projected around every join (days 3, 5,
   10, 14, 16, 18, 19, 22). Minimal fix: a value-preserving idempotent
   reducer — `distinct` on rows, or blessing `min` as the var-reducer idiom
   (days 17 and 22 show it works).
4. **No scalar let-bindings or user scalar functions**; repeated
   subexpressions are copy-pasted or staged through extra `map`s (days 1p2,
   7, 18, 19 — day 19's projections had to be machine-generated). Minimal
   fix: `let x = t in t` in the scalar grammar.
5. **No collection-level abstraction** (parameterized subgraphs): day
   14p2's four roll directions and day 16's two beam programs are
   copy-pasted pipelines. Minimal fix: named subgraph definitions with
   collection parameters.
6. **No string/char literals**; all text logic is numeric charcode
   comparison (days 1, 10, 13, 15, 16, 18, 19). Minimal fix: `'a'` char
   literals, `"abc"` as list-of-codes sugar.
7. **`a - b` is multiset subtraction**; the natural "set minus" reading
   silently corrupts counts when `b` has rows outside `a` (bit day 10;
   latent everywhere). Idiom: `a - (a |> join b)`. Minimal fix: an `except`
   operator.
8. **One `EDGES_FILE` round-robined across inputs** makes multi-relation
   programs awkward; the tag+filter workaround (days 5, 19, 22) then runs
   into corgi's arity-uniformity contract. Minimal fix: `EDGES_FILE_0`,
   `EDGES_FILE_1`, ...
9. **No extrinsic iteration bound** (SQL's `RETURN AT RECURSION LIMIT`);
   bounds must be encoded in the data. This is ergonomics, not expressive
   power: a one-round stall (`var d = x;` then `x + (d | negate)` fires at
   round 0 only) seeds a marker that marches `k -> k+1` under
   `filter($0[0] < K)` with O(1) live rows per round — the full
   delayed-pulse stepwise driver, in the language today. The residual ask
   is sugar: an iteration cap on scopes, or a blessed pulse idiom in the
   examples.
10. **No substring/window primitive**: k-char windows need k−1 self-joins
    (day 1p2); line equality via collected lists (day 13). Minimal fix: a
    windowing flatmap or list-slicing scalars.
11. **A reduce's List value cannot be re-projected as a single field**:
    bare `$1` splices, `$1[0]` takes an element; idiom `if(1, $1, 0)`
    (day 13). Minimal fix: a whole-value field form, e.g. `val($n)`.
12. **`&&` exists but `||` doesn't** (workaround `or(a,b)`); trivial
    grammar fix (day 10).
13. **No non-integer arithmetic** (float/decimal, sqrt): the day 25
    blocker; contributes to 6 and 24. Worth fixing only if DDIR wants
    numeric-analysis workloads — everything else in AoC fits i64.

## Near-misses that are not gaps

Two things initially read as gaps and turned out not to be. The
delayed-pulse driver above (entry 9) is fully expressible via the `var`
stall — the survey's first pass got this wrong, and the construction is
worth knowing. And several days where SQL's stepwise idioms would not
transliterate ended up with *better* programs for it: day 14's closed-form
roll and day 22's interval-overlap fall are shorter, clearer, and more
incremental than their SQL counterparts. Absence of a feature is
occasionally load-bearing.
