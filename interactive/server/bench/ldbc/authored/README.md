# Authored DDP plans for the 41 SNB reads

`plans/` holds one hand-written DDP program per read (IS1–7, IC1–14 v2, BI1–20).
They are written to be read: named, keyed collections, destructured fields, and
comments that say what each step is for. They are an alternative to the plans
that `suite.py` generates from `snb/queries.py`, kept apart from that generator:
nothing here changes the generated plans or the query definitions.

Every authored plan answers exactly like the generated plan for the same query.
`check.py` runs both against the same data and bindings and fails on any difference.

## The contract

An authored plan is a drop-in replacement for the generated one:

- **Request input.** `input 0` is the request relation, with the fields of the
  query's `@query(...)` declaration in `snb/queries.py`, prefixed by `rid`. For
  example IC3 is `(rid, person, country X, country Y, start, days)`. Byte strings
  are `List(int)`.
- **Graph.** Plans import the arrangements the suite's shared graph program publishes
  (`snb/indexes.py`), and join against them directly:
  - **primary keys**, `ldbc.person`, `ldbc.message`, `ldbc.forum`, `ldbc.tag`,
    `ldbc.tagclass`, `ldbc.place`, `ldbc.org`: `(id, (the other fields))`, in
    `snb/data.py`'s field order;
  - **secondary indexes** from a foreign key to the id, such as
    `ldbc.message.by_creator : (int, int)` (author, message) or `ldbc.person.by_city`;
  - **relations without an id**, by the endpoint looked up from, such as
    `ldbc.likes.by_message : (int, (int, int))` (message, (person, when));
  - **derived**: `ldbc.friend` (person, (friend, since)), friendships in both
    directions between people who exist; `ldbc.friend.pair` ((person, friend), ());
    `ldbc.message.root` (message, root post) and `ldbc.message.by_root`.

  Every query's server shares these, so a plan pays only for what it builds itself.
  A `map` or `filter` on an import makes a private copy; the plans do that only for
  parts kept for all requests, and each header says which.
- **Answer.** `export "<query>.answer"` is arranged as `((rid, rank), (fields…))`,
  where rank is the 0-based position in the query's order and the fields are the
  query's outputs, in the order `finish(...)` lists them. One-field outputs are
  written `(n)`: parentheses make a one-field tuple.

## Shared indexes, measured

All 41 plans installed in one server (SF1, 4 workers, the 521 update epochs of the BI
stream, each plan's `suite-0` binding standing; identical answers at every capture),
with the graph program publishing whole rows (as in #1003) and with the indexes above:

| | Whole rows | Indexes |
| --- | ---: | ---: |
| Mean cost per update epoch | 144 ms | 56 ms |
| Settle after loading | 16.7 s | 4.1 s |
| Updates held in arrangements (the server's `sizes`) | 506 M | 179 M |
| Physical footprint late in the stream (`vmmap`) | 24.8 GB | 9.6 GB |

The gain is in sharing: each plan used to keep its own copies of the large relations
(a few hundred arrangements of about 3.6 M updates each). With one query per server the
indexed graph program costs more on its own: 2.9 ms against 0.9 ms per epoch, and
3.7 GB against 2.1 GB, most of the difference being the thread-root loop and the
secondary indexes. On macOS, resident set size understates memory under pressure: the
whole-row run's RSS read 4.3 GB while 21.4 GB of its 24.8 GB footprint was swapped out.

## Checking and timing them

```sh
cargo build --release -p ddir-server
# No data needed: the suite's 199-row witness graph.
python3 interactive/server/bench/ldbc/authored/check.py --server target/release/ddir_server \
    --fixture --workers 4 --output /tmp/authored-fixture
# A BI composite-merged-fk snapshot, with the official SF1 BI parameters as extra bindings.
python3 interactive/server/bench/ldbc/authored/check.py --server target/release/ddir_server \
    --snapshot /path/to/graphs/csv/bi/composite-merged-fk/initial_snapshot \
    --parameters interactive/server/bench/ldbc/bindings/bi-official-sf1.json \
    --workers 4 --output /tmp/authored-sf1 [query ...]
```

For each query it starts a fresh server for the generated plan and another for
the authored one, loads the snapshot, settles, then binds, peeks and releases
each binding (one warmup, three measured runs). It prints both plans' median
bind-plus-peek latencies and whether the answers match, and writes `report.json`.
`--prepared` reads the snapshot from an `update_stream.py --prepare-only` journal
instead of the CSVs, which is faster at SF1. To maintain an authored plan over
the update stream, pass it to `update_stream.py --plan`.

## What each plan does

"Same as the generated plan" means the same joins in the same order, the same
filters at the same points, and the same parts kept for all requests; only the
spelling differs (below), and the authored plan looks rows up in the published
arrangements where the generated plan rebuilds whole rows from them. The others follow a rewritten strategy, measured
against the generated plan in [../baselines/METHOD.md](../baselines/METHOD.md).

| Query | What it asks | Strategy |
| --- | --- | --- |
| [IS1](plans/is01.ddp) | Profile of a person: name, birthday, IP, browser, city, gender and creation date | Same as the generated plan |
| [IS2](plans/is02.ddp) | A person's 10 most recent messages, each with the original post of its thread and that post's author | Rank on narrow rows, then fetch text and names; the root post's author only for the 10 winners |
| [IS3](plans/is03.ddp) | A person's friends, with the date each friendship was made, newest first | Same as the generated plan |
| [IS4](plans/is04.ddp) | A message's creation date and content (or image file) | Same as the generated plan |
| [IS5](plans/is05.ddp) | The author of a message | Same as the generated plan |
| [IS6](plans/is06.ddp) | The forum that contains a message's thread, and its moderator | Same as the generated plan |
| [IS7](plans/is07.ddp) | Replies to a message, flagging whether each replier knows the original author | Same as the generated plan. The 'knows' flag is computed as known replies plus the rest, rather than a left join with a default |
| [IC1](plans/ic01.ddp) | People named X within three friendship hops, with their emails, languages, city, universities and companies; 20 closest, then by last name | Same as the generated plan, except that emails, languages, study and work are looked up for the people found rather than kept for everyone. Missing lists are attached as empty lists by subtraction rather than a left join |
| [IC2](plans/ic02.ddp) | The 20 most recent messages written by a person's friends before a date | Each person's 20 newest messages kept for all requests, with a fallback for friends who posted after the cut-off |
| [IC3](plans/ic03.ddp) | Friends and friends-of-friends living outside countries X and Y who posted from both X and Y in a time window; top 20 by message count | The two countries found by name through `place.by_name`, their messages in the window joined on country id, then the circle |
| [IC4](plans/ic04.ddp) | Tags that first appear on friends' posts inside a window (never before it); top 10 by post count | Same as the generated plan |
| [IC5](plans/ic05.ddp) | Forums that friends and friends-of-friends joined after a date, ranked by how many posts those people made there | Per-(person, forum) post counts kept for all requests, and at most 40 zero-count forums per person |
| [IC6](plans/ic06.ddp) | Tags that co-occur with a given tag on posts by friends and friends-of-friends; top 10 | The tag's posts first, then those by the circle |
| [IC7](plans/ic07.ddp) | The latest likes on a person's messages: who liked, when, how long after posting, and whether the liker is a friend | Same as the generated plan |
| [IC8](plans/ic08.ddp) | The 20 most recent replies to a person's messages | Rank on narrow rows, then fetch text and names |
| [IC9](plans/ic09.ddp) | The 20 most recent messages by friends and friends-of-friends before a date | As IC2, over friends and friends-of-friends |
| [IC10](plans/ic10.ddp) | Friend recommendation: friends-of-friends born near a given month, scored by how many of their posts match the person's interests | Same as the generated plan. Scores are one sum over +1, -1 and 0 contributions |
| [IC11](plans/ic11.ddp) | Friends and friends-of-friends who started working at a company in a given country before a year | Same as the generated plan |
| [IC12](plans/ic12.ddp) | Friends who replied to posts tagged within a tag class (or its subclasses); count of such replies and the tags involved | Same as the generated plan |
| [IC13](plans/ic13.ddp) | Length of the shortest friendship path between two people (-1 if none) | Bidirectional search |
| [IC14](plans/ic14.ddp) | The cheapest interaction-weighted path between two people (v2 definition), with ties broken by the lexicographically smallest path | Same as the generated plan |
| [BI1](plans/bi01.ddp) | Posting summary: messages before a date grouped by year, post/comment and length category, with counts, average length and share | Per-day totals for all requests, plus the cut-off day's own messages |
| [BI2](plans/bi02.ddp) | Tags in a tag class whose usage changed most between two consecutive 100-day windows | Tagged messages keyed by (class, day) for all requests; the window's days come from that table |
| [BI3](plans/bi03.ddp) | Forums moderated by people in a country, ranked by messages tagged with a given tag class | Starts from the country: its cities, residents and the forums they moderate; the class's tagged messages reach forums through their thread root |
| [BI4](plans/bi04.ddp) | Top 100 forums (by largest single-country membership) created after a date; their members ranked by messages posted in those forums | Popularity kept per forum, a request filters by date and ranks |
| [BI5](plans/bi05.ddp) | Most active posters on a tag, scored by messages, replies and likes | Same as the generated plan. Messages, replies and likes are counted in one count keyed by kind, then pivoted, instead of three counts and two left joins |
| [BI6](plans/bi06.ddp) | Most authoritative users on a tag: authors scored by the popularity of the people who liked their tagged messages | Same as the generated plan. Scores are one sum over the pairs plus a zero per author, instead of a left join |
| [BI7](plans/bi07.ddp) | Tags on replies to messages with a given tag (replies not themselves carrying the tag) | Same as the generated plan |
| [BI8](plans/bi08.ddp) | Central people for a tag: interest and recent tagged messages score each person, plus their friends' scores | Same as the generated plan |
| [BI9](plans/bi09.ddp) | Top thread initiators: people whose posts in a window started threads with the most messages in the window | Thread sizes kept per post, minus messages after the end |
| [BI10](plans/bi10.ddp) | Experts in a social circle: people at 'minimum' to 'maximum' hops in a country, counting tags on their messages that carry a tag class | The search stops at maximum hops, and the class's messages are found first |
| [BI11](plans/bi11.ddp) | Number of friendship triangles among people in a country, with all three friendships made inside a window | Same as the generated plan. Edges are not degree-oriented (a measured variant that orients them is described in baselines/METHOD.md) |
| [BI12](plans/bi12.ddp) | Distribution of people by how many short messages they wrote after a date in given languages | Messages keyed by their thread's language for all requests |
| [BI13](plans/bi13.ddp) | Zombies in a country: people posting less than once a month, and how many of their likes come from other zombies | Same as the generated plan. Like tallies are one sum, instead of counts joined back with defaults |
| [BI14](plans/bi14.ddp) | International dialog: the best-scoring friend pair per city between two countries, scored by replies and likes | Same as the generated plan. Pair scores are one sum over five contribution streams |
| [BI15](plans/bi15.ddp) | Cheapest path between two people where friendship weights shrink with interactions in forums created inside a window | Bidirectional search with costs computed on demand; pairs need no friendship check, since the search only steps along friendships |
| [BI16](plans/bi16.ddp) | People active on tag A on day A and tag B on day B who know at most 'maxknows' others in each group | Same as the generated plan |
| [BI17](plans/bi17.ddp) | Information propagation: people who posted a tag in forum1 before a discussion of it in forum2 among forum1's members | Memberships restricted to tagged forums, and each person's earliest tagged message per forum |
| [BI18](plans/bi18.ddp) | Friend recommendation: pairs of people interested in a tag with the most mutual friends, who are not already friends | Both hops restricted to interested people, meeting in the middle; 'already friends' is checked against the fans' own friendships |
| [BI19](plans/bi19.ddp) | Cheapest interaction-weighted paths between anyone in city 1 and anyone in city 2 | Same as the generated plan |
| [BI20](plans/bi20.ddp) | Recruitment: the closest employees of a company to a person, over a graph of classmates weighted by graduation-year gaps | Classmate friendships found from each friendship and both people's schools, rather than all pairs at a school filtered to friends |

Two conventions apply throughout:

- **Ranking with a limit** uses the same bounded `reduce` the generator emits
  (`snb/topk.py`, single stage): a key's values arrive in sort order, the first k are
  kept (copies capped at k) and emitted as `(rank, value)`, so changes below the top k
  produce no output. Rankings without a limit (IS3, IS7, BI1, BI12, BI19, and IC11's
  numbering of company names) use `collect` and `flatmap`, which emit every row anyway.
  Ranking by `collect` with a limit reads more simply but is costly to maintain: a change
  to any candidate re-ranks everything below it before the limit applies. With the
  bounded `reduce`, BI4, BI9 and IC5 cost 4.2, 3.6 and 6.0 ms per SF1 update batch,
  against 10.3, 6.7 and 17.6 ms with `collect`. IC5 stays above the generated plan's
  2.6 ms because its rewrite also maintains each person's 40 smallest forums for every
  request: faster reads, more work per update. For very large groups, `snb/topk.py`
  can also stage the ranking through hash buckets.
- **Only the fields a step needs** are carried, so the plans stay narrow without
  the projection pass.

## Idioms

- **Keyed collections.** Keep a collection as `(key, value)` by the key it is
  joined on: `friend` is `(person, friend)`, so `join(friend, (_, req, f) -> …)`
  reads as "for each friend". Map back to `((fields), ())` only where a step needs
  a whole-row key.
- **Destructuring.** Lambdas name what they take apart:
  `map(((id, _, created, author, …), ()) -> (author, (id, created)))`.
- **Loops.** A `var` inside a scope is a fixpoint, and its rows are unit-valued:
  `((rid, node, cost), ())`. The tag-class hierarchy and the shortest-path searches
  are loops; threads (each message's root post) are a loop in the graph program.
- **`distinct` takes unit values.** Re-key to `((fields), ())` first. Many
  `distinct`s are unnecessary: a message has one thread language, tag names are
  unique, and (message, tag) pairs are a set. Each plan says why where it omits one.
- **Sums go in the multiplicity.** To add up a value per key, move it into the diff and
  let `count` total it: `join_function((key, v) -> ((key, ()), (0), v) enlist) | count`
  (inside a loop the time is `(0, 0)`). This is differential's `explode`: the sum is
  maintained incrementally, where `collect` and `fold` would rebuild a list per change.
  A total of zero has no row, so a default of 0 is added for keys that must appear.
  `collect` remains only for lists that are part of the answer (IC1, IC12), for
  pivoting two or three counts of one key into a tuple (BI1, BI2, BI5, BI13), and for
  rankings without a limit.
- **Tuples.** `(e)` is a one-field tuple, not `e`; the compiler says so. Around a
  list this matters: `-> (x enlist)` is a tuple holding a list.
- **Captured values in folds.** A fold's lambda sees only the accumulator and the
  item, so carry other values in the accumulator. The interaction weight for IC14
  and BI19, 1 + #{k in 1..39 : k(k-1) + 1 > n}, is
  `((0, n), (1, 40) range) fold (((acc, n), k) -> ((acc, (… , n) gt …) add, n)) .0`.
- **Paths under `min`.** IC14's cheapest path, ties broken by the smallest list of
  people, is `min` over `(cost, path)` values; paths grow with `(path, next enlist) append`.
- **Bidirectional search** (IC13, BI15): rows `(rid, side, node, cost)` grow from
  both ends in one loop; `best` is the least meeting cost, and a row keeps growing
  only while `2 · cost < best`. BI15 computes each friendship's cost only when the
  search reaches it.

Recurring fragments (where people live, the interaction weights) are repeated in
each file, so that every plan is complete on its own; friendships and threads are
imported.
