# Ledgers: the model (draft)

This states what a ledger is, what may change a set of ledgers, and what any implementation must guarantee.
It is a draft, written from the design notes of 9 and 10 November 2026.
The DDIR in it is excerpted from [`model/ledger.ddp`](model/ledger.ddp), which runs on the server.
The rules it describes are [`model/resolve.ddp`](model/resolve.ddp), and [`tests/ledgers.rs`](tests/ledgers.rs) checks each against the other.

## Ledgers

A LEDGER is a collection of updates `(data, time, diff)` that only grows.
Its data has a key shape and a value shape, which are Corgi shapes.
A TIME is a list of ints, one per coordinate, and the number of coordinates is the ledger's WIDTH.
Times are ordered coordinate by coordinate, so two times may be incomparable.
A FRONTIER is a set of times, none before another, written as a list of times.
A frontier stands for all the times at or after any of its elements.
The empty frontier stands for no times at all.

A SPAN is a durable statement about part of a ledger: `(seqno, lower, upper, since, batch)`.

* `lower` and `upper` are frontiers; the span holds exactly the ledger's updates at times at or after `lower` and not at or after `upper`.
* `since` is a frontier; the span's updates accumulate correctly at any time at or after `since`.
* `batch` says where the updates are: nowhere, because there are none; inline; or in a blob named by its hash.
* `seqno` numbers the ledger's spans: 0, 1, 2 and so on.
  It lets a reader tell a span that is missing from a stretch of time that a clock passed over.

A ledger's VERSION is the number of its spans.
A ledger is identified by an id, which reveals nothing about it.

## State

The state of a set of ledgers is two collections.

```
type Batch = Empty | Inline List(int) | Blob List(int);
type Clock = Free | Follows (int, int);

-- registrations : ((id), (shape, width, start, clock))
-- spans         : ((id), (seqno, lower, upper, since, batch))
let registrations = input 0 : (((int, int)), (List(int), int, List(List(int)), Clock));
let spans = input 1 : (((int, int)), (int, List(List(int)), List(List(int)), List(List(int)), Batch));
```

A registration records a ledger's shape, its width, the frontier it starts at, and the clock it follows, if any.
Everything else about a ledger is computed from these two collections.
Nothing else is a source of truth.

## Clocks

A CLOCK is an ordinary ledger, usually one that holds no data.
A ledger may FOLLOW one clock, and a clock may itself follow a clock.
What a ledger follows is fixed when it is created.
A follower has the same width as its clock.

A ledger's WRITE FRONTIER is the frontier before which it is complete.
It is the join of the ledger's own upper with the write frontier of the clock it follows.
The ledger's own upper is the upper of its last span, or its start if it has none.

```
-- OWN: a ledger's version is the number of its spans, and its own upper is the upper
-- of its last span, or its start if it has none. Marks sort by seqno, the start first.
let marks = (registrations | map((id, (_, _, start, _)) -> (id, (-1, start))))
          + (spans | map((id, (seqno, _, upper, _, _)) -> (id, (seqno, upper))));
let own = marks | reduce((_, vals) ->
    let entries = vals map ((mark, _) -> mark) in
    let last = ((entries len, 1) sub enlist, entries) gather in
    last map ((seqno, upper) -> (((seqno, 1) add, upper), 1)));

-- WRITE FRONTIER: the join of a ledger's own upper with the write frontier of the clock
-- it follows, which is in turn joined with that of the clock above it, and so on.
wf: {
    let lifted = follows | map(((id), (clock)) -> ((clock), (id)))
                         | join(write, ((clock), (id), (frontier)) -> ((id), (frontier)));
    set write = ((own | map((id, (_, upper)) -> (id, (upper)))) + lifted) | reduce((_, vals) ->
        let none = [] : List(List(int)) in
        let joined = ((0, none), vals) fold (((seen, sofar), ((frontier), _)) ->
            (1, (seen, (sofar, frontier) call frontier_join, frontier) select)) in
        ((joined.1), 1) enlist);
}
```

One append to a clock therefore advances every ledger below it, with no span for any of them.
After such an advance a follower's spans no longer abut: its next span begins at the new write frontier, past where its last span ended.
The times in between hold no updates.

This definition does work for every ledger.
An implementation need not: it may compute the write frontier only for the ledgers it is asked about.

## Invariants

These hold of every reachable state.
Each is a view in `model/ledger.ddp` that must be empty.

1. **malformed.** A span's lower is strictly before its upper.
2. **orphaned.** Every span belongs to a registered ledger.
3. **forked, skipped.** A ledger's spans are numbered 0, 1, 2 and so on, with no number used twice and none skipped.
4. **early.** Each span begins no earlier than the span before it ended, and the first no earlier than the ledger's start.
5. **unclocked, mismatched.** A followed clock is registered, and has its follower's width.

Invariant 3 is what lets a reader holding spans 0 to n know that none is missing.
Invariant 4 does not ask spans to abut, because of clocks.

## Transactions

The one way to change the state is a TRANSACTION: a list of actions that all take effect, or none do.

```
type Action = Create (id, shape, width, start, clock)
            | Append (id, lower, upper, batch, seqno);
-- requests : ((tx), (List(Action)))
```

A transaction lists its actions in order of ledger id, a `Create` before an `Append` of the same ledger.
So it names a ledger in at most one `Create` and at most one `Append`, and this can be checked from the request alone.

* `Create` succeeds if the id is not registered and, when it names a clock, that clock was registered before the round, with the same width.
  It adds the registration.
* `Append` succeeds if the ledger is registered, `seqno` equals its version, `lower` equals its write frontier, and `lower` is strictly before `upper`.
  It adds the span `(seqno, lower, upper, start, batch)`: a new span's `since` is the ledger's start.

A transaction COMMITS if every one of its actions succeeds, in order, and otherwise it is REJECTED and changes nothing.
A rejection names the first action that failed and why: a stale append, an id already registered, an id not registered, or a request that is invalid in itself.
A stale append is told the ledger's version and write frontier as the round leaves them, which is where a retry must begin.
A request is invalid in itself if its actions are out of order, a frontier's times differ in width, an append's lower is not strictly before its upper, or its times are not of the ledger's width.

The writer of a batch vouches for it: its updates have the ledger's shapes, and lie in the span.
The rules do not open a batch.

### Rounds

Transactions are resolved in ROUNDS.
Within a round they are taken in the order of their keys, which is the order they arrived in.

* An action sees the registrations and spans left by the earlier transactions of the round that committed.
* A write frontier in a round is the join of the ledger's own upper, as those earlier transactions left it, with its clock's write frontier as it stood when the round began.

So an append to a clock takes effect after every other append of its round.
It never causes an append to a follower in the same round to be rejected, and every later round sees it.

Among independent writers of one ledger, at most one commits in a round, and the others try again.

This is weaker than running whole transactions one after another.
Two transactions that each append to one ledger and advance the other's clock both commit in one round, though neither order of running them whole would commit both.
What a round guarantees is that each transaction is all or nothing, and that the state it leaves satisfies the invariants.

## Committing

A RESOLVER holds the state of some ledgers and resolves rounds of transactions on them.
It is an ordinary DDIR node, and the rules above are a DDIR program it runs.

* **Writers are dumb.** A writer puts its batches in the blob store, submits transactions to a resolver, and waits for their outcomes.
  A writer never commits anything itself.
* **A manifest is a ledger.** A resolver's MANIFEST is the ledger of changes to its registrations and spans.
  Its time has one coordinate, the round.
  Each round, the resolver writes that round's changes as a blob and appends one span to its manifest.
* **A resolver is a writer too.** It commits its manifest's span by submitting an `Append` to its parent resolver, which treats the batch as opaque.
* **A root ends this.** A ROOT is a resolver whose last step is a compare-and-swap on a durable register.
  There may be several roots.
* **A ledger is in one manifest, and a manifest has one parent.**
  Several instances may try to advance the same manifest; one wins each round, and the others take up the span that committed.

Any implementation must guarantee:

1. An outcome is reported only after it is durable at every level up to a root.
2. A committed transaction survives the failure of any process, and a rejected or lost one leaves no visible effect, even if its blobs were written.
3. Every blob a span names is durable before the span commits.
4. Rounds of one manifest are totally ordered, and a transaction that completed before another was submitted is ordered before it.

## Reading

A reader of a ledger needs its registration, its spans, and the write frontiers of the clocks above it.
The ledger's contents at every time not at or after its write frontier are exactly the updates in its spans.

* Blobs are immutable and named by their contents, so the value of a root's register names one consistent state of everything beneath it.
  A reader needs that one value, fresh or stale, and after that reads only blobs.
* A resolver may also serve readers directly: it looks a ledger up by id, and sends subscribers the non-empty spans and the clock advances.
* A reader may begin computing from blobs before their spans are confirmed.
  It must not submit transactions built on them until they are.

## Housekeeping

None of this is built.

* **Compaction.** Consecutive spans may be replaced by one span that covers their seqnos, holds their updates consolidated, and has a later `since`.
  Adding such a span is always safe, since it is determined by the spans it covers.
  Removing the spans it covers is what must be guarded.
* **Holds.** What history must be kept is recorded as data: a ledger of intents, each saying what it still needs.
  The compaction allowed is computed from that ledger.
* **Collection.** A blob that no root reaches may be deleted, once no transaction in flight could still name it.

## Not settled

1. **Rounds are weaker than serial transactions,** as described under Rounds.
   This follows from letting a clock's advance not contend with appends; is it acceptable as stated?
   It matters if clocks are to be used for mutual exclusion, where advancing a clock is meant to fence another writer out.
2. **A slow writer behind a busy clock.** An append's lower must equal the write frontier, and a clock's advance moves the write frontier of every follower.
   A writer that takes longer than a round to answer a rejection is stale again when it does.
   In the resolver demo a slow writer landed nothing while its clock advanced every round, and everything once the clock stopped.
   Letting a lower be later than the write frontier, which amounts to an implied empty span, would let such a writer aim ahead of the clock.
3. **Ties.** Among transactions that could each commit alone, the one with the least key wins, every round.
   In the resolver demo one of three writers to a shared ledger won 15 rounds running.
4. **Width and clocks.** A follower must have its clock's width, so a follower's width could be taken from its clock instead of stated.
5. **What a root's register holds.** Perhaps the list of the root manifest's spans, or the hash of a blob that holds it.
6. **Whether DDIR's own times should be ints.** Ledger times are ints and DDIR's are unsigned. They match in structure, and the question is put off.
7. **Transactions across manifests, skipping resolution for deterministic ledgers, and commits across independent roots** are deferred or ruled out for now; see the design notes.

## Where the code stands

* **The model runs.** `model/ledger.ddp` is the state, the write frontier and the invariants.
  `server/demo/ledger_views.txt` shows the write frontiers under a two-level clock tree, and then breaks each invariant once.
* **The rules run.** `model/resolve.ddp` resolves one round as described above.
  `server/demo/ledger_rounds.txt` runs three rounds on the stock server.
* **They are checked against each other.** `tests/ledgers.rs` runs rounds of the rules, and after each has the model derive the indexes from the manifest alone; they must agree, and every invariant must hold.
* **Many sessions can feed one resolver.** `server/demo/ledger_resolver.py` has nine client sessions and a tenth that pumps the rounds.
* **Rounds can be made durable, and recovered.** `bench/ledger/fabric.py` writes each round's change to the manifest as a blob and swaps a root file.
  With `--check` it kills the server, rebuilds the manifest from the log, derives the indexes with the model, and checks the invariants.
  Its measurements are in `bench/ledger/README.md`.
* **Frontier comparisons never fail.** They take times of different widths, reading a missing coordinate as before every int, so a ragged request cannot stop a server.
  The rules reject such requests before comparing them.

What the server does not do itself, and a driver does from outside:

* commit a round durably, and release its outcomes only afterwards;
* rebuild its state from a manifest when it starts;
* pump the rounds, and assign transaction keys;
* read a ledger's batches from blobs, or write them: every batch so far is empty.
