# Ledgers: a demonstration of durable ledger logic

This directory demonstrates the logic of durable ledgers in DDIR.
DDIR resolves the transactions, and a driver outside the server makes their results durable.
It is the logic and a test harness, not an integration: nothing here makes a DDIR program durable.

[`model.md`](model.md) is the place to start.
It says what a ledger is, what may change a set of ledgers, what is not settled, and what comes next.

## The files, by purpose

| Purpose | File | What it is |
| --- | --- | --- |
| The model | [`model.md`](model.md) | The model, in prose. |
| | [`ledger.ddp`](ledger.ddp) | The model's state: two collections, what is derived from them, and seven invariants as views that must be empty. |
| | [`resolve.ddp`](resolve.ddp) | The model's rules: one round of transactions, each committed or rejected. |
| | [`check.ddp`](check.ddp) | What a client checks of its own requests before it submits them. It reads no state. |
| Checks | [`../../tests/ledgers.rs`](../../tests/ledgers.rs) | Runs rounds of a client's checks and then the rules, and after each has the state program rebuild the indexes from the manifest. They must agree, and every invariant must hold. |
| | [`views.txt`](views.txt) | A session: the derived views under a tree of clocks, then each invariant broken once. |
| The harness | [`fabric.py`](fabric.py) | A driver that pumps rounds, makes each durable, rebuilds a server from the log, and measures. |
| | [`state.ddp`](state.ddp) | What a resolver keeps between rounds. It has no logic: five inputs, published. |
| | [`requests.ddp`](requests.ddp) | Shapes the driver's flat rows of ints into requests. It has no rules. |
| | [`MEASUREMENTS.md`](MEASUREMENTS.md) | Recorded results. |
| Independent writers | [`resolver.py`](resolver.py) | Nine client sessions submit transactions to one resolver, and a tenth pumps it. Nothing is durable. |

Outside this directory, [`../../src/lambda/frontiers.rs`](../../src/lambda/frontiers.rs) adds the four frontier functions the programs call.

## Why five programs

**`ledger.ddp` is the definition.**
Everything about a ledger is derived from the manifest, which is its registrations and spans.
That derivation does work for every ledger, so it is used to check and to rebuild, and not in each round.

**`resolve.ddp` is the rules, written to cost what a round touches.**
It does not read the manifest.
It reads three indexes of it: each ledger's version and own upper, its width and start, and the clock it follows.
It reports the round's change to the manifest, and the changes to those indexes.
Its last section only words rejections, and changes nothing that commits.

**`check.ddp` is what the rules trust a client to have checked.**
Each check reads the request alone: its actions are in order, its times have one width, and each append's lower is before its upper.
The test runs it in front of the rules.
The drivers here make well-formed requests, and do not run it.

**`state.ddp` holds the manifest and the three indexes between rounds.**
A driver feeds it each round's conclusions.
After a restart, the manifest comes from the log, and `ledger.ddp` derives the indexes from it.

**`requests.ddp` is a convenience of the driver**, which sends plain columns of ints.

## How a round goes

`state` stands for the whole run.
The rules are loaded for one round, bound to `state` and to the round's requests, and then dropped.

1. Load the rules, and feed the round's requests.
2. Tick, and the rules decide every request.
3. Read the round's change to the manifest.
4. Feed the rules' conclusions into `state`, drop the rules, and tick.
5. Write the change as one blob, flush, swap the root file, and flush again.

`fabric.py` does all five, and a round's outcomes count as reported when step 5 returns.
Its log is a chain of blobs, each naming the one before, and a root file naming the last.
`resolver.py` does the first four, with requests arriving from other sessions.

## Run it

From the repository root:

```sh
cargo build --release -p ddir-server

# The check of the rules against the model.
cargo test --release -p interactive --test ledgers

# The model's views, and each invariant broken once.
./target/release/ddir_server < interactive/examples/ledger/views.txt

# Durable rounds, then a kill, a rebuild from the log, and a check of what was rebuilt.
python3 interactive/examples/ledger/fabric.py --dir /tmp/fabric --ledgers 100000 --clocks 1 --spans 0,5000 --check

# Nine sessions and a pump.
python3 interactive/examples/ledger/resolver.py
```

With `--check`, `fabric.py` kills the server and starts another.
It feeds the log into the new server's manifest, derives the indexes with `ledger.ddp`, and checks the invariants.
What it rebuilt must equal what the first server held, and one more round must resolve on it.
Every round of every run also checks that exactly the expected number of actions committed.
