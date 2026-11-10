# Ledger rounds, resolved by DDIR and made durable

`fabric.py` runs rounds of ledger transactions on a stock `ddir_server`, and makes each round durable.
The rules are [`../../model/resolve.ddp`](../../model/resolve.ddp), and the model they implement is [`../../model.md`](../../model.md).
Everything the server runs is `.ddp`; the driver feeds requests, reads conclusions, writes them to disk, and pumps.

## How a round goes

Three kinds of program run on the server.

- `state`, from [`state.ddp`](state.ddp), stands for the whole run: the manifest, and the three indexes the rules read.
- `gen`, from [`requests.ddp`](requests.ddp), lives for one round: it shapes flat columns of ints into requests.
- `res`, the rules, lives for one round, bound to `state` and `gen`.

The driver then does five things.

1. It loads `gen` and `res`, and feeds the round's requests to `gen` as binary frames.
2. It ticks, and the rules decide every request.
3. It reads the round's change to the manifest, as binary frames.
4. It feeds the rules' conclusions into `state`, drops `gen` and `res`, and ticks.
5. It writes the frames as one blob, flushes, swaps the root file, and flushes again.

A round's outcomes count as reported when step 5 returns.
In `pipelined` mode step 5 runs on another thread while the next round resolves, and rounds that queue behind a commit share the next one.
The log is a chain of blobs, each naming the one before, and a root file naming the last.

## Run it

```sh
cargo build --release -p ddir-server
python3 interactive/bench/ledger/fabric.py --dir /tmp/fabric --ledgers 100000 --clocks 1 --spans 0,4096 --check
```

`--check` kills the server afterwards and starts another.
It feeds the log into the new server's manifest, derives the indexes from the manifest with [`../../model/ledger.ddp`](../../model/ledger.ddp), and checks that model's invariants.
What it rebuilt must equal what the first server held, and one more round must resolve on it.
Every round of every run also checks that exactly the expected number of actions committed.

## Recorded results

Apple M4 Mac mini (mini-03), 2026-11-10, four workers, one million ledgers, means over 25 rounds after 5 of warmup (10 after 2 for the largest).
Every ledger has one time coordinate and every batch is empty, so these are spans of metadata.
Unless said otherwise: every ledger follows one clock, which advances every round; the manifest is resident; commits use `F_FULLFSYNC`.
A span is one committed append, to a ledger or to a clock.

### Spans per round, from none to many

| Spans with data per round | Serial ms per round | Serial spans/s | Pipelined ms per round | Pipelined spans/s | Compute ms | Commit ms | Latency p50 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 16.1 | | 8.1 | | 7.6 | 7.7 | 18.9 |
| 256 | 16.5 | 15.6k | 9.0 | 28.6k | 8.6 | 7.9 | 16.5 |
| 1,024 | 18.3 | 55.9k | 10.6 | 97.1k | 10.2 | 7.9 | 17.9 |
| 4,096 | 25.8 | 159k | 17.9 | 229k | 17.5 | 8.0 | 23.5 |
| 16,384 | 48.4 | 339k | 40.9 | 401k | 40.5 | 8.3 | 38.5 |
| 65,536 | 145.9 | 449k | 136.9 | 479k | 136.4 | 10.9 | 133.4 |
| 262,144 | | | 393.7 | 666k | 391.0 | 25.0 | 432 |

Compute, commit and latency are from the pipelined runs; latency is from a round's start to its commit's return.

- **The fixed cost of durability is about 8 ms a commit**, nearly all of it the two flushes: 3.5 ms after the blob and 4 ms after the root.
  It does not grow until a round's record passes a megabyte.
- **Compute passes commit within a few hundred spans a round.**
  An empty round already computes for 7.6 ms, against 7.7 ms to commit.
- **Past that, rounds are bound by compute**, and pipelining gains little: at 65,536 spans a round, commit is 11 ms of 137.
- **Throughput rises with round size**, from 229k spans/s at 4,096 a round to 666k at 262,144, as fixed costs spread; latency rises with it.

### Where a round's compute goes

Pipelined, in ms.

| Spans per round | Load | Feed | Resolve | Read | Absorb | Total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 5.8 | 0.1 | 0.9 | 0.2 | 0.6 | 7.6 |
| 4,096 | 6.2 | 0.2 | 7.7 | 1.8 | 1.7 | 17.5 |
| 65,536 | 6.6 | 0.9 | 80.8 | 32.8 | 15.3 | 136.4 |

- **Loading the two programs costs about 6 ms every round**, most of an empty round. The server parses and plans them afresh each time.
  Without it, compute would pass commit near 3,000 spans a round, not a few hundred.
- **Reading the conclusions out to the driver is a quarter of a large round.** A server that wrote its own blob would not pay it.
- **Resolving costs about 1.2 µs a span** at 65,536 a round, summed over the round's wall time.

### What the flush costs

Serial, in ms.

| Sync | Commit, empty round | Round, empty | Commit, 16,384 spans | Round, 16,384 spans |
| --- | ---: | ---: | ---: | ---: |
| none | 0.4 | 7.8 | 0.9 | 45.1 |
| fsync | 0.4 | 7.9 | 1.1 | 45.1 |
| full | 8.3 | 16.1 | 9.4 | 48.4 |

On macOS, fsync alone hands data to the drive without making the drive store it.

### Clocks

Pipelined.

| Spans with data per round | No clocks, ms per round | No clocks, spans/s | One clock, ms per round | One clock, spans/s |
| ---: | ---: | ---: | ---: | ---: |
| 4,096 | 14.1 | 290k | 17.9 | 229k |
| 16,384 | 35.9 | 457k | 40.9 | 401k |
| 65,536 | 114.7 | 571k | 136.9 | 479k |

An append to a ledger under a clock costs about a fifth more than to a free one.

With every clock advanced every round, a clock's advance is a span like any other:

| Clocks | Spans with data per round | ms per round | All spans/s | Spans with data/s |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0 | 8.7 | 11.5k | 0 |
| 100 | 4,096 | 16.1 | 260k | 254k |
| 100 | 16,384 | 39.2 | 420k | 418k |
| 10,000 | 0 | 24.1 | 415k | 0 |
| 10,000 | 4,096 | 32.1 | 440k | 128k |
| 10,000 | 16,384 | 54.1 | 487k | 303k |
| 100,000 | 0 | 161.8 | 618k | 0 |
| 100,000 | 4,096 | 179.4 | 580k | 22.8k |
| 100,000 | 16,384 | 205.5 | 566k | 79.7k |

- **Advancing every clock lands 400k to 600k spans/s**, the same rate as appends with data.
- **What a clock saves is the spans it stands for.** Ten thousand clocks advance all million ledgers in 24 ms.

### Kinds of commit

Pipelined, one clock, 16,384 appends with data per round unless said.

| Kind | ms per round | Committed/s | Note |
| --- | ---: | ---: | --- |
| One append per transaction | 40.9 | 401k spans | |
| Four appends per transaction | 40.6 | 404k spans | 101k transactions/s |
| With 8,192 more that repeat an append and are rejected | 53.7 | 305k spans | 457k transactions decided/s |
| Creations under a clock, 50,000 per round | 61 | 810k ledgers | |
| Creations of free ledgers, 50,000 per round | 41 | 1.21M ledgers | |
| Manifest not resident, 65,536 appends per round | 101.6 | 645k spans | 479k with it resident |

### Recovery

With one million ledgers and a log of 51 records, the new server rebuilt the manifest, derived the indexes and checked the invariants in 1.2 to 1.4 s.
What it rebuilt matched, no invariant was broken, and it resolved the next round.

## What these runs do not show

- Batches with data in them: writing them as blobs, or reading them back.
- A log that is ever shortened: recovery reads every record.
- More than one resolver, or a resolver that commits through a parent.
- A power failure: the server is killed, but the machine stays up.
