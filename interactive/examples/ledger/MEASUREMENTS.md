# Ledger rounds: measurements

These are results from [`fabric.py`](fabric.py), the driver that makes each round durable.
[`README.md`](README.md) says how a round goes and how to run it.

## Recorded results

Apple M4 Mac mini (4 performance and 6 efficiency cores), 2026-11-10, four workers, one million ledgers.
Each figure is from its own run on a fresh log: the mean of 128 rounds after 5 of warmup (64 for the largest rounds).
Every ledger has one time coordinate and every batch is empty, so these are spans of metadata.
Unless said otherwise: every ledger follows one clock, which advances every round; the manifest is resident; commits use `F_FULLFSYNC`.
A span is one committed append, to a ledger or to a clock.

### Spans per round, from none to many

| Appends to ledgers per round | Serial ms per round | Serial spans/s | Pipelined ms per round | Pipelined spans/s | Compute ms | Commit ms | Latency p50 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 14.5 | | 7.8 | | 7.7 | 6.8 | 14.6 |
| 300 | 15.8 | 19.1k | 8.7 | 34.6k | 8.6 | 6.6 | 15.4 |
| 1,200 | 18.7 | 64.4k | 10.9 | 111k | 10.8 | 6.8 | 17.1 |
| 5,000 | 24.7 | 203k | 17.2 | 291k | 17.1 | 7.0 | 22.1 |
| 20,000 | 49.7 | 402k | 41.5 | 482k | 41.4 | 7.2 | 40.9 |
| 50,000 | 95.0 | 526k | 85.4 | 585k | 85.3 | 8.2 | 79.1 |
| 100,000 | 171.3 | 584k | 160.0 | 625k | 159.9 | 9.5 | 165.6 |
| 200,000 | | | 299.2 | 668k | 299.0 | 11.6 | 284.4 |

Compute, commit and latency are from the pipelined runs; latency is from a round's start to its commit's return.

- **The fixed cost of durability is about 7 ms a commit**, nearly all of it the two flushes: 3 ms after the blob and 3.5 ms after the root.
  It grows slowly with the record, which is 21 bytes a span.
- **An empty round already computes for longer than it commits**: 7.7 ms against 6.8 ms.
- **Rounds of any size are bound by compute**, and pipelining gains less as they grow: at 50,000 spans a round, commit is 8 ms of 85.
- **Throughput rises with round size**, from 291k spans/s at 5,000 a round to 668k at 200,000, as fixed costs spread; latency rises with it.

### Where a round's compute goes

Pipelined, in ms.

| Appends per round | Load | Feed | Resolve | Read | Absorb | Total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 5.9 | 0.1 | 0.9 | 0.2 | 0.6 | 7.7 |
| 5,000 | 6.1 | 0.2 | 8.4 | 0.6 | 1.9 | 17.1 |
| 50,000 | 6.5 | 0.7 | 63.5 | 4.5 | 10.2 | 85.3 |

- **Loading the two programs costs about 6 ms every round**, most of an empty round.
  The server parses, optimizes and checks them afresh each time.
  Without it, compute would pass commit between 1,200 and 5,000 spans a round.
- **Resolving costs about 1.3 µs a span** at 50,000 a round, summed over the round's wall time.

### Where the workers' time goes

From a sampling profile at 18,000 appends a round, as shares of the four workers' wall time.

| What | Share |
| --- | ---: |
| Merging the standing arrangements, at each tick | 24% |
| Join operators | 13% |
| Reduce operators, of which the rules' own lambda is 5 | 12% |
| Arrange operators, which sort each round's rows into batches | 10% |
| Map and filter, exchange, progress tracking | 7% |
| Installing the two programs | 13% |
| Idle, waiting on another worker or on the driver | 15% |
| Feed, read out, drop | 4% |

Merging, installing and idling are half of it.
A merge writes its key and value columns three times: after merging, after compacting times, and when settling.

### Round sizes near a power of two

Avoid them when measuring.
A round within about 3% of a power of two lands 20 to 30% fewer spans a second than its neighbours: 16,384 a round lands 378k, where 15,000 and 18,000 land 455k and 457k.

The cause is in how an arrangement schedules its merges.
It files each batch in a layer by its length rounded up to a power of two.
At these sizes each worker's batch falls just under or just over a boundary at random, so the workers' merges fall out of step.
Each round then waits for the one worker that is merging, while the others idle.
No more work is done: the workers use 6.9 µs of CPU a span at 16,384 against 7.4 at 18,000, but are busy 65% of the time, not 84%.
With three workers the slow size moves to 12,288, as this predicts.

Sizes run one after another in one process also affect each other, which is why each figure here has its own run.

### What the flush costs

Serial, in ms.

| Sync | Commit, empty round | Round, empty | Commit, 20,000 spans | Round, 20,000 spans |
| --- | ---: | ---: | ---: | ---: |
| none | 0.4 | 7.9 | 0.8 | 42.3 |
| fsync | 0.5 | 7.9 | 1.0 | 42.4 |
| full | 7.0 | 14.5 | 8.2 | 49.7 |

On macOS, fsync alone hands data to the drive without making the drive store it.

### Clocks

Pipelined.

| Appends per round | No clocks, ms per round | No clocks, spans/s | One clock, ms per round | One clock, spans/s |
| ---: | ---: | ---: | ---: | ---: |
| 5,000 | 15.4 | 325k | 17.2 | 291k |
| 20,000 | 35.5 | 563k | 41.5 | 482k |
| 50,000 | 71.1 | 703k | 85.4 | 585k |

An append to a ledger under a clock costs 12 to 20% more than to a free one.

With every clock advanced every round, a clock's advance is a span like any other:

| Clocks | Appends to ledgers per round | ms per round | All spans/s | Appends to ledgers/s |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0 | 8.3 | 12.1k | 0 |
| 100 | 5,000 | 17.6 | 290k | 285k |
| 100 | 20,000 | 41.3 | 487k | 485k |
| 10,000 | 0 | 25.1 | 398k | 0 |
| 10,000 | 5,000 | 35.0 | 429k | 143k |
| 10,000 | 20,000 | 58.6 | 512k | 341k |
| 100,000 | 0 | 161.3 | 620k | 0 |
| 100,000 | 5,000 | 184.3 | 570k | 27.1k |
| 100,000 | 20,000 | 203.8 | 589k | 98.1k |

- **Advancing every clock lands 400k to 600k spans/s**, the same rate as appends to ledgers.
- **What a clock saves is the spans it stands for.** Ten thousand clocks advance all million ledgers in 25 ms.

### Kinds of commit

Pipelined, one clock, 20,000 appends per round unless said.

| Kind | ms per round | Committed/s | Note |
| --- | ---: | ---: | --- |
| One append per transaction | 41.5 | 482k spans | |
| Four appends per transaction | 42.7 | 469k spans | 117k transactions/s |
| With 10,000 more that repeat an append and are rejected | 59.0 | 339k spans | 508k transactions decided/s |
| Creations under a clock, 50,000 per round | 61 | 818k ledgers | |
| Creations of free ledgers, 50,000 per round | 42 | 1.20M ledgers | |
| Manifest not resident, 50,000 appends per round | 72.3 | 692k spans | 585k with it resident |

### Workers

Pipelined, spans/s.
The fifth worker and beyond run on efficiency cores.

| Workers | 11,585 appends per round | 25,600 | 92,682 |
| ---: | ---: | ---: | ---: |
| 1 | 202k | 227k | 248k |
| 2 | 299k | 362k | 408k |
| 3 | 375k | 450k | 525k |
| 4 | 402k | 523k | 616k |
| 6 | 372k | 465k | 627k |
| 8 | 388k | 552k | 712k |

Larger rounds gain more from each worker, because loading the programs and the driver's own work do not shrink: four workers are 2.0, 2.3 and 2.5 times one.

### Recovery

With one million ledgers and a log of 71 records, the new server rebuilt the manifest, derived the indexes and checked the invariants in 1.2 s.
What it rebuilt matched, no invariant was broken, and it resolved the next round.

## What these runs do not show

- Batches with data in them: writing them as blobs, or reading them back.
- A log that is ever shortened: recovery reads every record.
- More than one resolver, or a resolver that commits through a parent.
- A power failure: the server is killed, but the machine stays up.
