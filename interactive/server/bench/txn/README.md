# Transactions on the live DDIR server

The optimistic read-set/write-set scheme from
[Transaction Processing in the Data Plane][post], run two ways: as one
maintained program, and as a resolver installed for each epoch over a separate
store. [`bench_txn.py`](bench_txn.py) drives either through the live server,
closed loop, with compare-and-set transactions (one read and one write of a
key, `--shape cas`, the default) or the post's two reads and two writes
(`--shape blog`). Every rollback set is checked against a serial reference,
and the final store is compared in full.

[post]: https://github.com/frankmcsherry/blog/blob/master/posts/2025-04-27.md

## Two programs

**Maintained** ([`maintained.ddp`](maintained.ddp)). One program over a log of
intents. The store is not a separate collection: it is the log's current
committed writes, which keep their transaction ids. After a tick seals the
epoch's ids, everything else in the log (reads, rejected writes, superseded
writes) is `log - store`, exported as `retire`, and

    feed bank 0 from retire diff=-1

leaves the log equal to the store. A second tick applies the deletions. That
tick is the cost of the design: every row of the batch goes into the
resolver's joins, `min`, and recursion, and then comes back out.

**One-off** ([`store.ddp`](store.ddp), [`resolve.ddp`](resolve.ddp)). The store
is its own program: `((key) ; (txn id, value))`. Each epoch installs a resolver
that imports it, takes only that epoch's intents, and exports `rollback` and
`delta` (each written key's latest committed write in, its prior store row
out). The client reads `rollback`, runs `feed store 0 from delta`, and drops
the resolver. Nothing the resolver holds is ever retracted. Imports arrive as
the producer's arrangement (#918), and the resolver reads the store only in
joins outside the recursion, so it touches the store at the keys its batch
reads or writes. The store change takes effect at the next tick, together with
the next batch, so one tick per cycle; its merge work is counted there.
[`resolve_by_key.ddp`](resolve_by_key.ddp) is the same resolver with each key
answered by one reduce (below).

## Results

Measured 2026-10-20 on an Apple M4 Mac mini (4 performance + 6 efficiency
cores, 16 GiB), master-next `854ade89` plus these programs, release build,
diagnostics off. Five measured cycles after two warmups. Every transaction's
ids increase; "cycle" is everything from admission to the store being ready
for the next batch. Intents go to the server as one binary frame of columns
per batch (`feed … frame`, the default; `--feed text` sends framed text rows).
Encoding the frame in Python happens before the clock starts.

One read and one write per transaction, sparse random keys from 10M:

| Workers | Batch | Maintained cycle p50 | One-off cycle p50 | Maintained commits/s | One-off commits/s |
|---:|---:|---:|---:|---:|---:|
| 4 | 10,000 | 7.2 ms | 3.6 ms | 1.39M | 2.72M |
| 4 | 50,000 | 30.7 ms | 12.8 ms | 1.50M | 3.73M |
| 4 | 200,000 | 146.9 ms | 49.9 ms | 1.32M | 3.68M |
| 8 | 10,000 | 7.5 ms | 4.1 ms | 1.33M | 2.37M |
| 8 | 50,000 | 26.0 ms | 11.1 ms | 1.80M | 4.21M |
| 8 | 200,000 | 115.7 ms | 41.8 ms | 1.63M | 4.42M |

Exactly `N` populated keys, each updated once per cycle (every write
supersedes a store row):

| Workers | Objects | Maintained cycle p50 | One-off cycle p50 |
|---:|---:|---:|---:|
| 4 | 100 | 0.8 ms | 1.0 ms |
| 4 | 1,000 | 1.8 ms | 1.4 ms |
| 4 | 10,000 | 11.1 ms | 5.1 ms |
| 4 | 100,000 | 115.0 ms | 41.9 ms |
| 8 | 100,000 | 91.8 ms | 33.0 ms |

Mean phases, sparse, 4 workers, batch 50,000:

| Phase | Maintained | One-off |
|---|---:|---:|
| install resolver | — | 0.8 ms |
| admission (one binary frame) | 1.3 ms | 1.3 ms |
| resolution tick | 16.5 ms | 10.7 ms |
| read rollbacks | 0.2 ms | 0.2 ms |
| feed from trace | 1.0 ms | 0.3 ms |
| maintenance tick | 14.2 ms | — |
| drop resolver | — | 0.2 ms |

The one-off resolver is 2–3x the maintained program from 10,000 rows up.
Installing and dropping a resolver costs about 1 ms, so at 100 rows per epoch
the maintained program is faster (0.8 vs 1.0 ms). As framed text rows,
admission of the same batch took 13.1 ms, parsed on one session thread; as a
frame it is 1.3 ms, and the resolution tick is the cycle.

### Against the 2026-09-01 spike

The spike (`c93072c6`, rebuilt with timely pinned to `c2c336c5`, its own
`bench_txn.py` and `txn.ddp`) run on the same machine, sparse keys, both
feeding text:

| Workers | Batch | Spike commits/s | Now, the spike's three-part retire | Now, maintained.ddp |
|---:|---:|---:|---:|---:|
| 4 | 10,000 | 370k | 1.03M | 1.03M |
| 4 | 50,000 | 377k | 1.08M | 1.08M |
| 8 | 10,000 | 317k | 0.98M | 1.00M |
| 8 | 50,000 | 379k | 1.22M | 1.22M |

The one-line `retire` changes nothing measurable; the 3x is the server and
Corgi work merged since September. At 4 workers and batch 50,000 the spike's
phases were 23 ms admission, 66–68 ms resolution, 47 ms maintenance.

These are not comparable to the post's Materialize numbers (17.5k–27.7k
closed-loop transactions/s at low contention). There a transaction is two
reads and two writes over 10,000 locations, commands go through SQL and the
coordinator, and writes are durable. Here a transaction is one read and one
write over 10M keys, in memory, not durable, from one closed-loop client.

### The post's workload

`--shape blog`: two reads and two writes at distinct random locations, reads
observing the store as of the batch's start (as the post's `INSERT` does).
Committed transactions per second, 4 workers:

| Locations | Batch | Commit | Maintained | One-off |
|---:|---:|---:|---:|---:|
| 10,000 | 1,000 | 84% | 231k | 416k |
| 10,000 | 10,000 | 40% | 80k | 295k |
| 10,000 | 100,000 | 9% | 2.7k | 7.7k |
| 100,000 | 10,000 | 84% | 397k | 1.16M |
| 1,000,000 | 10,000 | 98% | 629k | 1.52M |
| 10,000,000 | 10,000 | 99.8% | 737k | 1.67M |
| 10,000,000 | 100,000 | 98% | 684k | 1.87M |

The September spike's server and program, given the same generator and text
feeds, commit the same transactions at 83k, 26k, 113k, 167k, and 184k/s for
the 1,000- and 10,000-batch rows above (10M locations: 152k at batch 1,000).

The 10,000-location, 10,000-transaction row is the post's own example. Large
batches over few locations hit the per-key quadratic the post's appendix
describes: every read of a key pairs with every earlier write to it in the
batch, so a 100,000-transaction batch over 10,000 keys takes over a second.

### How often can `N` objects be refreshed?

`--shape blog --keys-from-batch`: `N` transactions over `N` locations per
cycle, so each object is written about twice per cycle and 40% commit. A
cycle is a refresh of the whole set. Refreshes per second, from the cycle
p50:

| Objects | Maintained, 4w | Maintained, 8w | One-off, 4w | One-off, 8w |
|---:|---:|---:|---:|---:|
| 1,000 | 133 /s | 88 /s | 333 /s | 192 /s |
| 3,000 | 58 /s | 51 /s | 179 /s | 128 /s |
| 10,000 | 20 /s | 22 /s | 73 /s | 69 /s |
| 30,000 | 6.6 /s | 7.9 /s | 25 /s | 28 /s |
| 100,000 | 2.0 /s | 2.5 /s | 7.6 /s | 9.2 /s |
| 300,000 | 0.63 /s | 0.81 /s | 2.6 /s | 3.3 /s |

Throughput is about flat from 10,000 objects up — 200–250k attempts/s
maintained, 730k–1M one-off — so refreshes per second fall as `1/N`. With the
density of reads and writes per key fixed, a larger set costs proportionally
more and no worse.

With one read and one write of the same key (`--shape cas`), each object
exactly once per cycle (`--key-pattern round-robin`, every transaction
commits):

| Objects | Maintained, 4w | Maintained, 8w | One-off, 4w | One-off, 8w |
|---:|---:|---:|---:|---:|
| 1,000 | 526 /s | 357 /s | 714 /s | 500 /s |
| 3,000 | 270 /s | 222 /s | 455 /s | 357 /s |
| 10,000 | 88 /s | 96 /s | 196 /s | 196 /s |
| 30,000 | 31 /s | 38 /s | 76 /s | 88 /s |
| 100,000 | 8.7 /s | 11 /s | 24 /s | 31 /s |
| 300,000 | 2.6 /s | 3.4 /s | 7.4 /s | 10.5 /s |

That is 0.8–1.1M commits/s maintained and 2.0–3.2M one-off from 10,000
objects up: about 3x the two-read, two-write workload's attempts, and 8x its
commits. With random keys instead (`--key-pattern random`, the same key may
appear several times in a batch, and 63% commit), attempts run 8–25% slower
and commits 40–50% fewer.

Eight workers help from about 30,000 objects; below that, four have less
overhead per cycle. The M4 has four performance cores.

### Where a cycle goes

The post's example (10,000 locations, 10,000 transactions = 40,000 rows),
one-off, 4 workers: 13.9 ms.

| Phase | Mean |
|---|---:|
| install resolver | 0.7 ms |
| admission (one binary frame) | 0.6 ms |
| resolution tick | 12.0 ms |
| read rollbacks, feed delta, drop | 0.7 ms |

As framed text rows the same 40,000 rows took 4.9 ms to admit (about 125 ns
per row on one session thread), and the cycle 18.3 ms. The resolution tick
keeps all four workers busy. About 80% of it is inside the recursion, which
takes 7–9 rounds per epoch; the rollback set changes by about 11,500
transactions on its way to about 6,000. The whole cycle is 36.6, 22.0, 13.9,
and 14.6 ms at 1, 2, 4, and 8 workers.

The maintained program spends 21.5 ms resolving and 27.2 ms maintaining at
the same size; a quarter of its workers' time is merging trace batches,
against under 1% for the one-off resolver.

### The resolution tick's operators

Each operator's own scheduling time, summed over workers and the measured
rounds, for the case above, as `--profile 8` prints it (`DDIR_PROFILE=1`; see
the server README's `profile`). Each operator is labelled with the
`resolve.ddp` statement that built it:

| Label | Operator | Share |
|---|---|---:|
| `best` | `CorgiReduce` (the `min`) | 25.3% |
| `pairs` | `Join` | 10.4% |
| `best` | `CorgiArrange` (the `min`'s input) | 8.6% |
| `rollback` | `CorgiReduce` (the `distinct`) | 6.5% |
| `base` | `Join` (against the store) | 6.1% |
| `bad_absent` | `CorgiArrange` (`unseen`, for its join) | 4.1% |
| `wr_bad` | `Join` | 3.9% |
| `res` | the recursion's own scheduling | 3.4% |

### Answering each key with one reduce

[`resolve_by_key.ddp`](resolve_by_key.ddp) replaces the join of reads with
prior writes and the `min` per read by one reduce per key, which pairs each
read with its key's writes by `cap_list`. Same answers, one-off, 4 workers,
cycle p50:

| Workload | `resolve.ddp` | `resolve_by_key.ddp` |
|---|---:|---:|
| post's shape, 1,000 objects | 3.1 ms | 2.6 ms |
| post's shape, 10,000 objects | 13.9 ms | 12.0 ms |
| post's shape, 100,000 objects | 134.8 ms | 103.2 ms |
| one read one write, 10,000 objects | 5.0 ms | 4.7 ms |
| one read one write, 100,000 objects | 42.3 ms | 39.9 ms |

The reduce is then about 45% of the resolver, and of the reduce only about a
fifth is the lambda: the rest is the reduce's own work per key (presenting
each group's values, and the int-proxy bookkeeping).

## Reproduction

```console
cargo build --release -p ddir-server

# Sparse keys, both modes.
python3 interactive/server/bench/txn/bench_txn.py \
  --workers 4,8 --batch-sizes 10000,50000,200000 \
  --keys 10000000 --rounds 5 --warmup 2

# Exactly as many populated keys as commands.
python3 interactive/server/bench/txn/bench_txn.py \
  --workers 4,8 --batch-sizes 100,1000,10000,100000 \
  --keys-from-batch --key-pattern round-robin --rounds 5 --warmup 2

# The post's workload, and refreshing N objects.
python3 interactive/server/bench/txn/bench_txn.py --shape blog \
  --workers 4 --batch-sizes 1000,10000,100000 --keys 10000 --rounds 5 --warmup 2
DDIR_MAX_FEED_BYTES=268435456 python3 interactive/server/bench/txn/bench_txn.py \
  --shape blog --keys-from-batch --workers 4,8 \
  --batch-sizes 1000,3000,10000,30000,100000,300000 --rounds 5 --warmup 2
```

`--modes maintained` or `--modes oneoff` runs one; `--resolve-program` and
`--maintained-program` substitute a program (`resolve_by_key.ddp`, say);
`--feed text` sends framed text rows instead of a binary frame; `--replicas R`
makes groups of `R` transactions race the same expected value; `--wrap` runs
the server under another command (a profiler); `--profile N` prints the `N`
operators that took the most time; `--json` keeps per-cycle phases and the
client's encoding time.
