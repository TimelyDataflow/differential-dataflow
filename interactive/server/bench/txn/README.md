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

## Results

Measured 2026-10-20 on an Apple M4 Mac mini (4 performance + 6 efficiency
cores, 16 GiB), master-next `afd8182a` plus these programs, release build,
diagnostics off. Five measured cycles after two warmups. Every
transaction's ids increase; "cycle" is everything from admission to the store
being ready for the next batch. Commands are text, parsed by the server.

One read and one write per transaction, sparse random keys from 10M:

| Workers | Batch | Maintained cycle p50 | One-off cycle p50 | Maintained commits/s | One-off commits/s |
|---:|---:|---:|---:|---:|---:|
| 4 | 10,000 | 9.5 ms | 5.9 ms | 1.04M | 1.67M |
| 4 | 50,000 | 43.4 ms | 25.3 ms | 1.10M | 1.92M |
| 4 | 200,000 | 203.0 ms | 103.4 ms | 0.97M | 1.91M |
| 8 | 10,000 | 9.7 ms | 6.6 ms | 0.99M | 1.50M |
| 8 | 50,000 | 38.7 ms | 24.2 ms | 1.24M | 2.03M |
| 8 | 200,000 | 172.8 ms | 94.6 ms | 1.14M | 2.06M |

Exactly `N` populated keys, each updated once per cycle (every write
supersedes a store row):

| Workers | Objects | Maintained cycle p50 | One-off cycle p50 |
|---:|---:|---:|---:|
| 4 | 100 | 0.8 ms | 1.0 ms |
| 4 | 1,000 | 2.0 ms | 1.7 ms |
| 4 | 10,000 | 13.5 ms | 7.5 ms |
| 4 | 100,000 | 140.0 ms | 69.1 ms |
| 8 | 100,000 | 113.4 ms | 58.5 ms |

Mean phases, sparse, 4 workers, batch 50,000:

| Phase | Maintained | One-off |
|---|---:|---:|
| install resolver | — | 0.8 ms |
| text parse and admission | 12.8 ms | 13.1 ms |
| resolution tick | 17.5 ms | 11.5 ms |
| read rollbacks | 0.2 ms | 0.2 ms |
| feed from trace | 1.0 ms | 0.3 ms |
| maintenance tick | 13.9 ms | — |
| drop resolver | — | 0.2 ms |

The one-off resolver is 1.6–2x the maintained program from 10,000 rows up.
Installing and dropping a resolver costs about 1 ms, so below roughly 1,000
rows per epoch the maintained program is faster (0.8 vs 1.0 ms at 100). With
the maintenance tick gone, text admission is now half of a one-off cycle; the
next gain is a binary or columnar ingress, not the dataflow.

### Against the 2026-09-01 spike

The spike (`c93072c6`, rebuilt with timely pinned to `c2c336c5`, its own
`bench_txn.py` and `txn.ddp`) run on the same machine the same day, sparse
keys:

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
| 10,000 | 1,000 | 84% | 208k | 337k |
| 10,000 | 10,000 | 40% | 74k | 217k |
| 10,000 | 100,000 | 9% | 2.7k | 7.4k |
| 100,000 | 10,000 | 84% | 321k | 669k |
| 1,000,000 | 10,000 | 98% | 466k | 835k |
| 10,000,000 | 10,000 | 99.8% | 527k | 871k |
| 10,000,000 | 100,000 | 98% | 486k | 892k |

The September spike's server and program, given the same generator, commit
the same transactions at 83k, 26k, 113k, 167k, and 184k/s for the 1,000- and
10,000-batch rows above (10M locations: 152k at batch 1,000).

The 10,000-location, 10,000-transaction row is the post's own example. Large
batches over few locations hit the per-key quadratic the post's appendix
describes: every read of a key pairs with every earlier write to it in the
batch, so a 100,000-transaction batch over 10,000 keys takes over a second.

### How often can `N` objects be refreshed?

`--shape blog --keys-from-batch`: `N` transactions over `N` locations per
cycle, so each object is written about twice per cycle and 40% commit. A
cycle is a refresh of the whole set. `DDIR_MAX_FEED_BYTES=268435456` for the
300,000 row.

| Objects | Maintained, 4w | Maintained, 8w | One-off, 4w | One-off, 8w |
|---:|---:|---:|---:|---:|
| 1,000 | 125 /s | 85 /s | 278 /s | 175 /s |
| 3,000 | 54 /s | 48 /s | 141 /s | 110 /s |
| 10,000 | 18 /s | 19 /s | 53 /s | 51 /s |
| 30,000 | 6.0 /s | 7.0 /s | 17.7 /s | 19.0 /s |
| 100,000 | 1.8 /s | 2.2 /s | 5.4 /s | 6.1 /s |
| 300,000 | 0.56 /s | 0.70 /s | 1.76 /s | 2.0 /s |

(Cycles per second from the cycle p50.) Throughput is flat from 10,000
objects up — about 180–220k attempts/s maintained and 530–615k one-off — so
refreshes per second fall as `1/N`. With the density of reads and writes per
key fixed, a larger set costs proportionally more and no worse.

With one read and one write of the same key (`--shape cas`), each object
exactly once per cycle (`--key-pattern round-robin`, every transaction
commits), refreshes per second:

| Objects | Maintained, 4w | Maintained, 8w | One-off, 4w | One-off, 8w |
|---:|---:|---:|---:|---:|
| 1,000 | 500 /s | 323 /s | 588 /s | 435 /s |
| 3,000 | 227 /s | 189 /s | 357 /s | 278 /s |
| 10,000 | 75 /s | 79 /s | 137 /s | 133 /s |
| 30,000 | 25 /s | 30 /s | 49 /s | 54 /s |
| 100,000 | 7.2 /s | 8.7 /s | 14.5 /s | 17.6 /s |
| 300,000 | 2.2 /s | 2.7 /s | 4.7 /s | 5.4 /s |

That is 650–890k commits/s maintained and 1.4–1.8M one-off from 10,000
objects up: about 3x the two-read, two-write workload's attempts, and 7x its
commits. With random keys instead (`--key-pattern random`, the same key may
appear several times in a batch, and 63% commit), attempts run about 10%
slower and commits about 40% fewer.

### Where a cycle goes

The post's example (10,000 locations, 10,000 transactions = 40,000 rows),
one-off, 4 workers: 18.5 ms.

| Phase | Mean |
|---|---:|
| install resolver | 0.7 ms |
| text parse and admission | 5.1 ms |
| resolution tick | 12.1 ms |
| read rollbacks, feed delta, drop | 0.7 ms |

Admission is one session thread parsing text (about 125 ns per row, the same
at 1–8 workers): reading the line, splitting it on whitespace twice (once to
look for `end-feed`, once to read the row), and parsing integers. The
resolution tick keeps all four workers busy. About two thirds of it is inside
the recursion, which takes 7–9 rounds per epoch; the rollback set changes by
about 11,500 transactions on its way to about 6,000. Of the workers' busy time,
reduce (the recursion's `min` and `distinct`) is about a third, building
arrangements (sorting new batches into chunks) about a quarter, and joins
about a fifth. Resolution is 32, 19, 12, and 12.7 ms at 1, 2, 4, and 8
workers; the M4 has four performance cores.

The maintained program spends 21.7 ms resolving and 27 ms maintaining at the
same size; a quarter of its workers' time is merging trace batches, against
under 1% for the one-off resolver.

### The resolution tick's operators

Each operator's own scheduling time, from Timely's logs, summed over workers
and the measured rounds, for the case above. Operators are named by their
address in the resolver; inside `res` the addresses follow `resolve.ddp`'s
order:

| Operator | Line | Share |
|---|---|---:|
| `CorgiReduce [3, 8, 18]` | `best = (pairs + base) \| min` | 25.8% |
| `Join [3, 8, 14]` | `pairs = rk \| join(wk, …)` | 10.5% |
| `CorgiArrange [3, 8, 17]` | the `min`'s input | 8.2% |
| `CorgiReduce [3, 8, 31]` | `rollback = … \| distinct` | 6.5% |
| `Join [3, 7]` | `base = rk \| join(store, …)` | 5.8% |
| `CorgiArrange [3, 8, 25]` | `unseen`'s arrangement | 4.2% |
| `Join [3, 8, 8]` | `wr_bad = wid \| join(rollback, …)` | 4.0% |

### Answering each key with one reduce

[`resolve_by_key.ddp`](resolve_by_key.ddp) replaces the join of reads with
prior writes and the `min` per read by one reduce per key, which pairs each
read with its key's writes by `cap_list`. Same answers, one-off, 4 workers:

| Workload | `resolve.ddp` | `resolve_by_key.ddp` |
|---|---:|---:|
| post's shape, 1,000 objects | 3.4 ms | 3.0 ms |
| post's shape, 10,000 objects | 18.5 ms | 17.0 ms |
| post's shape, 100,000 objects | 186.9 ms | 157.2 ms |
| one read one write, 10,000 objects | 7.4 ms | 6.9 ms |
| one read one write, 100,000 objects | 64.7 ms | 63.1 ms |

(Cycle p50; `--resolve-program resolve_by_key.ddp`.) The reduce is then about
45% of the resolver, and of the reduce only about a fifth is the lambda: the
rest is the reduce's own work per key (presenting each group's values, and
the int-proxy bookkeeping).

## Reproduction

```console
cargo build --release -p ddir-server

# Sparse keys, both modes.
python3 interactive/server/bench/txn/bench_txn.py \
  --workers 4,8 --batch-sizes 10000,50000,200000 \
  --keys 10000000 --rounds 5 --warmup 2

# Exactly as many populated keys as commands.
python3 interactive/server/bench/txn/bench_txn.py \
  --workers 1,2,4,8 --batch-sizes 100,1000,10000,100000 \
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
`--replicas R` makes groups of `R` transactions race the same expected value;
`--wrap` runs the server under another command (a profiler); `--json` keeps
per-cycle phases.
