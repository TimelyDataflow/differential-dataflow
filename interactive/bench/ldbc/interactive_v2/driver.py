#!/usr/bin/env python3
"""An SNB Interactive v2 driver for the DDIR server, with DuckDB as a baseline.

    python3 interactive_v2/driver.py --system ddir --server target/release/ddir_server \\
        --data <bi composite-merged-fk dir> --snapshot <journal>/initial.pickle.gz \\
        --parameters parameters.json --tcr 0.01 --operations 100000 --output <dir>

A from-scratch reimplementation of the official Java driver's workload
(ldbc_snb_interactive_v2_driver), following its source where it and the spec
differ. See README.md for what it reproduces and where it departs.

- Updates: every insert and delete of the BI CSV export, one operation each, at
  its own simulation time (snb/operations.py).
- Complex reads: variant q runs at T0 + k * freq_q * update_interleave, with the
  first parameter row (cycling in file order) whose (useFrom, useUntil) contains it.
- Short reads: after each complex read, the driver's random walk over IS1-3 and
  IS4-7, with ids taken from shared buffers that every result refills.
- Time: operation time t is scheduled at wall time W + (t - T0) * TCR.
- Audit: an operation is late if it starts more than 1 s after its scheduled
  start; the run passes if at most 5% of each operation type is late.

Execution: one update thread submits every update as it falls due, and
--threads read workers each run one read at a time (systems.py says how each
system executes them). An operation's start is when the system begins it, and
its duration ends when its result is ready. --validate runs one operation at a
time in a fixed order, ignoring the clock, and writes every answer so that runs
on different systems can be compared.
"""
import argparse
from collections import defaultdict, deque
import gc
import gzip
import heapq
import json
import pickle
import random
import statistics
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from snb.operations import operations  # noqa: E402
from systems import SYSTEMS, now_ms  # noqa: E402

# Simulation milliseconds per update, and each complex read's frequency in updates
# (ldbc_snb_interactive_v2_driver configuration/ldbc/snb/interactive/sf1.properties).
UPDATE_INTERLEAVE = {'1': 1619}
FREQUENCY = {'1': {'1': 26, '2': 37, '3a': 140, '3b': 140, '4': 36, '5': 57, '6': 129, '7': 87, '8': 45,
                   '9': 157, '10': 30, '11': 16, '12': 44, '13a': 38, '13b': 38, '14a': 96, '14b': 96}}
DISSIPATION = 0.2
BUFFER = 1024

UPDATE_NAMES = {
    'insert-person': 'LdbcInsert1AddPerson', 'insert-like-post': 'LdbcInsert2AddPostLike',
    'insert-like-comment': 'LdbcInsert3AddCommentLike', 'insert-forum': 'LdbcInsert4AddForum',
    'insert-member': 'LdbcInsert5AddForumMembership', 'insert-post': 'LdbcInsert6AddPost',
    'insert-comment': 'LdbcInsert7AddComment', 'insert-knows': 'LdbcInsert8AddFriendship',
    'delete-person': 'LdbcDelete1RemovePerson', 'delete-like-post': 'LdbcDelete2RemovePostLike',
    'delete-like-comment': 'LdbcDelete3RemoveCommentLike', 'delete-forum': 'LdbcDelete4RemoveForum',
    'delete-member': 'LdbcDelete5RemoveForumMembership', 'delete-post': 'LdbcDelete6RemovePostThread',
    'delete-comment': 'LdbcDelete7RemoveCommentSubthread', 'delete-knows': 'LdbcDelete8RemoveFriendship',
}
SHORT_NAMES = {1: 'LdbcShortQuery1PersonProfile', 2: 'LdbcShortQuery2PersonPosts',
               3: 'LdbcShortQuery3PersonFriends', 4: 'LdbcShortQuery4MessageContent',
               5: 'LdbcShortQuery5MessageCreator', 6: 'LdbcShortQuery6MessageForum',
               7: 'LdbcShortQuery7MessageReplies'}
# Answer columns that refill the person and message id buffers (the driver's
# ResultBufferReplenishFun), as positions in each plan's answer values.
REFILL = {
    'ic1': ((0,), ()), 'ic2': ((0,), (3,)), 'ic3': ((0,), ()), 'ic7': ((0,), (4,)), 'ic8': ((0,), (4,)),
    'ic9': ((0,), (3,)), 'ic10': ((0,), ()), 'ic11': ((0,), ()), 'ic12': ((0,), ()), 'ic14': ((0,), ()),
    'is2': ((4,), (0, 3)), 'is3': ((0,), ()), 'is5': ((0,), ()), 'is6': ((2,), ()), 'is7': ((3,), (0,)),
}


class Read:
    """A complex read (variant '3a', ...) or a short read (1..7), and the walk it belongs to."""
    __slots__ = ('kind', 'plan', 'binding', 'sim', 'scheduled', 'walk')

    def __init__(self, kind, plan, binding, sim, scheduled, walk):
        self.kind, self.plan, self.binding = kind, plan, binding
        self.sim, self.scheduled, self.walk = sim, scheduled, walk

    @property
    def name(self):
        return SHORT_NAMES[self.kind] if isinstance(self.kind, int) else f'LdbcQuery{self.kind}'


def complex_reads(bank, t0, horizon, sf):
    """Every complex read up to simulation time `horizon` as (time, variant, plan, binding)."""
    interleave, streams = UPDATE_INTERLEAVE[sf], []
    for variant, freq in FREQUENCY[sf].items():
        rows, cursor, out, k = bank[variant], 0, [], 1
        while t0 + k * freq * interleave <= horizon:
            t = t0 + k * freq * interleave
            for _ in range(min(100, len(rows))):
                row = rows[cursor % len(rows)]
                cursor += 1
                if row['useFrom'] < t < row['useUntil']:
                    break
            binding = {f: v for f, v in row.items() if f not in ('useFrom', 'useUntil')}
            out.append((t, variant, 'ic' + variant.rstrip('ab'), binding))
            k += 1
        streams.append(out)
    return list(heapq.merge(*streams, key=lambda r: r[0]))


class Walk:
    """The short-read walk after one complex read: IS1-IS3, coin, IS4-IS7, coin, ...

    Walks alternate between starting with IS1 and with IS4 (one round-robin shared by
    all). A coin continues while the walk's probability, 1.0 less `DISSIPATION` after
    each IS3 or IS7, exceeds a uniform draw. Each short read takes the oldest id of
    its buffer; an empty buffer ends the walk."""
    starts = 0

    def __init__(self, rng):
        self.rng, self.state, self.coin = rng, 1.0, True
        self.next = 1 if Walk.starts % 2 == 0 else 4
        Walk.starts += 1

    def step(self, buffers):
        if self.coin:
            if not self.state > self.rng.random():
                return None
            self.coin = False
        kind = self.next
        buffer = buffers['person' if kind <= 3 else 'message']
        if not buffer:
            return None
        ident = buffer.popleft()
        if kind in (3, 7):
            self.state -= DISSIPATION
            self.next, self.coin = (4 if kind == 3 else 1), True
        else:
            self.next = kind + 1
        return kind, ident


def refill(read, rows, buffers):
    persons, messages = REFILL.get(read.plan, ((), ()))
    for row in rows:
        for i in persons:
            for v in (row[i] if isinstance(row[i], list) else [row[i]]):
                if v >= 0:
                    buffers['person'].append(v)
        for i in messages:
            if row[i] >= 0:
                buffers['message'].append(row[i])


def follow(read, end, buffers):
    """The walk's next short read, scheduled at `end` (its predecessor's finish), or None."""
    step = read.walk.step(buffers)
    if step is None:
        return None
    kind, ident = step
    return Read(kind, f'is{kind}', {'pid' if kind <= 3 else 'mid': ident}, read.sim, end, read.walk)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--system', default='ddir', choices=sorted(SYSTEMS))
    p.add_argument('--server', type=Path, help='ddir_server binary (--system ddir)')
    p.add_argument('--data', required=True, type=Path, help='BI CSV composite-merged-fk directory')
    p.add_argument('--snapshot', required=True, type=Path, help="update_stream.py's journal initial.pickle.gz")
    p.add_argument('--parameters', required=True, type=Path, help='interactive_v2/parameters.py output')
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--sf', default='1', choices=sorted(FREQUENCY))
    p.add_argument('--tcr', type=float, default=0.01, help='time compression ratio: wall ms per simulation ms')
    p.add_argument('--warmup', type=int, default=0, help='operations run before measuring')
    p.add_argument('--operations', type=int, default=100000, help='operations measured')
    p.add_argument('--threads', type=int, default=1, help="read workers (the driver's thread_count)")
    p.add_argument('--validate', action='store_true', help='one operation per batch, in order; save answers')
    p.add_argument('--workers', type=int, default=4, help='DDIR timely workers')
    p.add_argument('--duck-threads', type=int, default=None, help='DuckDB threads (default: all cores)')
    p.add_argument('--timeout', type=int, default=600)
    p.add_argument('--max-rss-gib', type=int, default=24)
    p.add_argument('--tick-log', action='store_true', help="DDIR: write each tick's updates and reads to ticks.jsonl")
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    t = time.time()
    with gzip.open(args.snapshot) as f:
        graph = pickle.load(f)
    updates = operations(args.data)
    for i, op in enumerate(updates):
        op.index = i
    t0 = updates[0].time
    reads = complex_reads(json.loads(args.parameters.read_text()), t0, updates[-1].time, args.sf)
    print(f'{len(updates)} updates and {len(reads)} complex reads over {(updates[-1].time - t0) / 86400000:.1f} '
          f'simulated days, prepared in {time.time() - t:.0f} s', flush=True)

    t = time.time()
    sut = SYSTEMS[args.system](args, graph)
    load_s = time.time() - t
    # The graph and indexes are tens of millions of long-lived objects; keep the
    # cyclic collector from rescanning them, which stalls every client thread.
    gc.collect()
    gc.freeze()
    print(f'{args.system} loaded in {load_s:.1f} s', flush=True)

    run = (validate if args.validate else timed)(args, sut, updates, reads, t0)
    sut.close()
    if not args.validate:
        report(args, *run, load_s, dict(getattr(sut, 'profile', {})))


class Recorder:
    """Completed operations, in completion order; the first `warmup` are not measured."""

    def __init__(self, args):
        self.lock, self.warmup, self.total = threading.Lock(), args.warmup, args.warmup + args.operations
        self.rows, self.finished = [], threading.Event()

    def add(self, name, scheduled, start, end, code, sim):
        with self.lock:
            self.rows.append((name, scheduled, start, end, code, sim))
            if len(self.rows) >= self.total:
                self.finished.set()

    @property
    def measured(self):
        return self.rows[self.warmup:self.total]


def sleep_until(wall_ms, stop):
    delay = (wall_ms - now_ms()) / 1000
    if delay > 0:
        stop.wait(delay)


def timed(args, sut, updates, reads, t0):
    """The driver's execution: one update thread and --threads read workers, each waiting
    for its operation's scheduled start. A worker takes the next complex read, then runs
    its short-read walk, each short read scheduled at its predecessor's finish."""
    rng, rng_lock = random.Random(42), threading.Lock()
    buffers = {'person': deque(maxlen=BUFFER), 'message': deque(maxlen=BUFFER)}
    w0 = now_ms() + 1000
    wall = lambda sim: w0 + (sim - t0) * args.tcr
    record, stop = Recorder(args), threading.Event()
    queue, queue_lock = iter(reads), threading.Lock()
    errors = []

    def updater():
        i = 0
        while i < len(updates) and not stop.is_set():
            sleep_until(wall(updates[i].time), stop)
            now, batch = now_ms(), []
            while i < len(updates) and wall(updates[i].time) <= now:
                batch.append(updates[i])
                i += 1
            if batch:
                def done(start, end, batch=batch):
                    for op in batch:
                        record.add(UPDATE_NAMES[op.kind], wall(op.time), start, end, 0, op.time)
                sut.apply(batch, done)

    def worker():
        while not stop.is_set():
            with queue_lock:
                item = next(queue, None)
            if item is None:
                return
            sim, variant, plan, binding = item
            with rng_lock:
                walk = Walk(rng)
            read = Read(variant, plan, binding, sim, wall(sim), walk)
            sleep_until(read.scheduled, stop)
            while read is not None and not stop.is_set():
                rows, start, end = sut.read(read)
                record.add(read.name, read.scheduled, start, end, len(rows), read.sim)
                with rng_lock:
                    refill(read, rows, buffers)
                    read = follow(read, end, buffers)

    def guard(fn):
        def run():
            try:
                fn()
            except BaseException as error:
                errors.append(error)
                stop.set()
                record.finished.set()
        return run

    threads = [threading.Thread(target=guard(updater), daemon=True)]
    threads += [threading.Thread(target=guard(worker), daemon=True) for _ in range(args.threads)]
    for t in threads:
        t.start()
    record.finished.wait()
    stop.set()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    with (args.output / 'results_log.csv').open('w') as log:
        log.write('operation_type|scheduled_start_time|actual_start_time|execution_duration_MICROSECONDS'
                  '|result_code|original_start_time\n')
        for name, scheduled, start, end, code, sim in sorted(record.rows, key=lambda r: r[2]):
            log.write(f'{name}|{int(scheduled)}|{int(start)}|{int((end - start) * 1000)}|{code}|{sim}\n')
    return record.measured, getattr(sut, 'ticks', None)


def validate(args, sut, updates, reads, t0):
    """One operation at a time: updates and complex reads by time (updates first on ties),
    each complex read followed by its walk, as a single driver thread runs them."""
    rng = random.Random(42)
    buffers = {'person': deque(maxlen=BUFFER), 'message': deque(maxlen=BUFFER)}
    out = (args.output / 'answers.jsonl').open('w')
    ui = ri = done = 0
    t = time.time()
    applied = threading.Event()
    while done < args.operations and (ui < len(updates) or ri < len(reads)):
        if ui < len(updates) and (ri == len(reads) or updates[ui].time <= reads[ri][0]):
            applied.clear()
            sut.apply([updates[ui]], lambda start, end: applied.set())
            applied.wait()
            ui, done = ui + 1, done + 1
            continue
        sim, variant, plan, binding = reads[ri]
        ri += 1
        read = Read(variant, plan, binding, sim, 0, Walk(rng))
        while read is not None and done < args.operations:
            rows, _, _ = sut.read(read)
            out.write(json.dumps(dict(op=done, name=read.name, sim=sim, binding=read.binding, rows=rows)) + '\n')
            done += 1
            refill(read, rows, buffers)
            read = follow(read, 0, buffers)
    out.close()
    print(f'{done} operations ({ui} updates) in {time.time() - t:.1f} s; answers in {args.output / "answers.jsonl"}')


def report(args, measured, batches, load_s, profile):
    by_type = defaultdict(list)
    for name, scheduled, start, end, _, _ in measured:
        by_type[name].append((start - scheduled, end - start))
    pct = lambda xs, q: sorted(xs)[min(len(xs) - 1, int(q * len(xs)))]
    summary, failed = {}, []
    for name, rows in sorted(by_type.items()):
        delays, durations = [d for d, _ in rows], [u for _, u in rows]
        late = sum(d > 1000 for d in delays)
        if late > round(len(rows) * 0.05):
            failed.append(name)
        summary[name] = dict(count=len(rows), late=late, mean_ms=statistics.mean(durations),
                             p50_ms=pct(durations, .5), p90_ms=pct(durations, .9), p95_ms=pct(durations, .95),
                             p99_ms=pct(durations, .99), max_ms=max(durations), max_delay_ms=max(delays))
    first, last = min(m[2] for m in measured), max(m[3] for m in measured)
    result = dict(system=args.system, tcr=args.tcr, threads=args.threads, workers=args.workers,
                  operations=len(measured), ticks=batches, load_s=load_s, seconds=(last - first) / 1000,
                  throughput=1000 * len(measured) / (last - first), audit='FAILED' if failed else 'PASSED',
                  late_types=failed, client_profile=profile, types=summary)
    (args.output / 'results.json').write_text(json.dumps(result, indent=1) + '\n')
    print(f"{result['audit']} schedule audit, {args.system} at TCR {args.tcr} with {args.threads} threads: "
          f"{len(measured)} operations in {result['seconds']:.1f} s, {result['throughput']:.0f} ops/s"
          + (f", {batches} ticks" if batches else '') + (f"; late: {', '.join(failed)}" if failed else ''))
    for name, s in summary.items():
        print(f"  {name:36} {s['count']:7}  p50 {s['p50_ms']:7.1f}  p99 {s['p99_ms']:7.1f} ms  late {s['late']}")


if __name__ == '__main__':
    main()
