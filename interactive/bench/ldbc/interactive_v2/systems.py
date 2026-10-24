"""Systems under test for the Interactive v2 driver: DDIR, and DuckDB as a baseline.

Both receive the same operations and the same exact deltas. Delete cascades are
resolved in the client by `snb.operations.Cascade` for both, inside the timed
update, so neither system is credited or charged for them differently.

A system offers two calls, made from the driver's threads:
- `apply(updates, done)`: apply a batch of updates in order, then call
  `done(start_ms, end_ms)`. One thread makes these calls, in time order.
- `read(read)`: answer one read; returns (rows, start_ms, end_ms). Each read
  worker thread calls it for one read at a time.

DDIR runs both through one loop that ticks the server: a tick applies every
update batch and answers every read submitted since the previous tick began,
and retracts the previous tick's reads. DuckDB applies each batch as one
transaction on the update thread and answers each read on its worker's own
connection, so reads run concurrently with each other and with updates.
"""
from collections import Counter, defaultdict
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from client import Server, decode, parse_shape  # noqa: E402
from snb import data  # noqa: E402
from snb.operations import Cascade  # noqa: E402
from snb.parameters import requests  # noqa: E402
from snb.rel import source_shape  # noqa: E402
from snb.updates import project_rows  # noqa: E402
from suite import compile_queries, graph_program  # noqa: E402
from graph import DELETES, cascade_program, delete_row  # noqa: E402
from stream import snapshot_rows  # noqa: E402

PLANS = [f'is{i}' for i in range(1, 8)] + [f'ic{i}' for i in range(1, 15)]


def now_ms():
    return time.time() * 1000


def cpu_seconds(pid):
    """A process's user+system CPU time so far, from ps (`[[dd-]hh:]mm:ss.ss`)."""
    text = subprocess.run(['ps', '-o', 'time=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
    days, _, text = text.rpartition('-')
    seconds = 0.0
    for part in text.split(':'):
        seconds = seconds * 60 + float(part)
    return seconds + 86400 * int(days or 0)


def net_changes(cascade, updates):
    """Updates applied in order, as one delta per relation: {name: {removed: [...], added: [...]}}."""
    net = defaultdict(Counter)
    for op in updates:
        for name, delta in cascade.apply(op.kind, op.rows).items():
            for row in delta['removed']:
                net[name][row] -= 1
            for row in delta['added']:
                net[name][row] += 1
    changes = {}
    for name, counts in net.items():
        if any(abs(d) > 1 for d in counts.values()):
            raise ValueError(f'{name}: a row changed twice in one direction')
        removed = sorted(r for r, d in counts.items() if d < 0)
        added = sorted(r for r, d in counts.items() if d > 0)
        if removed or added:
            changes[name] = dict(removed=removed, added=added)
    return changes


class Ddir:
    """The DDIR server with the graph program and the 21 authored Interactive plans.

    A read is a request row in its plan's input, retracted in the following tick."""
    name = 'ddir'

    def __init__(self, args, graph):
        self.server = Server(args.server, args.output / 'server.log', args.workers, args.timeout, args.max_rss_gib)
        # With cascades in the dataflow, the driver keeps no graph: inserts are fed as rows
        # and deletes as their explicit targets (graph.py). Otherwise it retracts exact rows.
        self.cascade = Cascade(graph) if getattr(args, 'cascade', 'client') == 'client' else None
        self.plans = compile_queries(PLANS)
        self.shapes = {n: (parse_shape(source_shape(p['parameters'])), 'unit') for n, p in self.plans.items()}
        self.relations = list(data.SCHEMA)
        self.graph_shapes = [(parse_shape(source_shape(s)), 'unit') for s in data.SCHEMA.values()]
        self.graph_shapes = self.graph_shapes[:17] + [(parse_shape(DELETES[i][1]), 'unit') for i in sorted(DELETES)]
        plan_dir = Path(os.environ.get('DDIR_PLAN_DIR', HERE.parent / 'authored' / 'plans'))
        program = graph_program() if self.cascade else cascade_program()
        self.server.commands([f'load graph begin\n{program}\n{{rid}} end-load', 'tick'])
        # Plans answered one-off: each read installs its own copy of the plan's one-off half,
        # which imports what the standing half publishes, and drops it at the next tick.
        self.oneoff = {}
        oneoff_dir = getattr(args, 'oneoff_dir', None)
        for name in getattr(args, 'oneoff', None) or []:
            stem = f'{name[:2]}{int(name[2:]):02}'
            standing = Path(oneoff_dir) / f'{stem}.standing.ddp'
            if standing.exists():
                self.server.commands([f'load {name}s begin\n{standing.read_text()}\n{{rid}} end-load', 'tick'])
            self.oneoff[name] = (Path(oneoff_dir) / f'{stem}.oneoff.ddp').read_text()
        for name in PLANS:
            if name in self.oneoff:
                continue
            text = (plan_dir / f'{name[:2]}{int(name[2:]):02}.ddp').read_text()
            self.server.commands([f'load {name} begin\n{text}\n{{rid}} end-load', 'tick'])
        if self.cascade:
            chunks = ((name, sorted(graph[name])[off:off + 20000]) for name in self.relations
                      for off in range(0, len(graph[name]), 20000))
        else:
            chunks = snapshot_rows(args.data / 'initial_snapshot')
        for name, rows in chunks:
            i = self.relations.index(name)
            self.server.commands([Server.feed('graph', i, rows, 1, self.graph_shapes[i])])
        self.server.commands(['tick'])
        self.live, self.rid, self.ticks = [], 0, 0
        self.profile = Counter()   # client-side time per tick phase, ms, and operation counts
        self.cpu_start = cpu_seconds(self.server.process.pid)
        self.client_cpu_start = cpu_seconds(os.getpid())
        self.tick_log = (args.output / 'ticks.jsonl').open('w') if getattr(args, 'tick_log', False) else None
        self.lock = threading.Condition()
        self.queued_updates, self.queued_reads, self.closing = [], [], False
        # Hold a tick until its oldest operation has waited this long, so ticks are fuller.
        self.batch_ms, self.first_queued = getattr(args, 'batch_ms', 0), None
        self.error = None
        self.loop = threading.Thread(target=self.run, daemon=True)
        self.loop.start()

    def apply(self, updates, done):
        with self.lock:
            self.first_queued = self.first_queued or now_ms()
            self.queued_updates.append((updates, done))
            self.lock.notify()

    def read(self, read):
        slot = dict(event=threading.Event())
        with self.lock:
            self.first_queued = self.first_queued or now_ms()
            self.queued_reads.append((read, slot))
            self.lock.notify()
        slot['event'].wait()
        if 'error' in slot:
            raise RuntimeError(slot['error'])
        return slot['rows'], slot['start'], slot['end']

    def run(self):
        while True:
            with self.lock:
                while not (self.queued_updates or self.queued_reads or self.closing):
                    self.lock.wait()
                if self.closing and not (self.queued_updates or self.queued_reads):
                    break
                while not self.closing and self.first_queued and now_ms() < self.first_queued + self.batch_ms:
                    self.lock.wait((self.first_queued + self.batch_ms - now_ms()) / 1000)
                self.first_queued = None
                updates, self.queued_updates = self.queued_updates, []
                reads, self.queued_reads = self.queued_reads, []
            start = now_ms()
            try:
                answers = self.tick([op for batch, _ in updates for op in batch], [r for r, _ in reads])
            except Exception as error:   # surface to every waiting reader, then stop
                self.error = error
                for _, slot in reads:
                    slot.update(error=repr(error))
                    slot['event'].set()
                raise
            end = now_ms()
            if self.tick_log:
                self.tick_log.write(json.dumps(dict(
                    u=[op.index for batch, _ in updates for op in batch],
                    r=[[r.plan, r.binding] for r, _ in reads], ms=round(end - start, 3))) + '\n')
            for _, done in updates:
                done(start, end)
            for (_, slot), rows in zip(reads, answers):
                slot.update(rows=rows, start=start, end=end)
                slot['event'].set()
        self.tick([], [])   # retract the last reads

    def tick(self, updates, reads):
        self.ticks += 1
        t0 = time.perf_counter()
        commands = []
        if self.cascade:
            for name, delta in net_changes(self.cascade, updates).items():
                i = self.relations.index(name)
                for side, diff in (('removed', -1), ('added', 1)):
                    if delta[side]:
                        commands.append(Server.feed('graph', i, delta[side], diff, self.graph_shapes[i]))
        else:
            fed = defaultdict(list)
            for op in updates:
                if op.kind.startswith('insert-'):
                    for name, rows in project_rows(op.rows).items():
                        fed[self.relations.index(name)].extend(rows)
                else:
                    i, row = delete_row(op.kind, op.rows, op.time)
                    fed[i].append(row)
            for i, rows in sorted(fed.items()):
                commands.append(Server.feed('graph', i, sorted(rows), 1, self.graph_shapes[i]))
        for plan, rows in self.live:
            if plan in self.plans:
                commands.append(Server.feed(plan, 0, rows, -1, self.shapes[plan]))
            else:
                commands.append(f'drop {plan}')   # a one-off program from the previous tick
        live, rids, loads = [], [], []
        for read in reads:
            self.rid += 1
            rows = sorted(requests(read.plan, self.plans[read.plan]['parameters'], read.binding, self.rid))
            target = read.plan
            if read.plan in self.oneoff:
                target = f'{read.plan}x{self.rid}'
                text = self.oneoff[read.plan].replace(f'export "{read.plan}.answer"', f'export "{target}.answer"')
                loads.append(f'load {target} begin\n{text}\n{{rid}} end-load')
            commands.append(Server.feed(target, 0, rows, 1, self.shapes[read.plan]))
            live.append((target, rows))
            rids.append(self.rid)
        commands.append('tick')
        plans = sorted({target for target, _ in live})
        commands.extend(f'peek {p}.answer as corgi' for p in plans)
        t1 = time.perf_counter()
        if loads:
            self.server.commands(loads)
        _, replies = self.server.commands(commands)
        t2 = time.perf_counter()
        self.live = live
        answers = defaultdict(list)
        for lines in replies[len(replies) - len(plans):] if plans else []:
            for (rid, _rank), values, _diff in decode(lines):
                answers[rid].append(values)
        t3 = time.perf_counter()
        self.profile.update(prepare_ms=1000 * (t1 - t0), server_ms=1000 * (t2 - t1), decode_ms=1000 * (t3 - t2),
                            updates=len(updates), reads=len(reads))
        return [answers.get(rid, []) for rid in rids]

    def close(self):
        with self.lock:
            self.closing = True
            self.lock.notify()
        self.loop.join()
        self.profile['server_cpu_s'] = cpu_seconds(self.server.process.pid) - self.cpu_start
        self.profile['client_cpu_s'] = cpu_seconds(os.getpid()) - self.client_cpu_start
        if self.tick_log:
            self.tick_log.close()
        self.server.close()


class Duck:
    """DuckDB with the baselines' schema, derived tables and SQL (baselines/sql/duckdb).

    An update batch is one transaction (baselines/sqlsystems.duckdb_apply, which also
    maintains the `thread` table)."""
    name = 'duckdb'

    def __init__(self, args, graph):
        sys.path.insert(0, str(HERE.parent / 'baselines'))
        import sqlsystems as S
        self.S = S
        self.cascade = Cascade(graph)
        self.con = S.duckdb_connect(threads=args.duck_threads)
        S.duckdb_load(self.con, args.data / 'initial_snapshot')
        self.sql = S.queries('duckdb')
        self.columns = {r: [c[0] for c in self.con.execute(f'DESCRIBE {r}').fetchall()] for r in S.RELATIONS}
        self.types = {r: [c[1] for c in self.con.execute(f'DESCRIBE {r}').fetchall()] for r in S.RELATIONS}
        self.writer = self.cursor()
        self.local = threading.local()
        self.profile = Counter()
        self.cpu_start = cpu_seconds(os.getpid())   # DuckDB runs in this process, with the driver

    def cursor(self):
        cursor = self.con.cursor()
        cursor.execute("SET TimeZone='UTC'")
        return cursor

    def apply(self, updates, done):
        start = now_ms()
        changes = net_changes(self.cascade, updates)
        if changes:
            self.S.duckdb_apply(self.writer, changes, self.columns, self.types)
        done(start, now_ms())

    def read(self, read):
        if not hasattr(self.local, 'cursor'):
            self.local.cursor = self.cursor()
        start = now_ms()
        rows = self.S.duckdb_run(self.local.cursor, read.plan, self.sql[read.plan], read.binding)
        return [values for _key, values, _diff in rows], start, now_ms()

    def close(self):
        self.profile['process_cpu_s'] = cpu_seconds(os.getpid()) - self.cpu_start
        self.con.close()


SYSTEMS = {'ddir': Ddir, 'duckdb': Duck}
