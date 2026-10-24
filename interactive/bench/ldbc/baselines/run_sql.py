#!/usr/bin/env python3
"""Check and time the DuckDB and Umbra baselines (Python >= 3.11; pip install duckdb psycopg[binary] pyarrow).

    # Answers against the Python reference (reference.py output), each binding run twice.
    run_sql.py check   --system duckdb --snapshot SNAP --bindings ref.json
    # One-off latency: load once, each binding warm, 1 warmup + 5 runs, median.
    run_sql.py oneoff  --system duckdb --snapshot SNAP --bindings ref.json --threads 4 --output out.json
    # Per-batch maintenance: apply each journal epoch, then re-run every query (suite-0 bindings).
    run_sql.py maintain --system duckdb --snapshot SNAP --journal JOURNAL --bindings ref.json --threads 4 --output DIR

For Umbra, start the server first (umbra_server.sh), write the projected CSVs once
(run_sql.py export --snapshot SNAP --output DIR, where DIR is mounted in the container),
and pass --csv-dir with the container-side path instead of --snapshot.
"""
import argparse
import gzip
import json
import resource
import statistics
import time
from pathlib import Path

import sqlsystems as S


def norm(v):
    if isinstance(v, float): return round(v, 9)
    if isinstance(v, list): return tuple(norm(x) for x in v)
    return v


def compare(got, want):
    key = lambda r: r[0][1]
    g = [norm(r[1]) for r in sorted(got, key=key)]; w = [norm(r[1]) for r in sorted(want, key=key)]
    return 'exact' if g == w else 'DIFF'


def open_system(args):
    if args.system == 'duckdb':
        con = S.duckdb_connect(args.threads)
        t = time.perf_counter(); S.duckdb_load(con, args.snapshot); load_ms = 1000 * (time.perf_counter() - t)
        return con, S.duckdb_run, load_ms
    con = S.umbra_connect(args.port)
    t = time.perf_counter(); S.umbra_load(con, args.csv_dir); load_ms = 1000 * (time.perf_counter() - t)
    return con, S.umbra_run, load_ms


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('mode', choices=('check', 'oneoff', 'maintain', 'export'))
    p.add_argument('--system', choices=('duckdb', 'umbra'), default='duckdb')
    p.add_argument('--snapshot', help='BI composite-merged-fk initial_snapshot (DuckDB loads it)')
    p.add_argument('--csv-dir', help='container-side directory of projected CSVs (Umbra COPYs them)')
    p.add_argument('--port', type=int, default=5433)
    p.add_argument('--bindings', type=Path, help='reference.py output: query -> [{label, binding, answer, status}]')
    p.add_argument('--journal', type=Path, help='update_stream.py --prepare-only journal directory')
    p.add_argument('--threads', type=int, help='DuckDB threads (Umbra: set by the container CPU set)')
    p.add_argument('--reps', type=int, default=5)
    p.add_argument('--budget', type=float, default=60, help='maintain: stop after this many seconds ...')
    p.add_argument('--min-epochs', type=int, default=16, help='... but not before this many epochs (one BI day)')
    p.add_argument('--output', type=Path)
    p.add_argument('queries', nargs='*')
    args = p.parse_args()

    if args.mode == 'export':
        con = S.duckdb_connect(args.threads); S.duckdb_load(con, args.snapshot); S.export_projected(con, args.output)
        return
    bank = json.loads(args.bindings.read_text())
    names = args.queries or S.NAMES
    sql = S.queries(args.system)
    con, run, load_ms = open_system(args)
    print(f'{args.system}: loaded in {load_ms:.0f} ms', flush=True)

    if args.mode == 'check':
        tally = {}
        for q in names:
            for e in bank[q]:
                if e.get('status') != 'ok': continue
                verdicts = {compare(run(con, q, sql[q], e['binding']), e['answer']) for _ in range(2)}
                v = 'DIFF' if 'DIFF' in verdicts else 'exact'
                tally[v] = tally.get(v, 0) + 1
                if v != 'exact': print(q, e['label'], v, flush=True)
        print(tally)
        return

    if args.mode == 'oneoff':
        result = dict(system=args.system, threads=args.threads, load_ms=load_ms, queries={})
        for q in names:
            entries = []
            for e in bank[q]:
                times = []
                for rep in range(1 + args.reps):
                    t = time.perf_counter(); got = run(con, q, sql[q], e['binding']); ms = 1000 * (time.perf_counter() - t)
                    if rep: times.append(ms)
                check = compare(got, e['answer']) if e.get('status') == 'ok' else 'noref'
                entries.append(dict(label=e['label'], median_ms=statistics.median(times), samples=times, rows=len(got), check=check))
                print(f'{q} {e["label"]}: {statistics.median(times):.2f} ms {check}', flush=True)
            result['queries'][q] = entries
        result['peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        args.output.write_text(json.dumps(result, indent=1))
        return

    # maintain
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((args.journal / 'manifest.json').read_text())
    binding = {q: next(e for e in bank[q] if e['label'] == 'suite-0')['binding'] for q in names}
    if args.system == 'duckdb':
        columns = {r: [c[0] for c in con.execute(f'DESCRIBE {r}').fetchall()] for r in S.RELATIONS}
        types = {r: [c[1] for c in con.execute(f'DESCRIBE {r}').fetchall()] for r in S.RELATIONS}
        apply = lambda changes: S.duckdb_apply(con, changes, columns, types)
    else:
        columns = {r: [d.name for d in con.execute(f'SELECT * FROM {r} LIMIT 0').description] for r in S.RELATIONS}
        apply = lambda changes: S.umbra_apply(con, changes, columns)
    log = dict(system=args.system, threads=args.threads, load_ms=load_ms, queries=names, epochs=[])
    started = time.perf_counter()
    for b in manifest['batches']:
        if b['epoch'] > args.min_epochs and time.perf_counter() - started > args.budget:
            break
        with gzip.open(args.journal / b['path'], 'rt') as f:
            changes = json.load(f)['changes']
        t = time.perf_counter(); apply(changes); update_ms = 1000 * (time.perf_counter() - t)
        query_ms, saved = {}, {}
        for q in names:
            t = time.perf_counter(); got = run(con, q, sql[q], binding[q]); query_ms[q] = 1000 * (time.perf_counter() - t)
            saved[q] = got
        if b['epoch'] % 16 == 0:
            (args.output / f'answers-{b["epoch"]:04}.json').write_text(json.dumps(saved))
        log['epochs'].append(dict(epoch=b['epoch'], day=b['day'], update_class=b['update_class'], update_ms=update_ms, query_ms=query_ms))
        print(f"epoch {b['epoch']} {b['update_class']}: update {update_ms:.1f} ms, queries {sum(query_ms.values()):.0f} ms", flush=True)
    (args.output / 'log.json').write_text(json.dumps(log))


if __name__ == '__main__':
    main()
