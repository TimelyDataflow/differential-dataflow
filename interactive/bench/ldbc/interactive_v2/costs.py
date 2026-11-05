#!/usr/bin/env python3
"""Each read of a tick log answered on its own, totalled per plan: DDIR or DuckDB.

    python3 interactive_v2/costs.py --system ddir --server target/release/ddir_server \\
        --snapshot journal/initial.pickle.gz --ticks <run>/ticks.jsonl --count 8000 --workers 1
    python3 interactive_v2/costs.py --system duckdb --data $B --ticks <run>/ticks.jsonl --count 8000 --threads 1

For DDIR each read is one tick that installs its request and one that retracts
it, each less an empty tick's cost; for DuckDB it is the query. Every read is
timed, so the totals are sums, not a typical read times a count (a few
bindings can dominate a plan). Reads run against the initial graph, with no
updates. `--sample N` times only the first N reads of each plan and scales up.
"""
import argparse
from collections import Counter, defaultdict
import gc
import gzip
import json
import pickle
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--system', default='ddir', choices=('ddir', 'duckdb'))
    p.add_argument('--server', type=Path)
    p.add_argument('--snapshot', type=Path, help='DDIR: the initial graph pickle')
    p.add_argument('--data', type=Path, help='DuckDB: the BI CSV composite-merged-fk directory')
    p.add_argument('--ticks', required=True, type=Path)
    p.add_argument('--count', type=int, default=8000)
    p.add_argument('--sample', type=int, default=0)
    p.add_argument('--workers', type=int, default=1)
    p.add_argument('--threads', type=int, default=1)
    p.add_argument('--output', type=Path, default=Path('/tmp/ddir-costs'))
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    reads = [r for _, line in zip(range(args.count), args.ticks.open()) for r in json.loads(line)['r']]
    count = Counter(plan for plan, _ in reads)
    if args.sample:
        seen = Counter()
        reads = [r for r in reads if seen.update([r[0]]) or seen[r[0]] <= args.sample]
    first, second = defaultdict(float), defaultdict(float)
    timed = Counter()
    if args.system == 'ddir':
        import systems
        from client import Server
        from snb.parameters import requests
        with gzip.open(args.snapshot) as f:
            graph = pickle.load(f)
        sut = systems.Ddir(argparse.Namespace(server=args.server, output=args.output, workers=args.workers,
                                              timeout=900, max_rss_gib=24), graph)
        gc.collect()
        gc.freeze()
        def run(commands):
            start = time.perf_counter()
            sut.server.commands(commands)
            return time.perf_counter() - start
        empty = statistics.median(run(['tick']) for _ in range(50))
        for k, (plan, binding) in enumerate(reads):
            rows = sorted(requests(plan, sut.plans[plan]['parameters'], binding, 10**6 + k))
            first[plan] += run([Server.feed(plan, 0, rows, 1, sut.shapes[plan]), 'tick', f'peek {plan}.answer as corgi']) - empty
            second[plan] += run([Server.feed(plan, 0, rows, -1, sut.shapes[plan]), 'tick']) - empty
            timed[plan] += 1
        sut.server.close()
    else:
        sys.path.insert(0, str(HERE.parent / 'baselines'))
        import sqlsystems as S
        con = S.duckdb_connect(threads=args.threads)
        S.duckdb_load(con, args.data / 'initial_snapshot')
        sql = S.queries('duckdb')
        for plan, binding in reads:
            start = time.perf_counter()
            S.duckdb_run(con, plan, sql[plan], binding)
            first[plan] += time.perf_counter() - start
            timed[plan] += 1
    scale = {plan: count[plan] / timed[plan] for plan in timed}
    out = {plan: dict(count=count[plan], answer_s=round(first[plan] * scale[plan], 2),
                      retract_s=round(second[plan] * scale[plan], 2))
           for plan in sorted(timed, key=lambda p: (p[:2], int(p[2:])))}
    for group in ('is', 'ic'):
        rows = [v for plan, v in out.items() if plan.startswith(group)]
        print(f"{group.upper()}: {sum(r['count'] for r in rows)} reads, answer {sum(r['answer_s'] for r in rows):.1f} s"
              + (f", retract {sum(r['retract_s'] for r in rows):.1f} s" if args.system == 'ddir' else ''))
    for plan, v in out.items():
        print(f"  {plan:5} {v['count']:6}  answer {v['answer_s']:7.2f} s" + (f"  retract {v['retract_s']:7.2f} s" if args.system == 'ddir' else ''))
    (args.output / f'costs-{args.system}.json').write_text(json.dumps(out, indent=1) + '\n')


if __name__ == '__main__':
    main()
