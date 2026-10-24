#!/usr/bin/env python3
"""Replay a DDIR run's ticks back to back, to see where the server's time goes.

    python3 interactive_v2/replay.py --server target/release/ddir_server --data $B \\
        --snapshot journal/initial.pickle.gz --ticks <run>/ticks.jsonl --count 8000 --mode full

`driver.py --tick-log` records what each tick held: its updates and its reads.
This runs those ticks again with no clock and no driver threads, one after
another, and reports wall time and server CPU. Varying what the ticks hold
attributes the time:

- `full`: each tick as recorded; the previous tick's reads are retracted in it.
- `split`: the same, with the retractions in a tick of their own, timed apart.
- `updates`: only the updates.
- `reads`: only the reads (answers and retractions), no updates.

`--merge K` groups K consecutive ticks into one, keeping their order, which is
what holding ticks longer (`driver.py --batch-ms`) does. The replay's `full`
time matched the live run's server time to within 1%.
"""
import argparse
import gc
import gzip
import json
import pickle
import sys
import time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import systems  # noqa: E402
from snb.operations import operations  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--server', required=True, type=Path)
    p.add_argument('--data', required=True, type=Path, help='BI CSV composite-merged-fk directory')
    p.add_argument('--snapshot', required=True, type=Path)
    p.add_argument('--ticks', required=True, type=Path, help="driver.py --tick-log's ticks.jsonl")
    p.add_argument('--count', type=int, default=8000, help='ticks to replay, from the start')
    p.add_argument('--mode', default='full', choices=('full', 'split', 'updates', 'reads'))
    p.add_argument('--merge', type=int, default=1)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--oneoff', type=lambda s: s.split(','), default=None)
    p.add_argument('--oneoff-dir', type=Path)
    p.add_argument('--only', choices=('is', 'ic'), help='keep only short or only complex reads')
    p.add_argument('--output', type=Path, default=Path('/tmp/ddir-replay'))
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    ticks = [json.loads(line) for _, line in zip(range(args.count), args.ticks.open())]
    k = args.merge
    ticks = [dict(u=[i for t in ticks[j:j + k] for i in t['u']], r=[r for t in ticks[j:j + k] for r in t['r']])
             for j in range(0, len(ticks), k)]
    ops = operations(args.data)
    with gzip.open(args.snapshot) as f:
        graph = pickle.load(f)
    sut = systems.Ddir(argparse.Namespace(server=args.server, output=args.output, workers=args.workers,
                                          timeout=900, max_rss_gib=24, oneoff=args.oneoff,
                                          oneoff_dir=args.oneoff_dir), graph)
    gc.collect()
    gc.freeze()
    pid = sut.server.process.pid
    cpu, start = systems.cpu_seconds(pid), time.perf_counter()
    retract = 0.0
    for t in ticks:
        updates = [ops[i] for i in t['u']] if args.mode != 'reads' else []
        reads = [SimpleNamespace(plan=p, binding=b) for p, b in t['r']
                 if not args.only or p.startswith(args.only)] if args.mode != 'updates' else []
        sut.tick(updates, reads)
        if args.mode == 'split':
            s = time.perf_counter()
            sut.tick([], [])
            retract += time.perf_counter() - s
    wall = time.perf_counter() - start
    print(json.dumps(dict(mode=args.mode, merge=k, workers=args.workers, ticks=len(ticks),
                          updates=sum(len(t['u']) for t in ticks), reads=sum(len(t['r']) for t in ticks),
                          wall_s=round(wall, 1), retract_s=round(retract, 1),
                          server_cpu_s=round(systems.cpu_seconds(pid) - cpu, 1))))
    sut.server.close()


if __name__ == '__main__':
    main()
