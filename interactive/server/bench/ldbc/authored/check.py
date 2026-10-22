#!/usr/bin/env python3
"""Run hand-authored DDP plans against the plans generated from snb/queries.py.

For each selected query, start a fresh server twice: once with the generated
plan, once with authored/plans/<query>.ddp. Each server installs the shared
graph program and the query, loads the same snapshot, settles, and then
answers every binding (bind, peek, release; warmup plus measured repetitions).
The two plans' answers must be identical for every binding; the report records
both plans' latencies (bind + peek wire time, median) and setup phases.

    python3 interactive/server/bench/ldbc/authored/check.py --server target/release/ddir_server \\
        --snapshot /path/to/graphs/csv/bi/composite-merged-fk/initial_snapshot \\
        --parameters interactive/server/bench/ldbc/bindings/bi-official-sf1.json \\
        --workers 4 --output /tmp/authored-check [query ...]

Bindings are the suite's smoke bindings (`parameters` and `alternate`, labelled
suite-0 and suite-1) plus, with --parameters, each listed binding (official-N).
Without data, --fixture uses the suite's 199-row witness graph.
"""
import argparse
import gzip
import json
import pickle
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from client import Server, parse_shape  # noqa: E402
import measure  # noqa: E402
from snb import data, witness  # noqa: E402
from snb.parameters import alternate, parameters, requests  # noqa: E402
from snb.rel import source_shape  # noqa: E402
from suite import CATALOGUE, compile_queries, graph_program  # noqa: E402


def norm(v):
    if isinstance(v, float):
        return round(v, 9)
    if isinstance(v, list):
        return tuple(norm(x) for x in v)
    return v


def same(a, b):
    key = lambda row: tuple(row[0])
    return [norm(r) for r in sorted(a, key=key)] == [norm(r) for r in sorted(b, key=key)]


def run_plan(args, name, program, schema, bank, graph):
    shapes = [(parse_shape(source_shape(s)), 'unit') for s in data.SCHEMA.values()]
    request_shape = (parse_shape(source_shape(schema)), 'unit')
    record = dict(phases={}, bindings={})
    log = args.output / f'{name}-{program["kind"]}.log'
    with Server(args.server, log, args.workers, args.timeout, args.max_rss_gib) as server:
        def command(make):
            return measure.commands(server, make)
        text = graph_program()
        m, _ = command(lambda: [f'load graph begin\n{text}\n{{rid}} end-load', 'tick'])
        query = program['text']
        m, _ = command(lambda: [f'load {name} begin\n{query}\n{{rid}} end-load', 'tick'])
        record['phases']['install_ms'] = m['wire_ms']
        feed_ms = 0
        for i, table in enumerate(data.SCHEMA):
            rows = sorted(graph[table])
            for offset in range(0, len(rows), 1000):
                m, _ = command(lambda: [Server.feed('graph', i, rows[offset:offset + 1000], 1, shapes[i])])
                feed_ms += m['wire_ms']
        m, _ = command(lambda: ['tick'])
        record['phases'].update(feed_ms=feed_ms, settle_ms=m['wire_ms'])
        for label, binding in bank:
            rows = sorted(requests(name, schema, binding, 1))
            samples, answer = [], None
            for rep in range(args.warmup + args.reps):
                mb, _ = command(lambda: [Server.feed(name, 0, rows, 1, request_shape), 'tick'])
                mp, (lines,) = command(lambda: [f'peek {name}.answer as corgi'])
                got = measure.decode_answer(lines, mp)
                command(lambda: [Server.feed(name, 0, rows, -1, request_shape), 'tick'])
                answer = got if answer is None else answer
                if rep >= args.warmup:
                    samples.append(mb['wire_ms'] + mp['wire_ms'])
            record['bindings'][label] = dict(median_ms=statistics.median(samples), answer=answer)
        record['peak_server_rss_bytes_sampled'] = server.peak_rss
    return record


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--server', type=Path, required=True)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--snapshot', type=Path, help='BI composite-merged-fk initial_snapshot directory')
    source.add_argument('--prepared', type=Path, help='update_stream.py --prepare-only journal (reads initial.pickle.gz)')
    source.add_argument('--fixture', action='store_true', help="the suite's 199-row witness graph")
    p.add_argument('--parameters', type=Path, help='JSON: query name -> list of extra bindings (labelled official-N)')
    p.add_argument('--plans', type=Path, default=HERE / 'plans')
    p.add_argument('--workers', type=int, default=1)
    p.add_argument('--reps', type=int, default=3)
    p.add_argument('--warmup', type=int, default=1)
    p.add_argument('--timeout', type=int, default=600)
    p.add_argument('--max-rss-gib', type=int, default=9)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('queries', nargs='*')
    args = p.parse_args()
    args.server = args.server.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=True)
    names = args.queries or CATALOGUE

    if args.fixture:
        graph = witness.graph()
    elif args.prepared:
        with gzip.open(args.prepared / 'initial.pickle.gz') as f:
            graph = pickle.load(f)
    else:
        graph = data.load(args.snapshot)
    extra = json.loads(args.parameters.read_text()) if args.parameters else {}

    report, failures = {}, []
    for name in names:
        plan = compile_queries([name])[name]
        base = parameters(graph, name)
        if args.fixture:
            base.update(witness.params(name))
        second = alternate(graph, base)
        if args.fixture:
            second.update(witness.alternate_params(name))
        bank = [('suite-0', base), ('suite-1', second)]
        bank += [(f'official-{i}', {**base, **b}) for i, b in enumerate(extra.get(name, []))]
        programs = [dict(kind='generated', text=plan['program']),
                    dict(kind='authored', text=(args.plans / f'{name}.ddp').read_text())]
        started = time.perf_counter()
        runs = {}
        for program in programs:
            try:
                runs[program['kind']] = run_plan(args, name, program, plan['parameters'], bank, graph)
            except Exception as error:  # report and continue with the next query
                runs[program['kind']] = dict(error=f'{type(error).__name__}: {error}'[:500])
        gen, auth = runs['generated'], runs['authored']
        ok = 'error' not in gen and 'error' not in auth and all(
            same(gen['bindings'][label]['answer'], auth['bindings'][label]['answer']) for label, _ in bank)
        if not ok:
            failures.append(name)
        report[name] = dict(match=ok, elapsed_s=time.perf_counter() - started, **runs)
        line = ' '.join(f"{label} {gen['bindings'][label]['median_ms']:.1f}/{auth['bindings'][label]['median_ms']:.1f}"
                        for label, _ in bank) if ok else (gen.get('error') or auth.get('error') or 'answers differ')
        print(f'{name:5} {"same" if ok else "DIFF"}  ms generated/authored: {line}', flush=True)
        for run in runs.values():
            for b in run.get('bindings', {}).values():
                b['rows'] = len(b.pop('answer'))
        (args.output / 'report.json').write_text(json.dumps(report, indent=1) + '\n')
    print(f'{len(names) - len(failures)}/{len(names)} queries: authored answers identical to generated', flush=True)
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
