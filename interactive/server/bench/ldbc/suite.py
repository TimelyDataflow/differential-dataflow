#!/usr/bin/env python3
"""Run the complete SNB read catalogue through a private, real ddir_server."""
import argparse
from collections import defaultdict, deque
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

from client import Server, decode
from run import checked, digest, positive
from snb import data, witness
from snb.parameters import alternate, parameters, reference, requests
from snb.queries import Context, QUERIES
from snb.rel import Compiler, R, source_shape

HERE = Path(__file__).resolve().parent
SPEC = 'b2269610f433da72e7c97041f01680aae369a903'
CATALOGUE = [f'{family}{i}' for family, end in (('is', 8), ('ic', 15), ('bi', 21)) for i in range(1, end)]
assert set(CATALOGUE) == set(QUERIES)


def compile_queries(names):
    result = {}
    for name in names:
        fn, schema, title = QUERIES[name]
        plan = fn(Context(), R.source('request', schema))
        compiler = Compiler(imports=True)
        program = compiler.compile_many({name + '.answer': plan})
        result[name] = dict(plan=plan, sources=compiler.inputs, program=program,
                            parameters=schema, title=title)
    return result


def graph_program():
    return '\n'.join(f'export "ldbc.{name}" = input {i} : ({source_shape(schema)} ; ()) | arrange;'
                     for i, (name, schema) in enumerate(data.SCHEMA.items())) + '\n'


def fingerprint(graph):
    h = hashlib.sha256()
    for name, rows in sorted(graph.items()):
        h.update(name.encode())
        for row in sorted(rows):
            h.update((json.dumps(row, ensure_ascii=True) + '\n').encode())
    return h.hexdigest()


def run_server(args, backend, names, graph, changed, bank, plans, report):
    suffix = '-'.join(names) if args.isolated else 'all'
    record = dict(backend=backend, workers=args.workers, queries=names, events=[], bindings=[])
    report['runs'].append(record)
    standing = names if args.mode == 'maintained' else [n for n in names if n.startswith('bi')]
    dynamic = [n for n in names if n not in standing]
    active = {name: set() for name in names}
    with Server(args.server, args.output / f'{backend}-{suffix}.log', backend, args.workers,
                args.timeout, args.max_rss_gib) as server:
        context = dict(round=-1, warmup=True, state='setup')

        def command(phase, make_commands):
            start = time.perf_counter()
            commands = make_commands()
            prepared = time.perf_counter()
            metrics, replies = server.commands(commands)
            metrics.update(prepare_ms=1000*(prepared-start), client_ms=1000*(time.perf_counter()-start))
            record['events'].append(dict(context, phase=phase, **metrics))
            return replies

        def read(current, selected=names):
            for name in selected:
                # Same logical plan, evaluated from scratch. Outside all timers;
                # checks lowering/maintenance, not independent spec conformance.
                want = reference(plans[name]['plan'], plans[name]['sources'],
                                 {**current, 'request': active[name]}) if active[name] else []
                phase = 'empty' if not active[name] else 'maintained' if name in standing else 'read'
                lines, = command(phase + ':' + name, lambda: [f'peek {name}.answer'])
                start = time.perf_counter()
                actual = decode(lines)
                event = record['events'][-1]
                event.update(decode_ms=1000*(time.perf_counter()-start), rows=len(actual))
                event['client_ms'] += event['decode_ms']
                checked(actual, want, f'{backend}/{context}/{name}')
                event['answer_sha256'] = hashlib.sha256(json.dumps(actual).encode()).hexdigest()

        def binding(name, cycle):
            schema = plans[name]['parameters']
            # Two equal bindings under distinct IDs, plus varying values as the
            # batch grows. Reusing IDs on the next round must give fresh answers.
            return set().union(*(requests(name, schema, bank[name][(cycle+i//2) % len(bank[name])], i+1)
                                 for i in range(args.batch_size)))

        def update(before, after, phase):
            def commands():
                result = []
                for i, table in enumerate(data.SCHEMA):
                    for rows, diff in ((before[table]-after[table], -1), (after[table]-before[table], 1)):
                        if rows:
                            result.append(Server.feed('graph', i, sorted(rows), diff))
                return [*result, 'tick']
            command(phase, commands)

        for name, program in [('graph', graph_program()), *[(n, plans[n]['program']) for n in names]]:
            command('install:' + name, lambda: [f'load {name} begin\n{program}\n{{rid}} end-load', 'tick'])
        for i, table in enumerate(data.SCHEMA):
            rows = sorted(graph[table])
            for offset in range(0, len(rows), 1000):
                command('load:' + table, lambda: [Server.feed('graph', i, rows[offset:offset+1000], 1)])
        for name in standing:
            active[name] = requests(name, plans[name]['parameters'], bank[name][0], 0)
            command('standing:' + name, lambda: [Server.feed(name, 0, sorted(active[name]), 1)])
        command('initial_tick', lambda: ['tick'])
        read(graph)

        for cycle in range(args.warmup + args.rounds):
            context.update(round=cycle-args.warmup, warmup=cycle < args.warmup)
            for state, current in (('initial', graph), ('changed', changed)):
                context['state'] = state
                if state == 'changed':
                    update(graph, changed, 'update')
                    read(current)
                if dynamic:
                    for name in dynamic:
                        active[name] = binding(name, 2*cycle + int(state == 'changed'))
                    record['bindings'].append(dict(context, rows={n: sorted(active[n]) for n in dynamic}))
                    first = len(record['events'])
                    command('bind', lambda: [Server.feed(n, 0, sorted(active[n]), 1) for n in dynamic] + ['tick'])
                    read(current, dynamic)
                    elapsed = sum(e['client_ms'] for e in record['events'][first:])
                    record['events'].append(dict(context, phase='batch_answers', client_ms=elapsed,
                                                  derived=True, requests=len(dynamic)*args.batch_size))
                    command('release', lambda: [Server.feed(n, 0, sorted(active[n]), -1) for n in dynamic] + ['tick'])
                    for name in dynamic:
                        active[name] = set()
                    read(current, dynamic)
            context['state'] = 'restored'
            update(changed, graph, 'restore')
            read(graph)
            print(f'{backend}/{suffix}: round {cycle-args.warmup+1}/{args.rounds}, {len(names)} queries checked', flush=True)
        context.update(state='retired', warmup=True)
        if standing:
            command('retire', lambda: [Server.feed(n, 0, sorted(active[n]), -1) for n in standing] + ['tick'])
            for name in standing:
                active[name] = set()
            read(graph)
        record['peak_server_rss_bytes_sampled'] = server.peak_rss
    buckets = defaultdict(list)
    for event in record['events']:
        if not event['warmup']:
            buckets[(event['state'], event['phase'])].append(event['client_ms'])
    record['summary'] = {f'{state}/{phase}': dict(samples=len(xs), median_ms=statistics.median(xs),
                                                min_ms=min(xs), max_ms=max(xs))
                         for (state, phase), xs in sorted(buckets.items())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, help='existing ddir_server (release for timing)')
    parser.add_argument('--snapshot', type=Path, help='existing BI composite-merged-fk initial_snapshot directory')
    parser.add_argument('--parameters', type=Path, help='JSON object: query name -> list of parameter objects')
    parser.add_argument('--backend', choices=('vec', 'corgi', 'both'), default='both')
    parser.add_argument('--workers', type=positive, default=1)
    parser.add_argument('--queries', nargs='+', default=['all'], help='all, is, ic, bi, or individual names')
    parser.add_argument('--mode', choices=('mixed', 'maintained'), default='mixed',
                        help='mixed: standing BI, transient IS/IC; maintained: standing bindings for every read')
    parser.add_argument('--isolated', action='store_true', help='one fresh server per query, for attribution')
    parser.add_argument('--rounds', type=positive, default=3)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--batch-size', type=positive, default=3)
    parser.add_argument('--changes', type=positive, default=1, help='snapshot mode: retractions per edge table')
    parser.add_argument('--timeout', type=positive, default=120)
    parser.add_argument('--max-rss-gib', type=positive, default=6)
    parser.add_argument('--output', type=Path, help='new artifact directory')
    parser.add_argument('--emit', action='store_true', help='emit all selected DDP plans without starting a server')
    parser.add_argument('--list', action='store_true', help='list query names, titles, and parameter fields')
    args = parser.parse_args()
    if args.warmup < 0:
        parser.error('warmup must be non-negative')
    names = []
    for selector in args.queries:
        selected = CATALOGUE if selector == 'all' else [n for n in CATALOGUE if n.startswith(selector)] if selector in ('is', 'ic', 'bi') else [selector]
        for name in selected:
            if name not in QUERIES:
                parser.error(f'unknown query {name}')
            if name not in names:
                names.append(name)
    if args.list:
        for name in names:
            print(f'{name}: {QUERIES[name][2]} ({", ".join(QUERIES[name][1])})')
        return
    if not args.emit and args.server is None:
        parser.error('--server is required unless --emit or --list is used')
    if args.server:
        args.server = args.server.resolve(strict=True)
    if args.snapshot:
        args.snapshot = args.snapshot.resolve(strict=True)
    args.output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix='ddir-snb-'))
    if not args.output.exists():
        args.output.mkdir(parents=True)
    elif any(args.output.iterdir()):
        parser.error('--output must be empty or new')
    print(f'Artifacts: {args.output}', flush=True)
    plans = compile_queries(names)
    for name, plan in plans.items():
        (args.output / f'{name}.ddp').write_text(plan['program'])
    (args.output / 'graph.ddp').write_text(graph_program())
    catalogue = {n: dict(title=plans[n]['title'], parameters=plans[n]['parameters'],
                         outputs=list(plans[n]['plan'].fields[2:])) for n in names}
    (args.output / 'catalogue.json').write_text(json.dumps(catalogue, indent=2) + '\n')
    if args.emit:
        return
    repo = HERE.parents[3]
    def git(*command):
        return subprocess.run(['git', '-C', str(repo), *command], capture_output=True, text=True, check=False).stdout.strip()
    report = dict(format_version='snb-suite-1', status='running', spec_commit=SPEC, catalogue=catalogue,
                  config={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  platform=platform.platform(), python=platform.python_version(), logical_cpus=os.cpu_count(),
                  revision=git('rev-parse', 'HEAD'), worktree=git('status', '--porcelain'),
                  binary_sha256=digest(args.server),
                  plans_sha256={p.name: digest(p) for p in sorted(args.output.glob('*.ddp'))},
                  sources_sha256={str(p.relative_to(HERE)): digest(p) for p in sorted(HERE.rglob('*.py'))}, runs=[])
    lock = repo / 'Cargo.lock'
    if lock.exists():
        shutil.copyfile(lock, args.output / 'Cargo.lock')
        report['cargo_lock_sha256'] = digest(lock)
    try:
        graph = data.load(args.snapshot) if args.snapshot else witness.graph()
        if args.snapshot:
            changed = {n: set(rows) for n, rows in graph.items()}
            for table in ('knows', 'member', 'likes'):
                changed[table].difference_update(sorted(graph[table])[:args.changes])
        else:
            changed = witness.changed(graph)
        overrides = json.loads(args.parameters.read_text()) if args.parameters else {}
        if set(overrides) - set(QUERIES):
            raise ValueError('unknown queries in --parameters')
        bank = {}
        for name in names:
            base = parameters(graph, name)
            if not args.snapshot:
                base.update(witness.params(name))
            bank[name] = [base, alternate(graph, base)]
            if name in overrides:
                if not isinstance(overrides[name], list) or not overrides[name]:
                    raise ValueError(f'{name}: parameters must be a nonempty list of objects')
                bank[name] = [{**base, **p} for p in overrides[name]]
            for binding in bank[name]:
                requests(name, plans[name]['parameters'], binding, 0)
        report.update(data=dict(rows={n: len(rows) for n, rows in graph.items()}, sha256=fingerprint(graph)),
                      parameter_bank=bank, changed_sha256=fingerprint(changed),
                      changes={n: dict(removed=sorted(graph[n]-changed[n]), added=sorted(changed[n]-graph[n]))
                               for n in graph if graph[n] != changed[n]})
        print(f'{len(names)} queries; {sum(map(len, graph.values()))} projected rows', flush=True)
        for backend in ('vec', 'corgi') if args.backend == 'both' else (args.backend,):
            for selected in [[n] for n in names] if args.isolated else [names]:
                run_server(args, backend, selected, graph, changed, bank, plans, report)
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error=f'{type(error).__name__}: {error}')
        for log in args.output.glob('*.log'):
            with log.open(errors='replace') as source:
                tail = ''.join(deque(source, maxlen=15))[-6000:]
            if tail:
                print(f'{log.name}:\n{tail}', file=sys.stderr)
        raise
    finally:
        (args.output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print(f'Report: {args.output / "report.json"}', flush=True)


if __name__ == '__main__':
    main()
