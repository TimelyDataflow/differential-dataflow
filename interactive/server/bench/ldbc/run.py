#!/usr/bin/env python3
"""Reproducible maintained/parameterized LDBC-derived server workload (stdlib only)."""
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
from workload import HERE, TABLES, QUERIES, Reference, changes, expected, fingerprint, load, parameters


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def positive(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError('must be positive')
    return value


def checked(actual, want, label):
    if actual != want:
        mismatch = next((i for i, pair in enumerate(zip(actual, want)) if pair[0] != pair[1]),
                        min(len(actual), len(want)))
        raise AssertionError(f'{label}: row {mismatch}: expected {want[mismatch:mismatch+1]!r} '
                             f'({len(want)} rows), got {actual[mismatch:mismatch+1]!r} ({len(actual)} rows)')


def run_server(args, backend, graph, delta, bank, standing, answers, report):
    interactive = [q for q in args.queries if q.startswith('i')]
    bi = [q for q in args.queries if q.startswith('b')]
    record = dict(backend=backend, workers=args.workers, events=[], requests=[])
    report['runs'].append(record)
    with Server(args.server, args.output / f'{backend}.log', backend, args.workers,
                args.timeout, args.max_rss_gib) as server:
        context = dict(round=-1, warmup=True, state='setup')

        def command(phase, make_commands):
            start = time.perf_counter()
            commands = make_commands()
            prepared = time.perf_counter()
            metrics, replies = server.commands(commands)
            metrics['prepare_ms'] = 1000*(prepared-start)
            metrics['client_ms'] = 1000*(time.perf_counter()-start)
            record['events'].append(dict(context, phase=phase, **metrics))
            return replies

        def read(name, want, phase='read'):
            lines, = command(f'{phase}:{name}', lambda: [f'peek {name}.answer'])
            start = time.perf_counter()
            actual = decode(lines)
            event = record['events'][-1]
            event['decode_ms'] = 1000*(time.perf_counter()-start)
            event['client_ms'] += event['decode_ms']
            event['rows'] = len(actual)
            # Validation/reference work is deliberately outside all timings.
            checked(actual, want, f'{backend}/{context}/{phase}/{name}')

        for name in ('graph', *args.queries):
            program = (HERE / f'{name}.ddp').read_text()
            command(f'install:{name}', lambda: [f'load {name} begin\n{program}\n{{rid}} end-load', 'tick'])
        for index, name in enumerate(TABLES):
            rows = sorted(graph[name])
            for offset in range(0, len(rows), 1000):
                chunk = rows[offset:offset+1000]
                command(f'load:{name}', lambda: [Server.feed('graph', index, chunk, 1)])
        for name in bi:
            command(f'standing:{name}', lambda: [Server.feed(name, 0, [(0, *standing[name])], 1)])
        command('initial_tick', lambda: ['tick'])
        for name in bi:
            read(name, answers['initial'][name][standing[name]])
        for name in interactive:
            read(name, [], 'empty')

        for cycle in range(args.warmup + args.rounds):
            context.update(round=cycle-args.warmup, warmup=cycle < args.warmup)
            for state_index, state in enumerate(('initial', 'changed')):
                context['state'] = state
                if state == 'changed':
                    command('update', lambda: [Server.feed('graph', TABLES.index(t), rows, -1)
                                              for t, rows in delta.items() if rows] + ['tick'])
                    for name in bi:
                        read(name, answers[state][name][standing[name]], 'maintained')
                bindings = {name: [(rid, *bank[name][((2*cycle+state_index)*args.batch_size+rid) % len(bank[name])])
                                   for rid in range(args.batch_size)] for name in interactive}
                record['requests'].append(dict(context, bindings=bindings))
                if interactive:
                    first_event = len(record['events'])
                    command('bind', lambda: [Server.feed(name, 0, bindings[name], 1) for name in interactive] + ['tick'])
                    for name in interactive:
                        want = []
                        for rid, *params in bindings[name]:
                            for key, value, diff in answers[state][name][tuple(params)]:
                                want.append([[rid, key[1]], value, diff])
                        read(name, want)
                    # Sum measured phases, excluding the intervening answer checks.
                    elapsed = sum(e['client_ms'] for e in record['events'][first_event:])
                    record['events'].append(dict(context, phase='batch_answers', client_ms=elapsed,
                                                 derived=True, requests=len(interactive)*args.batch_size))
                    command('release', lambda: [Server.feed(name, 0, bindings[name], -1) for name in interactive] + ['tick'])
                    for name in interactive:
                        read(name, [], 'empty')
            context['state'] = 'restored'
            command('restore', lambda: [Server.feed('graph', TABLES.index(t), rows, 1)
                                       for t, rows in delta.items() if rows] + ['tick'])
            for name in bi:
                read(name, answers['initial'][name][standing[name]], 'maintained')
            print(f'{backend}: round {cycle-args.warmup + 1}/{args.rounds}, answers verified', flush=True)
        record['peak_server_rss_bytes_sampled'] = server.peak_rss
    buckets = defaultdict(list)
    for event in record['events']:
        if not event['warmup']:
            buckets[(event['state'], event['phase'])].append(event['client_ms'])
    record['summary'] = {f'{state}/{phase}': dict(samples=len(values), median_ms=statistics.median(values),
                                                min_ms=min(values), max_ms=max(values))
                         for (state, phase), values in sorted(buckets.items())}
    for label, summary in record['summary'].items():
        if not label.split('/')[-1].startswith('empty'):
            print(f'  {label}: {summary["median_ms"]:.3f} ms median ({summary["samples"]} samples)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True, help='existing ddir_server binary (release for timing)')
    parser.add_argument('--snapshot', type=Path, help='SNB BI composite-merged-fk initial_snapshot directory; default: tiny fixture')
    parser.add_argument('--backend', choices=('vec', 'corgi', 'both'), default='both')
    parser.add_argument('--workers', type=positive, default=1)
    parser.add_argument('--queries', nargs='+', choices=QUERIES, default=list(QUERIES))
    parser.add_argument('--rounds', type=positive, default=5)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--batch-size', type=positive, default=4, help='bindings per interactive query per tick')
    parser.add_argument('--changes', type=positive, default=4, help='maximum deleted rows per mutable edge table')
    parser.add_argument('--timeout', type=positive, default=120, help='seconds per command group')
    parser.add_argument('--max-rss-gib', type=positive, default=6, help='sampled server RSS ceiling, not a hard OS limit')
    parser.add_argument('--output', type=Path, help='new result directory; default: a fresh temporary directory')
    args = parser.parse_args()
    if args.warmup < 0 or len(set(args.queries)) != len(args.queries):
        parser.error('warmup must be non-negative and queries must be unique')
    args.server = args.server.resolve(strict=True)
    if args.snapshot:
        args.snapshot = args.snapshot.resolve(strict=True)
    if args.output:
        args.output = args.output.resolve()
        args.output.mkdir(parents=True, exist_ok=False)
    else:
        args.output = Path(tempfile.mkdtemp(prefix='ddir-ldbc-'))
    print(f'Artifacts: {args.output}', flush=True)
    repo = HERE.parents[3]
    def git(*command):
        return subprocess.run(['git', '-C', str(repo), *command], capture_output=True,
                              text=True, check=False).stdout.strip()
    report = dict(format_version=1, status='running', config={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  platform=platform.platform(), python=platform.python_version(), logical_cpus=os.cpu_count(),
                  revision=git('rev-parse', 'HEAD'), worktree=git('status', '--porcelain'),
                  binary_sha256=digest(args.server),
                  sources_sha256={p.name: digest(p) for p in sorted(HERE.iterdir()) if p.suffix in ('.py', '.ddp', '.json')}, runs=[])
    lock = repo / 'Cargo.lock'
    if lock.exists():
        shutil.copyfile(lock, args.output / 'Cargo.lock')
        report['cargo_lock_sha256'] = digest(lock)
    try:
        started = time.perf_counter()
        graph = load(args.snapshot)
        report['data'] = dict(rows={t: len(rows) for t, rows in graph.items()}, sha256=fingerprint(graph))
        reference = Reference(graph)
        if args.snapshot is None:
            # Anchor the independent oracle in a few hand-countable fixture facts.
            checked(reference.answer('is3', (1,)), [(3, 'Cy', 'Q', 200), (2, 'Bo', 'Long', 100)], 'tiny IS3')
            checked(reference.answer('ic6', (1, 'Topic')), [('A', 2), ('Alphabet', 2), ('Z', 2), ('é', 1)], 'tiny IC6')
            checked(reference.answer('bi5', ('Topic',))[:1], [(2, 2, 2, 1, 25)], 'tiny BI5')
            checked(reference.answer('bi11', ('CountryA', 100, 700)), [(2,)], 'tiny BI11')
        bank, standing = parameters(reference)
        delta = changes(graph, args.changes)
        report.update(parameter_bank=bank, standing=standing, retracted_rows=delta)
        answers = {}
        for state in ('initial', 'changed'):
            if state == 'changed':
                altered = {t: rows - set(delta[t]) if t in delta else rows for t, rows in graph.items()}
                reference = Reference(altered)
                del altered
            answers[state] = {name: {params: expected(reference, name, [(0, *params)])
                                    for params in (bank[name] if name.startswith('i') else [standing[name]])}
                              for name in args.queries}
        del reference
        report['reference_ms'] = 1000*(time.perf_counter()-started)
        report['reference_answers'] = {state: {name: [dict(parameters=p, rows=rows) for p, rows in values.items()]
                                              for name, values in queries.items()} for state, queries in answers.items()}
        print(f'Projected rows: {report["data"]["rows"]}', flush=True)
        for name, cases in answers['initial'].items():
            nonempty = sum(bool(rows) for rows in cases.values())
            print(f'{name}: {nonempty}/{len(cases)} distinct parameter cases have nonempty answers', flush=True)
        if 'bi11' in args.queries:
            print(f'bi11: {answers["initial"]["bi11"][standing["bi11"]][0][1][0]} initial triangles', flush=True)
        for backend in ('vec', 'corgi') if args.backend == 'both' else (args.backend,):
            run_server(args, backend, graph, delta, bank, standing, answers, report)
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error=f'{type(error).__name__}: {error}')
        for log in args.output.glob('*.log'):
            with log.open(errors='replace') as source:
                tail = ''.join(deque(source, maxlen=20))[-8000:]
            if tail:
                print(f'{log.name} (tail):\n{tail}', file=sys.stderr)
        raise
    finally:
        (args.output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print(f'Report: {args.output / "report.json"}', flush=True)


if __name__ == '__main__':
    main()
