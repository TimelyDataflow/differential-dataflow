#!/usr/bin/env python3
"""Check bounded/hash-bucketed ranking against a direct multiset oracle."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys

from client import Server, decode
from bounded_topk import program as query_program
from run import checked
from snb import witness
from snb.rel import Compiler, R, col
from snb.topk import pipeline
from test_update_stream import fixture


VARIANTS = {'flat': ([1], 'value'), 'two': ([64, 1], 'value'),
            'three': ([1024, 32, 1], 'value'),
            'four': ([4096, 256, 16, 1], 'value'),
            'id': ([1024, 32, 1], 'identity')}
# (sort-field positions, limit, group-field positions). Exercise zero, one and
# three sort fields, composite groups and limits other than twenty.
COMPILED = {'zero': ((), 0, (0,)), 'first': ((1,), 1, (0,)),
            'ten': ((1,2,3), 10, (0,)), 'twenty': ((1,2), 20, (0,1)),
            'hundred': ((), 100, (0,)), 'all': ((1,2), None, (0,))}


def program():
    compiler = Compiler()
    source = R.source('rows', dict(group='int',score='int',identity='int',content='bytes'))
    initial = compiler.emit(source)
    exports = []
    plans = {name:source.rank([col(source.fields[i]) for i in order],limit,
                              groups=tuple(source.fields[i] for i in keys))
             for name,(order,limit,keys) in COMPILED.items()}
    for name, ranked in plans.items():
        name = 'compiled_'+name
        exports.append(f'export "{name}" = {compiler.emit(ranked)} | arrange;')
    lines = [*compiler.lines, *exports, f'let values = {initial} '
             '| map((row, ()) -> (row.0, (row.1, row.2, row)));',
             'let baseline = values | collect | flatmap((_, vals) -> vals) '
             '| filter((_, (rank, _)) -> (rank, 20) lt);',
             'export "baseline" = baseline | map((key, value) -> ((key, value.0), value.1)) | arrange;']
    for name, (buckets, hash_key) in VARIANTS.items():
        stages, last = pipeline('values', name, buckets, hash_key)
        lines.extend(stages)
        lines.append(f'export "{name}" = {last} '
                     '| map((key, value) -> ((key, value.0), value.1)) | arrange;')
    return '\n'.join(lines)


def expected(state):
    groups = {}
    for (group, score, identity, text), count in state.items():
        if count > 0:
            row = [group, score, identity, list(text.encode())]
            groups.setdefault(group, []).extend([[score, identity, row]]*count)
    return [[[group, rank], value, 1]
            for group, values in sorted(groups.items())
            for rank, value in enumerate(sorted(values)[:20])]


def compiled_expected(state, order, limit, keys):
    groups = {}
    for row, count in state.items():
        if count > 0:
            value = [*row[:3],list(row[3].encode())]
            groups.setdefault(tuple(row[i] for i in keys),[]).extend([value]*count)
    rows = []
    for values in groups.values():
        values = sorted(values,key=lambda row:(tuple(row[i] for i in order),row))[:limit]
        rows.extend([[[*row,rank],[],1] for rank,row in enumerate(values)])
    return sorted(rows)


def check(server, output, workers):
    state = Counter()
    initial = [((group, i//4, i, 'content λ '+str(i)), 1)
               for group in (1, 4) for i in range(64)]
    initial += [((1, -1, 1000, 'many equal copies'), 125),
                ((2, 3, 2000, 'one row'), 1),
                *[((4, -20, i, 'negative accumulation'), -1) for i in range(100, 125)]]
    ticks = [initial,
             [((1, -1, 1000, 'many equal copies'), -125),
              ((1, 0, -1, 'new best'), 1), ((2, 3, 2000, 'one row'), 22)],
             [((1, 0, -1, 'new best'), -1),
              *[(row, -count) for row, count in initial if row[0] in (1, 4) and count > 0
                and row[2] != 1000]],
             [((2, 3, 2000, 'one row'), -23),
              *[(row, count) for row, count in initial if row[0] == 1 and count > 0]],
             [((3, 0, 3000, 'a new group'), 1),
              ((1, -1, 1000, 'many equal copies'), -120)]]
    with Server(server, output/f'reducer-w{workers}.log', workers, 120, 6) as engine:
        engine.commands([f'load test begin\n{program()}\n{{rid}} end-load', 'tick'])
        for epoch, delta in enumerate(ticks):
            commands = []
            for row, diff in delta:
                state[row] += diff
                commands.append(Server.feed('test', 0, [row], diff))
            engine.commands([*commands, 'tick'])
            want = expected(state)
            for name in ('baseline', *VARIANTS):
                frames, = engine.commands([f'peek {name} as corgi'])[1]
                checked(decode(frames), want, f'{workers} workers/{epoch}/{name}')
            for name, options in COMPILED.items():
                frames, = engine.commands([f'peek compiled_{name} as corgi'])[1]
                checked(decode(frames), compiled_expected(state,*options),
                        f'{workers} workers/{epoch}/compiled_{name}')
    print(f'{workers} workers: collect, five lambda plans and six compiler cases match all five states', flush=True)


def check_queries(server, output, workers):
    """Check all three query emitters through every cascade/drain/restore epoch."""
    for query in ('ic2', 'ic5', 'ic9'):
        for variant in (('default', 'flat', 'three', 'id') if query=='ic5' else ('default', 'flat', 'three')):
            destination = output/f'{query}-{variant}-w{workers}'
            with destination.with_suffix('.log').open('w') as log:
                command = [sys.executable,str(Path(__file__).with_name('update_stream.py')),
                    '--server',str(server),'--workers',str(workers),'--queries',query,
                    '--prepared',str(output/'journal'),'--parameters',str(output/'bindings.json'),
                    '--output',str(destination)]
                if variant != 'default': command.extend(['--plan',str(output/f'{query}-{variant}.ddp')])
                subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
            record = json.loads((destination/f'{query}.json').read_text())
            assert record['status'] == 'passed' and record['retirement_checked']
            assert len(record['checks']) == 48
            print(f'{workers} workers: {query}/{variant} matches Python at all 49 states',flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True)
    fixture(args.output/'journal')
    (args.output/'bindings.json').write_text(json.dumps({q:witness.params(q) for q in ('ic2','ic9')}))
    for query in ('ic2', 'ic5', 'ic9'):
        for variant in (('flat', 'three', 'id') if query=='ic5' else ('flat', 'three')):
            (args.output/f'{query}-{variant}.ddp').write_text(query_program(*VARIANTS[variant],query=query))
    for workers in (1, 4):
        check(args.server.resolve(), args.output, workers)
        check_queries(args.server.resolve(), args.output, workers)


if __name__ == '__main__':
    main()
