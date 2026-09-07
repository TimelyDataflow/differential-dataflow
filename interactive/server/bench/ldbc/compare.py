#!/usr/bin/env python3
"""Compare matching, successful workload runs. Ratios above 1 mean faster."""
import argparse
import json
from pathlib import Path

from measure import TIMINGS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline', type=Path)
    parser.add_argument('candidate', type=Path)
    parser.add_argument('--metric', choices=TIMINGS, default='wire_ms',
                        help='wire_ms is client-observed request/response time, not server CPU time')
    args = parser.parse_args()
    before, after = [json.loads(path.read_text()) for path in (args.baseline, args.candidate)]
    suite = before['format_version'] == 'snb-suite-2'
    for report in (before, after):
        if report['status'] != 'passed' or report['format_version'] != ('snb-suite-2' if suite else 2):
            parser.error('both reports must be successful runs of the same v2 timing format; establish a fresh baseline')
    fields = ('catalogue', 'spec_commit', 'changed_sha256', 'changes') if suite else ('standing', 'retracted_rows', 'reference_answers')
    for key in ('data', 'parameter_bank', 'platform', 'python', 'logical_cpus', *fields):
        if before[key] != after[key]:
            parser.error(f'incomparable {key}')
    for key in ('workers', 'queries', 'rounds', 'warmup', 'batch_size', 'changes', *(('mode', 'isolated') if suite else ())):
        if before['config'][key] != after['config'][key]:
            parser.error(f'incomparable configuration: {key}')
    sources = ('client.py', 'measure.py', 'snapshot.py', 'run.py', *(
        ('suite.py', 'snb/data.py', 'snb/parameters.py', 'snb/witness.py') if suite else ('workload.py',)))
    for source in sources:
        if before['sources_sha256'][source] != after['sources_sha256'][source]:
            parser.error(f'measurement/reference code changed: {source}; establish a fresh baseline')
    plan_field = 'plans_sha256' if suite else 'sources_sha256'
    changed_plans = [name for name, sha in before[plan_field].items()
                     if name.endswith('.ddp') and sha != after[plan_field].get(name)]
    if changed_plans:
        print('Changed plans (answers checked): ' + ', '.join(changed_plans))
    def run_key(r):
        return r['backend'] + ('/' + ','.join(r['queries']) if suite else '')
    a = {run_key(r): r for r in before['runs']}
    b = {run_key(r): r for r in after['runs']}
    if a.keys() != b.keys():
        parser.error('backend sets differ')
    for backend in a:
        bindings = 'bindings' if suite else 'requests'
        if a[backend][bindings] != b[backend][bindings]:
            parser.error(f'{backend}: request schedule changed')
        if suite:
            def answers(run):
                return [{k: e[k] for k in ('round', 'state', 'phase', 'rows', 'answer_sha256')}
                        for e in run['events'] if 'answer_sha256' in e]
            if answers(a[backend]) != answers(b[backend]):
                parser.error(f'{backend}: answers changed')
        if a[backend]['summary'].keys() != b[backend]['summary'].keys():
            parser.error(f'{backend}: measured phases changed')
        print(f'\n{backend}: {args.metric}, milliseconds, baseline / candidate; >1x is faster')
        print(f'{"phase":36} {"baseline":>10} {"candidate":>10} {"ratio":>8}')
        for phase, metrics in a[backend]['summary'].items():
            old = metrics[args.metric]
            new = b[backend]['summary'][phase][args.metric]
            ratio = old['median_ms'] / new['median_ms'] if new['median_ms'] else float('inf')
            print(f'{phase:36} {old["median_ms"]:10.3f} {new["median_ms"]:10.3f} {ratio:7.2f}x')


if __name__ == '__main__':
    main()
