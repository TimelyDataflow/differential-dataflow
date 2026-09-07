#!/usr/bin/env python3
"""Compare matching, successful workload runs. Ratios above 1 mean faster."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline', type=Path)
    parser.add_argument('candidate', type=Path)
    args = parser.parse_args()
    before, after = [json.loads(path.read_text()) for path in (args.baseline, args.candidate)]
    for report in (before, after):
        if report['status'] != 'passed' or report['format_version'] != 1:
            parser.error('both reports must be successful format-version-1 runs')
    for key in ('data', 'parameter_bank', 'standing', 'retracted_rows', 'reference_answers',
                'platform', 'python', 'logical_cpus'):
        if before[key] != after[key]:
            parser.error(f'incomparable {key}')
    for key in ('workers', 'queries', 'rounds', 'warmup', 'batch_size', 'changes'):
        if before['config'][key] != after['config'][key]:
            parser.error(f'incomparable configuration: {key}')
    for source in ('client.py', 'workload.py', 'run.py'):
        if before['sources_sha256'][source] != after['sources_sha256'][source]:
            parser.error(f'measurement/reference code changed: {source}; establish a fresh baseline')
    changed_plans = [name for name, sha in before['sources_sha256'].items()
                     if name.endswith('.ddp') and sha != after['sources_sha256'].get(name)]
    if changed_plans:
        print('Changed plans (answers checked): ' + ', '.join(changed_plans))
    a = {r['backend']: r for r in before['runs']}
    b = {r['backend']: r for r in after['runs']}
    if a.keys() != b.keys():
        parser.error('backend sets differ')
    for backend in a:
        if a[backend]['requests'] != b[backend]['requests']:
            parser.error(f'{backend}: request schedule changed')
        print(f'\n{backend}: milliseconds, baseline / candidate; >1x is faster')
        print(f'{"phase":36} {"baseline":>10} {"candidate":>10} {"ratio":>8}')
        for phase, old in a[backend]['summary'].items():
            new = b[backend]['summary'][phase]
            ratio = old['median_ms'] / new['median_ms']
            print(f'{phase:36} {old["median_ms"]:10.3f} {new["median_ms"]:10.3f} {ratio:7.2f}x')


if __name__ == '__main__':
    main()
