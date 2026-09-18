#!/usr/bin/env python3
"""Render v2 suite cycle accounting; does not certify provenance or comparability."""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics

from measure import TIMINGS


def account(run, rounds):
    """Retirement is separate from warmup; derived events never count twice."""
    groups = defaultdict(list)
    for event in run['events']:
        if event.get('derived', False):
            continue
        if event['state'] == 'setup':
            key = 'setup'
        elif event['state'] == 'retired':
            key = 'retirement'
        elif event['warmup']:
            key = 'warmup'
        else:
            key = event['round']
            if type(key) is not int or not 0 <= key < rounds:
                raise ValueError('invalid measured round')
        for metric in TIMINGS:
            value = event[metric]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f'invalid {metric}')
        groups[key].append(event)
    if {key for key in groups if type(key) is int} != set(range(rounds)):
        raise ValueError('incomplete measured rounds')
    # A successful suite round always ends by restoring and reading the graph.
    # Do not turn a truncated report into a successful shorter cycle.
    for cycle in range(rounds):
        phases = {(e['state'], e['phase']) for e in groups[cycle]}
        if ('restored', 'restore') not in phases:
            raise ValueError('incomplete cycle: missing restore')
        for name in run['queries']:
            if not any(e['state'] == 'restored' and e['phase'] in
                       ('empty:' + name, 'maintained:' + name) and 'answer_sha256' in e
                       for e in groups[cycle]):
                raise ValueError('incomplete cycle: missing checked restored answer')
    return {key: {m: sum(e[m] for e in events) for m in TIMINGS}
            for key, events in groups.items()}


def render(report, sha):
    if report['format_version'] != 'snb-suite-2' or report['status'] != 'passed':
        raise ValueError('requires a successful snb-suite-2 report; preserve failures separately')
    rounds = report['config']['rounds']
    if type(rounds) is not int or rounds < 1 or not report['runs']:
        raise ValueError('no measured runs')
    lines = ['# Suite readout', '', f'Report SHA-256: `{sha}`.', '',
             f'Workload revision (not binary provenance): `{report["revision"]}`.', '',
             f'Binary SHA-256: `{report["binary_sha256"]}`.', '',
             f'Input SHA-256: `{report["data"]["sha256"]}`.', '',
             f'Platform: {report["platform"]}; Python {report["python"]}; {report["logical_cpus"]} logical CPUs.', '',
             f'Mode: {report["config"]["mode"]}; workers: {report["config"]["workers"]}; '
             f'measured rounds: {rounds}; warmup rounds: {report["config"]["warmup"]}; '
             f'request batch size: {report["config"]["batch_size"]}.', '',
             'Units: milliseconds summed over recorded phases; not server CPU or wall time including the oracle.',
             'Setup/retirement are shown separately. See REFRESH.md for qualification.', '']
    for run in report['runs']:
        totals = account(run, rounds)
        cycles = [totals[i] for i in range(rounds)]
        lines.extend([f'## {run["backend"]}: {", ".join(run["queries"])}', '',
                      '| Period | ' + ' | '.join(TIMINGS) + ' |',
                      '| --- | ' + ' | '.join('---:' for _ in TIMINGS) + ' |'])
        rows = [(k, totals[k]) for k in ('setup', 'warmup') if k in totals]
        rows += [(f'cycle {i}', totals[i]) for i in range(rounds)]
        rows += [(name, {m: fn(c[m] for c in cycles) for m in TIMINGS})
                 for name, fn in [('cycle median', statistics.median), ('cycle min', min), ('cycle max', max)]]
        if 'retirement' in totals:
            rows.append(('final retirement', totals['retirement']))
        for name, values in rows:
            lines.append('| ' + name + ' | ' + ' | '.join(f'{values[m]:.3f}' for m in TIMINGS) + ' |')
        lines.append('')
    return '\n'.join(lines)


def summary(reports):
    """Median of fresh-trial cycle medians, with no pooling across binaries."""
    def identity(report):
        fields = ('format_version', 'binary_sha256', 'data', 'changed_sha256',
                  'parameter_bank', 'changes', 'plans_sha256', 'sources_sha256',
                  'platform', 'python', 'logical_cpus', 'spec_commit', 'catalogue')
        config = ('queries', 'backend', 'mode', 'isolated', 'workers', 'rounds',
                  'warmup', 'batch_size', 'changes', 'timeout', 'max_rss_gib')
        return ({k: report[k] for k in fields}, {k: report['config'][k] for k in config},
                [(r['queries'], r['backend'], r['bindings']) for r in report['runs']])
    if any(identity(r) != identity(reports[0]) for r in reports[1:]):
        raise ValueError('summary requires repeated trials of the same binary, workload and environment')
    lines = ['# Repeated standard-suite trials', '',
             f'{len(reports)} fresh trials. Each cell is the median of trial cycle medians, in milliseconds.',
             'Wire includes transport; client also includes preparation/encoding/decoding. Neither includes the oracle.', '',
             '| Query / panel | Backend | Cycle wire | Trial-median wire range | Cycle client |',
             '| --- | --- | ---: | ---: | ---: |']
    for index, run in enumerate(reports[0]['runs']):
        medians = []
        for report in reports:
            rounds = report['config']['rounds']
            totals = account(report['runs'][index], rounds)
            medians.append({m: statistics.median(totals[i][m] for i in range(rounds)) for m in TIMINGS})
        wire = [m['wire_ms'] for m in medians]
        client = [m['client_ms'] for m in medians]
        lines.append(f'| {", ".join(run["queries"])} | {run["backend"]} | '
                     f'{statistics.median(wire):.3f} | {min(wire):.3f}–{max(wire):.3f} | '
                     f'{statistics.median(client):.3f} |')
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', type=Path, nargs='+')
    parser.add_argument('--summary', action='store_true', help='compact repeated-trial table; requires identical binary/workload')
    args = parser.parse_args()
    rendered = []
    reports = []
    try:
        for path in args.reports:
            raw = path.read_bytes()
            if path.suffix == '.gz':
                raw = gzip.decompress(raw)
            report = json.loads(raw)
            rendered.append(render(report, hashlib.sha256(raw).hexdigest()))
            reports.append(report)
        result = summary(reports) if args.summary else '\n'.join(rendered)
    except (OSError, KeyError, TypeError, ValueError) as error:
        parser.error(f'{path}: {error}')
    print(result)


if __name__ == '__main__':
    main()
