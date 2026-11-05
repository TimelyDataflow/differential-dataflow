#!/usr/bin/env python3
"""Summarize saved update runs without loading data, checking, or running DDIR.

All times are milliseconds. Phase totals overlap replay and query elapsed time;
wire time includes transport and client protocol work, not just server execution.
"""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics


def summarize_query(query, slowest):
    phases = defaultdict(list)
    for event in query.get('events', []):
        phases[event['phase']].append(event)
    classes = defaultdict(list)
    updates = query.get('updates', [])
    for update in updates:
        classes[update['update_class']].append(update)
    captures = [query.get('initial', {}), *query.get('checks', [])]
    result = dict(
        query=query['query'], status=query['status'], binding=query.get('binding'),
        completed_epochs=len(updates),
        projected_row_changes=sum(u['projected_row_changes'] for u in updates),
        validated_states=sum(c.get('status') == 'passed' for c in captures),
        captured_answers=sum('actual_path' in c for c in captures),
        python_reference_calls=len(query.get('reference_calls', [])),
        python_reference_ms=sum(c['duration_ms'] for c in query.get('reference_calls', [])),
        replay=query.get('replay'),
        command_phases={phase: dict(
            commands=len(events),
            **{metric: sum(e.get(metric, 0) for e in events)
               for metric in ('prepare_ms', 'encode_ms', 'wire_ms', 'decode_ms', 'client_ms')})
            for phase, events in sorted(phases.items())},
        update_classes={kind: dict(
            epochs=len(rows),
            projected_row_changes=sum(u['projected_row_changes'] for u in rows),
            wire_ms=sum(u['update_ms'] for u in rows),
            median_wire_ms=statistics.median(u['update_ms'] for u in rows),
            max_wire_ms=max(u['update_ms'] for u in rows))
            for kind, rows in sorted(classes.items())},
        slowest_updates=[{k: u[k] for k in ('epoch', 'day', 'update_class',
                                           'projected_row_changes', 'update_ms', 'prepare_ms')}
                         for u in sorted(updates, key=lambda u: u['update_ms'], reverse=True)[:slowest]])
    for key in ('elapsed_ms', 'snapshot_load_ms', 'binding_prepare_ms', 'snapshot_sort_ms',
                'validation_snapshot_load_ms', 'validation_replay', 'retirement_checked',
                'peak_server_rss_bytes_sampled', 'error', 'failure_kind', 'failure_interval'):
        if key in query:
            result[key] = query[key]
    return result


def summarize_run(path, queries, slowest):
    if path.is_dir():
        path = path/'report.json'
    raw = path.read_bytes()
    report = json.loads(raw)
    if report.get('format_version') != 'snb-update-coverage-2':
        raise ValueError(f'{path}: expected an snb-update-coverage-2 report')
    selected = [q for q in report['queries'] if not queries or q['query'] in queries]
    result = dict(report=str(path), report_sha256=hashlib.sha256(raw).hexdigest(),
                  status=report['status'], selected_epochs=len(report['journal']['batches']),
                  journal_initial_sha256=report['journal']['initial_sha256'],
                  queries=[summarize_query(q, slowest) for q in selected])
    for key in ('revision', 'git_status', 'host', 'platform', 'workers', 'binary_sha256',
                'plan_sha256', 'runner_sha256', 'reference_sha256', 'adapter_sha256',
                'command', 'validation', 'limits'):
        if key in report:
            result[key] = report[key]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reports', type=Path, nargs='+', help='report.json files or run directories')
    parser.add_argument('--query', action='append', help='select a query; repeat to select several')
    parser.add_argument('--slowest', type=int, default=5, help='slowest update epochs per query')
    args = parser.parse_args()
    if args.slowest < 0:
        parser.error('--slowest must be nonnegative')
    runs = [summarize_run(p, args.query, args.slowest) for p in args.reports]
    if not any(r['queries'] for r in runs):
        parser.error('no matching query records')
    print(json.dumps(dict(format_version='snb-update-summary-1', units='milliseconds',
                          timing_note='Phase totals overlap replay/query elapsed time. Wire time is client-observed.',
                          runs=runs), indent=2))


if __name__ == '__main__':
    main()
