"""Client-observed phase timings; none of these measures server CPU time."""
from collections import defaultdict
import statistics
import time

from client import decode

TIMINGS = ('prepare_ms', 'encode_ms', 'wire_ms', 'decode_ms', 'client_ms')


def commands(server, make_commands):
    start = time.perf_counter()
    prepared_commands = make_commands()
    prepared = time.perf_counter()
    metrics, replies = server.commands(prepared_commands)
    metrics.update(prepare_ms=1000*(prepared-start), decode_ms=0,
                   client_ms=1000*(time.perf_counter()-start))
    return metrics, replies


def decode_answer(lines, event):
    start = time.perf_counter()
    rows = decode(lines)
    event['decode_ms'] = 1000*(time.perf_counter()-start)
    event['client_ms'] += event['decode_ms']
    event['rows'] = len(rows)
    return rows


def total(events):
    """Sum measured phases, excluding intervening oracle/validation work."""
    return {metric: sum(event[metric] for event in events) for metric in TIMINGS}


def summarize(events):
    buckets = defaultdict(list)
    for event in events:
        if not event['warmup']:
            buckets[event['state'] + '/' + event['phase']].append(event)
    def stats(values):
        return dict(samples=len(values), median_ms=statistics.median(values),
                    min_ms=min(values), max_ms=max(values))
    return {phase: {metric: stats([event[metric] for event in rows]) for metric in TIMINGS}
            for phase, rows in sorted(buckets.items())}
