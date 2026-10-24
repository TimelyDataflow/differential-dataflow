#!/usr/bin/env python3
"""Bindings and Python reference answers for the SQL baselines (run with the suite's Python).

    python3 reference.py --prepared JOURNAL --parameters ../bindings/bi-official-sf1.json --output ref.json [query ...]

For each query: the suite's smoke bindings (suite-0 = snb.parameters.parameters,
suite-1 = alternate) and, with --parameters, each official binding (official-N),
each with the answer of snb.parameters.reference on the initial snapshot. The
reference is plain Python and slow at SF1 (seconds to minutes per binding);
--timeout bounds each one, and --bindings-only records bindings without answers
(status 'noref'), for checking systems against each other instead.
The output is resumable: existing labels are skipped.
"""
import argparse
import gzip
import json
import pickle
import signal
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from snb import data  # noqa: E402
from snb.parameters import alternate, parameters, reference, requests  # noqa: E402
from suite import CATALOGUE, compile_queries  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--snapshot', type=Path)
    source.add_argument('--prepared', type=Path, help='update_stream.py journal (reads initial.pickle.gz)')
    p.add_argument('--parameters', type=Path, help='JSON: query -> list of official bindings')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--timeout', type=int, default=1800, help='seconds per reference evaluation')
    p.add_argument('--bindings-only', action='store_true')
    p.add_argument('queries', nargs='*')
    args = p.parse_args()
    if args.prepared:
        with gzip.open(args.prepared / 'initial.pickle.gz') as f:
            graph = pickle.load(f)
    else:
        graph = data.load(args.snapshot)
    official = json.loads(args.parameters.read_text()) if args.parameters else {}
    names = args.queries or CATALOGUE
    plans = compile_queries(names)
    result = json.loads(args.output.read_text()) if args.output.exists() else {}

    def alarm(*_):
        raise TimeoutError
    signal.signal(signal.SIGALRM, alarm)
    for name in names:
        base = parameters(graph, name)
        bank = [('suite-0', base), ('suite-1', alternate(graph, base))]
        bank += [(f'official-{i}', {**base, **b}) for i, b in enumerate(official.get(name, []))]
        entries = result.setdefault(name, [])
        done = {e['label'] for e in entries}
        for label, binding in bank:
            if label in done:
                continue
            entry = dict(label=label, binding=binding, answer=None, status='noref', reference_ms=0)
            if not args.bindings_only:
                active = requests(name, plans[name]['parameters'], binding, 1)
                started = time.time()
                signal.alarm(args.timeout)
                try:
                    entry.update(answer=reference(plans[name]['plan'], plans[name]['sources'], {**graph, 'request': active}), status='ok')
                except TimeoutError:
                    entry['status'] = 'timeout'
                finally:
                    signal.alarm(0)
                entry['reference_ms'] = 1000 * (time.time() - started)
            entries.append(entry)
            print(f'{name} {label}: {entry["status"]} {entry["reference_ms"]:.0f} ms', flush=True)
            args.output.write_text(json.dumps(result))


if __name__ == '__main__':
    main()
