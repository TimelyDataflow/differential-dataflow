"""Feeds as binary frames against a real server: what a frame stages is what a peek answers with,
and the same as the rows fed as text."""
import argparse
from pathlib import Path
import sys
import tempfile

from client import Server, decode, encode_frame, parse_shape

SHAPE = '(int, List(int), float, List(List(int)))'
ROWS = [
    (0, '', 0.0, []),
    (1, 'hé', 2.5, [[1, 2], []]),
    (-7, [300, 1], -1.25, [[255]]),
    (2**40, 'text', 1e300, [[-1], [2**62]]),
]


def text_row(row):
    """A row as `feed … begin` reads it: a tuple, its lists and floats spelled out."""
    def term(value):
        if isinstance(value, float):
            return repr(value)
        if isinstance(value, str):
            value = list(value.encode())
        if isinstance(value, list):
            return 'list(' + ','.join(map(term, value)) + ')'
        return str(value)
    return 'tuple(' + ','.join(map(term, row)) + ')'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=1)
    args = parser.parse_args()
    shape = (parse_shape(SHAPE), 'unit')
    diffs = [1, 3, -2, 1]
    with tempfile.TemporaryDirectory() as folder:
        with Server(args.server, Path(folder) / 'server.log', args.workers, 60, 4) as server:
            program = '\n'.join(f'output "{name}" = input {slot} : ({SHAPE}, ());'
                                for slot, name in enumerate(('framed', 'texted', 'shared')))
            times = [1, 0, 1, 0]  # Deliberately out of arrival order.
            frame = encode_frame(shape, ROWS, diffs, times=times)
            shared = encode_frame(shape, ROWS, diffs, times=None)
            body = '\n'.join(f'{text_row(row)} diff={diff} time={t}'
                             for row, diff, t in zip(ROWS, diffs, times))
            server.commands([server.load('feeds', program),
                (f'feed feeds 0 frame {len(frame)}', frame),
                f'feed feeds 1 begin\n{body}\n{{rid}} end-feed',
                (f'feed feeds 2 time=0 frame {len(shared)}', shared),
                *(f'seal {program} {slot} {server.epoch + 1}' for program, slot in server.input_ports),
                *(['diag'] if server.diagnostics else []),
                'spin',
                *(f'trim {trace} {server.epoch}' for trace in (*server.output_traces, *server.diagnostics))])
            server.epoch += 1
            def key(row):
                text = list(row[1].encode()) if isinstance(row[1], str) else row[1]
                return [row[0], text, row[2], row[3]]
            want = sorted(([key(row), [], diff] for row, diff in zip(ROWS, diffs)), key=lambda r: r[0])
            early = sorted(([key(row), [], diff] for row, diff, t in zip(ROWS, diffs, times) if t == 0),
                           key=lambda r: r[0])
            _, (framed, texted, shared_rows) = server.commands([
                f'peek feeds::{name} as-of 0 as corgi' for name in ('framed', 'texted', 'shared')])
            assert decode(framed) == decode(texted) == early
            assert decode(shared_rows) == want
            # A shared header cannot override the timestamps already in a frame.
            try:
                server.commands([(f'feed feeds 0 time=1 frame {len(frame)}', frame)])
                raise AssertionError('a shared header overrode per-row times')
            except RuntimeError as error:
                assert 'header' in str(error), error
            # A zero-lane frame's shared timestamp must still be writable.
            try:
                server.commands([(f'feed feeds 2 time=0 frame {len(shared)}', shared)])
                raise AssertionError('a zero-lane frame wrote behind the sealed frontier')
            except RuntimeError as error:
                assert 'frontier' in str(error), error
            # A frame of other shapes is refused, and the session reads on.
            other = encode_frame((parse_shape('(int)'), 'unit'), [(1,)], [1])
            try:
                server.commands([(f'feed feeds 0 time={server.epoch} frame {len(other)}', other)])
                raise AssertionError('a frame of other shapes was accepted')
            except RuntimeError as error:
                assert 'shape' in str(error), error
            server.commands([
                *(f'seal {program} {slot} {server.epoch + 1}' for program, slot in server.input_ports),
                *(['diag'] if server.diagnostics else []),
                'spin',
                *(f'trim {trace} {server.epoch}' for trace in (*server.output_traces, *server.diagnostics))])
            server.epoch += 1
            _, (framed, texted) = server.commands([f'peek feeds::{name} as-of 1 as corgi'
                                                  for name in ('framed', 'texted')])
            assert decode(framed) == decode(texted) == want
    print(f'OK ({args.workers} workers)')


if __name__ == '__main__':
    sys.exit(main())
