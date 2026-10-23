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
            program = f'export "framed" = input 0 : ({SHAPE}, ());\nexport "texted" = input 1 : ({SHAPE}, ());'
            frame = encode_frame(shape, ROWS, diffs)
            body = '\n'.join(f'{text_row(row)} diff={diff}' for row, diff in zip(ROWS, diffs))
            server.commands([f'load feeds begin\n{program}\n{{rid}} end-load',
                             (f'feed feeds 0 frame {len(frame)}', frame),
                             f'feed feeds 1 begin\n{body}\n{{rid}} end-feed', 'tick'])
            _, (framed,) = server.commands(['peek framed as corgi'])
            _, (texted,) = server.commands(['peek texted as corgi'])
            def key(row):
                text = list(row[1].encode()) if isinstance(row[1], str) else row[1]
                return [row[0], text, row[2], row[3]]
            want = sorted(([key(row), [], diff] for row, diff in zip(ROWS, diffs)), key=lambda r: r[0])
            assert decode(framed) == want, (decode(framed), want)
            assert decode(framed) == decode(texted)
            # A frame of other shapes is refused, and the session reads on.
            other = encode_frame((parse_shape('(int)'), 'unit'), [(1,)], [1])
            try:
                server.commands([(f'feed feeds 0 frame {len(other)}', other)])
                raise AssertionError('a frame of other shapes was accepted')
            except RuntimeError as error:
                assert 'shape' in str(error), error
            server.commands(['tick'])
            _, (again,) = server.commands(['peek framed as corgi'])
            assert decode(again) == want
    print(f'OK ({args.workers} workers)')


if __name__ == '__main__':
    sys.exit(main())
