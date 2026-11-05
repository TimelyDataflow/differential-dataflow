"""The graph program's own delete cascades (interactive_v2/graph.py) against updates.State.

    python3 interactive/bench/ldbc/test_cascade.py --server target/release/ddir_server

Each case starts from test_updates' fixture graph, applies a sequence of inserts
and explicit deletes, and compares every live relation the program derives with
the oracle's graph.
"""
import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / 'interactive_v2'))
from client import Server, decode, parse_shape  # noqa: E402
from graph import DELETES, cascade_program, delete_row  # noqa: E402
from snb import data, updates  # noqa: E402
from snb.rel import source_shape  # noqa: E402
from snb.updates import project_rows  # noqa: E402
from test_updates import graph  # noqa: E402

SHAPES = [(parse_shape(source_shape(s)), 'unit') for s in data.SCHEMA.values()] \
       + [(parse_shape(DELETES[i][1]), 'unit') for i in sorted(DELETES)]
RELATIONS = list(data.SCHEMA)
COMMENT = dict(id='205', creationDate='2012-12-01T00:00:00Z', locationIP='1.2.3.4', browserUsed='Firefox',
               content='quoted | é', length='10', CreatorPersonId='3', LocationCountryId='0',
               ParentPostId='', ParentCommentId='201')
CASES = {
    'person': [('delete-person', {'Person': [{'id': '1'}]})],
    'forum': [('delete-forum', {'Forum': [{'id': '10'}]})],
    'post': [('delete-post', {'Post': [{'id': '100'}]})],
    'comment': [('delete-comment', {'Comment': [{'id': '200'}]})],
    'member': [('delete-member', {'Forum_hasMember_Person': [{'ForumId': '12', 'PersonId': '1'}]})],
    'knows': [('delete-knows', {'Person_knows_Person': [{'Person1Id': '2', 'Person2Id': '1'}]})],
    'like': [('delete-like-post', {'Person_likes_Post': [{'PersonId': '3', 'PostId': '100'}]})],
    'sequence': [('insert-comment', {'Comment': [COMMENT], 'Comment_hasTag_Tag': [{'CommentId': '205', 'TagId': '9'}]}),
                 ('delete-person', {'Person': [{'id': '1'}]}),
                 ('delete-post', {'Post': [{'id': '100'}]}),
                 ('delete-post', {'Post': [{'id': '100'}]}),
                 ('delete-person', {'Person': [{'id': '2'}]})],
}


def typed(g):
    """The fixture's rows with text fields as text, as the CSV adapter makes them."""
    return {n: {tuple(str(x) if t == 'bytes' and not isinstance(x, str) else x
                      for x, t in zip(r, data.SCHEMA[n].values())) for r in rows} for n, rows in g.items()}


def mismatches(server, workers, steps):
    s = Server(server, Path('/tmp/test-cascade.log'), workers, 60, 4)
    try:
        s.commands([f'load graph begin\n{cascade_program(export_live=True)}\n{{rid}} end-load', 'tick'])
        g = typed(graph())
        oracle = updates.State({n: set(v) for n, v in g.items()})
        for i, name in enumerate(RELATIONS):
            if g[name]:
                s.commands([Server.feed('graph', i, sorted(g[name]), 1, SHAPES[i])])
        s.commands(['tick'])
        for t, (kind, rows) in enumerate(steps):
            oracle.apply(kind, rows)
            if kind.startswith('insert-'):
                for name, new in project_rows(rows).items():
                    if new:
                        i = RELATIONS.index(name)
                        s.commands([Server.feed('graph', i, sorted(new), 1, SHAPES[i])])
            else:
                i, row = delete_row(kind, rows, t)
                s.commands([Server.feed('graph', i, [row], 1, SHAPES[i])])
            s.commands(['tick'])
        bad = []
        for name in RELATIONS:
            _, (lines,) = s.commands([f'peek live.{name} as corgi'])
            got = {tuple(bytes(x).decode() if isinstance(x, list) else x for x in key)
                   for key, _, diff in decode(lines) for _ in range(diff)}
            if got != oracle.graph[name]:
                bad.append(name)
        return bad
    finally:
        s.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--server', required=True, type=Path)
    args = p.parse_args()
    failed = 0
    for workers in (1, 2):
        for name, steps in CASES.items():
            bad = mismatches(args.server, workers, steps)
            failed += bool(bad)
            print(f'{workers} worker(s) {name}: ' + (f'MISMATCH in {bad}' if bad else 'ok'))
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
