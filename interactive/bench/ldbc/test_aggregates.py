#!/usr/bin/env python3
"""Weighted integer aggregates preserve zero sums and retract empty groups."""
import argparse
from collections import Counter
from pathlib import Path

from client import Server, decode
from run import checked
from snb.rel import Compiler, R, c


AGGREGATES = {
    'counts': (('group',), {'n':('count', 1)}),
    'sums': (('group',), {'s':('sum', c.weight)}),
    'mixed': (('group',), {'n':('count', 1), 's':('sum', c.weight), 't':('sum', c.second)}),
    'zeros': (('group',), {'s':('sum', 0)}),
    'global': ((), {'n':('count', 1), 's':('sum', c.weight)}),
}


def program():
    source = R.source('rows', {'group':'int', 'identity':'int', 'weight':'int', 'second':'int'})
    compiler = Compiler()
    plans = {name:source.group(keys, **aggs) for name,(keys,aggs) in AGGREGATES.items()}
    exports = [f'export "{name}" = {compiler.emit(plan)} | arrange;' for name,plan in plans.items()]
    return '\n'.join([*compiler.lines, *exports])


def expected(state, name):
    keys, aggs = AGGREGATES[name]
    groups = {}
    for (group, identity, weight, second), count in state.items():
        assert count >= 0
        if not count:
            continue
        key = (group,) if keys else ()
        totals = groups.setdefault(key, [0]*len(aggs))
        values = {'n':count, 's':0 if name == 'zeros' else weight*count, 't':second*count}
        for i, field in enumerate(aggs):
            totals[i] += values[field]
    return [[[*key, *totals], [], 1] for key, totals in sorted(groups.items())]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True)
    text = program()
    assert '| collect' not in text and 'fold' not in text
    (args.output/'aggregates.ddp').write_text(text)
    initial = [((1,1,7,1),1), ((1,2,-7,-1),1), ((2,3,0,0),3), ((3,4,-5,8),1),
               ((4,5,1000000000000,0),1)]
    deltas = [initial,
              [((1,1,7,1),-1), ((1,6,4,2),2), ((2,3,0,0),-3), ((3,4,-5,8),-1)],
              None, initial, [((2,3,0,0),-2), ((1,1,7,1),-1)]]
    for workers in (1,4):
        state = Counter()
        with Server(args.server.resolve(), args.output/f'w{workers}.log', workers, 120, 6) as server:
            server.commands([f'load test begin\n{text}\n{{rid}} end-load', 'tick'])
            for epoch, delta in enumerate(deltas):
                if delta is None:
                    delta = [(row,-count) for row,count in state.items() if count]
                commands = []
                for row, count in delta:
                    state[row] += count
                    commands.append(Server.feed('test', 0, [row], count))
                server.commands([*commands, 'tick'])
                for name in AGGREGATES:
                    frames, = server.commands([f'peek {name} as corgi'])[1]
                    checked(decode(frames), expected(state,name), f'w{workers}/{epoch}/{name}')
        print(f'{workers} workers: five weighted aggregates match all five states',flush=True)


if __name__ == '__main__':
    main()
