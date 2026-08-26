#!/usr/bin/env python3
"""Regenerate the i64 fact files the .ddp programs consume from the dense
puzzle inputs committed as dayNN/input.txt (the compact text form of the
Materialize sqllogictest oracles). Output goes to gen/dayNN/ (gitignored);
run.sh invokes this before running the suite.

Per-day modes: grids become "row col charcode" (or digit) cell facts, 0- or
1-indexed to mirror each day's SQL; text lines become charcode or parsed
numeric facts; numeric rows pass through; day05 and day15 split one dense
input into the two per-part fact files. Passing --pad additionally writes
day05's zero-padded uniform-arity copies (input1p.txt / input2p.txt) for
the corgi backend, which crashes on mixed-arity inputs.
"""
import os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))


def dense(day):
    with open(os.path.join(HERE, day, 'input.txt')) as f:
        return f.read().rstrip('\n')


def emit(day, rows, name='input.txt'):
    path = os.path.join(HERE, 'gen', day)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, name), 'w') as f:
        for row in rows:
            print(*row, file=f)


def cells(text, base, cell=ord):
    """Grid/text -> one fact per character: (row, col, cell(ch))."""
    return [(r, c, cell(ch)) for r, line in enumerate(text.split('\n'), base)
            for c, ch in enumerate(line, base)]


def day02(text):
    """'Game N: 1 red, 4 green; ...' -> (game, set, green, red, blue)."""
    rows = []
    for line in text.split('\n'):
        m = re.match(r"Game (\d+): (.*)", line)
        for k, s in enumerate(m.group(2).split(';')):
            cnt = dict.fromkeys(('green', 'red', 'blue'), 0)
            for n, color in re.findall(r"(\d+) (\w+)", s):
                cnt[color] = int(n)
            rows.append((m.group(1), k, cnt['green'], cnt['red'], cnt['blue']))
    return rows


def day04(text):
    """'Card N: w.. | h..' -> (card, side, idx, value); side 0 = winning."""
    rows = []
    for line in text.split('\n'):
        m = re.match(r"Card\s+(\d+): (.*)", line)
        for side, nums in enumerate(m.group(2).split('|')):
            rows += [(int(m.group(1)), side, i, v)
                     for i, v in enumerate(nums.split())]
    return rows


def day05(pad):
    """Seeds + 7 maps -> input1: '0 seed' / '1 stage dst src len';
    input2: seeds read as (start, len) pairs, same map entries."""
    blocks = dense('day05').split('\n\n')
    seeds = [int(x) for x in blocks[0].split(':')[1].split()]
    entries = [(1, stage, *line.split())
               for stage, b in enumerate(blocks[1:])
               for line in b.strip().split('\n')
               if line.strip() and not line.endswith('map:')]
    parts = {
        'input1.txt': [(0, s) for s in seeds] + entries,
        'input2.txt': [(0, seeds[i], seeds[i + 1])
                       for i in range(0, len(seeds), 2)] + entries,
    }
    for name, rows in parts.items():
        emit('day05', rows, name)
        if pad:
            arity = max(len(r) for r in rows)
            emit('day05', [r + (0,) * (arity - len(r)) for r in rows],
                 name.replace('.txt', 'p.txt'))


def day07(text):
    """'XXXXX bid' -> (id, c1..c5, bid) with hand charcodes."""
    return [(i, *(ord(c) for c in hand), bid)
            for i, (hand, bid) in enumerate(ln.split() for ln in text.split('\n'))]


def day13(text):
    """Blank-line-separated grids -> (block, row, col, charcode), 1-indexed."""
    return [(b, r, c, ord(ch)) for b, blk in enumerate(text.split('\n\n'), 1)
            for r, line in enumerate(blk.split('\n'), 1)
            for c, ch in enumerate(line, 1)]


def day15():
    """Comma-separated commands -> input1: (cmd, pos, charcode);
    input2: (cmd, l1, l2, opchar, digitcode_or_0)."""
    cmds = dense('day15').split(',')
    emit('day15', [(r, i, ord(ch)) for r, cmd in enumerate(cmds, 1)
                   for i, ch in enumerate(cmd, 1)], 'input1.txt')
    rows = []
    for r, cmd in enumerate(cmds, 1):
        if cmd.endswith('-'):
            rows.append((r, ord(cmd[0]), ord(cmd[1]), ord('-'), 0))
        else:
            lab, foc = cmd.split('=')
            rows.append((r, ord(lab[0]), ord(lab[1]), ord('='), ord(foc)))
    emit('day15', rows, 'input2.txt')


def day18(text):
    """'D N (#xxxxxx)' -> (line, dircode, N, h1..h6) with hex charcodes."""
    rows = []
    for i, line in enumerate(text.split('\n'), 1):
        m = re.match(r"([UDLR]) (\d+) \(#([0-9a-f]{6})\)", line)
        rows.append((i, ord(m.group(1)), m.group(2), *(ord(c) for c in m.group(3))))
    return rows


def day19(text):
    """Workflows + parts. State names -> ids (in=0, A=-1, R=-2, rest 3+);
    rules: (1, state, prio, field, cmp, val, next); parts: (0, x, m, a, s, 0, 0)."""
    wf_txt, parts_txt = text.split('\n\n')
    ids = {'in': 0, 'A': -1, 'R': -2}
    sid = lambda name: ids.setdefault(name, len(ids))
    rows = []
    for line in wf_txt.strip().split('\n'):
        m = re.match(r"(\w+)\{(.*)\}", line)
        st = sid(m.group(1))
        for prio, rule in enumerate(m.group(2).split(','), 1):
            mm = re.match(r"([xmas])([<>])(\d+):(\w+)", rule)
            if mm:
                rows.append((1, st, prio, 'xmas'.index(mm.group(1)),
                             0 if mm.group(2) == '<' else 1,
                             int(mm.group(3)), sid(mm.group(4))))
            else:
                rows.append((1, st, prio, 0, 1, 0, sid(rule)))  # always: x > 0
    for line in parts_txt.strip().split('\n'):
        x, m_, a, s = map(int, re.findall(r"=(\d+)", line))
        rows.append((0, x, m_, a, s, 0, 0))
    return rows


def day22(text):
    """'x,y,z~x,y,z' -> (line, x1, y1, z1, x2, y2, z2), 1-indexed."""
    return [(i, *a.split(','), *b.split(','))
            for i, (a, b) in enumerate((ln.split('~') for ln in text.split('\n')), 1)]


MODES = {
    'day01': lambda t: cells(t, 0),                     # text lines, 0-indexed
    'day02': day02,
    'day03': lambda t: cells(t, 0),                     # grid, 0-indexed
    'day04': day04,
    'day06': lambda t: [ln.split() for ln in t.split('\n')],  # numeric passthrough
    'day07': day07,
    'day10': lambda t: cells(t, 1),                     # grid, 1-indexed
    'day11': lambda t: cells(t, 1),
    'day13': day13,
    'day14': lambda t: cells(t, 1),
    'day16': lambda t: cells(t, 1),
    'day17': lambda t: cells(t, 1, cell=int),           # digit grid
    'day18': day18,
    'day19': day19,
    'day22': day22,
}

if __name__ == '__main__':
    days = [a for a in sys.argv[1:] if not a.startswith('--')]
    for day in days or sorted(MODES) + ['day05', 'day15']:
        if day == 'day05':
            day05('--pad' in sys.argv[1:])
        elif day == 'day15':
            day15()
        else:
            emit(day, MODES[day](dense(day)))
