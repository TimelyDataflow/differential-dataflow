"""Recover the whole --emit-ddp-stage views from reviewed corpus bytes."""
from pathlib import Path
import re

ORDER = ['parsed', 'lowered', 'optimized', 'projected', 'as-of']


def apply_diff(before, diff):
    if not diff:
        return before
    source = before.splitlines(keepends=True)
    lines = diff.splitlines(keepends=True)
    assert lines[0].startswith('--- ') and lines[1].startswith('+++ ')
    output, cursor = [], 0
    for line in lines[2:]:
        if line.startswith('@@ '):
            match = re.match(r'@@ -(\d+)(?:,\d+)? \+\d+(?:,\d+)? @@', line)
            assert match, line
            start = max(int(match[1]) - 1, 0)
            assert start >= cursor
            output.extend(source[cursor:start])
            cursor = start
        elif line.startswith(' '):
            assert source[cursor] == line[1:]
            output.append(source[cursor])
            cursor += 1
        elif line.startswith('-'):
            assert source[cursor] == line[1:]
            cursor += 1
        elif line.startswith('+'):
            output.append(line[1:])
        else:
            raise ValueError(f'Unexpected diff line: {line!r}')
    return ''.join(output + source[cursor:])


def read_stages(pairs, name):
    expected = Path(pairs) / 'expected'
    first = 'parsed' if (expected / f'{name}.parsed.ddp').exists() else 'lowered'
    result = {first: (expected / f'{name}.{first}.ddp').read_text()}
    order = ORDER[ORDER.index(first):]
    for previous, current in zip(order, order[1:]):
        diff = (expected / f'{name}.{previous}--{current}.diff').read_text()
        result[current] = apply_diff(result[previous], diff)
    assert result['as-of'] == (expected / f'{name}.ddp').read_text(), name
    return result
