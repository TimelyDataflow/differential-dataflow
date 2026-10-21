#!/usr/bin/env python3
"""Emit IC2, IC5 or IC9 with a bounded Corgi reduction and nested hash buckets.

The logical query and its Python reference are unchanged. All stages use the
original complete sort value; each bucket count must divide the previous one.
"""
import argparse
from pathlib import Path

from snb.rel import Compiler, product, scalar
from snb.queries import Context, QUERIES
from snb.rel import R


# reduce's input is already sorted by value, with consolidated nonzero diffs.
# Read the first twenty POSITIVE entries, then cap their combined multiplicity
# at twenty. Filtering positions first avoids expanding/copying large values.
# Capping each count before the prefix sum also bounds that sum by 20 * 20.
CHOOSE = '''let positive = (vals map ((_, d) -> (d, 0) gt), (0, vals len) range) zip filter in
let indices = ((0, (positive len, 20) min) range, positive) gather in
let prefix = (indices, vals) gather in
let ends = prefix map ((_, d) -> (d, 20) min) scan_add in
let chosen = (prefix, ends) zip map (((item, d), end) ->
    let count = (d, 20) min in
    let before = (end, count) sub in
    let keep = (((20, before) sub, 0) max, count) min in
    ((keep, 0) gt, (item, keep))) filter in'''


def reducer(ranked):
    if not ranked:
        return ' '.join(('(_, vals) -> '+CHOOSE+' chosen').split())
    # Only the final stage assigns ranks. Expand at most twenty output copies.
    return ' '.join(('''(_, vals) -> '''+CHOOSE+'''
let ends = chosen map ((_, d) -> d) scan_add in
(chosen, ends) zip map (((item, d), end) ->
    (item, ((end, d) sub, end) range) cap_list
        map ((item, rank) -> ((rank, item), 1))) flatten .1''').split())


def validate_buckets(buckets):
    if not buckets or buckets[-1] != 1 or any(n < 1 for n in buckets):
        raise ValueError('bucket counts must be positive and end in 1')
    if any(a <= b or a % b for a, b in zip(buckets, buckets[1:])):
        raise ValueError('each bucket count must be smaller and divide its predecessor')


def pipeline(source, prefix, buckets, hash_key):
    """A keyed, sorted value stream -> final values `(rank, original_value)`."""
    validate_buckets(buckets)
    lines = []
    for i, count in enumerate(buckets):
        pattern = '(key, value)' if i == 0 else '((key, _), value)'
        if count == 1:
            key = 'key'
        else:
            hashed = 'value' if hash_key == 'value' else 'value.1'
            # Hash is a 64-bit word. Logical shift gives a nonnegative integer;
            # nested moduli then use the same stable hash at every level.
            key = f'(key, (({hashed} hash shr_b64 1, {count}) rem))'
        name = f'{prefix}_{i}'
        lines.append(f'let {name} = {source} | map({pattern} -> ({key}, value)) '
                     f'| reduce({reducer(count == 1)});')
        source = name
    return lines, source


class TopKCompiler(Compiler):
    def __init__(self, buckets, hash_key):
        super().__init__(imports=True)
        validate_buckets(buckets)
        self.buckets, self.hash_key = buckets, hash_key

    def emit(self, relation):
        if relation.op != 'rank' or id(relation) in self.names:
            return super().emit(relation)
        name = super().emit(relation)
        source, order, limit, keys = relation.args
        if limit != 20 or len(order) != 2:
            raise ValueError('this compiler expects a two-field order and limit 20')
        refs = {n: f'row.{i}' for i, n in enumerate(source.fields)}
        key = product(refs[n] for n in keys)
        value = product([*(scalar(e, refs) for e in order), 'row'])
        initial = name+'_input'
        lines = [f'let {initial} = {self.names[id(source)]} | map((row, ()) -> ({key}, {value}));']
        stages, last = pipeline(initial, name+'_top', self.buckets, self.hash_key)
        lines.extend(stages)
        restored = product([*(f'item.2.{i}' for i in range(len(source.fields))), 'rank'])
        lines.append(f'let {name} = {last} | map((_, (rank, item)) -> ({restored}, ()));')
        # super.emit appended the original rank last, after emitting its inputs.
        assert self.lines[-1].splitlines()[-1].startswith(f'let {name} = ')
        self.lines[-1] = '\n'.join(lines)
        return name


def program(buckets, hash_key, query='ic9'):
    if query not in ('ic2', 'ic5', 'ic9'):
        raise ValueError('this experiment supports IC2, IC5 and IC9')
    fn, schema, _ = QUERIES[query]
    plan = fn(Context(), R.source('request', schema))
    return TopKCompiler(buckets, hash_key).compile_many({query+'.answer': plan})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--query', choices=('ic2', 'ic5', 'ic9'), default='ic9')
    parser.add_argument('--buckets', type=int, nargs='+', default=[1])
    parser.add_argument('--hash-key', choices=('value', 'identity'), default='value')
    parser.add_argument('--output', type=Path, required=True, help='new .ddp file')
    args = parser.parse_args()
    text = program(args.buckets, args.hash_key, args.query)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as target:
        target.write(text)


if __name__ == '__main__':
    main()
