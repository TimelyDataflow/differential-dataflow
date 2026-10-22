#!/usr/bin/env python3
"""Override the default IC2/IC5/IC9 bucket choices for ranking experiments.

The logical query and its Python reference are unchanged. All stages use the
original complete sort value; each bucket count must divide the previous one.
"""
import argparse
from pathlib import Path

from snb.rel import Compiler, R
from snb.queries import Context, QUERIES


def program(buckets, hash_key, query='ic9'):
    if query not in ('ic2', 'ic5', 'ic9'):
        raise ValueError('this command supports IC2, IC5 and IC9')
    fn, schema, _ = QUERIES[query]
    plan = fn(Context(), R.source('request', schema))
    return Compiler(imports=True, rank_buckets=buckets, rank_hash_key=hash_key).compile_many({query+'.answer': plan})


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
