#!/usr/bin/env python3
"""Regenerate twice and require every corpus byte to stay unchanged.

First accept intended changes with UPDATE_PLAN_PAIRS=1 cargo test ... .
Then run this script to check the default build. Extra Cargo arguments are optional.
"""
from pathlib import Path
import hashlib
import os
import subprocess
import sys

sys.dont_write_bytecode = True
from stages import read_stages

pairs = Path(__file__).resolve().parent
root = pairs.parents[2]


def digest():
    return {
        str(path.relative_to(pairs)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(pairs.rglob('*')) if path.is_file()
    }


env = dict(os.environ, CARGO_NET_OFFLINE='true', UPDATE_PLAN_PAIRS='1')
env['PATH'] = str(Path.home() / '.cargo/bin') + os.pathsep + env['PATH']
command = ['cargo', 'test', '-p', 'interactive', '--test', 'plan_pairs', *sys.argv[1:]]
before = digest()
for iteration in (1, 2):
    subprocess.run(command, cwd=root, env=env, check=True)
    after = digest()
    changed = [name for name in sorted(before.keys() | after.keys()) if before.get(name) != after.get(name)]
    if changed:
        raise SystemExit(f'Regeneration {iteration} changed bytes: {changed}')
    print(f'Regeneration {iteration}: all {len(after)} files byte-identical', flush=True)

for path in sorted((pairs / "expected").glob("*.ddp")):
    if "." not in path.stem:
        read_stages(pairs, path.stem)
print("All stage diffs reconstruct the reviewed final DDP bytes")
