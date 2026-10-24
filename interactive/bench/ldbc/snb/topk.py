"""Bounded Corgi ranking over DD's sorted values, optionally in nested buckets.

Every stage retains the complete sort value and bag multiplicity. Only the
last stage assigns ranks; DD retains rejected inputs so deletions can expose
new candidates without collecting and copying the complete group.
"""

# reduce's input is already sorted by value, with consolidated nonzero diffs.
# Read at most `limit` POSITIVE entries, then cap their combined multiplicity
# at that limit. Filtering positions first avoids expanding/copying large values.
# Capping each count before the prefix sum also bounds that sum by limit².
def reducer(ranked, limit):
    if not isinstance(limit, int) or limit < 0:
        raise ValueError('rank limit must be a nonnegative integer')
    choose = f'''let positive = (vals map ((_, d) -> (d, 0) gt), (0, vals len) range) zip filter in
let indices = ((0, (positive len, {limit}) min) range, positive) gather in
let prefix = (indices, vals) gather in
let ends = prefix map ((_, d) -> (d, {limit}) min) scan_add in
let chosen = (prefix, ends) zip map (((item, d), end) ->
    let count = (d, {limit}) min in
    let before = (end, count) sub in
    let keep = ((({limit}, before) sub, 0) max, count) min in
    ((keep, 0) gt, (item, keep))) filter in'''

    if not ranked:
        return ' '.join(('(_, vals) -> '+choose+' chosen').split())
    # Only the final stage assigns ranks. Expand at most `limit` output copies.
    return ' '.join(('''(_, vals) -> '''+choose+'''
let ends = chosen map ((_, d) -> d) scan_add in
(chosen, ends) zip map (((item, d), end) ->
    (item, ((end, d) sub, end) range) cap_list
        map ((item, rank) -> ((rank, item), 1))) flatten .1''').split())


def validate_buckets(buckets):
    if not buckets or buckets[-1] != 1 or any(n < 1 for n in buckets):
        raise ValueError('bucket counts must be positive and end in 1')
    if any(a <= b or a % b for a, b in zip(buckets, buckets[1:])):
        raise ValueError('each bucket count must be smaller and divide its predecessor')


def pipeline(source, prefix, buckets, hash_key, limit=20):
    """A keyed, sorted value stream -> final values `(rank, original_value)`."""
    validate_buckets(buckets)
    if hash_key not in ('value', 'identity'):
        raise ValueError('hash key must be value or identity')
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
                     f'| reduce({reducer(count == 1, limit)});')
        source = name
    return lines, source
