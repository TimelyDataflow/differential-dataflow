#!/usr/bin/env python3
"""Convert LDBC SNB BI substitution parameters into the DDIR suite's --parameters JSON.

    ldbc_params.py <parameters-sfX dir> <out.json> [bindings per query]

Dates become milliseconds since the epoch at UTC midnight, datetimes milliseconds, as the
suite's snapshot adapter derives them. Query variants (bi2a/bi2b, ...) are interleaved.
"""
import csv, json, sys
from datetime import datetime, timezone
from pathlib import Path

# LDBC parameter name -> suite field, per query.
FIELDS = {
    'bi1': {'datetime': 'end'},
    'bi2': {'date': 'date', 'tagClass': 'classname'},
    'bi3': {'tagClass': 'classname', 'country': 'countryname'},
    'bi4': {'date': 'date'},
    'bi5': {'tag': 'tagname'},
    'bi6': {'tag': 'tagname'},
    'bi7': {'tag': 'tagname'},
    'bi8': {'tag': 'tagname', 'startDate': 'start', 'endDate': 'end'},
    'bi9': {'startDate': 'start', 'endDate': 'end'},
    'bi10': {'personId': 'pid', 'country': 'countryname', 'tagClass': 'classname',
             'minPathDistance': 'minimum', 'maxPathDistance': 'maximum'},
    'bi11': {'country': 'countryname', 'startDate': 'start', 'endDate': 'end'},
    'bi12': {'startDate': 'start', 'lengthThreshold': 'length', 'languages': 'language'},
    'bi13': {'country': 'countryname', 'endDate': 'end'},
    'bi14': {'country1': 'country1', 'country2': 'country2'},
    'bi15': {'person1Id': 'p1', 'person2Id': 'p2', 'startDate': 'start', 'endDate': 'end'},
    'bi16': {'tagA': 'taga', 'dateA': 'datea', 'tagB': 'tagb', 'dateB': 'dateb', 'maxKnowsLimit': 'maxknows'},
    'bi17': {'tag': 'tagname', 'delta': 'delta'},
    'bi18': {'tag': 'tagname'},
    'bi19': {'city1Id': 'city1', 'city2Id': 'city2'},
    'bi20': {'company': 'company', 'person2Id': 'p2'},
}

def millis(text, kind):
    if kind == 'DATE':
        moment = datetime.strptime(text, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        moment = datetime.fromisoformat(text.replace('Z', '+00:00'))
    return int(moment.timestamp() * 1000)

def convert(value, kind):
    if kind in ('DATE', 'DATETIME'):
        return millis(value, kind)
    if kind in ('INT', 'ID'):
        return int(value)
    if kind == 'STRING[]':
        return value.split(';') if value else []
    return value

def load(path, fields):
    with open(path, newline='') as f:
        rows = csv.reader(f, delimiter='|')
        header = [h.split(':') for h in next(rows)]
        out = []
        for row in rows:
            binding = {fields[name]: convert(value, kind) for (name, kind), value in zip(header, row)}
            out.append(binding)
        return out

def main(src, dst, per_query=5):
    src, bank = Path(src), {}
    for query, fields in FIELDS.items():
        number = query[2:]
        files = sorted(src.glob(f'bi-{number}.csv')) + sorted(src.glob(f'bi-{number}[ab].csv'))
        variants = [load(f, fields) for f in files]
        interleaved = [b for group in zip(*variants) for b in group] if len(variants) > 1 else variants[0]
        for b in interleaved:
            if query == 'bi12':
                b['enabled'] = 1
        bank[query] = interleaved[:per_query]
    Path(dst).write_text(json.dumps(bank, indent=1) + '\n')
    print(f'{len(bank)} queries, {sum(map(len, bank.values()))} bindings -> {dst}')

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 5)
