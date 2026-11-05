#!/usr/bin/env python3
"""The paramgen's interactive-<q>.parquet files as one JSON file in the suite's field names.

    parameters.py <paramgen parameters dir> <out.json>

Rows keep file order, which the driver cycles through. Dates and timestamps
become epoch milliseconds (UTC); useFrom/useUntil bound when a row may be used.
Needs duckdb; the driver itself does not.
"""
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

import duckdb

VARIANTS = ('1', '2', '3a', '3b', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13a', '13b', '14a', '14b')
# paramgen column -> suite field (snb/queries.py request schemas)
FIELDS = {
    'personId': 'pid', 'person1Id': 'p1', 'person2Id': 'p2', 'firstName': 'firstname',
    'maxDate': 'maxdate', 'minDate': 'mindate', 'startDate': 'start', 'durationDays': 'days',
    'countryXName': 'countryx', 'countryYName': 'countryy', 'tagName': 'tagname', 'month': 'month',
    'countryName': 'countryname', 'workFromYear': 'workyear', 'tagClassName': 'classname',
}


def main(src, dst):
    db = duckdb.connect()
    db.execute("SET TimeZone = 'UTC'")
    bank = {}
    for variant in VARIANTS:
        rel = db.execute(f"SELECT * FROM read_parquet('{Path(src) / f'interactive-{variant}.parquet'}')")
        names = [d[0] for d in rel.description]
        rows = []
        for values in rel.fetchall():
            row = {}
            for name, value in zip(names, values):
                if isinstance(value, datetime):
                    value = int(value.replace(tzinfo=value.tzinfo or timezone.utc).timestamp() * 1000)
                elif isinstance(value, date):
                    value = int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000)
                row[FIELDS.get(name, name)] = value
            rows.append(row)
        bank[variant] = rows
    Path(dst).write_text(json.dumps(bank) + '\n')
    print(', '.join(f'{v}: {len(r)}' for v, r in bank.items()))


if __name__ == '__main__':
    main(*sys.argv[1:])
