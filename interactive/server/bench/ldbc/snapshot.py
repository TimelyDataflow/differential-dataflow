"""Shared reader for Spark's default SNB BI pipe-delimited CSV export.

Double quotes delimit fields; backslashes escape quotes/backslashes *inside*
quoted fields. Raw-generator CSV and custom export dialects are not supported.
"""
import csv
import gzip


def csv_rows(source):
    # Normalize Spark's quoted-field escapes to the doubled quotes understood
    # by csv.reader. Setting escapechar='\\' on csv.reader would also consume
    # literal backslashes in UNQUOTED fields, unlike Spark's writer/reader.
    def normalized():
        quoted, field_start = False, True
        for line in source:
            if not quoted and '"' not in line:
                yield line
                field_start = True
                continue
            out, i = [], 0
            while i < len(line):
                ch = line[i]
                if quoted:
                    if ch == '\\' and i + 1 < len(line) and line[i+1] in ('\\', '"'):
                        out.append('""' if line[i+1] == '"' else '\\')
                        i += 2
                        continue
                    if ch == '"':
                        quoted = False
                else:
                    if ch == '"' and field_start:
                        quoted = True
                    field_start = ch in '|\r\n'
                out.append(ch)
                i += 1
            yield ''.join(out)

    reader = csv.DictReader(normalized(), delimiter='|', strict=True)
    fields = reader.fieldnames
    if not fields or len(set(fields)) != len(fields):
        raise ValueError('missing or duplicate CSV header')
    for row in reader:
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f'CSV row ending at line {reader.line_num} has the wrong field count')
        yield row


def read_table(snapshot, category, entity):
    directory = snapshot / category / entity
    # A decompressed partition and its archive are the same logical input.
    paths = sorted([*directory.glob('*.csv'),
                    *(p for p in directory.glob('*.csv.gz') if not p.with_suffix('').exists())])
    if not paths:
        raise FileNotFoundError(f'missing {entity} CSV in {directory}')
    for path in paths:
        opener = gzip.open if path.suffix == '.gz' else open
        with opener(path, 'rt', newline='', encoding='utf-8') as source:
            try:
                yield from csv_rows(source)
            except (csv.Error, ValueError) as error:
                raise ValueError(f'{path}: {error}') from error
