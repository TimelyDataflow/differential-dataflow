"""The update stream and the initial snapshot, read a piece at a time.

`snb.operations.operations` reads every operation up front, as CSV row dictionaries,
which at SF1 costs about a gigabyte. Here the operations come one simulated day at
a time (the BI export is partitioned by day), and the snapshot comes as chunks of
projected rows, so the driver holds neither the graph nor the whole stream.
"""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import gzip
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from snapshot import csv_rows, read_table  # noqa: E402
from snb import data  # noqa: E402
from snb.operations import OWNER, ORDER, Operation  # noqa: E402
from snb.updates import CLASSES, partitions, project_rows  # noqa: E402


def days(root):
    """Every day with a batch, in order."""
    found = set()
    for phase in ('inserts', 'deletes'):
        for entity_dir in (root / phase / 'dynamic').iterdir():
            found.update(p.name.removeprefix('batch_id=') for p in entity_dir.glob('batch_id=*'))
    return sorted(found)


def _rows(root, phase, entity, day):
    directory = root / phase / 'dynamic' / entity / f'batch_id={day}'
    for path in partitions(directory) if directory.is_dir() else ():
        opener = gzip.open if path.suffix == '.gz' else open
        with opener(path, 'rt', newline='', encoding='utf-8') as source:
            yield from csv_rows(source, entity)


def operations(root):
    """Every operation, one day's worth at a time, each day in time order (as snb.operations)."""
    for day in days(root):
        owned = defaultdict(lambda: defaultdict(list))
        for entity, (_, column) in OWNER.items():
            for row in _rows(root, 'inserts', entity, day):
                owned[entity][int(row[column])].append(row)
        batch = []
        for kind, entities in CLASSES.items():
            phase = 'inserts' if kind.startswith('insert-') else 'deletes'
            head, attributes = entities[0], entities[1:]
            stamp = 'creationDate' if phase == 'inserts' else 'deletionDate'
            for row in _rows(root, phase, head, day):
                rows = {head: [row]}
                for entity in attributes if phase == 'inserts' else ():
                    rows[entity] = owned[entity].pop(int(row['id']), [])
                batch.append(Operation(data.millis(row[stamp]), ORDER[kind], kind, rows))
        stray = sum(len(v) for by in owned.values() for v in by.values())
        if stray:
            raise ValueError(f'{day}: {stray} attribute edges without an inserted owner')
        batch.sort()
        yield from batch


def horizon(root):
    """The end of the last day with a batch, in ms: no update is later."""
    last = date.fromisoformat(days(root)[-1]) + timedelta(days=1)
    return int(datetime(last.year, last.month, last.day, tzinfo=timezone.utc).timestamp() * 1000)


def snapshot_rows(snapshot, chunk=20000):
    """The initial snapshot's projected rows, as (relation, rows) chunks."""
    for entity in (*data.ENTITIES, *data.STATIC):
        rows = []
        for row in read_table(snapshot, 'static' if entity in data.STATIC else 'dynamic', entity):
            rows.append(row)
            if len(rows) == chunk:
                yield from _project(entity, rows)
                rows = []
        yield from _project(entity, rows)


def _project(entity, rows):
    for name, projected in project_rows({entity: rows}).items():
        if projected:
            yield name, sorted(projected)
