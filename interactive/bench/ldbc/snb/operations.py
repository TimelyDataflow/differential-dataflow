"""SNB Interactive v2 update operations, read from the BI CSV inserts and deletes.

Interactive v2 replays the same simulated activity as BI, but one operation at a
time at its own timestamp instead of one day at a time. Each operation is one
inserted entity with its attribute edges (a person with its interests, studies
and jobs; a forum, post or comment with its tags), one inserted edge, or one
explicit delete target. Cascades are not part of the operation; `Cascade`
resolves them against the current graph, as a system under test would.

`Cascade` produces the same projected delta as `updates.State`, from indexes
instead of scans, so a single delete costs time proportional to what it removes.
"""
from collections import defaultdict, deque
from dataclasses import dataclass, field
import gzip

from snapshot import csv_rows
from . import data
from .updates import CLASSES, RELATIONS, partitions, project_rows

# Operation kinds in the order ties at one timestamp are applied: inserts in
# dependency order, then deletes.
KINDS = tuple(CLASSES)
ORDER = {kind: i for i, kind in enumerate(KINDS)}
OWNER = {  # attribute-edge entity -> (operation entity, the column naming it)
    'Person_hasInterest_Tag': ('Person', 'PersonId'),
    'Person_studyAt_University': ('Person', 'PersonId'),
    'Person_workAt_Company': ('Person', 'PersonId'),
    'Forum_hasTag_Tag': ('Forum', 'ForumId'),
    'Post_hasTag_Tag': ('Post', 'PostId'),
    'Comment_hasTag_Tag': ('Comment', 'CommentId'),
}


@dataclass(order=True)
class Operation:
    time: int                     # simulation time, ms since the epoch
    order: int                    # ORDER[kind], for ties
    kind: str = field(compare=False)
    rows: dict = field(compare=False)   # entity -> [CSV row dicts], as `updates.State.apply` takes


def _rows(root, phase, entity):
    directory = root / phase / 'dynamic' / entity
    for partition in sorted(directory.glob('batch_id=*')):
        for path in partitions(partition):
            opener = gzip.open if path.suffix == '.gz' else open
            with opener(path, 'rt', newline='', encoding='utf-8') as source:
                yield from csv_rows(source, entity)


def operations(root):
    """Every insert and delete operation under a BI composite-merged-fk export, in time order."""
    owned = defaultdict(lambda: defaultdict(list))
    for entity, (_, column) in OWNER.items():
        for row in _rows(root, 'inserts', entity):
            owned[entity][int(row[column])].append(row)
    result = []
    for kind, entities in CLASSES.items():
        phase = 'inserts' if kind.startswith('insert-') else 'deletes'
        head, attributes = entities[0], entities[1:]
        stamp = 'creationDate' if phase == 'inserts' else 'deletionDate'
        for row in _rows(root, phase, head):
            rows = {head: [row]}
            for entity in attributes if phase == 'inserts' else ():
                rows[entity] = owned[entity].pop(int(row['id']), [])
            result.append(Operation(data.millis(row[stamp]), ORDER[kind], kind, rows))
    stray = sum(len(v) for by in owned.values() for v in by.values())
    if stray:
        raise ValueError(f'{stray} attribute edges without an inserted owner')
    result.sort()
    return result


# Columns indexed per relation: the cascades and edge deletes look rows up by these.
INDEXED = {
    'person': (0,), 'forum': (0, 3), 'message': (0, 3, 9, 10),
    'knows': (0, 1), 'member': (0, 1), 'likes': (0, 1), 'mtag': (0,), 'ftag': (0,),
    'interest': (0,), 'study': (0,), 'work': (0,), 'email': (0,), 'language': (0,),
}


class Cascade:
    """The graph plus the indexes that resolve one operation's exact delta."""

    def __init__(self, graph):
        self.graph = graph
        self.index = {(name, c): defaultdict(set) for name, cols in INDEXED.items() for c in cols}
        for name, cols in INDEXED.items():
            for row in graph[name]:
                for c in cols:
                    self.index[name, c][row[c]].add(row)

    def lookup(self, name, column, values):
        index = self.index[name, column]
        return {row for v in values for row in index.get(v, ())}

    def apply(self, kind, rows):
        """One operation's delta, in `updates.State.apply`'s form, applied to the graph."""
        if kind.startswith('insert-'):
            added, removed = project_rows(rows), {}
            for name, new in added.items():
                for row in new:
                    if self.index.get((name, 0)) is not None and name in ('person', 'forum', 'message') \
                            and self.index[name, 0].get(row[0]):
                        raise ValueError(f'{kind}: existing {name} {row[0]}')
        else:
            entity = CLASSES[kind][0]
            removed, added = self.delete(entity, {data.key(entity, row) for row in rows.get(entity, [])})
        changes = {}
        for name in data.SCHEMA:
            gone, new = removed.get(name, set()), added.get(name, set())
            if gone != new and (gone or new):
                changes[name] = dict(removed=sorted(gone - new), added=sorted(new - gone))
        for name, delta in changes.items():
            cols = INDEXED.get(name, ())
            for row in delta['removed']:
                self.graph[name].remove(row)
                for c in cols:
                    bucket = self.index[name, c][row[c]]
                    bucket.discard(row)
                    if not bucket:
                        del self.index[name, c][row[c]]
            for row in delta['added']:
                if row in self.graph[name]:
                    raise ValueError(f'{kind}: duplicate {name} row')
                self.graph[name].add(row)
                for c in cols:
                    self.index[name, c][row[c]].add(row)
        return changes

    def delete(self, entity, targets):
        relation = RELATIONS[entity]
        if entity in data.EDGE_KEYS:
            removed = {r for r in self.lookup(relation, 0, {t[0] for t in targets}) if r[:2] in targets}
            return {relation: removed}, {}
        kind = {'Post': 0, 'Comment': 1}.get(entity)
        found = {r[0] for r in self.lookup(relation, 0, targets) if kind is None or r[1] == kind}
        people = found if entity == 'Person' else set()
        forums = set(found) if entity == 'Forum' else set()
        messages = set(found) if entity in ('Post', 'Comment') else set()
        group_before, group_after = set(), set()
        for row in self.lookup('forum', 3, people):
            if row[1].startswith(('Album ', 'Wall ')):
                forums.add(row[0])
            elif row[1].startswith('Group '):
                group_before.add(row)
                group_after.add((*row[:3], -1))
            else:
                raise ValueError(f'cannot determine generated forum kind: {row[1]!r}')
        messages.update(r[0] for r in self.lookup('message', 3, people))
        messages.update(r[0] for r in self.lookup('message', 9, forums) if r[1] == 0)
        pending = deque(messages)
        while pending:
            for child in self.lookup('message', 10, {pending.popleft()}):
                if child[1] == 1 and child[0] not in messages:
                    messages.add(child[0])
                    pending.append(child[0])
        removed = {
            'person': self.lookup('person', 0, people),
            'forum': self.lookup('forum', 0, forums) | group_before,
            'message': self.lookup('message', 0, messages),
            'knows': self.lookup('knows', 0, people) | self.lookup('knows', 1, people),
            'member': self.lookup('member', 0, forums) | self.lookup('member', 1, people),
            'likes': self.lookup('likes', 0, people) | self.lookup('likes', 1, messages),
            'mtag': self.lookup('mtag', 0, messages),
            'ftag': self.lookup('ftag', 0, forums),
            **{n: self.lookup(n, 0, people) for n in ('interest', 'study', 'work', 'email', 'language')},
        }
        return {k: v for k, v in removed.items() if v}, {'forum': group_after}
