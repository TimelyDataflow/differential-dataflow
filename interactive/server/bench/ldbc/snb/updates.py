"""BI CSV batches and their exact projected insertions/retractions.

Delete files contain explicit targets, not their cascades. Person deletion keeps
Groups (clearing the moderator), removes Walls/Albums, and deletes authored
messages and their descendants. See the pinned spec's delete-01/04/06/07 and
ldbc_snb_bi/umbra/dml/apply-deletes.sql. Generated forum titles carry the kind.
"""
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date
import gzip

from snapshot import csv_rows, read_table
from . import data


INSERTS = {
    'person': ('Person', 'Person_hasInterest_Tag', 'Person_studyAt_University', 'Person_workAt_Company'),
    'forum': ('Forum', 'Forum_hasTag_Tag'),
    'post': ('Post', 'Post_hasTag_Tag'),
    'comment': ('Comment', 'Comment_hasTag_Tag'),
    'like-post': ('Person_likes_Post',),
    'like-comment': ('Person_likes_Comment',),
    'knows': ('Person_knows_Person',),
    'member': ('Forum_hasMember_Person',),
}
DELETES = {name: (entities[0],) for name, entities in INSERTS.items()}
CLASSES = {**{'insert-'+k: v for k, v in INSERTS.items()},
           **{'delete-'+k: v for k, v in DELETES.items()}}
RELATIONS = {'Person': 'person', 'Forum': 'forum', 'Post': 'message', 'Comment': 'message',
             'Person_likes_Post': 'likes', 'Person_likes_Comment': 'likes',
             'Person_knows_Person': 'knows', 'Forum_hasMember_Person': 'member'}


def partitions(directory):
    return sorted([*directory.glob('*.csv'),
                   *(p for p in directory.glob('*.csv.gz') if not p.with_suffix('').exists())])


@dataclass(frozen=True)
class Batch:
    day: str
    kind: str
    files: tuple

    def metadata(self, root):
        return dict(day=self.day, update_class=self.kind,
                    files=[str(p.relative_to(root)) for _, p in self.files])

    def rows(self):
        result = {}
        for entity, path in self.files:
            opener = gzip.open if path.suffix == '.gz' else open
            with opener(path, 'rt', newline='', encoding='utf-8') as source:
                result.setdefault(entity, []).extend(csv_rows(source, entity))
        return result


def batches(root, classes=CLASSES, first=None, last=None):
    """One (date, update class) batch per epoch, inserts before deletes each day."""
    unknown = set(classes)-set(CLASSES)
    if unknown:
        raise ValueError(f'unknown update classes: {sorted(unknown)}')
    result = []
    for kind in CLASSES:
        if kind not in classes:
            continue
        phase = 'inserts' if kind.startswith('insert-') else 'deletes'
        days = defaultdict(list)
        for entity in CLASSES[kind]:
            directory = root / phase / 'dynamic' / entity
            if not directory.is_dir():
                raise FileNotFoundError(directory)
            for partition in sorted(directory.glob('batch_id=*')):
                day = partition.name.removeprefix('batch_id=')
                date.fromisoformat(day)
                if (first and day < first) or (last and day > last):
                    continue
                days[day].extend((entity, p) for p in partitions(partition))
        result.extend(Batch(day, kind, tuple(files)) for day, files in days.items() if files)
    order = {kind: i for i, kind in enumerate(CLASSES)}
    return sorted(result, key=lambda b: (b.day, order[b.kind]))


def project_rows(rows):
    tables = {entity: {} for entity in (*data.ENTITIES, *data.STATIC)}
    for entity, records in rows.items():
        for row in records:
            identity = data.key(entity, row)
            if identity in tables[entity]:
                raise ValueError(f'duplicate {entity} {identity}')
            tables[entity][identity] = row
    return data.project(tables)


def load(snapshot):
    """The same projection as data.load, without retaining all raw CSV dictionaries."""
    graph = {name: set() for name in data.SCHEMA}
    for entity in (*data.ENTITIES, *data.STATIC):
        seen, chunk = set(), []
        for row in read_table(snapshot, 'static' if entity in data.STATIC else 'dynamic', entity):
            identity = data.key(entity, row)
            if identity in seen:
                raise ValueError(f'duplicate {entity} {identity}')
            seen.add(identity)
            chunk.append(row)
            if len(chunk) == 1000:
                for name, rows in project_rows({entity: chunk}).items():
                    graph[name].update(rows)
                chunk.clear()
        for name, rows in project_rows({entity: chunk}).items():
            graph[name].update(rows)
    return graph


def apply_delta(graph, changes):
    """Apply exactly the journalled rows that the server receives, with set semantics."""
    validated = {}
    for name, delta in changes.items():
        removed, added = set(map(tuple, delta['removed'])), set(map(tuple, delta['added']))
        # Inspect only the delta: subtracting removals from the whole relation
        # copies millions of unchanged rows even for a one-row update.
        if not removed <= graph[name] or (added & graph[name])-removed:
            raise ValueError(f'inconsistent {name} delta')
        validated[name] = removed, added
    for name, (removed, added) in validated.items():
        graph[name].difference_update(removed)
        graph[name].update(added)


class State:
    def __init__(self, graph):
        self.graph = graph
        self.nodes = {name: {row[0]: row for row in graph[name]} for name in ('person', 'forum', 'message')}
        if any(len(index) != len(graph[name]) for name, index in self.nodes.items()):
            raise ValueError('duplicate node IDs, including overlapping Post/Comment IDs')
        self.children = defaultdict(set)
        for row in graph['message']:
            if row[1] == 1:
                self.children[row[10]].add(row[0])

    def apply(self, kind, rows):
        """Compute one delta, then apply it to the oracle graph and cascade indexes."""
        if kind not in CLASSES or set(rows)-set(CLASSES[kind]):
            raise ValueError(f'rows outside update class {kind}')
        if kind.startswith('insert-'):
            added = project_rows(rows)
            # Entity/edge identity must be fresh even if another attribute differs.
            for name, new in added.items():
                if not new:
                    continue
                if name in self.nodes:
                    duplicate = any(r[0] in self.nodes[name] for r in new)
                else:
                    keys = {r[:2] for r in new}
                    duplicate = any(r[:2] in keys for r in self.graph[name])
                if duplicate:
                    raise ValueError(f'{kind}: existing {name} identity')
            removed = {}
            targets = sum(map(len, rows.values()))
            matched = targets
        else:
            entity = CLASSES[kind][0]
            targets = {data.key(entity, row) for row in rows.get(entity, [])}
            removed, added, matched = self.delete(entity, targets)
            targets = len(targets)
        changes = {name: dict(removed=sorted(removed.get(name, set())-added.get(name, set())),
                              added=sorted(added.get(name, set())-removed.get(name, set())))
                   for name in data.SCHEMA if removed.get(name) != added.get(name)
                   and (removed.get(name) or added.get(name))}
        apply_delta(self.graph, changes)
        for name, delta in changes.items():
            if name in self.nodes:
                for row in delta['removed']:
                    self.nodes[name].pop(row[0])
                for row in delta['added']:
                    self.nodes[name][row[0]] = row
            if name == 'message':
                for side in ('removed', 'added'):
                    for row in delta[side]:
                        if row[1] == 1:
                            if side == 'removed':
                                self.children[row[10]].discard(row[0])
                            else:
                                self.children[row[10]].add(row[0])
        return dict(update_class=kind, explicit_targets=targets, matched_targets=matched,
                    absent_targets=targets-matched, changes=changes)

    def delete(self, entity, targets):
        relation = RELATIONS[entity]
        if entity in data.EDGE_KEYS:
            removed = {r for r in self.graph[relation] if r[:2] in targets}
            return {relation: removed}, {}, len(removed)
        kind = {'Post': 0, 'Comment': 1}.get(entity)
        found = {i for i in targets if i in self.nodes[relation]
                 and (kind is None or self.nodes[relation][i][1] == kind)}
        people = found if entity == 'Person' else set()
        forums = set(found) if entity == 'Forum' else set()
        messages = set(found) if entity in ('Post', 'Comment') else set()
        group_before, group_after = set(), set()
        for row in self.graph['forum'] if people else ():
            if row[3] in people:
                if row[1].startswith(('Album ', 'Wall ')):
                    forums.add(row[0])
                elif row[1].startswith('Group '):
                    group_before.add(row)
                    group_after.add((*row[:3], -1))
                else:
                    raise ValueError(f'cannot determine generated forum kind: {row[1]!r}')
        if people or forums:
            messages.update(r[0] for r in self.graph['message']
                            if r[3] in people or (r[1] == 0 and r[9] in forums))
        pending = deque(messages)
        while pending:
            for child in self.children.get(pending.popleft(), ()):
                if child not in messages:
                    messages.add(child)
                    pending.append(child)
        predicates = {
            'person': (people, lambda r: r[0] in people), 'forum': (forums, lambda r: r[0] in forums),
            'message': (messages, lambda r: r[0] in messages),
            'knows': (people, lambda r: r[0] in people or r[1] in people),
            'member': (forums or people, lambda r: r[0] in forums or r[1] in people),
            'likes': (people or messages, lambda r: r[0] in people or r[1] in messages),
            'mtag': (messages, lambda r: r[0] in messages), 'ftag': (forums, lambda r: r[0] in forums),
            **{n: (people, lambda r: r[0] in people) for n in ('interest', 'study', 'work', 'email', 'language')},
        }
        removed = {name: {r for r in self.graph[name] if predicate(r)}
                   for name, (active,predicate) in predicates.items() if active}
        if group_before:
            removed.setdefault('forum',set()).update(group_before)
        return removed, {'forum': group_after}, len(found)
