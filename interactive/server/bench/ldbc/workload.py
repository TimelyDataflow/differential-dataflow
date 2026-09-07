"""Narrow SNB snapshot adapter and independent, untimed reference queries."""
from collections import Counter, defaultdict
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
TABLES = ('person', 'knows', 'message', 'tag', 'message_tag', 'likes', 'place')
QUERIES = ('is3', 'ic6', 'bi5', 'bi11')


def millis(text):
    return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp() * 1000)


def load(snapshot):
    if snapshot is None:
        raw = json.loads((HERE / 'tiny.json').read_text())
        return {name: {tuple(row) for row in raw[name]} for name in TABLES}
    # Accept the initial_snapshot directory, not an ambiguous graph/history root.
    result = {name: set() for name in TABLES}
    entities = {
        'Person': 'person', 'Person_knows_Person': 'knows',
        'Post': 'message', 'Comment': 'message', 'Tag': 'tag',
        'Post_hasTag_Tag': 'message_tag', 'Comment_hasTag_Tag': 'message_tag',
        'Person_likes_Post': 'likes', 'Person_likes_Comment': 'likes', 'Place': 'place',
    }
    for entity, table in entities.items():
        category = 'static' if entity in ('Tag', 'Place') else 'dynamic'
        paths = sorted((snapshot / category / entity).glob('*.csv'))
        if not paths:
            raise ValueError(f'missing CSV partition(s): {category}/{entity}')
        for path in paths:
            with path.open(newline='', encoding='utf-8') as source:
                for r in csv.DictReader(source, delimiter='|', quoting=csv.QUOTE_NONE):
                    if table == 'person':
                        row = (int(r['id']), r['firstName'], r['lastName'], int(r['LocationCityId']))
                    elif table == 'knows':
                        a, b = sorted((int(r['Person1Id']), int(r['Person2Id'])))
                        row = (a, b, millis(r['creationDate']))
                    elif table == 'message':
                        parent = r.get('ParentPostId') or r.get('ParentCommentId') or '-1'
                        row = (int(r['id']), int(entity == 'Comment'), int(r['CreatorPersonId']), int(parent))
                    elif table == 'tag':
                        row = (int(r['id']), r['name'])
                    elif table == 'place':
                        row = (int(r['id']), r['name'], int(r.get('PartOfPlaceId') or '-1'))
                    else:
                        mid = int(r['PostId'] if 'PostId' in r else r['CommentId'])
                        row = (mid, int(r['TagId'])) if table == 'message_tag' else (int(r['PersonId']), mid)
                    if any(isinstance(v, str) and not v for v in row):
                        raise ValueError(f'{path}: empty strings require typed server inputs; not padded here')
                    result[table].add(row)
    return result


def fingerprint(graph):
    digest = hashlib.sha256()
    for name in TABLES:
        digest.update(name.encode())
        for row in sorted(graph[name]):
            digest.update((json.dumps(row, ensure_ascii=True) + '\n').encode())
    return digest.hexdigest()


class Reference:
    """Direct graph traversal/counting, deliberately independent of the DDP plans."""
    def __init__(self, graph):
        self.people = {p[0]: p[1:] for p in graph['person']}
        self.messages = {m[0]: m[1:] for m in graph['message']}
        self.places = {p[0]: p[1:] for p in graph['place']}
        self.tag_names = {t[0]: t[1] for t in graph['tag']}
        self.edges = defaultdict(dict)
        for a, b, created in graph['knows']:
            if a == b or a not in self.people or b not in self.people:
                raise ValueError('friendship must connect two distinct existing people')
            if b in self.edges[a]:
                raise ValueError('multiple creation dates for a friendship')
            self.edges[a][b] = self.edges[b][a] = created
        self.tags = defaultdict(set)
        for mid, tag in graph['message_tag']:
            if mid not in self.messages:
                raise ValueError(f'tag on missing message {mid}')
            self.tags[mid].add(self.tag_names[tag])
        self.liked = Counter(mid for _, mid in graph['likes'])
        self.replied = Counter(parent for kind, _, parent in self.messages.values() if kind == 1)
        self.posts = defaultdict(list)
        for mid, (kind, author, _) in self.messages.items():
            if kind == 0:
                self.posts[author].append(mid)

    def country(self, pid):
        city = self.people[pid][2]
        return self.places[self.places[city][1]][0]

    def answer(self, name, params):
        if name == 'is3':
            pid, = params
            rows = [(friend, *self.people[friend][:2], date) for friend, date in self.edges[pid].items()]
            return sorted(rows, key=lambda r: (-r[3], r[0]))
        if name == 'ic6':
            pid, tag = params
            authors = set(self.edges[pid])
            for friend in self.edges[pid]:
                authors.update(self.edges[friend])
            authors.discard(pid)
            counts = Counter()
            for author in authors:
                for mid in self.posts[author]:
                    if tag in self.tags[mid]:
                        counts.update(self.tags[mid] - {tag})
            return sorted(counts.items(), key=lambda r: (-r[1], r[0].encode()))[:10]
        if name == 'bi5':
            tag, = params
            totals = defaultdict(lambda: [0, 0, 0])
            for mid, names in self.tags.items():
                if tag in names:
                    total = totals[self.messages[mid][1]]
                    total[0] += self.replied[mid]
                    total[1] += self.liked[mid]
                    total[2] += 1
            rows = [(pid, replies, likes, messages, messages + 2*replies + 10*likes)
                    for pid, (replies, likes, messages) in totals.items()]
            return sorted(rows, key=lambda r: (-r[4], r[0]))[:100]
        if name == 'bi11':
            country, start, end = params
            residents = {p for p in self.people if self.country(p) == country}
            neighbors = {p: {f for f, d in self.edges[p].items()
                             if f in residents and start <= d <= end} for p in residents}
            count = sum(1 for a in residents for b in neighbors[a] if a < b
                        for c in neighbors[a] & neighbors[b] if b < c)
            return [(count,)]
        raise ValueError(name)


def parameters(reference):
    # Stratify people by degree. Choose tags present on reachable multi-tag
    # posts when possible, so a small snapshot is not mostly empty IC6 lookups.
    # This is data-derived parameter generation, not the official distribution.
    people = sorted(reference.people, key=lambda p: (len(reference.edges[p]), p))
    if not people or not reference.tags:
        raise ValueError('workload needs people and tagged messages')
    popular = Counter(t for tags in reference.tags.values() for t in tags)
    tags = sorted(popular, key=lambda t: (-popular[t], t))[:8]
    bank = [people[i * (len(people)-1) // 7] for i in range(8)]
    absent = max(people) + 1
    tagged_requests = []
    for i, pid in enumerate(bank):
        authors = set(reference.edges[pid])
        for friend in reference.edges[pid]:
            authors.update(reference.edges[friend])
        authors.discard(pid)
        nearby = Counter(tag for author in authors for mid in reference.posts[author]
                         if len(reference.tags[mid]) > 1 for tag in reference.tags[mid])
        choices = sorted(nearby, key=lambda tag: (-nearby[tag], tag))[:8] or tags
        tagged_requests.append((pid, choices[i % len(choices)]))
    requests = {'is3': [(p,) for p in bank] + [(absent,)],
                'ic6': tagged_requests + [(absent, tags[0])]}
    countries = Counter(reference.country(p) for p in people)
    country = min(countries, key=lambda c: (-countries[c], c))
    dates = [d for neighbors in reference.edges.values() for d in neighbors.values()]
    standing = {'bi5': (tags[0],), 'bi11': (country, min(dates, default=0), max(dates, default=0))}
    return requests, standing


def changes(graph, limit):
    # Bounded churn: detach selected friendship, tag and like edges; then restore
    # the exact same rows. No node deletion/cascades or synthetic timestamps.
    return {name: sorted(graph[name])[:limit] for name in ('knows', 'message_tag', 'likes')}


def wire_value(value):
    if isinstance(value, str):
        return list(value.encode())
    if isinstance(value, (tuple, list)):
        return [wire_value(v) for v in value]
    return value


def expected(reference, name, requests):
    return [[[rid, rank], wire_value(row), 1]
            for rid, *params in requests
            for rank, row in enumerate(reference.answer(name, params))]
