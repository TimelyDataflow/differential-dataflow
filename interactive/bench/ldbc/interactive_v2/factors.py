#!/usr/bin/env python3
"""Datagen's raw tables and paramgen factor tables, rebuilt from a BI CSV export.

    factors.py <bi composite-merged-fk dir> <out dir>

The Interactive v2 paramgen (ldbc_snb_interactive_v2_driver/paramgen) reads
factor tables that Datagen writes with --generate-factors, plus raw Person,
Person_knows_Person, Comment and Post parquet. The published copies are not
reachable from here, so this rebuilds them from the BI CSV, which holds the same
simulated network: the raw graph is the initial snapshot plus every insert, with
explicit deletion dates from the delete files.

Definitions follow ldbc_snb_datagen_spark FactorGenerationStage.scala, one SQL
statement per factor. Two cannot match Datagen exactly:
- people4Hops samples people in China with Spark's seeded sample(); this takes
  the same number of people by md5 order instead.
- The raw graph lacks entities deleted before the BI snapshot was cut.
Output: <out>/raw/composite-merged-fk/dynamic/<Entity>/part-0.parquet (dates as
epoch ms, as paramgen's path curation compares them) and
<out>/factors/<name>/part-0.parquet (dates as timestamps).
"""
from collections import deque
import math
from pathlib import Path
import sys

import duckdb

FAR = 253402214400000  # 9999-12-31: Datagen's deletion date for entities never deleted


def main(src, out):
    src, out = Path(src), Path(out)
    db = duckdb.connect()

    def csv(phase, entity, columns):
        """One entity's partitions as a relation, reading only `columns` (name: type)."""
        globs = [str(src / 'initial_snapshot' / phase / entity / '*.csv*')]
        if phase == 'dynamic' and (src / 'inserts' / 'dynamic' / entity).is_dir():
            globs.append(str(src / 'inserts' / 'dynamic' / entity / 'batch_id=*' / '*.csv*'))
        cols = ', '.join(f"'{k}': '{v}'" for k, v in columns.items())
        return (f"read_csv({globs}, delim='|', header=true, quote='\"', escape='\\', "
                f"columns={{{cols}}}, hive_partitioning=false, timestampformat='%Y-%m-%dT%H:%M:%S.%g%z')")

    def deletes(entity, columns):
        cols = ', '.join(f"'{k}': '{v}'" for k, v in columns.items())
        path = str(src / 'deletes' / 'dynamic' / entity / 'batch_id=*' / '*.csv*')
        return (f"read_csv('{path}', delim='|', header=true, columns={{{cols}}}, hive_partitioning=false, "
                f"timestampformat='%Y-%m-%dT%H:%M:%S.%g%z')")

    ms = lambda c: f"epoch_ms({c})"
    db.execute(f"""
        CREATE TABLE person_del AS SELECT id, min({ms('deletionDate')}) AS d
          FROM {deletes('Person', {'deletionDate': 'TIMESTAMPTZ', 'id': 'BIGINT'})} GROUP BY id;
        CREATE TABLE knows_del AS SELECT least(Person1Id, Person2Id) AS a, greatest(Person1Id, Person2Id) AS b,
               min({ms('deletionDate')}) AS d
          FROM {deletes('Person_knows_Person', {'deletionDate': 'TIMESTAMPTZ', 'Person1Id': 'BIGINT', 'Person2Id': 'BIGINT'})}
         GROUP BY ALL;
        CREATE TABLE Person AS
        SELECT p.id, {ms('p.creationDate')} AS creationDate, coalesce(d.d, {FAR}) AS deletionDate,
               d.d IS NOT NULL AS explicitlyDeleted, p.firstName, p.LocationCityId
          FROM {csv('dynamic', 'Person', {'creationDate': 'TIMESTAMPTZ', 'id': 'BIGINT', 'firstName': 'VARCHAR',
                    'lastName': 'VARCHAR', 'gender': 'VARCHAR', 'birthday': 'VARCHAR', 'locationIP': 'VARCHAR',
                    'browserUsed': 'VARCHAR', 'LocationCityId': 'BIGINT', 'language': 'VARCHAR', 'email': 'VARCHAR'})} p
          LEFT JOIN person_del d ON d.id = p.id;
        CREATE TABLE Person_knows_Person AS
        SELECT {ms('k.creationDate')} AS creationDate,
               least(coalesce(kd.d, {FAR}), p1.deletionDate, p2.deletionDate) AS deletionDate,
               kd.d IS NOT NULL AS explicitlyDeleted, k.Person1Id, k.Person2Id
          FROM {csv('dynamic', 'Person_knows_Person', {'creationDate': 'TIMESTAMPTZ', 'Person1Id': 'BIGINT', 'Person2Id': 'BIGINT'})} k
          JOIN Person p1 ON p1.id = k.Person1Id JOIN Person p2 ON p2.id = k.Person2Id
          LEFT JOIN knows_del kd ON kd.a = least(k.Person1Id, k.Person2Id) AND kd.b = greatest(k.Person1Id, k.Person2Id);
        CREATE TABLE Post AS SELECT {ms('creationDate')} AS creationDate, id, CreatorPersonId
          FROM {csv('dynamic', 'Post', {'creationDate': 'TIMESTAMPTZ', 'id': 'BIGINT', 'imageFile': 'VARCHAR',
                    'locationIP': 'VARCHAR', 'browserUsed': 'VARCHAR', 'language': 'VARCHAR', 'content': 'VARCHAR',
                    'length': 'BIGINT', 'CreatorPersonId': 'BIGINT', 'ContainerForumId': 'BIGINT', 'LocationCountryId': 'BIGINT'})};
        CREATE TABLE Comment AS SELECT {ms('creationDate')} AS creationDate, id, CreatorPersonId
          FROM {csv('dynamic', 'Comment', {'creationDate': 'TIMESTAMPTZ', 'id': 'BIGINT', 'locationIP': 'VARCHAR',
                    'browserUsed': 'VARCHAR', 'content': 'VARCHAR', 'length': 'BIGINT', 'CreatorPersonId': 'BIGINT',
                    'LocationCountryId': 'BIGINT', 'ParentPostId': 'BIGINT', 'ParentCommentId': 'BIGINT'})};
        CREATE TABLE hasMember AS SELECT ForumId, PersonId
          FROM {csv('dynamic', 'Forum_hasMember_Person', {'creationDate': 'TIMESTAMPTZ', 'ForumId': 'BIGINT', 'PersonId': 'BIGINT'})};
        CREATE TABLE interest AS SELECT personId AS PersonId, interestId AS TagId
          FROM {csv('dynamic', 'Person_hasInterest_Tag', {'creationDate': 'TIMESTAMPTZ', 'personId': 'BIGINT', 'interestId': 'BIGINT'})};
        CREATE TABLE Place AS SELECT * FROM {csv('static', 'Place', {'id': 'BIGINT', 'name': 'VARCHAR', 'url': 'VARCHAR', 'type': 'VARCHAR', 'PartOfPlaceId': 'BIGINT'})};
        CREATE TABLE Tag AS SELECT * FROM {csv('static', 'Tag', {'id': 'BIGINT', 'name': 'VARCHAR', 'url': 'VARCHAR', 'TypeTagClassId': 'BIGINT'})};
        CREATE TABLE TagClass AS SELECT * FROM {csv('static', 'TagClass', {'id': 'BIGINT', 'name': 'VARCHAR', 'url': 'VARCHAR', 'SubclassOfTagClassId': 'BIGINT'})};
        CREATE TABLE knows AS SELECT Person1Id, Person2Id FROM Person_knows_Person
                    UNION ALL SELECT Person2Id, Person1Id FROM Person_knows_Person;
        CREATE TABLE persondates AS SELECT id, make_timestamp(creationDate * 1000) AS creationDate,
               make_timestamp(deletionDate * 1000) AS deletionDate FROM Person;
    """)

    for entity in ('Person', 'Person_knows_Person', 'Post', 'Comment'):
        path = out / 'raw' / 'composite-merged-fk' / 'dynamic' / entity
        path.mkdir(parents=True, exist_ok=True)
        db.execute(f"COPY {entity} TO '{path / 'part-0.parquet'}' (FORMAT parquet)")

    # A walk-sum over undirected knows: sum of the neighbours' `value`, 0 without neighbours.
    def walk(base, value):
        return f"""SELECT b.Person1Id, coalesce(sum(n.{value}), 0) AS s
                     FROM {base} b LEFT JOIN knows k ON k.Person1Id = b.Person1Id
                     LEFT JOIN {base} n ON n.Person1Id = k.Person2Id GROUP BY b.Person1Id"""

    factors = {
        'personNumFriends': """
            SELECT p.id, p.creationDate, p.deletionDate, count(k.Person2Id) AS frequency
              FROM persondates p LEFT JOIN knows k ON k.Person1Id = p.id GROUP BY ALL""",
        'personNumFriendsOfFriendsOfFriends': f"""
            WITH f1 AS (SELECT p.id AS Person1Id, p.creationDate, p.deletionDate, count(k.Person2Id) AS numFriends
                          FROM persondates p LEFT JOIN knows k ON k.Person1Id = p.id GROUP BY ALL),
                 f2 AS (SELECT f1.*, w.s AS numFriendsOfFriends FROM f1 JOIN ({walk('f1', 'numFriends')}) w USING (Person1Id)),
                 f3 AS (SELECT f2.*, w.s AS numFriendsOfFriendsOfFriends FROM f2 JOIN ({walk('f2', 'numFriendsOfFriends')}) w USING (Person1Id))
            SELECT * FROM f3""",
        'personNumFriendOfFriendPosts': f"""
            WITH f1 AS (SELECT p.id AS Person1Id, p.creationDate, p.deletionDate, count(m.id) AS numDirectPosts
                          FROM persondates p LEFT JOIN Post m ON m.CreatorPersonId = p.id GROUP BY ALL),
                 f2 AS (SELECT f1.*, w.s AS numFriendPosts FROM f1 JOIN ({walk('f1', 'numDirectPosts')}) w USING (Person1Id)),
                 f3 AS (SELECT f2.*, w.s AS numFriendOfFriendPosts FROM f2 JOIN ({walk('f2', 'numFriendPosts')}) w USING (Person1Id))
            SELECT * FROM f3""",
        'personNumFriendOfFriendForums': f"""
            WITH f1 AS (SELECT p.id AS Person1Id, p.creationDate, p.deletionDate, count(h.ForumId) AS numDirectForums
                          FROM persondates p LEFT JOIN hasMember h ON h.PersonId = p.id GROUP BY ALL),
                 f2 AS (SELECT f1.*, w.s AS numFriendForums FROM f1 JOIN ({walk('f1', 'numDirectForums')}) w USING (Person1Id)),
                 f3 AS (SELECT f2.*, w.s AS numFriendOfFriendForums FROM f2 JOIN ({walk('f2', 'numFriendForums')}) w USING (Person1Id))
            SELECT * FROM f3""",
        'personFirstNames': "SELECT firstName, count(id) AS frequency FROM Person GROUP BY ALL",
        'creationDayNumMessages': """
            SELECT date_trunc('day', make_timestamp(creationDate * 1000)) AS creationDay, count(*) AS frequency
              FROM (SELECT creationDate FROM Post UNION ALL SELECT creationDate FROM Comment) GROUP BY ALL""",
        'countryNumPersons': """
            SELECT co.id, co.name, count(p.id) AS frequency FROM Person p
              JOIN Place ci ON ci.id = p.LocationCityId AND ci.type = 'City'
              JOIN Place co ON co.id = ci.PartOfPlaceId AND co.type = 'Country' GROUP BY ALL""",
        'countryPairsNumFriends': """
            SELECT co1.id AS country1Id, co2.id AS country2Id, co1.name AS country1Name, co2.name AS country2Name,
                   count(*) AS frequency
              FROM knows k JOIN Person p1 ON p1.id = k.Person1Id JOIN Person p2 ON p2.id = k.Person2Id
              JOIN Place c1 ON c1.id = p1.LocationCityId AND c1.type = 'City'
              JOIN Place c2 ON c2.id = p2.LocationCityId AND c2.type = 'City'
              JOIN Place co1 ON co1.id = c1.PartOfPlaceId AND co1.type = 'Country'
              JOIN Place co2 ON co2.id = c2.PartOfPlaceId AND co2.type = 'Country'
             WHERE co1.id < co2.id GROUP BY ALL""",
        'tagNumPersons': "SELECT t.id, t.name, count(i.PersonId) AS frequency FROM interest i JOIN Tag t ON t.id = i.TagId GROUP BY ALL",
        'tagClassNumTags': "SELECT c.id, c.name, count(t.id) AS frequency FROM Tag t JOIN TagClass c ON c.id = t.TypeTagClassId GROUP BY ALL",
    }
    for name, sql in factors.items():
        write(db, out, name, sql)

    # Connected components of the all-time knows graph (GraphX labels a component by its least id).
    people = [r[0] for r in db.execute("SELECT id FROM Person").fetchall()]
    parent = {p: p for p in people}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for a, b in db.execute("SELECT Person1Id, Person2Id FROM Person_knows_Person").fetchall():
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    comp = {p: find(p) for p in people}
    db.execute("CREATE TABLE cc (PersonId BIGINT, Component BIGINT)")
    db.executemany("INSERT INTO cc VALUES (?, ?)", list(comp.items()))
    write(db, out, 'personKnowsPersonConnected', """
        SELECT PersonId, Component, count(*) OVER (PARTITION BY Component) AS count FROM cc""")

    # people4Hops: pairs at minimum distance exactly 4 from a sample of people in China.
    china = [r[0] for r in db.execute("""
        SELECT p.id FROM Person p JOIN Place c ON c.id = p.LocationCityId WHERE c.PartOfPlaceId = 1
         ORDER BY md5(p.id::VARCHAR)""").fetchall()]
    n = len(china)
    size = min(n, max(100.0, n / (1 + math.exp(min(n * 1e-3, 700))) * 2))
    adjacency = {}
    for a, b in db.execute("SELECT Person1Id, Person2Id FROM knows").fetchall():
        adjacency.setdefault(a, []).append(b)
    pairs = []
    for source in china[:int(size)]:
        depth, frontier = {source: 0}, deque([source])
        while frontier:
            x = frontier.popleft()
            if depth[x] == 4:
                continue
            for y in adjacency.get(x, ()):
                if y not in depth:
                    depth[y] = depth[x] + 1
                    frontier.append(y)
        pairs.extend((source, y) for y, d in depth.items() if d == 4)
    db.execute("CREATE TABLE p4 (Person1Id BIGINT, Person2Id BIGINT)")
    db.executemany("INSERT INTO p4 VALUES (?, ?)", pairs)
    write(db, out, 'people4Hops', """
        SELECT p4.Person1Id, p4.Person2Id, a.creationDate AS Person1CreationDate, a.deletionDate AS Person1DeletionDate,
               b.creationDate AS Person2CreationDate, b.deletionDate AS Person2DeletionDate
          FROM p4 JOIN persondates a ON a.id = p4.Person1Id JOIN persondates b ON b.id = p4.Person2Id
         ORDER BY md5(concat(p4.Person1Id, '-', p4.Person2Id)) LIMIT 10000""")
    print(f'{len(people)} people, {len(set(comp.values()))} components, {int(size)} of {n} in China, {len(pairs)} 4-hop pairs')


def write(db, out, name, sql):
    path = out / 'factors' / name
    path.mkdir(parents=True, exist_ok=True)
    db.execute(f"COPY ({sql}) TO '{path / 'part-0.parquet'}' (FORMAT parquet)")
    rows = db.execute(f"SELECT count(*) FROM read_parquet('{path / 'part-0.parquet'}')").fetchone()[0]
    print(f'{name}: {rows} rows', flush=True)


if __name__ == '__main__':
    main(*sys.argv[1:])
