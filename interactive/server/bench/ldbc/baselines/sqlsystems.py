"""DuckDB and Umbra baselines for the 41 SNB reads: loading, the SQL in sql/<system>/,
running a binding, and applying an update epoch from update_stream.py's journal.

DuckDB (>= 2.0, Python >= 3.11) loads the BI composite-merged-fk CSVs itself into the
suite's 17 projected relations (snb/data.py SCHEMA). Umbra runs as a server
(see umbra_server.sh) and COPYs CSV files that DuckDB writes from the same
projection (export_projected). Both then build sql/<system>/derived.sql.
"""
import datetime
import decimal
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
DAY = 86400000
NAMES = [f'{f}{i}' for f, n in (('is', 8), ('ic', 15), ('bi', 21)) for i in range(1, n)]
RELATIONS = ('person message forum knows member tag tagclass place org interest mtag ftag likes '
             'study work email language').split()


def sql_dir(system):
    return HERE / 'sql' / system


def queries(system):
    return {q: (sql_dir(system) / f'{q}.sql').read_text() for q in NAMES}


def statements(path):
    text = '\n'.join(line for line in Path(path).read_text().splitlines() if not line.lstrip().startswith('--'))
    return [s.strip() for s in text.split(';') if s.strip()]


def bind(name, sql, params, style):
    """Suite binding -> (SQL, arguments). style 'duckdb' keeps $name; 'umbra' uses %(name)s."""
    p = dict(params)
    if name == 'bi12':
        langs = p['language'] if isinstance(p['language'], list) else [p['language']]
        p.update(languages=langs, enabled=1 if langs else 0)
    if name == 'bi13':
        end = datetime.datetime.fromtimestamp(p['end'] / 1000, datetime.timezone.utc)
        p['endmonth'] = end.year * 12 + end.month
    wanted = set(re.findall(r'\$([a-z0-9]+)', sql))
    if style == 'umbra':
        sql = re.sub(r'\$([a-z0-9]+)', r'%(\1)s', sql)
    return sql, {k: p[k] for k in wanted}


def jsonish(v):
    """Python values in the suite's answer encoding: strings as UTF-8 byte lists, tuples as lists."""
    if isinstance(v, str): return list(v.encode())
    if isinstance(v, decimal.Decimal):
        assert v == int(v), v
        return int(v)
    if isinstance(v, (list, tuple)): return [jsonish(x) for x in v]
    if isinstance(v, dict): return [jsonish(x) for x in v.values()]
    return v


def answer(name, rows):
    """Result rows in rank order -> the suite's ((rid, rank), values) answer, rid 1."""
    out = []
    for i, r in enumerate(rows):
        vals = list(r)
        if name == 'ic1' and len(vals) == 18:   # Umbra returns IC1's nested triples as parallel arrays
            *head, un_n, un_y, un_p, co_n, co_y, co_p = vals
            vals = head + [list(zip(un_n or [], un_y or [], un_p or [])), list(zip(co_n or [], co_y or [], co_p or []))]
        out.append([[1, i], [jsonish(x) for x in vals], 1])
    return out


# ---------------------------------------------------------------- DuckDB

def duckdb_connect(threads=None, path=':memory:'):
    import duckdb
    con = duckdb.connect(path)
    con.execute("SET TimeZone='UTC'")
    if threads:
        con.execute(f'SET threads={threads}')
    return con


HEADERS = {
    'Person': 'creationDate id firstName lastName gender birthday locationIP browserUsed LocationCityId language email',
    'Post': 'creationDate id imageFile locationIP browserUsed language content length CreatorPersonId ContainerForumId LocationCountryId',
    'Comment': 'creationDate id locationIP browserUsed content length CreatorPersonId LocationCountryId ParentPostId ParentCommentId',
    'Forum': 'creationDate id title ModeratorPersonId',
    'Person_knows_Person': 'creationDate Person1Id Person2Id',
    'Forum_hasMember_Person': 'creationDate ForumId PersonId',
    'Forum_hasTag_Tag': 'creationDate ForumId TagId',
    'Person_hasInterest_Tag': 'creationDate PersonId TagId',
    'Person_likes_Post': 'creationDate PersonId PostId',
    'Person_likes_Comment': 'creationDate PersonId CommentId',
    'Person_studyAt_University': 'creationDate PersonId UniversityId classYear',
    'Person_workAt_Company': 'creationDate PersonId CompanyId workFrom',
    'Post_hasTag_Tag': 'creationDate PostId TagId',
    'Comment_hasTag_Tag': 'creationDate CommentId TagId',
    'Place': 'id name url type PartOfPlaceId',
    'Organisation': 'id type name url LocationPlaceId',
    'Tag': 'id name url TypeTagClassId',
    'TagClass': 'id name url SubclassOfTagClassId',
}
STATIC = ('Place', 'Organisation', 'Tag', 'TagClass')
def _csv(snapshot, kind, entity):
    d = Path(snapshot) / kind / entity
    files = sorted(str(p) for p in d.glob('*.csv')) or sorted(str(p) for p in d.glob('*.csv.gz'))
    if not files:
        raise FileNotFoundError(d)
    lst = '[' + ','.join(f"'{f}'" for f in files) + ']'
    return (f"read_csv({lst}, delim='|', header=true, quote='\"', escape='\\', "
            f"all_varchar=true, auto_detect=false, columns=COLS)")


HEADERS = {
    'Person': 'creationDate id firstName lastName gender birthday locationIP browserUsed LocationCityId language email',
    'Post': 'creationDate id imageFile locationIP browserUsed language content length CreatorPersonId ContainerForumId LocationCountryId',
    'Comment': 'creationDate id locationIP browserUsed content length CreatorPersonId LocationCountryId ParentPostId ParentCommentId',
    'Forum': 'creationDate id title ModeratorPersonId',
    'Person_knows_Person': 'creationDate Person1Id Person2Id',
    'Forum_hasMember_Person': 'creationDate ForumId PersonId',
    'Forum_hasTag_Tag': 'creationDate ForumId TagId',
    'Person_hasInterest_Tag': 'creationDate PersonId TagId',
    'Person_likes_Post': 'creationDate PersonId PostId',
    'Person_likes_Comment': 'creationDate PersonId CommentId',
    'Person_studyAt_University': 'creationDate PersonId UniversityId classYear',
    'Person_workAt_Company': 'creationDate PersonId CompanyId workFrom',
    'Post_hasTag_Tag': 'creationDate PostId TagId',
    'Comment_hasTag_Tag': 'creationDate CommentId TagId',
    'Place': 'id name url type PartOfPlaceId',
    'Organisation': 'id type name url LocationPlaceId',
    'Tag': 'id name url TypeTagClassId',
    'TagClass': 'id name url SubclassOfTagClassId',
}
STATIC = ('Place', 'Organisation', 'Tag', 'TagClass')


def src(snapshot, entity):
    cols = '{' + ','.join(f"'{h}':'VARCHAR'" for h in HEADERS[entity].split()) + '}'
    return _csv(snapshot, 'static' if entity in STATIC else 'dynamic', entity).replace('COLS', cols)


# Integer columns: an absent value is -1, as in snb/data.py `number`.
def num(x): return f"coalesce(CAST(nullif({x},'') AS BIGINT), -1)"
def ms(x): return f"epoch_ms(CAST({x} AS TIMESTAMPTZ))"
def s(x): return f"coalesce({x}, '')"
def message_select(kind, alias):
    """Project Post (kind 0) or Comment (kind 1) rows exactly as snb/data.py does."""
    created = ms(f'{alias}.creationDate')
    image = s(f'{alias}.imageFile') if kind == 0 else "''"
    lang = s(f'{alias}.language') if kind == 0 else "''"
    forum = num(f'{alias}.ContainerForumId') if kind == 0 else '-1'
    parent = '-1' if kind == 0 else (f"CASE WHEN coalesce({alias}.ParentPostId,'')<>'' THEN {num(f'{alias}.ParentPostId')} "
                                     f"ELSE {num(f'{alias}.ParentCommentId')} END")
    return (f"SELECT CAST({alias}.id AS BIGINT), {kind}, {created} AS c, {num(f'{alias}.CreatorPersonId')}, "
            f"{num(f'{alias}.LocationCountryId')}, {s(f'{alias}.content')}, {image}, {num(f'{alias}.length')}, {lang}, "
            f"{forum}, {parent}, year(epoch_ms(c)), year(epoch_ms(c))*12+month(epoch_ms(c)), (c // {DAY})*86400000::BIGINT")


def duckdb_load_statements(snapshot):
    S = lambda e: src(snapshot, e)
    yield from statements(sql_dir("duckdb") / "schema.sql")
    yield (f"INSERT INTO person SELECT CAST(id AS BIGINT), {s('firstName')}, {s('lastName')}, {s('gender')}, "
           f"epoch_ms(CAST(birthday AS DATE)), {ms('creationDate')}, {s('locationIP')}, {s('browserUsed')}, "
           f"{num('LocationCityId')}, month(CAST(birthday AS DATE)), day(CAST(birthday AS DATE)), "
           f"year(epoch_ms({ms('creationDate')}))*12+month(epoch_ms({ms('creationDate')})) FROM {S('Person')}")
    for col, rel in (('email', 'email'), ('language', 'language')):
        yield (f"INSERT INTO {rel} SELECT DISTINCT CAST(id AS BIGINT), v FROM "
               f"(SELECT id, unnest(string_split({col}, ';')) AS v FROM {S('Person')} WHERE {col} IS NOT NULL) WHERE v<>''")
    yield f"INSERT INTO message {message_select(0, 'p')} FROM {S('Post')} p"
    yield f"INSERT INTO message {message_select(1, 'm')} FROM {S('Comment')} m"
    yield f"INSERT INTO forum SELECT CAST(id AS BIGINT), {s('title')}, {ms('creationDate')}, {num('ModeratorPersonId')} FROM {S('Forum')}"
    yield (f"INSERT INTO knows SELECT least(a,b), greatest(a,b), c FROM (SELECT CAST(Person1Id AS BIGINT) a, "
           f"CAST(Person2Id AS BIGINT) b, {ms('creationDate')} c FROM {S('Person_knows_Person')})")
    yield f"INSERT INTO member SELECT {num('ForumId')}, {num('PersonId')}, {ms('creationDate')} FROM {S('Forum_hasMember_Person')}"
    yield f"INSERT INTO tag SELECT {num('id')}, {s('name')}, {num('TypeTagClassId')} FROM {S('Tag')}"
    yield f"INSERT INTO tagclass SELECT {num('id')}, {s('name')}, {num('SubclassOfTagClassId')} FROM {S('TagClass')}"
    yield f"INSERT INTO place SELECT {num('id')}, {s('name')}, {s('type')}, {num('PartOfPlaceId')} FROM {S('Place')}"
    yield f"INSERT INTO org SELECT {num('id')}, {s('name')}, {s('type')}, {num('LocationPlaceId')} FROM {S('Organisation')}"
    yield f"INSERT INTO interest SELECT DISTINCT {num('PersonId')}, {num('TagId')} FROM {S('Person_hasInterest_Tag')}"
    yield f"INSERT INTO mtag SELECT DISTINCT {num('PostId')}, {num('TagId')} FROM {S('Post_hasTag_Tag')}"
    yield f"INSERT INTO mtag SELECT DISTINCT {num('CommentId')}, {num('TagId')} FROM {S('Comment_hasTag_Tag')}"
    yield f"INSERT INTO ftag SELECT DISTINCT {num('ForumId')}, {num('TagId')} FROM {S('Forum_hasTag_Tag')}"
    yield f"INSERT INTO likes SELECT {num('PersonId')}, {num('PostId')}, {ms('creationDate')} FROM {S('Person_likes_Post')}"
    yield f"INSERT INTO likes SELECT {num('PersonId')}, {num('CommentId')}, {ms('creationDate')} FROM {S('Person_likes_Comment')}"
    yield f"INSERT INTO study SELECT DISTINCT {num('PersonId')}, {num('UniversityId')}, {num('classYear')} FROM {S('Person_studyAt_University')}"
    yield f"INSERT INTO work SELECT DISTINCT {num('PersonId')}, {num('CompanyId')}, {num('workFrom')} FROM {S('Person_workAt_Company')}"
    yield from statements(sql_dir("duckdb") / "derived.sql")


def duckdb_load(con, snapshot):
    for stmt in duckdb_load_statements(snapshot):
        con.execute(stmt)


def duckdb_run(con, name, sql, params):
    sql, args = bind(name, sql, params, 'duckdb')
    return answer(name, con.execute(sql, args).fetchall())


THREAD_REFRESH = """
INSERT INTO thread
WITH RECURSIVE new AS (SELECT * FROM message WHERE id IN (SELECT unnest($ids))),
t(mid, root, forum, author, language) AS (
  SELECT id, id, forum, creator, language FROM new WHERE kind=0
  UNION ALL
  SELECT n.id, th.root, th.forum, th.author, th.language FROM new n JOIN thread th ON n.parent=th.mid WHERE n.kind=1
  UNION ALL
  SELECT n.id, t.root, t.forum, t.author, t.language FROM t JOIN new n ON n.parent=t.mid AND n.kind=1
) SELECT * FROM t"""


THREAD_REFRESH_UMBRA = """
INSERT INTO thread
WITH RECURSIVE new AS (SELECT * FROM message WHERE id = ANY(%(ids)s)),
t(mid, root, forum, author, language) AS (
  (SELECT id, id, forum, creator, language FROM new WHERE kind=0
   UNION ALL
   SELECT n.id, th.root, th.forum, th.author, th.language FROM new n JOIN thread th ON n.parent=th.mid WHERE n.kind=1)
  UNION ALL
  SELECT n.id, t.root, t.forum, t.author, t.language FROM t JOIN new n ON n.parent=t.mid AND n.kind=1
) SELECT * FROM t"""


def duckdb_apply(con, changes, columns, types):
    """One journal epoch in one transaction: deletions, insertions, then `thread` upkeep."""
    import pyarrow as pa
    def table(rel, rows):
        return pa.table({c: pa.array(list(v), type=pa.int64() if t == 'BIGINT' else pa.string())
                         for c, v, t in zip(columns[rel], zip(*rows), types[rel])})
    staged = [(rel, side, table(rel, d[side])) for rel, d in changes.items() for side in ('removed', 'added') if d[side]]
    msg = changes.get('message', {})
    con.execute('BEGIN')
    for rel, side, t in staged:
        con.register('delta', t)
        if side == 'removed':
            on = ' AND '.join(f't.{c} = delta.{c}' for c in columns[rel])
            con.execute(f'DELETE FROM {rel} t USING delta WHERE {on}')
        else:
            con.execute(f'INSERT INTO {rel} SELECT * FROM delta')
        con.unregister('delta')
    removed = [r[0] for r in msg.get('removed', [])]
    added = [r[0] for r in msg.get('added', [])]
    if removed:
        con.execute('DELETE FROM thread WHERE mid IN (SELECT unnest($ids))', dict(ids=removed))
    if added:
        con.execute(THREAD_REFRESH, dict(ids=added))
    con.execute('COMMIT')


def export_projected(con, out_dir):
    """Write the 17 projected relations as CSV for Umbra's COPY (strings always quoted)."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for r in RELATIONS:
        con.execute(f"COPY {r} TO '{Path(out_dir) / (r + '.csv')}' (HEADER, DELIMITER '|', FORCE_QUOTE *)")


# ---------------------------------------------------------------- Umbra

def umbra_connect(port=5433):
    import psycopg
    return psycopg.connect(f'host=localhost port={port} user=postgres password=postgres dbname=postgres',
                           autocommit=True, prepare_threshold=None)


def umbra_load(con, csv_dir):
    """`csv_dir` is the container-side directory holding <relation>.csv files."""
    for v in ('weighted', 'interactions', 'replies', 'tagged', 'residents', 'edges'):
        con.execute(f'DROP VIEW IF EXISTS {v}')
    for t in ('thread', 'ancestry', *RELATIONS):
        con.execute(f'DROP TABLE IF EXISTS {t}')
    for stmt in statements(sql_dir('umbra') / 'schema.sql'):
        con.execute(stmt)
    for r in RELATIONS:
        con.execute(f"COPY {r} FROM '{csv_dir}/{r}.csv' (FORMAT csv, HEADER true, DELIMITER '|')")
    for stmt in statements(sql_dir('umbra') / 'derived.sql'):
        con.execute(stmt)


def umbra_run(con, name, sql, params):
    sql, args = bind(name, sql, params, 'umbra')
    return answer(name, con.execute(sql, args).fetchall())


def umbra_apply(con, changes, columns):
    msg = changes.get('message', {})
    con.execute('BEGIN')
    cur = con.cursor()
    for rel, d in changes.items():
        if d['removed']:
            cur.execute(f'CREATE TEMP TABLE IF NOT EXISTS d_{rel} AS SELECT * FROM {rel} WHERE false')
            with cur.copy(f'COPY d_{rel} FROM STDIN') as cp:
                for row in d['removed']: cp.write_row(row)
            on = ' AND '.join(f't.{c} = d.{c}' for c in columns[rel])
            cur.execute(f'DELETE FROM {rel} t USING d_{rel} d WHERE {on}')
            cur.execute(f'DELETE FROM d_{rel}')
    for rel, d in changes.items():
        if d['added']:
            with cur.copy(f'COPY {rel} FROM STDIN') as cp:
                for row in d['added']: cp.write_row(row)
    removed = [r[0] for r in msg.get('removed', [])]
    added = [r[0] for r in msg.get('added', [])]
    if removed:
        cur.execute('DELETE FROM thread WHERE mid = ANY(%(ids)s)', dict(ids=removed))
    if added:
        cur.execute(THREAD_REFRESH_UMBRA, dict(ids=added))
    con.execute('COMMIT')
