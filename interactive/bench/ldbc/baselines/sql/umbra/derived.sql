-- Built after loading. `thread` (each message's root post, forum, author, language) and
-- `ancestry` (tag-class hierarchy) are tables, as in LDBC's own SQL implementations;
-- `thread` is maintained on every update batch. The rest are views, recomputed per query.

CREATE TABLE thread AS
WITH RECURSIVE t(mid, root, forum, author, language) AS (
  SELECT id, id, forum, creator, language FROM message WHERE kind=0
  UNION ALL
  SELECT m.id, t.root, t.forum, t.author, t.language FROM t JOIN message m ON m.parent=t.mid AND m.kind=1
) SELECT * FROM t;

CREATE TABLE ancestry AS
WITH RECURSIVE a(descendant, ancestor) AS (
  SELECT id, id FROM tagclass
  UNION
  SELECT a.descendant, tc.parent FROM a JOIN tagclass tc ON tc.id=a.ancestor WHERE tc.parent>=0
) SELECT * FROM a;

CREATE VIEW edges AS
SELECT src, dst, created FROM (SELECT src, dst, created FROM knows UNION SELECT dst, src, created FROM knows) e
WHERE src IN (SELECT id FROM person) AND dst IN (SELECT id FROM person);

CREATE VIEW residents AS
SELECT p.*, ci.name AS city_name, ci.parent AS city_parent, co.id AS country_id, co.name AS country_name
FROM person p JOIN place ci ON p.city=ci.id JOIN place co ON ci.parent=co.id;

CREATE VIEW tagged AS SELECT mt.message, mt.tag, t.name AS tag_name, t.class AS tag_class FROM mtag mt JOIN tag t ON mt.tag=t.id;

CREATE VIEW replies AS
SELECT m.*, p.kind AS parent_kind, p.creator AS parent_creator FROM message m JOIN message p ON m.parent=p.id WHERE m.kind=1;

CREATE VIEW interactions AS
WITH a AS (SELECT id AS comment, creator AS src, parent_creator AS dst, CASE WHEN parent_kind=0 THEN 2 ELSE 1 END AS score2
           FROM replies WHERE creator<>parent_creator)
SELECT * FROM a UNION ALL SELECT comment, dst, src, score2 FROM a;

CREATE VIEW weighted AS
WITH n AS (SELECT i.src, i.dst, count(*) AS n FROM interactions i WHERE (i.src, i.dst) IN (SELECT src, dst FROM edges) GROUP BY i.src, i.dst)
SELECT n.src, n.dst, 1 + (SELECT count(*) FROM generate_series(1,39) g(k) WHERE k*(k-1)+1 > n.n) AS weight FROM n;
