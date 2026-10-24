-- BI5. Most active posters on a tag, scored by messages, replies and likes.
-- Parameters: $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH ms AS (SELECT DISTINCT m.id, m.creator FROM tagged t JOIN message m ON t.message=m.id WHERE t.tag_name=$tagname),
counts AS (SELECT creator, count(*) AS n FROM ms GROUP BY creator),
lk AS (SELECT ms.creator, count(*) AS n FROM ms JOIN likes l ON l.message=ms.id GROUP BY ms.creator),
rp AS (SELECT ms.creator, count(*) AS n FROM ms JOIN message r ON r.parent=ms.id AND r.kind=1 GROUP BY ms.creator),
s AS (SELECT c.creator, coalesce(rp.n,0) AS replies, coalesce(lk.n,0) AS likes, c.n AS messages,
      c.n + 2*coalesce(rp.n,0) + 10*coalesce(lk.n,0) AS score
  FROM counts c LEFT JOIN lk ON lk.creator=c.creator LEFT JOIN rp ON rp.creator=c.creator)
SELECT creator, replies, likes, messages, score FROM s ORDER BY score DESC, creator LIMIT 100
