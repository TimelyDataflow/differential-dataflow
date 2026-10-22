-- IC10. Friend recommendation: friends-of-friends born near a given month, scored by how many of their posts match the person's interests.
-- Parameters: $month, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2) WHERE node<>$pid GROUP BY node),
cand AS (SELECT f.node, p.first, p.last, p.gender, p.city_name FROM f JOIN residents p ON f.node=p.id
  WHERE f.distance=2 AND ((p.birthmonth=$month AND p.birthdaynum>=21)
    OR (p.birthmonth=(CASE WHEN $month=12 THEN 1 ELSE $month+1 END) AND p.birthdaynum<22))),
posts AS (SELECT c.node, m.id FROM cand c JOIN message m ON m.creator=c.node AND m.kind=0),
common AS (SELECT DISTINCT p.node, p.id FROM posts p JOIN mtag t ON t.message=p.id
           WHERE t.tag IN (SELECT tag FROM interest WHERE person=$pid)),
sc AS (SELECT node, sum(s) AS score FROM (SELECT node, 1 AS s FROM common UNION ALL
         SELECT node, -1 FROM posts p WHERE NOT EXISTS (SELECT 1 FROM common c WHERE c.node=p.node AND c.id=p.id)) GROUP BY node)
SELECT c.node, c.first, c.last, coalesce(sc.score, 0), c.gender, c.city_name
FROM cand c LEFT JOIN sc ON sc.node=c.node ORDER BY coalesce(sc.score,0) DESC, c.node LIMIT 10
