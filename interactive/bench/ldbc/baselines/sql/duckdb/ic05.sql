-- IC5. Forums that friends and friends-of-friends joined after a date, ranked by how many posts those people made there.
-- Parameters: $mindate, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2) WHERE node<>$pid GROUP BY node),
members AS (SELECT DISTINCT f.node, mb.forum FROM f JOIN member mb ON mb.person=f.node WHERE mb.created > $mindate),
posts AS (SELECT mb.forum, count(*) AS n FROM members mb JOIN message m ON m.creator=mb.node AND m.forum=mb.forum AND m.kind=0 GROUP BY mb.forum),
r AS (SELECT DISTINCT forum FROM members)
SELECT fo.title, coalesce(p.n, 0)
FROM r LEFT JOIN posts p ON p.forum=r.forum JOIN forum fo ON fo.id=r.forum ORDER BY coalesce(p.n,0) DESC, r.forum LIMIT 20
