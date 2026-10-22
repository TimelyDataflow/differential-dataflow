-- IC6. Tags that co-occur with a given tag on posts by friends and friends-of-friends; top 10.
-- Parameters: $pid, $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2) u WHERE node<>$pid GROUP BY node),
ms AS (SELECT DISTINCT m.id FROM f JOIN message m ON m.creator=f.node AND m.kind=0 JOIN tagged t ON t.message=m.id
       WHERE t.tag_name=$tagname),
g AS (SELECT o.tag_name, count(*) AS n FROM ms JOIN tagged o ON o.message=ms.id WHERE o.tag_name<>$tagname GROUP BY o.tag_name)
SELECT tag_name, n FROM g ORDER BY n DESC, tag_name LIMIT 10
