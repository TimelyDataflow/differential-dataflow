-- IC9. The 20 most recent messages by friends and friends-of-friends before a date.
-- Parameters: $maxdate, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2) WHERE node<>$pid GROUP BY node)
SELECT f.node, p.first, p.last, m.id, CASE WHEN length(m.content)>0 THEN m.content ELSE m.image END, m.created
FROM f JOIN message m ON m.creator=f.node JOIN person p ON f.node=p.id
WHERE m.created < $maxdate ORDER BY m.created DESC, m.id LIMIT 20
