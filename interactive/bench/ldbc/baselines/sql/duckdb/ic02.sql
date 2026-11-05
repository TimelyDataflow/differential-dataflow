-- IC2. The 20 most recent messages written by a person's friends before a date.
-- Parameters: $maxdate, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1) WHERE node<>$pid GROUP BY node)
SELECT f.node, p.first, p.last, m.id, CASE WHEN length(m.content)>0 THEN m.content ELSE m.image END, m.created
FROM f JOIN message m ON m.creator=f.node JOIN person p ON f.node=p.id
WHERE m.created < $maxdate ORDER BY m.created DESC, m.id LIMIT 20
