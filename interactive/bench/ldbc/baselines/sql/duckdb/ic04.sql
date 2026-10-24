-- IC4. Tags that first appear on friends' posts inside a window (never before it); top 10 by post count.
-- Parameters: $days, $pid, $start. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1) WHERE node<>$pid GROUP BY node),
r AS (SELECT m.created, t.tag, t.tag_name FROM f JOIN message m ON m.creator=f.node AND m.kind=0 JOIN tagged t ON t.message=m.id),
old AS (SELECT DISTINCT tag FROM r WHERE created < $start),
g AS (SELECT tag_name, count(*) AS n FROM r WHERE created >= $start AND created < $start + $days*86400000::BIGINT
      AND tag NOT IN (SELECT tag FROM old) GROUP BY tag_name)
SELECT tag_name, n FROM g ORDER BY n DESC, tag_name LIMIT 10
