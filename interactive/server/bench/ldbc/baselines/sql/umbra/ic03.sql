-- IC3. Friends and friends-of-friends living outside countries X and Y who posted from both X and Y in a time window; top 20 by message count.
-- Parameters: $countryx, $countryy, $days, $pid, $start. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2) u WHERE node<>$pid GROUP BY node),
r AS (SELECT f.node, p.first, p.last, loc.name AS loc
  FROM f JOIN residents p ON f.node=p.id JOIN message m ON m.creator=f.node JOIN place loc ON m.country=loc.id
  WHERE p.country_name<>$countryx AND p.country_name<>$countryy
    AND m.created >= $start AND m.created < $start + $days*86400000::bigint),
g AS (SELECT node, first, last, sum(CASE WHEN loc=$countryx THEN 1 ELSE 0 END) AS nx,
             sum(CASE WHEN loc=$countryy THEN 1 ELSE 0 END) AS ny FROM r GROUP BY node, first, last)
SELECT node, first, last, nx, ny, nx+ny
FROM g WHERE nx>0 AND ny>0 ORDER BY nx+ny DESC, node LIMIT 20
