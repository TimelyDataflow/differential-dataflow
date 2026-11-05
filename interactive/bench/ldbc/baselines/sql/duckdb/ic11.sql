-- IC11. Friends and friends-of-friends who started working at a company in a given country before a year.
-- Parameters: $countryname, $pid, $workyear. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2) WHERE node<>$pid GROUP BY node),
r AS (SELECT f.node, w.year, o.name AS o_name, p.first, p.last FROM f JOIN work w ON w.person=f.node JOIN org o ON w.org=o.id
  JOIN place pl ON o.place=pl.id JOIN person p ON p.id=f.node WHERE w.year < $workyear AND pl.name=$countryname),
n AS (SELECT *, row_number() OVER (PARTITION BY year, node ORDER BY o_name) - 1 AS nameorder FROM r)
SELECT node, first, last, o_name, year
FROM n ORDER BY year, node, nameorder DESC LIMIT 10
