-- BI20. Recruitment: the closest employees of a company to a person, over a graph of classmates weighted by graduation-year gaps.
-- Parameters: $company, $p2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE emp AS (SELECT DISTINCT w.person FROM org o JOIN work w ON w.org=o.id WHERE o.name=$company),
se AS (SELECT a.person AS src, b.person AS dst, min(abs(a.year-b.year)+1) AS weight FROM study a JOIN study b ON a.org=b.org
       WHERE a.person<>b.person AND (a.person, b.person) IN (SELECT src, dst FROM edges) GROUP BY ALL),
sp(node, cost) USING KEY (node) AS (
  SELECT $p2::BIGINT, 0::BIGINT WHERE $p2 NOT IN (SELECT person FROM emp)
  UNION
  SELECT dst, min(cost) FROM (SELECT e.dst, sp.cost + e.weight AS cost FROM sp JOIN se e ON e.src=sp.node) cand
  WHERE NOT EXISTS (SELECT 1 FROM recurring.sp r WHERE r.node=cand.dst AND r.cost <= cand.cost)
  GROUP BY dst),
r AS (SELECT node, cost FROM sp WHERE node IN (SELECT person FROM emp) AND node<>$p2)
SELECT node, cost FROM r
WHERE cost = (SELECT min(cost) FROM r) ORDER BY cost, node LIMIT 20
