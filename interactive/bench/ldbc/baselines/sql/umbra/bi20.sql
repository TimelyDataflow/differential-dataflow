-- BI20. Recruitment: the closest employees of a company to a person, over a graph of classmates weighted by graduation-year gaps.
-- Parameters: $company, $p2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE emp AS (SELECT DISTINCT w.person FROM org o JOIN work w ON w.org=o.id WHERE o.name=$company),
se AS (SELECT a.person AS src, b.person AS dst, min(abs(a.year-b.year)+1) AS weight FROM study a JOIN study b ON a.org=b.org
       WHERE a.person<>b.person AND (a.person, b.person) IN (SELECT src, dst FROM edges) GROUP BY 1, 2),
sp(source, node, cost, path, changed, it) AS (
  SELECT s.*, true, 0 FROM (SELECT 0::bigint, $p2::bigint, 0::bigint, NULL::bigint[] WHERE $p2 NOT IN (SELECT person FROM emp)) s
  UNION ALL
  (WITH st AS (SELECT * FROM sp),
   cand0 AS (SELECT st.source, e.dst AS node, st.cost + e.weight AS cost,
                    NULL::bigint[] AS path
             FROM st JOIN se e ON e.src=st.node WHERE st.changed),
   cand AS (SELECT c.source, c.node, min(c.cost) AS cost,
                   NULL::bigint[] AS path
            FROM cand0 c GROUP BY c.source, c.node),
   better AS (SELECT c.* FROM cand c LEFT JOIN st ON st.source=c.source AND st.node=c.node
              WHERE st.node IS NULL OR c.cost < st.cost),
   merged AS (SELECT source, node, cost, path, true AS changed FROM better
              UNION ALL
              SELECT st.source, st.node, st.cost, st.path, false FROM st
              WHERE NOT EXISTS (SELECT 1 FROM better b WHERE b.source=st.source AND b.node=st.node))
   SELECT m.source, m.node, m.cost, m.path, m.changed, i.it + 1
   FROM merged m, (SELECT max(it) AS it FROM st) i
   WHERE EXISTS (SELECT 1 FROM better))
),
sp_final AS (SELECT source, node, cost, path FROM sp WHERE it = (SELECT max(it) FROM sp)),
r AS (SELECT node, cost FROM sp_final WHERE node IN (SELECT person FROM emp) AND node<>$p2)
SELECT node, cost FROM r
WHERE cost = (SELECT min(cost) FROM r) ORDER BY cost, node LIMIT 20
