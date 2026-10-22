-- BI19. Cheapest interaction-weighted paths between anyone in city 1 and anyone in city 2.
-- Parameters: $city1, $city2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(source, node, cost, path, changed, it) AS (
  SELECT s.*, true, 0 FROM (SELECT id, id, 0::bigint, NULL::bigint[] FROM person WHERE city=$city1) s
  UNION ALL
  (WITH st AS (SELECT * FROM sp),
   cand0 AS (SELECT st.source, e.dst AS node, st.cost + e.weight AS cost,
                    NULL::bigint[] AS path
             FROM st JOIN weighted e ON e.src=st.node WHERE st.changed),
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
r AS (SELECT sp_final.source, sp_final.node, sp_final.cost FROM sp_final JOIN person t ON t.id=sp_final.node WHERE t.city=$city2)
SELECT source, node, cost FROM r
WHERE cost = (SELECT min(cost) FROM r) ORDER BY source, node
