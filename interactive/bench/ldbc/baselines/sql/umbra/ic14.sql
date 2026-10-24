-- IC14. The cheapest interaction-weighted path between two people (v2 definition), with ties broken by the lexicographically smallest path.
-- Parameters: $p1, $p2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(source, node, cost, path, changed, it) AS (
  SELECT s.*, true, 0 FROM (SELECT 0::bigint, $p1::bigint, 0::bigint, ARRAY[$p1::bigint]) s
  UNION ALL
  (WITH st AS (SELECT * FROM sp),
   cand0 AS (SELECT st.source, e.dst AS node, st.cost + e.weight AS cost,
                    array_append(st.path, e.dst) AS path
             FROM st JOIN weighted e ON e.src=st.node WHERE st.changed),
   cand AS (SELECT c.source, c.node, (min(ARRAY[c.cost] || c.path))[1] AS cost,
                   (min(ARRAY[c.cost] || c.path))[2:] AS path
            FROM cand0 c GROUP BY c.source, c.node),
   better AS (SELECT c.* FROM cand c LEFT JOIN st ON st.source=c.source AND st.node=c.node
              WHERE st.node IS NULL OR ARRAY[c.cost] || c.path < ARRAY[st.cost] || st.path),
   merged AS (SELECT source, node, cost, path, true AS changed FROM better
              UNION ALL
              SELECT st.source, st.node, st.cost, st.path, false FROM st
              WHERE NOT EXISTS (SELECT 1 FROM better b WHERE b.source=st.source AND b.node=st.node))
   SELECT m.source, m.node, m.cost, m.path, m.changed, i.it + 1
   FROM merged m, (SELECT max(it) AS it FROM st) i
   WHERE EXISTS (SELECT 1 FROM better))
),
sp_final AS (SELECT source, node, cost, path FROM sp WHERE it = (SELECT max(it) FROM sp))
SELECT path, cost FROM sp_final WHERE node=$p2
