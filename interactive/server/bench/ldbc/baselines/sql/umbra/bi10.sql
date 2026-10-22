-- BI10. Experts in a social circle: people at 'minimum' to 'maximum' hops in a country, counting tags on their messages that carry a tag class.
-- Parameters: $classname, $countryname, $maximum, $minimum, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(source, node, cost, path, changed, it) AS (
  SELECT s.*, true, 0 FROM (SELECT 0::bigint, $pid::bigint, 0::bigint, NULL::bigint[]) s
  UNION ALL
  (WITH st AS (SELECT * FROM sp),
   cand0 AS (SELECT st.source, e.dst AS node, st.cost + e.weight AS cost,
                    NULL::bigint[] AS path
             FROM st JOIN (SELECT src, dst, 1::bigint AS weight FROM edges) e ON e.src=st.node WHERE st.changed),
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
paths AS (SELECT sp_final.node FROM sp_final JOIN residents p ON p.id=sp_final.node
  WHERE sp_final.cost >= $minimum AND sp_final.cost <= $maximum AND sp_final.node<>$pid AND p.country_name=$countryname),
q AS (SELECT DISTINCT paths.node, m.id FROM paths JOIN message m ON m.creator=paths.node JOIN tagged t ON t.message=m.id
      JOIN tagclass c ON c.id=t.tag_class WHERE c.name=$classname),
g AS (SELECT q.node, t.tag_name, count(*) AS n FROM q JOIN tagged t ON t.message=q.id GROUP BY 1, 2)
SELECT node, tag_name, n FROM g ORDER BY n DESC, tag_name, node LIMIT 100
