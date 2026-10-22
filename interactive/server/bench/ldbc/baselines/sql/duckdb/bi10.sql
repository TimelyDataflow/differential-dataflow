-- BI10. Experts in a social circle: people at 'minimum' to 'maximum' hops in a country, counting tags on their messages that carry a tag class.
-- Parameters: $classname, $countryname, $maximum, $minimum, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(node, cost) USING KEY (node) AS (
  SELECT $pid::BIGINT, 0::BIGINT
  UNION
  SELECT dst, min(cost) FROM (SELECT e.dst, sp.cost + e.weight AS cost FROM sp JOIN (SELECT src, dst, 1 AS weight FROM edges) e ON e.src=sp.node) cand
  WHERE NOT EXISTS (SELECT 1 FROM recurring.sp r WHERE r.node=cand.dst AND r.cost <= cand.cost)
  GROUP BY dst),
paths AS (SELECT sp.node FROM sp JOIN residents p ON p.id=sp.node
  WHERE sp.cost >= $minimum AND sp.cost <= $maximum AND sp.node<>$pid AND p.country_name=$countryname),
q AS (SELECT DISTINCT paths.node, m.id FROM paths JOIN message m ON m.creator=paths.node JOIN tagged t ON t.message=m.id
      JOIN tagclass c ON c.id=t.tag_class WHERE c.name=$classname),
g AS (SELECT q.node, t.tag_name, count(*) AS n FROM q JOIN tagged t ON t.message=q.id GROUP BY ALL)
SELECT node, tag_name, n FROM g ORDER BY n DESC, tag_name, node LIMIT 100
