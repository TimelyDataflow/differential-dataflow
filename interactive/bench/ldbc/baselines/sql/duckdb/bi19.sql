-- BI19. Cheapest interaction-weighted paths between anyone in city 1 and anyone in city 2.
-- Parameters: $city1, $city2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(source, node, cost) USING KEY (source, node) AS (
  SELECT id, id, 0::BIGINT FROM person WHERE city=$city1
  UNION
  SELECT source, dst, min(cost) FROM (SELECT sp.source, e.dst, sp.cost + e.weight AS cost FROM sp JOIN weighted e ON e.src=sp.node) cand
  WHERE NOT EXISTS (SELECT 1 FROM recurring.sp r WHERE r.source=cand.source AND r.node=cand.dst AND r.cost <= cand.cost)
  GROUP BY source, dst),
r AS (SELECT sp.source, sp.node, sp.cost FROM sp JOIN person t ON t.id=sp.node WHERE t.city=$city2)
SELECT source, node, cost FROM r
WHERE cost = (SELECT min(cost) FROM r) ORDER BY source, node
