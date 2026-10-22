-- IC13. Length of the shortest friendship path between two people (-1 if none).
-- Parameters: $p1, $p2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(node, cost) USING KEY (node) AS (
  SELECT $p1::BIGINT, 0::BIGINT
  UNION
  SELECT dst, min(cost) FROM (SELECT e.dst, sp.cost + e.weight AS cost FROM sp JOIN (SELECT src, dst, 1 AS weight FROM edges) e ON e.src=sp.node) cand
  WHERE NOT EXISTS (SELECT 1 FROM recurring.sp r WHERE r.node=cand.dst AND r.cost <= cand.cost)
  GROUP BY dst)
SELECT coalesce((SELECT cost FROM sp WHERE node=$p2), -1)
