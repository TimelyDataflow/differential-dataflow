-- IC14. The cheapest interaction-weighted path between two people (v2 definition), with ties broken by the lexicographically smallest path.
-- Parameters: $p1, $p2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE sp(node, cost, path) USING KEY (node) AS (
  SELECT $p1::BIGINT, 0::BIGINT, [$p1::BIGINT]
  UNION
  SELECT dst, (min({'c': cost, 'p': path})).c, (min({'c': cost, 'p': path})).p FROM (
    SELECT e.dst, sp.cost + e.weight AS cost, list_append(sp.path, e.dst) AS path
    FROM sp JOIN weighted e ON e.src=sp.node) cand
  WHERE NOT EXISTS (SELECT 1 FROM recurring.sp r WHERE r.node=cand.dst AND {'c': r.cost, 'p': r.path} <= {'c': cand.cost, 'p': cand.path})
  GROUP BY dst)
SELECT path, cost FROM sp WHERE node=$p2
