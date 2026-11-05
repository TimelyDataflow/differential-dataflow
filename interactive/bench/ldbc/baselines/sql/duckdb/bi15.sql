-- BI15. Cheapest path between two people where friendship weights shrink with interactions in forums created inside a window.
-- Parameters: $end, $p1, $p2, $start. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH RECURSIVE inter AS (SELECT i.src, i.dst, sum(i.score2) AS score2 FROM interactions i
    JOIN thread t ON t.mid=i.comment JOIN forum f ON f.id=t.forum
    WHERE f.created >= $start AND f.created <= $end GROUP BY i.src, i.dst),
w AS (SELECT e.src, e.dst, 2.0::DOUBLE / (coalesce(i.score2,0)+2)::DOUBLE AS weight
      FROM edges e LEFT JOIN inter i ON i.src=e.src AND i.dst=e.dst),
sp(node, cost) USING KEY (node) AS (
  SELECT $p1::BIGINT, 0.0::DOUBLE
  UNION
  SELECT dst, min(cost) FROM (SELECT e.dst, sp.cost + e.weight AS cost FROM sp JOIN w e ON e.src=sp.node) cand
  WHERE NOT EXISTS (SELECT 1 FROM recurring.sp r WHERE r.node=cand.dst AND r.cost <= cand.cost)
  GROUP BY dst)
SELECT coalesce((SELECT cost FROM sp WHERE node=$p2), -1.0::DOUBLE)
