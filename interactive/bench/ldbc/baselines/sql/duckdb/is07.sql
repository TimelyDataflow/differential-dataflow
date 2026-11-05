-- IS7. Replies to a message, flagging whether each replier knows the original author.
-- Parameters: $mid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
SELECT id, content, created, creator, first, last, knows FROM (SELECT r.id, r.content, r.created, r.creator, p.first, p.last,
    CASE WHEN r.creator<>r.parent_creator THEN (CASE WHEN e.src IS NULL THEN 0 ELSE 1 END) ELSE 0 END AS knows
  FROM replies r JOIN person p ON r.creator=p.id LEFT JOIN edges e ON e.src=r.creator AND e.dst=r.parent_creator
  WHERE r.parent=$mid) r  ORDER BY created DESC, creator
