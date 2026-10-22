-- BI2. Tags in a tag class whose usage changed most between two consecutive 100-day windows.
-- Parameters: $classname, $date. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH g AS (SELECT t.tag_name, sum(CASE WHEN m.created < $date + 100*86400000::BIGINT THEN 1 ELSE 0 END) AS n1,
    sum(CASE WHEN m.created >= $date + 100*86400000::BIGINT THEN 1 ELSE 0 END) AS n2
  FROM tagclass c JOIN tagged t ON t.tag_class=c.id JOIN message m ON t.message=m.id
  WHERE c.name=$classname AND m.created >= $date AND m.created < $date + 200*86400000::BIGINT GROUP BY t.tag_name)
SELECT tag_name, n1, n2, abs(n1-n2)
FROM g ORDER BY abs(n1-n2) DESC, tag_name LIMIT 100
