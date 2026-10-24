-- BI8. Central people for a tag: interest and recent tagged messages score each person, plus their friends' scores.
-- Parameters: $end, $start, $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH sc0 AS (
  SELECT i.person, 100 AS score FROM tag t JOIN interest i ON i.tag=t.id WHERE t.name=$tagname
  UNION ALL
  SELECT m.creator, 1 FROM tagged t JOIN message m ON t.message=m.id
    WHERE t.tag_name=$tagname AND m.created > $start AND m.created < $end),
sc AS (SELECT person, sum(score) AS score FROM sc0 GROUP BY person),
fr AS (SELECT a.person, sum(b.score) AS friends FROM sc a JOIN edges e ON e.src=a.person JOIN sc b ON b.person=e.dst GROUP BY a.person)
SELECT sc.person, sc.score, coalesce(fr.friends, 0)
FROM sc LEFT JOIN fr ON fr.person=sc.person ORDER BY sc.score + coalesce(fr.friends,0) DESC, sc.person LIMIT 100
