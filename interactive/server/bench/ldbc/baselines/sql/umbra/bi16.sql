-- BI16. People active on tag A on day A and tag B on day B who know at most 'maxknows' others in each group.
-- Parameters: $datea, $dateb, $maxknows, $taga, $tagb. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH sub(which, person, mid) AS (
  SELECT DISTINCT 'a', m.creator, m.id FROM tagged t JOIN message m ON t.message=m.id WHERE t.tag_name=$taga AND m.day=$datea
  UNION
  SELECT DISTINCT 'b', m.creator, m.id FROM tagged t JOIN message m ON t.message=m.id WHERE t.tag_name=$tagb AND m.day=$dateb),
counts AS (SELECT which, person, count(*) AS n FROM sub GROUP BY 1, 2),
deg AS (SELECT a.which, a.person, count(*) AS d FROM counts a JOIN edges e ON e.src=a.person
        JOIN counts b ON b.which=a.which AND b.person=e.dst GROUP BY 1, 2),
ok AS (SELECT c.* FROM counts c LEFT JOIN deg ON deg.which=c.which AND deg.person=c.person WHERE coalesce(deg.d,0) <= $maxknows)
SELECT a.person, a.n, b.n
FROM ok a JOIN ok b ON a.person=b.person AND a.which='a' AND b.which='b' ORDER BY a.n + b.n DESC, a.person LIMIT 20
