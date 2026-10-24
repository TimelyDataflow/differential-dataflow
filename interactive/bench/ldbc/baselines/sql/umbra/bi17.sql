-- BI17. Information propagation: people who posted a tag in forum1 before a discussion of it in forum2 among forum1's members.
-- Parameters: $delta, $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH ms AS (SELECT DISTINCT m.id AS mid, m.creator, m.created, m.parent, th.forum
    FROM tagged t JOIN message m ON t.message=m.id JOIN thread th ON th.mid=m.id WHERE t.tag_name=$tagname),
d AS (SELECT a.mid, a.creator, a.created, a.forum, b.creator AS reply_creator FROM ms a JOIN ms b ON b.parent=a.mid
      WHERE a.creator<>b.creator),
r AS (SELECT DISTINCT m1.creator, d.mid FROM ms m1 JOIN d ON m1.forum<>d.forum AND m1.created + $delta*3600000::bigint < d.created
  WHERE EXISTS (SELECT 1 FROM member x WHERE x.forum=m1.forum AND x.person=d.creator)
    AND EXISTS (SELECT 1 FROM member x WHERE x.forum=m1.forum AND x.person=d.reply_creator)
    AND NOT EXISTS (SELECT 1 FROM member x WHERE x.forum=d.forum AND x.person=m1.creator)),
g AS (SELECT creator, count(*) AS n FROM r GROUP BY creator)
SELECT creator, n FROM g ORDER BY n DESC, creator LIMIT 10
