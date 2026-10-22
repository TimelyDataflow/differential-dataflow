-- IC7. The latest likes on a person's messages: who liked, when, how long after posting, and whether the liker is a friend.
-- Parameters: $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH l AS (SELECT lk.person, min(struct_pack(w := -lk.created, m := m.id)) AS best
  FROM message m JOIN likes lk ON lk.message=m.id WHERE m.creator=$pid GROUP BY lk.person),
r AS (SELECT l.person, -(l.best.w) AS "when", l.best.m AS mid FROM l)
SELECT r.person, p.first, p.last, r."when", r.mid, CASE WHEN length(m.content)>0 THEN m.content ELSE m.image END, (CASE WHEN (r."when" - m.created)>=0 THEN (r."when" - m.created)//60000 ELSE -((-(r."when" - m.created))//60000) END),
  CASE WHEN e.src IS NULL THEN 1 ELSE 0 END
FROM r JOIN message m ON m.id=r.mid JOIN person p ON p.id=r.person
  LEFT JOIN edges e ON e.src=$pid AND e.dst=r.person
ORDER BY r."when" DESC, r.person LIMIT 20
