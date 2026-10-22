-- BI4. Top 100 forums (by largest single-country membership) created after a date; their members ranked by messages posted in those forums.
-- Parameters: $date. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH pop AS (SELECT f.id, p.country_id, count(*) AS n FROM forum f JOIN member mb ON mb.forum=f.id
    JOIN residents p ON mb.person=p.id WHERE f.created > $date GROUP BY 1, 2),
best AS (SELECT id, max(n) AS n FROM pop GROUP BY id),
tp AS (SELECT id AS forum FROM best ORDER BY n DESC, id LIMIT 100),
members AS (SELECT DISTINCT mb.person FROM tp JOIN member mb ON mb.forum=tp.forum),
counts AS (SELECT m.creator AS person, count(*) AS n FROM tp JOIN thread th ON th.forum=tp.forum
    JOIN message m ON m.id=th.mid GROUP BY m.creator)
SELECT mb.person, p.first, p.last, p.created, coalesce(c.n, 0)
FROM members mb LEFT JOIN counts c ON c.person=mb.person JOIN person p ON p.id=mb.person ORDER BY coalesce(c.n,0) DESC, mb.person LIMIT 100
