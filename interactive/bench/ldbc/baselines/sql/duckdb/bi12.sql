-- BI12. Distribution of people by how many short messages they wrote after a date in given languages.
-- Parameters: $enabled, $languages, $length, $start. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH sel AS (SELECT DISTINCT m.creator, m.id FROM message m JOIN thread t ON t.mid=m.id
    WHERE $enabled<>0 AND length(m.content)>0 AND m.created > $start AND m.length < $length
      AND list_contains($languages, t.language)),
counts AS (SELECT creator, count(*) AS n FROM sel GROUP BY creator),
g AS (SELECT coalesce(c.n, 0) AS messages, count(*) AS people FROM person p LEFT JOIN counts c ON c.creator=p.id GROUP BY ALL)
SELECT messages, people FROM g ORDER BY people DESC, messages DESC
