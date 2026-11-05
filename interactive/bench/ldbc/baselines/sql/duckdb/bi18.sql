-- BI18. Friend recommendation: pairs of people interested in a tag with the most mutual friends, who are not already friends.
-- Parameters: $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH people AS (SELECT DISTINCT i.person FROM tag t JOIN interest i ON i.tag=t.id WHERE t.name=$tagname),
r AS (SELECT p.person, f.dst AS other, count(*) AS n FROM people p JOIN edges e ON e.src=p.person JOIN edges f ON f.src=e.dst
      WHERE p.person<>f.dst AND f.dst IN (SELECT person FROM people)
        AND NOT EXISTS (SELECT 1 FROM edges x WHERE x.src=p.person AND x.dst=f.dst) GROUP BY ALL)
SELECT person, other, n FROM r ORDER BY n DESC, person, other LIMIT 20
