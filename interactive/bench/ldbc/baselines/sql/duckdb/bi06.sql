-- BI6. Most authoritative users on a tag: authors scored by the popularity of the people who liked their tagged messages.
-- Parameters: $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH ms AS (SELECT DISTINCT m.id, m.creator FROM tagged t JOIN message m ON t.message=m.id WHERE t.tag_name=$tagname),
authors AS (SELECT DISTINCT creator FROM ms),
pairs AS (SELECT DISTINCT ms.creator, l.person FROM ms JOIN likes l ON l.message=ms.id),
pop AS (SELECT m.creator, count(*) AS popularity FROM message m JOIN likes l ON l.message=m.id
        WHERE m.creator IN (SELECT person FROM pairs) GROUP BY m.creator),
sc AS (SELECT pairs.creator, sum(pop.popularity) AS score FROM pairs JOIN pop ON pop.creator=pairs.person GROUP BY pairs.creator)
SELECT a.creator, coalesce(sc.score, 0)
FROM authors a LEFT JOIN sc ON sc.creator=a.creator ORDER BY coalesce(sc.score,0) DESC, a.creator LIMIT 100
