-- BI14. International dialog: the best-scoring friend pair per city between two countries, scored by replies and likes.
-- Parameters: $country1, $country2. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH pairs AS (SELECT a.id AS p1, b.id AS p2, a.city, a.city_name
    FROM residents a JOIN edges e ON e.src=a.id JOIN residents b ON b.id=e.dst
    WHERE a.country_name=$country1 AND b.country_name=$country2),
rp AS (SELECT DISTINCT creator AS src, parent_creator AS dst FROM replies
       WHERE creator IN (SELECT p1 FROM pairs UNION SELECT p2 FROM pairs)),
lk AS (SELECT DISTINCT l.person AS src, m.creator AS dst FROM likes l JOIN message m ON m.id=l.message
       WHERE l.person IN (SELECT p1 FROM pairs UNION SELECT p2 FROM pairs)),
sc AS (SELECT p.*,
   (CASE WHEN EXISTS (SELECT 1 FROM rp WHERE rp.src=p.p1 AND rp.dst=p.p2) THEN 4 ELSE 0 END)
 + (CASE WHEN EXISTS (SELECT 1 FROM rp WHERE rp.src=p.p2 AND rp.dst=p.p1) THEN 1 ELSE 0 END)
 + (CASE WHEN EXISTS (SELECT 1 FROM lk WHERE lk.src=p.p1 AND lk.dst=p.p2) THEN 10 ELSE 0 END)
 + (CASE WHEN EXISTS (SELECT 1 FROM lk WHERE lk.src=p.p2 AND lk.dst=p.p1) THEN 1 ELSE 0 END) AS score FROM pairs p),
best AS (SELECT * FROM sc QUALIFY row_number() OVER (PARTITION BY city ORDER BY score DESC, p1, p2) = 1)
SELECT p1, p2, city_name, score
FROM best ORDER BY score DESC, p1, p2 LIMIT 100
