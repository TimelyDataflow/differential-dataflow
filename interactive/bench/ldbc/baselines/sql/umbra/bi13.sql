-- BI13. Zombies in a country: people posting less than once a month, and how many of their likes come from other zombies.
-- Parameters: $countryname, $end, $endmonth. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH cand AS (SELECT id, created, creationmonth FROM residents WHERE country_name=$countryname AND created < $end),
counts AS (SELECT c.id, count(*) AS n FROM cand c JOIN message m ON m.creator=c.id
           WHERE m.created >= c.created AND m.created <= $end GROUP BY c.id),
zombies AS (SELECT c.id FROM cand c LEFT JOIN counts n ON n.id=c.id
            WHERE coalesce(n.n,0) < $endmonth - c.creationmonth + 1),
liked AS (SELECT z.id AS zombie, l.person AS liker FROM zombies z JOIN message m ON m.creator=z.id
          JOIN likes l ON l.message=m.id JOIN person lp ON lp.id=l.person WHERE lp.created < $end),
lc AS (SELECT zombie, count(*) AS total, sum(CASE WHEN liker IN (SELECT id FROM zombies) THEN 1 ELSE 0 END) AS zl
       FROM liked GROUP BY zombie),
r AS (SELECT z.id, coalesce(lc.zl,0) AS zl, coalesce(lc.total,0) AS total FROM zombies z LEFT JOIN lc ON lc.zombie=z.id)
SELECT id, zl, total, CASE WHEN total>0 THEN zl::double precision/total::double precision ELSE 0.0::double precision END
FROM r ORDER BY (CASE WHEN total>0 THEN zl::double precision/total::double precision ELSE 0.0 END) DESC, id LIMIT 100
