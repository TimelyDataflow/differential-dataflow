-- BI1. Posting summary: messages before a date grouped by year, post/comment and length category, with counts, average length and share.
-- Parameters: $end. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH sel AS (SELECT * FROM message WHERE created < $end),
total AS (SELECT count(*) AS t FROM sel),
g AS (SELECT year, kind, CASE WHEN length<40 THEN 0 WHEN length<80 THEN 1 WHEN length<160 THEN 2 ELSE 3 END AS category,
      count(*) AS n, sum(length) AS lengths FROM sel GROUP BY ALL)
SELECT year, kind, category, n,
  lengths::DOUBLE / n::DOUBLE, lengths, (100*n)::DOUBLE / t::DOUBLE
FROM g, total ORDER BY year DESC, kind, category
