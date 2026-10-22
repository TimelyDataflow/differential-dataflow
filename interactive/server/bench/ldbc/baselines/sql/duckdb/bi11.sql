-- BI11. Number of friendship triangles among people in a country, with all three friendships made inside a window.
-- Parameters: $countryname, $end, $start. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH res AS (SELECT id FROM residents WHERE country_name=$countryname),
e AS (SELECT src, dst FROM edges WHERE src IN (SELECT id FROM res) AND dst IN (SELECT id FROM res)
      AND created >= $start AND created <= $end)
SELECT (SELECT count(*) FROM e a JOIN e b ON a.dst=b.src JOIN e c ON c.src=b.dst AND c.dst=a.src
                   WHERE a.src < a.dst AND a.dst < b.dst)
