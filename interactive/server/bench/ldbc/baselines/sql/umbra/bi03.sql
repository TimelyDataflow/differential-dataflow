-- BI3. Forums moderated by people in a country, ranked by messages tagged with a given tag class.
-- Parameters: $classname, $countryname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH ms AS (SELECT DISTINCT t.message FROM tagclass c JOIN tagged t ON t.tag_class=c.id WHERE c.name=$classname),
g AS (SELECT th.forum, f.title, f.created, f.moderator, count(*) AS n
  FROM ms JOIN thread th ON th.mid=ms.message JOIN forum f ON th.forum=f.id JOIN residents p ON f.moderator=p.id
  WHERE p.country_name=$countryname GROUP BY 1, 2, 3, 4)
SELECT forum, title, created, moderator, n FROM g ORDER BY n DESC, forum LIMIT 20
