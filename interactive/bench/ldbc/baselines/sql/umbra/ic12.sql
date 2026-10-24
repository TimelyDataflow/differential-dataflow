-- IC12. Friends who replied to posts tagged within a tag class (or its subclasses); count of such replies and the tags involved.
-- Parameters: $classname, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1) u WHERE node<>$pid GROUP BY node),
tags AS (SELECT t.message, t.tag_name FROM tagged t JOIN ancestry a ON t.tag_class=a.descendant
         JOIN tagclass c ON a.ancestor=c.id WHERE c.name=$classname),
r AS (SELECT f.node, rp.id AS reply, tg.tag_name FROM f JOIN replies rp ON rp.creator=f.node AND rp.parent_kind=0
      JOIN tags tg ON tg.message=rp.parent),
counts AS (SELECT node, count(DISTINCT reply) AS n FROM r GROUP BY node),
tl AS (SELECT node, array_agg(tag_name ORDER BY tag_name) AS tags FROM (SELECT DISTINCT node, tag_name FROM r) d GROUP BY node)
SELECT counts.node, p.first, p.last, tl.tags, counts.n
FROM counts JOIN tl ON tl.node=counts.node JOIN person p ON p.id=counts.node ORDER BY counts.n DESC, counts.node LIMIT 20
