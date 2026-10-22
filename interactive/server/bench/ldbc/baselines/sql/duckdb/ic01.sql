-- IC1. People named X within three friendship hops, with their emails, languages, city, universities and companies; 20 closest, then by last name.
-- Parameters: $firstname, $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH h1 AS (SELECT DISTINCT dst AS node FROM edges WHERE src=$pid), h2 AS (SELECT DISTINCT e.dst AS node FROM h1 JOIN edges e ON e.src=h1.node), h3 AS (SELECT DISTINCT e.dst AS node FROM h2 JOIN edges e ON e.src=h2.node), f AS (SELECT node, min(d) AS distance FROM (SELECT node, 1 AS d FROM h1 UNION ALL SELECT node, 2 AS d FROM h2 UNION ALL SELECT node, 3 AS d FROM h3) WHERE node<>$pid GROUP BY node),
r AS (SELECT f.node, f.distance, p.* FROM f JOIN residents p ON f.node=p.id WHERE p.first=$firstname),
em AS (SELECT person, list(value ORDER BY value) AS v FROM email WHERE person IN (SELECT node FROM r) GROUP BY person),
la AS (SELECT person, list(value ORDER BY value) AS v FROM language WHERE person IN (SELECT node FROM r) GROUP BY person),
un AS (SELECT s.person, list(struct_pack(n := o.name, y := s.year, p := pl.name) ORDER BY o.name, s.year, pl.name) AS v
       FROM study s JOIN org o ON s.org=o.id JOIN place pl ON o.place=pl.id WHERE s.person IN (SELECT node FROM r) GROUP BY s.person),
co AS (SELECT s.person, list(struct_pack(n := o.name, y := s.year, p := pl.name) ORDER BY o.name, s.year, pl.name) AS v
       FROM work s JOIN org o ON s.org=o.id JOIN place pl ON o.place=pl.id WHERE s.person IN (SELECT node FROM r) GROUP BY s.person)
SELECT r.node, r.last, r.distance, r.birthday, r.created, r.gender, r.browser, r.ip,
  coalesce(em.v, []), coalesce(la.v, []), r.city_name, coalesce(un.v, []), coalesce(co.v, [])
FROM r LEFT JOIN em ON em.person=r.node LEFT JOIN la ON la.person=r.node LEFT JOIN un ON un.person=r.node
  LEFT JOIN co ON co.person=r.node
ORDER BY r.distance, r.last, r.node LIMIT 20
