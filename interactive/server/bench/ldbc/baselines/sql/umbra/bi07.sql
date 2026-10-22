-- BI7. Tags on replies to messages with a given tag (replies not themselves carrying the tag).
-- Parameters: $tagname. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH ms AS (SELECT DISTINCT m.id FROM tagged t JOIN message m ON t.message=m.id WHERE t.tag_name=$tagname),
rp AS (SELECT r.id FROM ms JOIN message r ON r.parent=ms.id AND r.kind=1 WHERE r.id NOT IN (SELECT id FROM ms)),
g AS (SELECT t.tag_name, count(*) AS n FROM rp JOIN tagged t ON t.message=rp.id GROUP BY t.tag_name)
SELECT tag_name, n FROM g ORDER BY n DESC, tag_name LIMIT 100
