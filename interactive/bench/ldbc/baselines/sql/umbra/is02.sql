-- IS2. A person's 10 most recent messages, each with the original post of its thread and that post's author.
-- Parameters: $pid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
SELECT id, CASE WHEN length(content)>0 THEN content ELSE image END, created, root, author, first, last FROM (SELECT m.id, m.created, m.content, m.image, t.root, t.author, a.first, a.last
  FROM message m JOIN thread t ON m.id=t.mid JOIN person a ON t.author=a.id WHERE m.creator=$pid) r ORDER BY created DESC, id DESC LIMIT 10
