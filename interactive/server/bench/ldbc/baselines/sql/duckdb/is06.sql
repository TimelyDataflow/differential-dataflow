-- IS6. The forum that contains a message's thread, and its moderator.
-- Parameters: $mid. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
SELECT forum, title, moderator, first, last FROM (SELECT t.forum, f.title, f.moderator, p.first, p.last FROM thread t JOIN forum f ON t.forum=f.id
  JOIN person p ON f.moderator=p.id WHERE t.mid=$mid) r  ORDER BY forum
