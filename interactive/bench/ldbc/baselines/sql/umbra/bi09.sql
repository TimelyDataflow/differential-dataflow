-- BI9. Top thread initiators: people whose posts in a window started threads with the most messages in the window.
-- Parameters: $end, $start. Rows come back in rank order; the runner numbers them 0, 1, 2, ...
WITH posts AS (SELECT id, creator FROM message WHERE kind=0 AND created >= $start AND created <= $end),
th AS (SELECT creator, count(*) AS threads FROM posts GROUP BY creator),
ms AS (SELECT p.creator, count(*) AS messages FROM posts p JOIN thread t ON t.root=p.id JOIN message m ON m.id=t.mid
       WHERE m.created >= $start AND m.created <= $end GROUP BY p.creator)
SELECT th.creator, p.first, p.last, th.threads, ms.messages
FROM th JOIN ms ON ms.creator=th.creator JOIN person p ON p.id=th.creator ORDER BY ms.messages DESC, th.creator LIMIT 100
