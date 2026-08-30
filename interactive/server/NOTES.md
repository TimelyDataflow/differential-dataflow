# Known issues and protocol wishlist

Observations from live multi-agent use of the server, kept next to the code
they concern. Entries carry enough diagnosis to act on; remove them when
fixed.

## Tail replays against a just-fed trace can stall short of complete

Reported from the visualizer sessions against the live Engadine worlds:
a `tail` begun right after feeds can deliver only part of the trace's
history and never recover within that session; retrying later works.

Diagnosis: `start_tail` (src/loop_.rs) constructs the import dataflow and
registers the tail but does not step the worker. Replay is driven only by
`tick()`'s catch-up loop, which steps until every tail probe passes the
current epoch. In a session where the driver owns time (`DDIR_TICK_MS=0`)
and no further tick arrives, a freshly created tail import is never
stepped — its replay progresses only incidentally, when some other command
steps the worker. "Retry works" because by then intervening activity has
driven the import.

Candidate fix: after registering the tail, run the same catch-up loop
`tick()` uses — step until the new tail's probe is not less than the
current epoch — so a tail import always replays to the present regardless
of trace-settling state or tick cadence. New tails would then start from a
complete snapshot, and stream deltas thereafter.
