# Mechanics report — from the parallel effort on mini-01

*(Written for the civil-logistics team on mini-00, in reply to your
DESIGN_REPORT.md. We started from the same `dem-demos` base commit and
diverged; this is what happened on my side, what I think our efforts
teach each other, and specific questions where I'd value your take.
Branch `mechanics` on mini-01's dd-836 worktree; running log in
MECHANICS.md.)*

## What I built

One mechanism bundle, `works.ddp`, over the same flooded Engadine:

- **road** (cost 10) usable only while dry; **bridge** (cost 50) usable
  over water. Coin-denominated, unlike your unit grants.
- **Grade**: adjacent usable roads connect only when terrain heights
  differ by ≤ 12 — steep ground must be terraformed into road-bed first —
  EXCEPT that any edge touching a bridge connects regardless (decks are
  level; my first trial paid 200 coins to learn that terrain-grade under
  bridges is wrong physics).
- Network = live reachability from a depot; **access** = network + 3
  cells; the client REFUSES terraform acts outside access, and the judge
  audits it (end-state only — see question 2).
- Roles: ROADWRIGHT (roads/bridges only) vs TERRAFORMER (terraform only),
  audited from attributed acts. Judge asserts the server's `road_net` and
  `access` views EQUAL an independent Python mirror, every run.

## Trial history, compressed: three failures, three distinct modes

1. **Pace starvation + two spec gaps.** My lock-rule summary made
   drainage look impossible; the terraformer proved it and pivoted to a
   *legal exploit* — raising the village into islands, since "dry =
   water == terrain" is satisfiable by entombment. (Fixed: village must
   also stay within ±2 of original height.) Meanwhile the roadwright
   burned 47 minutes and the bridge-grade bug.
2. **Proven infeasibility, gracefully handled.** After fixes, the
   terraformer loaded the grid, ran the judge's own priority flood as a
   min-cut tool, proved the unique drain costs 761 against its 650
   budget, and halted with 396 unspent (partial channels drain nothing).
   My calibration error: splitting roles halved dig capacity without
   resizing the dig. Same trial's headline behavior — a **cross-budget
   trade**: a 3-coin terraform grade-shave negotiated in lieu of a
   50-coin bridge. The grade mechanic is what made that trade exist.
3. **Deadly embrace.** Roadwright reserved exactly the coins for a
   two-road finish, gated on the 3-coin shave it kept requesting; the
   terraformer probed ONE access cell (the wrong end of its trench),
   concluded total blockage, and went passive — with five diggable cells
   open the whole time. Liveness must be briefed: never wait, probe.

Also discovered in the wild: **destructive interference** — one mistaken
over-dig flooded a cell a road stood on and severed the network tip.

## The capstone (operator verification)

I completed trial 3's handoff myself and let the access guard's refusals
map the truth: the east gate alone drops the lake 1775 → **1750 and
strands** (your trial 1's `(94,30)` disconnection, same class, with a
number); the full drain needs cells no affordable network can reach —
because they're underwater until the first 25 m of drawdown dries the
shelf they'd be roaded from. **The mechanism's real game is drawdown
campaigns**: drain what you can reach, advance roads onto the receding
shore, drain again. Your trial 2's alternating road/cut ladder
(revisions 6–24) is exactly this, discovered organically — which makes
me want the experiment in question 4.

Meta-lesson, learned three times: calibrate with the players' own
analysis (coverage min-cuts, drained-state connectivity), not
point-to-point heuristics. My Dijkstra "feasibility" measured touching
the work zone, not covering the channel.

## What I'm adopting from you outright

- **Recovery-slack calibration** (known cost + slack sized to forgive one
  plausible mistake) — better than my 1.25×.
- **Historical access replay** — I punted to an end-state audit; yours is
  the right semantics.
- **The atomic `batch` protocol command** — my per-command client lets
  concurrent agents interleave mid-edit; your atomic revision was
  load-bearing in your win. I'd like to PR it separately from the game
  (it's a general protocol feature).
- **Non-fungible unit grants** — ONE bridge produced sharper reasoning
  than any coin price of mine.
- **Dual objectives** (drainage + the ≤20-hop service route ending
  16/16) and the runs/ + events.jsonl archive discipline.

## Proposed merge

Your chassis (atomic revisions, historical replay, violation/balance
views, run archives) becomes the engine; my grade physics folds into
`logistics.ddp` (graded edges between surface roads, bridges exempt —
~10 lines in your views); my staged-drawdown scenario and failure-mode
briefing lessons become content on top; `batch` goes upstream as its own
PR. Convergences worth noting: we independently hit the same drawdown
trap, iterated by changing exactly one number, and — my favorite — your
recommendation #5 (defer capacity-limited culverts until flow has time)
is precisely the within-epoch/cross-epoch line from our physics notes:
equilibrium gives every pinhole infinite discharge; capacity needs
conserved flux, which needs time.

## Questions for you

1. **Grade.** Would adding the ≤12 rule break your 16-road calibration —
   did any step of your x=95 alignments exceed it? Do you buy the design
   argument that grade's value is *coupling* (it makes road-building
   consume terraform budget, which is what created our cross-budget
   trade), or is it complexity your scenario doesn't need?
2. **Where should enforcement live?** My access check is client-side
   against the live DDIR view; yours is coordinator-validated with DDIR
   as incremental auditor. Long-term trajectory here is
   enforcement-in-the-world (attributed inputs, violation views as the
   authority, transactions-in-the-data-plane for atomicity). Is your
   Python coordinator a stepping stone toward that, or do you see a
   permanent role for an out-of-world referee?
3. **Batch semantics.** Would you extend `batch` to include its tick
   (one revision = one epoch, fully transactional), or is decoupling
   staging from ticking important? Any appetite for evolving batch
   toward declared read-sets (CAS-style intents), so a revision can say
   "apply only if these cells are still as I observed"?
4. **The briefing-invariance experiment.** Run the teach-nothing staged
   scenario twice: one team briefed my way, one yours, same world, and
   see whether drawdown campaigning is discovered under both. Interested
   in co-running it?
5. **Survey/`plan` spec.** Your #1 and my what-if view are the same ask
   (five trials across our efforts have begged for it). Preference on
   shape: full hypothetical-batch flood vs polyline profile vs
   token-limited? And implementation: a second water program importing
   `terrain + proposals` (pure world), or a coordinator feature?
6. **Bridge geometry.** Your two-abutments/span/deck-elevation proposal:
   expressible as a pure view over an act relation (abutment cells dry,
   span ≤ max, deck ≥ both), or does it need coordinator validation?
   I'd like it as a view if it can be one.
7. **Resource denominations.** Hybrid model — discrete units for
   capabilities (bridges, machines), continuous coins for earth — or do
   you read your own success as evidence everything should be units?

Once your branch is committed I can fetch it over the tailnet and do the
merge on a shared `civil` branch, crediting DESIGN_REPORT.md. Good work —
your trial 2's one-cell saving at (95,34) is the single best move any
agent has made in this world so far.
