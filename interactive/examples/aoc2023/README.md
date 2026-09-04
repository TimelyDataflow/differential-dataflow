# AoC 2023 in DDIR

A language-coverage and regression suite: 33 parts across 17 days of Advent
of Code 2023, expressed in pipe-form DDIR (`.ddp`) and checked against the
answers of Materialize's sqllogictest oracles
(`test/sqllogictest/advent-of-code/2023/`) — same inputs, same expected
answers, oracle quirks reproduced where needed to match.

Inputs are committed in their dense original text form (`dayNN/input.txt`,
the compact puzzle text as it appears in the slt oracles); `transcribe.py`
mechanically regenerates the i64 fact files the programs read (text →
`(line, pos, charcode)`, grids → `(row, col, cell)`, names → ids) into the
gitignored `gen/` directory at runtime. All puzzle logic lives in the
`.ddp` programs. `transcribe.py --pad` additionally writes day05's
arity-padded fact files for the corgi backend, which crashes on
mixed-arity inputs.

## Run

    cargo build --release --example ddir_server
    ./run.sh          # every part on the vec backend vs expected.txt; nonzero exit on any mismatch
    ./run.sh corgi    # the same through the Corgi columnar backend

`run.sh` first runs `transcribe.py` (python3) to regenerate `gen/`, then
runs each part as one server session — `install`, `load` the fact file into
input 0, `tick` — and reads the answer off the `[partN]` inspect line.

Both backends pass all 33 parts. Corgi needs day05's arity-padded inputs
(`run.sh corgi` transcribes with `--pad`).

## Verdicts

CLEAN = direct expression; AWKWARD = right answer via a workaround idiom.

| day | p1 | p2 | note |
|-----|----|----|------|
| 01 | clean | awkward | no string literals: digit words matched as charcode filters over k-wide windows (k−1 self-joins) |
| 02 | clean | clean | anti-join as `games - bad`; max via `min` of (BIG − x) |
| 03 | awkward | awkward | run assembly needs 2 fixpoints; `distinct` erases values, so loop state is packed into keys |
| 04 | clean | clean | p2: card copies ARE collection multiplicities; the cascade is a bare `var` fixpoint |
| 05 | clean | awkward | stage-chain fixpoint, identity fallback = anti-join; p2 range split/holes all key-packed |
| 06 | awkward | — | no `/` or sqrt: hold times enumerated by fixpoint; slt has no separate part 2 |
| 07 | clean | awkward | hand type = pairwise-equality count; p2 stages maps to stand in for scalar lets |
| 10 | clean | awkward | loop = fixpoint, n/2 via parity fold; p2: `-` is multiset subtraction, set-minus needs an explicit anti-join |
| 11 | clean | clean | gap rows/cols by anti-join; L1 over all-pairs (slt input has no gaps, so p1 = p2) |
| 13 | awkward | clean | whole-line equality via collected Lists; p2 smudge = exactly one mismatched mirrored cell |
| 14 | clean | awkward | p1: closed-form roll, zero recursion; p2: round-tagged fixpoint with 4 hand-unrolled direction pipelines |
| 15 | awkward | clean | no `%`: modulo done relationally via a k-table cross join; p2 boxes need no sequencing |
| 16 | clean | clean | beam automaton with a literal transition table via flatmap |
| 17 | clean | clean | Bellman-Ford: `var mc = seeds + moves \| min`; p2 = 3 filter tweaks on p1 |
| 18 | awkward | awkward | shoelace prefix fixpoint; /2 via a 40-step binary-decomposition gadget; p2 hex decoded from charcodes |
| 19 | clean | clean | first-match = min (priority, next) inside the fixpoint; p2 reproduces the slt's negative-product quirk |
| 22 | clean | clean | interval overlap, no cell expansion; p2's count-inside-fixpoint converges fine |

Days 08, 09, 12, 20, 21, 23, 24 are absent because their slt files carry no
usable oracle (EXPLAIN-only, stubbed, or self-inconsistent); day 25's oracle
is an artifact of floating-point spectral iteration, which DDIR (integers
only) cannot chase.

The survey behind this suite also produced a ranked catalogue of 13
recurring language gaps (integer division/modulo, `max`/`sum` reducers,
value-preserving `distinct`, scalar lets, ...): see [GAPS.md](GAPS.md).
