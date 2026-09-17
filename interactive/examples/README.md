# DDIR examples

All runnable examples use the `ddir-server` executable. The server is built
from the workspace root with `cargo build --release -p ddir-server`.

| Directory | Contents | How to run |
| --- | --- | --- |
| `programs/` | Small graph and language programs (`.ddp` and legacy `.ddir`) | Use a session from `server/`, or write `load`, `feed`, `tick`, and `peek` commands |
| `server/` | Checked-in command sessions and server-oriented programs | `cargo run --release -p ddir-server < examples/server/sessions/shared_trace.txt` |
| `aoc2023/` | 33 Advent of Code parts with generated fact inputs and answer checks | `cd examples/aoc2023 && ./run.sh` (or `./run.sh corgi`) |

The files under `server/` are protocol demonstrations: sessions are command
scripts, while files in `server/programs/` are programs loaded by those
scripts. The `server/demo/` directory contains longer stdin/TCP demonstrations
of feedback, contention, and intake limits. They use the same protocol and
runtime as the sessions here.

For a new example, keep its program and input/session together, state its
expected result, and add it to a test or to the AoC runner. Prefer `.ddp` for
new teaching examples; `.ddir` remains available for applicative-parser
coverage.
