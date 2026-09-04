#!/usr/bin/env python3
"""DDIR benchmark harness: every workload runs as a session on the one server binary.

Each workload is a program from `examples/programs/` (or an AoC part) plus a
recipe for its inputs. A run is `install`, `load` (bulk, sharded across
workers), one `tick` (the initial epoch: load + first computation), then
`tick R` (R epochs of `churn` replaced rows each). The server reports each
`tick`'s wall-clock time; this script collects them across backends, worker
counts, and repetitions, and writes a JSONL log plus a markdown summary.

    ./bench.py --scale small --runs 3                  # quick sanity pass
    ./bench.py --scale medium --backends vec,corgi --workers 1,4 --runs 5
    ./bench.py --only scc,reach --scale large
    ./bench.py --aoc                                   # the 33 AoC parts, one epoch each
    ./bench.py --profile scc --backend corgi --workers 1 --scale medium
                                                        # samply record -> reports/profiles/

Times are what the server prints; parse/lower/optimize (the intake thread) and
process startup are outside them. The report records the git revision so runs
are comparable across commits.
"""

import argparse
import datetime as dt
import json
import os
import re
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CRATE = os.path.dirname(HERE)                 # interactive/
ROOT = os.path.dirname(CRATE)                 # repo root
SERVER = os.path.join(ROOT, "target", "release", "examples", "ddir_server")
AOC = os.path.join(CRATE, "examples", "aoc2023")

# scale -> (nodes, edges, churn, rounds)
SCALES = {
    "small":  (10_000,    20_000,     10,   20),
    "medium": (100_000,   200_000,    100,  50),
    "large":  (1_000_000, 2_000_000,  1000, 20),
}

def graph(nodes, edges, churn, arity=2, seed=0):
    return f"random:nodes={nodes},edges={edges},arity={arity},seed={seed},churn={churn}"

# name -> (program, session builder). The builder gets (nodes, edges, churn) and
# returns the commands between `install` and the ticks.
WORKLOADS = {
    "scc":    ("examples/programs/scc.ddp",    lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
    "cc":     ("examples/programs/cc.ddp",     lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
    "triangles": ("examples/programs/triangles.ddp", lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
    "reach":  ("examples/programs/reach.ddp",  lambda n, e, c: [f"load p 0 {graph(n, e, c)}", "feed p 1 0"]),
    "kcore":  ("examples/programs/kcore.ddir", lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
    "stable": ("examples/programs/stable.ddp", lambda n, e, c: [f"load p 0 {graph(n, e, c, arity=4)}"]),
    "tour":   ("examples/programs/tour.ddp",   lambda n, e, c: [f"load p 0 {graph(n, e, c)}", "feed p 1 0"]),
    "adt":    ("examples/programs/adt.ddp",    lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
    "ast":    ("examples/programs/ast.ddp",    lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
    "unnest": ("examples/programs/unnest.ddp", lambda n, e, c: [f"load p 0 {graph(n, e, c)}"]),
}

UNITS = {"ns": 1e-9, "µs": 1e-6, "ms": 1e-3, "s": 1.0}
TICK = re.compile(r"^tick -> epoch (\d+) \(([\d.]+)(ns|µs|ms|s)\)$")
INSTALLED = re.compile(r'^installed "p" \((\d+) ops\)$')
LOADED = re.compile(r"^loaded (\d+) rows")


def run_session(lines, backend, workers, timeout=3600, wrap=None):
    """Run one session on the server; return (ops, rows, [(epoch, seconds)]) or raise."""
    cmd = [SERVER, f"--backend={backend}", f"-w{workers}"]
    if wrap:
        cmd = wrap + cmd
    proc = subprocess.run(cmd, input="\n".join(lines) + "\nexit\n", capture_output=True,
                          text=True, timeout=timeout, cwd=CRATE)
    ops = rows = None
    ticks = []
    for line in proc.stdout.splitlines():
        if m := INSTALLED.match(line):
            ops = int(m.group(1))
        elif m := LOADED.match(line):
            rows = int(m.group(1))
        elif m := TICK.match(line):
            ticks.append((int(m.group(1)), float(m.group(2)) * UNITS[m.group(3)]))
        elif line.startswith("error:") or "panicked" in line:
            raise RuntimeError(line)
    if proc.returncode != 0 or "panicked" in proc.stderr:
        raise RuntimeError(proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else f"exit {proc.returncode}")
    return ops, rows, ticks


def session(name, nodes, edges, churn, rounds):
    program, build = WORKLOADS[name]
    return [f"install p {without_inspects(program)}", *build(nodes, edges, churn), "tick", f"tick {rounds}"]


def without_inspects(program):
    """A copy of the program with its `| inspect(..)` taps removed, in `bench/.tmp/`.

    The example programs print what they compute; unnest's tap on 200k rows was 90% of
    its "load" time (one unbuffered stderr write per row). The taps are for reading, not
    for timing, so the benchmark runs the programs without them."""
    src = open(os.path.join(CRATE, program)).read()
    stripped = re.sub(r"\|\s*inspect\([^)]*\)", "", src)
    tmp = os.path.join(HERE, ".tmp")
    os.makedirs(tmp, exist_ok=True)
    path = os.path.join(tmp, os.path.basename(program))
    with open(path, "w") as f:
        f.write(stripped)
    return path


def aoc_sessions(backend):
    """One (label, session) per AoC part, inputs regenerated by transcribe.py."""
    pad = ["--pad"] if backend == "corgi" else []
    subprocess.run([sys.executable, "transcribe.py", *pad], cwd=AOC, check=True, capture_output=True)
    out = []
    for line in open(os.path.join(AOC, "expected.txt")):
        parts = line.split()
        if not parts or parts[0].startswith("#"):
            continue
        day, part = parts[0], parts[1]
        inp = f"gen/day{day}/input.txt"
        for cand in (f"gen/day{day}/input{part}.txt", f"gen/day{day}/input{part}p.txt" if pad else None):
            if cand and os.path.exists(os.path.join(AOC, cand)):
                inp = cand
        out.append((f"aoc{day}p{part}", [f"install p {AOC}/day{day}/part{part}.ddp", f"load p 0 {AOC}/{inp}", "tick"]))
    return out


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def fmt(seconds):
    if seconds is None:
        return "-"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.0f}µs"
    if seconds < 1:
        return f"{seconds * 1e3:.1f}ms"
    return f"{seconds:.2f}s"


def summarize(records, path):
    """Median per (workload, backend, workers); corgi/vec ratio alongside."""
    by = {}
    for r in records:
        by.setdefault((r["workload"], r["backend"], r["workers"]), []).append(r)
    workloads = sorted({k[0] for k in by}, key=lambda w: (w.startswith("aoc"), w))
    configs = sorted({(k[1], k[2]) for k in by}, key=lambda c: (c[1], c[0]))
    lines = []
    for phase, key in (("initial epoch (load + first computation)", "load_s"),
                       ("per churn epoch (median over the run's rounds)", "round_s")):
        if all(r.get(key) is None for r in records):
            continue
        lines.append(f"### {phase}\n")
        head = ["workload"] + [f"{b} w{w}" for b, w in configs]
        if any(b == "corgi" for b, _ in configs) and any(b == "vec" for b, _ in configs):
            head += [f"vec/corgi w{w}" for w in sorted({w for _, w in configs})]
        lines.append("| " + " | ".join(head) + " |")
        lines.append("|" + "---|" * len(head))
        for wl in workloads:
            row = [wl]
            med = {}
            for b, w in configs:
                rs = [r[key] for r in by.get((wl, b, w), []) if r.get(key) is not None]
                med[(b, w)] = statistics.median(rs) if rs else None
                row.append(fmt(med[(b, w)]) + (f" (n={len(rs)})" if rs and len(rs) > 1 else ""))
            if any(b == "corgi" for b, _ in configs) and any(b == "vec" for b, _ in configs):
                for w in sorted({w for _, w in configs}):
                    v, c = med.get(("vec", w)), med.get(("corgi", w))
                    row.append(f"{v / c:.2f}x" if v and c else "-")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", default="small", choices=SCALES)
    ap.add_argument("--backends", default="vec,corgi")
    ap.add_argument("--workers", default="1")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--only", help="comma-separated workload names")
    ap.add_argument("--aoc", action="store_true", help="run the AoC parts instead of the graph workloads")
    ap.add_argument("--profile", help="record one workload under samply instead of timing it")
    ap.add_argument("--backend", help="backend for --profile", default="corgi")
    ap.add_argument("--label", default="", help="suffix for the report file names")
    ap.add_argument("--no-build", action="store_true")
    args = ap.parse_args()

    if not args.no_build:
        subprocess.run(["cargo", "build", "--release", "-p", "interactive", "--example", "ddir_server"],
                       cwd=ROOT, check=True)
    rev = git("rev-parse", "--short", "HEAD")
    dirty = bool(git("status", "--porcelain", "--", "interactive"))
    stamp = dt.date.today().isoformat()
    nodes, edges, churn, rounds = SCALES[args.scale]
    workers = [int(w) for w in args.workers.split(",")]

    if args.profile:
        os.makedirs(os.path.join(HERE, "reports", "profiles"), exist_ok=True)
        w = workers[0]
        out = os.path.join(HERE, "reports", "profiles", f"{stamp}-{args.profile}-{args.backend}-w{w}-{args.scale}.json.gz")
        wrap = ["samply", "record", "--save-only", "-o", out, "--"]
        print(f"recording {args.profile} on {args.backend} w{w} at {args.scale} -> {out}")
        print(run_session(session(args.profile, nodes, edges, churn, rounds), args.backend, w, wrap=wrap))
        return

    records = []
    for backend in args.backends.split(","):
        if args.aoc:
            jobs = [(label, sess, None) for label, sess in aoc_sessions(backend)]
        else:
            names = args.only.split(",") if args.only else list(WORKLOADS)
            jobs = [(n, session(n, nodes, edges, churn, rounds), rounds) for n in names]
        for w in workers:
            for label, sess, rnds in jobs:
                for run in range(args.runs):
                    try:
                        ops, rows, ticks = run_session(sess, backend, w)
                    except Exception as e:  # noqa: BLE001 — a failing workload is a result, not a crash
                        print(f"{label:>10} {backend:>5} w{w} run{run}: FAILED {e}", file=sys.stderr)
                        records.append({"workload": label, "backend": backend, "workers": w, "run": run, "error": str(e)})
                        continue
                    load_s = ticks[0][1] if ticks else None
                    round_s = (ticks[1][1] / rnds) if (rnds and len(ticks) > 1) else None
                    rec = {"date": stamp, "rev": rev, "dirty": dirty, "scale": args.scale,
                           "workload": label, "backend": backend, "workers": w, "run": run,
                           "nodes": nodes, "edges": edges, "churn": churn, "rounds": rnds,
                           "ops": ops, "rows": rows, "load_s": load_s, "round_s": round_s,
                           "total_s": sum(t for _, t in ticks)}
                    records.append(rec)
                    print(f"{label:>10} {backend:>5} w{w} run{run}: load {fmt(load_s)}"
                          + (f", per round {fmt(round_s)}" if round_s else ""), file=sys.stderr)

    kind = "aoc" if args.aoc else args.scale
    base = os.path.join(HERE, "reports", f"{stamp}-{rev}-{kind}{('-' + args.label) if args.label else ''}")
    with open(base + ".jsonl", "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(summarize(records, base + ".md"))
    print(f"\nwrote {base}.jsonl and .md", file=sys.stderr)


if __name__ == "__main__":
    main()
