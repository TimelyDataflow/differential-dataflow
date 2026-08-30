#!/usr/bin/env python3
"""Three physics programs composing on the live server over one real DEM:
water (equilibrium fill), flow (routing + drainage accumulation — rivers),
and snow (abelian sandpile). Every phase is cross-checked exactly against
independent Python implementations.

Phases:
  1. init    — load all three programs, feed the terrain, tick.
             water == priority flood; accum == topological accumulation.
             The Inn appears as the high-accumulation spine.
  2. dig     — cut a trench across the valley: the river reroutes through
             it; water and accumulation re-derive incrementally.
  3. snow    — drop a pile of grains on one cell: the sandpile relaxes
             (== worklist toppling); then one more grain, and the
             avalanche is the measured delta.

  python3 run_physics.py [--grid engadin_128.txt] [--port 7995]
"""

import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run_dem import BIN, Client, load_grid, priority_flood

RIVER_MIN = 50    # accumulation threshold for calling a cell "river"
SNOW_AT = (60, 60)
SNOW_N = 500


def parse_rows(data_lines):
    """diff=d key=Tuple([Int..]) val=Tuple([Int..]) -> {key_tuple: val_tuple}"""
    out = {}
    for line in data_lines:
        key = line.split("key=Tuple([")[1].split("])")[0]
        val = line.split("val=Tuple([")[1].split("])")[0]
        k = tuple(int(p.split("(")[1].rstrip(")")) for p in key.split(", "))
        v = tuple(int(p.split("(")[1].rstrip(")")) for p in val.split(", ")) if val else ()
        out[k] = v
    return out


def py_flow_accum(water):
    """Ground truth: lex-(w,x,y) argmin routing, then accumulation."""
    def key(c):
        return (water[c], c[0], c[1])
    flow = {}
    for c in water:
        x, y = c
        nbrs = [n for n in [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]
                if n in water and key(n) < key(c)]
        if nbrs:
            flow[c] = min(nbrs, key=key)
    accum = {c: 1 for c in water}
    for c in sorted(water, key=key, reverse=True):
        if c in flow:
            accum[flow[c]] += accum[c]
    return flow, accum

def py_sandpile(cells, drops):
    """Ground truth: worklist toppling with boundary drain."""
    pile = dict(drops)
    odo = {}
    work = [c for c, n in pile.items() if n >= 4]
    while work:
        c = work.pop()
        if pile.get(c, 0) < 4:
            continue
        t = pile[c] // 4
        pile[c] -= 4 * t
        odo[c] = odo.get(c, 0) + t
        x, y = c
        for n in [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]:
            if n in cells:
                pile[n] = pile.get(n, 0) + t
                if pile[n] >= 4:
                    work.append(n)
    return {c: n for c, n in pile.items() if n > 0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grid", default=os.path.join(HERE, "engadin_128.txt"))
    ap.add_argument("--port", type=int, default=7995)
    args = ap.parse_args()

    terrain = load_grid(args.grid)
    print(f"terrain: {len(terrain)} cells")

    env = dict(
        os.environ,
        DDIR_BIND=f"127.0.0.1:{args.port}",
        DDIR_WS_BIND=f"127.0.0.1:{args.port + 1}",
        DDIR_DIAG_PORT=str(args.port + 2),
        DDIR_TICK_MS="0",
    )
    server = subprocess.Popen(
        [BIN], env=env, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, text=True)
    try:
        c = Client(args.port)
        for name in ("water", "flow", "snow"):
            src = open(os.path.join(HERE, f"{name}.ddp")).read()
            c.send_lines([f"r{name} load {name} begin"] + src.splitlines()
                         + [f"r{name} end-load"])
            while True:
                toks = c.read_line().split(" ", 2)
                if toks[0] == f"r{name}":
                    assert toks[1] == "ok", (name, toks)
                    break

        def feed(prog, items):
            lines = [f"feed {prog} 0 {x},{y} val={v} diff={d}" for (x, y), v, d in items]
            for i in range(0, len(lines), 2000):
                chunk = lines[i:i + 2000]
                c.send_lines(chunk)
                c.drain_oks(len(chunk))

        def snapshot(trace):
            return parse_rows(c.cmd(f"peek {trace}", collect=True))

        # --- init ---
        t0 = time.time()
        feed("water", [(cell, h, 1) for cell, h in terrain.items()])
        c.cmd("tick")
        dt = time.time() - t0
        water = {k: v[0] for k, v in snapshot("water").items()}
        accum = {k: v[0] for k, v in snapshot("accum").items()}
        assert water == priority_flood(terrain), "water disagrees with priority flood"
        pf, pa = py_flow_accum(water)
        assert accum == pa, "accumulation disagrees with topological ground truth"
        rivers = {k for k, a in accum.items() if a >= RIVER_MIN}
        print(f"init: {dt:.2f}s; max accumulation {max(accum.values())}, "
              f"{len(rivers)} river cells (A >= {RIVER_MIN}) — the Inn is visible")

        # --- dig a trench and watch the river reroute ---
        outlet = max(accum, key=lambda k: accum[k])
        trench = [(x, y) for (x, y) in terrain
                  if 88 <= x <= 92 and y == 30 and terrain[(x, y)] > 1700]
        edits = []
        for cell in trench:
            edits.append((cell, terrain[cell], -1))
            edits.append((cell, 1698, 1))
            terrain[cell] = 1698
        t0 = time.time()
        feed("water", edits)
        c.cmd("tick")
        dt = time.time() - t0
        water2 = {k: v[0] for k, v in snapshot("water").items()}
        accum2 = {k: v[0] for k, v in snapshot("accum").items()}
        assert water2 == priority_flood(terrain), "post-dig water disagrees"
        pf2, pa2 = py_flow_accum(water2)
        assert accum2 == pa2, "post-dig accumulation disagrees"
        moved = sum(1 for k in accum2 if accum2[k] != accum.get(k, 0))
        rivers2 = {k for k, a in accum2.items() if a >= RIVER_MIN}
        print(f"dig ({len(trench)} cells to 1698): {dt:.2f}s; "
              f"{moved} cells changed accumulation; river cells "
              f"{len(rivers)} -> {len(rivers2)} "
              f"(+{len(rivers2 - rivers)} new, -{len(rivers - rivers2)} gone)")

        # --- snow: a 500-grain dump, then the one-grain avalanche ---
        cells = set(terrain)
        t0 = time.time()
        feed("snow", [(SNOW_AT, "_", SNOW_N)])
        c.cmd("tick")
        dt = time.time() - t0
        pile = {k: v[0] for k, v in snapshot("pile").items()}
        truth = py_sandpile(cells, {SNOW_AT: SNOW_N})
        assert pile == truth, "sandpile disagrees with worklist toppling"
        print(f"snow: {SNOW_N} grains at {SNOW_AT} relax in {dt:.2f}s to "
              f"{len(pile)} cells (max height {max(pile.values())})")

        t0 = time.time()
        feed("snow", [(SNOW_AT, "_", 1)])
        c.cmd("tick")
        dt = time.time() - t0
        pile2 = {k: v[0] for k, v in snapshot("pile").items()}
        truth2 = py_sandpile(cells, {SNOW_AT: SNOW_N + 1})
        assert pile2 == truth2, "post-grain sandpile disagrees"
        aval = sum(1 for k in set(pile) | set(pile2) if pile.get(k) != pile2.get(k))
        print(f"one more grain: {dt:.2f}s; the avalanche touched {aval} cells")

        print("\nall phases agree exactly with the independent implementations")
    finally:
        try:
            server.stdin.write("exit\n")
            server.stdin.flush()
            server.wait(timeout=10)
        except Exception:
            server.kill()


if __name__ == "__main__":
    main()
