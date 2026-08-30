#!/usr/bin/env python3
"""Civil-works scenario driver (see PROTOCOL.md).

  python3 civil.py setup [--port 7997]   start the world, dam the gorge,
                                         flood the valley, pick the village,
                                         write briefing.json
  python3 civil.py judge [--port 7997]   tick, audit, judge, stop the server

Scenario: nature (agent 0) dams the Inn gorge; the lake rises to the dam
crest and floods the valley, village included. Participants must make every
village cell dry by judgment, each within its earthmoving budget, without
touching nature's locked dam cells.
"""

import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run_dem import BIN, Client, load_grid
from run_physics import parse_rows

DAM_X = 96
DAM_CREST = 1775
BUDGET = 600


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["setup", "judge"])
    ap.add_argument("--port", type=int, default=7997)
    args = ap.parse_args()
    briefing_path = os.path.join(HERE, "briefing.json")
    pid_path = os.path.join(HERE, "civil_server.pid")

    if args.mode == "setup":
        for f in os.listdir(HERE):
            if f.startswith("cw_cache_"):
                os.remove(os.path.join(HERE, f))
        env = dict(os.environ,
                   DDIR_BIND=f"127.0.0.1:{args.port}",
                   DDIR_WS_BIND=f"127.0.0.1:{args.port + 1}",
                   DDIR_DIAG_PORT=str(args.port + 2),
                   DDIR_TICK_MS="0")
        server = subprocess.Popen(
            [BIN], env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True)
        open(pid_path, "w").write(str(server.pid))

        c = Client(args.port)
        for name in ("water", "ledger"):
            src = open(os.path.join(HERE, f"{name}.ddp")).read()
            c.send_lines([f"r{name} load {name} begin"] + src.splitlines()
                         + [f"r{name} end-load"])
            while True:
                toks = c.read_line().split(" ", 2)
                if toks[0] == f"r{name}":
                    assert toks[1] == "ok", (name, toks)
                    break
        terrain = load_grid(os.path.join(HERE, "engadin_128.txt"))
        lines = [f"feed water 0 {x},{y} val={h}" for (x, y), h in terrain.items()]
        for i in range(0, len(lines), 2000):
            c.send_lines(lines[i:i + 2000])
            c.drain_oks(len(lines[i:i + 2000]))

        # Nature dams the gorge, by the protocol's own rules (agent 0).
        dam = [(x, y) for (x, y) in terrain
               if x == DAM_X and terrain[(x, y)] < DAM_CREST]
        for cell in dam:
            old = terrain[cell]
            c.cmd(f"feed water 0 {cell[0]},{cell[1]} val={old} diff=-1")
            c.cmd(f"feed water 0 {cell[0]},{cell[1]} val={DAM_CREST}")
            c.cmd(f"feed ledger 0 {cell[0]},{cell[1]} val=0,{old},{DAM_CREST}")
        c.cmd("tick")

        water = {k: v[0] for k, v in parse_rows(c.cmd("peek water", collect=True)).items()}
        flooded = {k for k, w in water.items()
                   if w > (DAM_CREST if k in dam else terrain[k])}
        flooded = {k for k, w in water.items() if w > terrain.get(k, 10**9) and k not in dam}
        candidates = sorted((k for k in flooded if 80 <= k[0] <= 92 and k[1] <= 45),
                            key=lambda k: terrain[k])
        village = candidates[:4]
        briefing = {
            "port": args.port,
            "budget": BUDGET,
            "village": village,
            "locked": sorted(dam),
            "lake_level": max(water[k] for k in village),
            "village_heights": {f"{x},{y}": terrain[(x, y)] for (x, y) in village},
        }
        json.dump(briefing, open(briefing_path, "w"), indent=1)
        print(f"flooded cells: {len(flooded)}; village {village} "
              f"(heights {list(briefing['village_heights'].values())}) under "
              f"{briefing['lake_level']} m of lake; briefing written")

    else:  # judge
        briefing = json.load(open(briefing_path))
        c = Client(args.port)
        c.cmd("tick")
        water = {k: v[0] for k, v in parse_rows(c.cmd("peek water", collect=True)).items()}
        terrain_now = {k: v[0] for k, v in parse_rows(c.cmd("peek terrain", collect=True)).items()}
        ledger = c.cmd("peek ledger", collect=True)
        spend = {k[0]: v[0] for k, v in parse_rows(c.cmd("peek spend", collect=True)).items()}

        base = load_grid(os.path.join(HERE, "engadin_128.txt"))
        # Audit 1: terrain == base + sum of declared (new - old).
        declared = dict(base)
        locked_violation = []
        for line in ledger:
            key = line.split("key=Tuple([")[1].split("])")[0]
            val = line.split("val=Tuple([")[1].split("])")[0]
            x, y = (int(p.split("(")[1].rstrip(")")) for p in key.split(", "))
            agent, old, new = (int(p.split("(")[1].rstrip(")")) for p in val.split(", "))
            declared[(x, y)] += new - old
            if agent != 0 and [x, y] in briefing["locked"]:
                locked_violation.append((agent, (x, y)))
        audit_ok = declared == terrain_now
        dry = {tuple(v): water[tuple(v)] == terrain_now[tuple(v)]
               for v in briefing["village"]}
        print("== JUDGMENT ==")
        print(f"village dry: {dry}")
        print(f"spend: {spend} (budget {briefing['budget']} each)")
        print(f"audit (terrain == base + ledger): {'CLEAN' if audit_ok else 'VIOLATION'}")
        print(f"locked-cell violations: {locked_violation or 'none'}")
        over = [a for a, s in spend.items() if a != 0 and s > briefing["budget"]]
        verdict = all(dry.values()) and audit_ok and not locked_violation and not over
        print(f"VERDICT: {'SUCCESS' if verdict else 'FAILURE'}"
              + (f" (over budget: {over})" if over else ""))
        try:
            os.kill(int(open(pid_path).read()), 15)
        except Exception:
            pass


if __name__ == "__main__":
    main()
