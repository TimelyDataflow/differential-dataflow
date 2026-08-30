#!/usr/bin/env python3
"""Civil-works scenario 2: asymmetric roles over roads + water (MECHANICS.md).

Nature dams the gorge and floods the village, as in civil.py — but now the
ROADWRIGHT (agent 1: roads/bridges only) must extend the depot's network to
within access range (3 cells) of any dig, before the TERRAFORMER (agent 2:
terraform only) can act there. Roads need grade <= 12 between neighbours
(steep ground must be cut by the terraformer first) and drown unless
bridged. Neither role is sufficient alone.

  python3 civil_roads.py setup [--port 7999]
  python3 civil_roads.py judge [--port 7999]
"""

import argparse
import heapq
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run_dem import BIN, Client, load_grid, priority_flood
from run_physics import parse_rows

DAM_X = 96
DAM_CREST = 1775
GRADE = 12
ACCESS_R = 3
ROAD_COST, BRIDGE_COST = 10, 50
WORK_ZONE = [(x, y) for x in (94, 95, 96, 97, 98) for y in range(26, 38)]


def nbrs(c):
    x, y = c
    return [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]


def py_road_views(acts, anchors, terrain, water):
    """Ground truth for works.ddp: usable cells, graded edges, network, access."""
    usable = {c for c, (agent, kind) in acts.items()
              if kind == 1 or water.get(c, terrain[c]) == terrain[c]}
    kind = {c: k for c, (_, k) in acts.items()}
    net = set()
    frontier = [a for a in anchors if a in usable or a in anchors]
    net.update(frontier)
    while frontier:
        nxt = []
        for c in frontier:
            for n in nbrs(c):
                if (n in usable and n not in net
                        and (kind.get(c) == 1 or kind.get(n) == 1
                             or abs(terrain[c] - terrain[n]) <= GRADE)):
                    net.add(n)
                    nxt.append(n)
        frontier = nxt
    access = set(net)
    for _ in range(ACCESS_R):
        access |= {n for c in access for n in nbrs(c)}
    return usable, net, access & set(terrain)


def feasibility(terrain, water, depot):
    """Cheapest road path depot -> within ACCESS_R of the work zone, pricing
    wet cells as bridges and grade violations as terraform assists."""
    targets = {n for c in WORK_ZONE for n in [c]}
    dist = {depot: 0}
    heap = [(0, 0, depot)]  # (road+bridge cost, assist cost, cell)
    best = None
    while heap:
        d, assist, c = heapq.heappop(heap)
        if dist.get(c, 10**9) < d:
            continue
        if any(abs(c[0] - t[0]) + abs(c[1] - t[1]) <= ACCESS_R for t in targets):
            best = (d, assist, c)
            break
        for n in nbrs(c):
            if n not in terrain:
                continue
            wet_c = water.get(c, terrain[c]) > terrain[c]
            wet_n = water.get(n, terrain[n]) > terrain[n]
            step = BRIDGE_COST if wet_n else ROAD_COST
            # Bridges span grade; between two plain dry roads a violation
            # is impassable (the terraformer would have to cut, priced 0
            # here -- report only routes that need no assist).
            if not wet_c and not wet_n and abs(terrain[c] - terrain[n]) > GRADE:
                continue
            nd = d + step
            if nd < dist.get(n, 10**9):
                dist[n] = nd
                heapq.heappush(heap, (nd, assist, n))
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["setup", "judge"])
    ap.add_argument("--port", type=int, default=7999)
    args = ap.parse_args()
    briefing_path = os.path.join(HERE, "briefing_roads.json")
    pid_path = os.path.join(HERE, "civil_roads.pid")

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
        for name in ("water", "ledger", "works"):
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

        # Depot: a dry, flat-ish 2x2 in the mid valley. Nature roads it.
        depot = None
        for y in range(50, 62):
            for x in range(72, 84):
                blk = [(x, y), (x + 1, y), (x, y + 1), (x + 1, y + 1)]
                hs = [terrain[b] for b in blk if b in terrain]
                if len(hs) == 4 and max(hs) - min(hs) <= GRADE:
                    depot = (x, y)
                    break
            if depot:
                break
        assert depot, "no depot site found"
        dblk = [(depot[0], depot[1]), (depot[0] + 1, depot[1]),
                (depot[0], depot[1] + 1), (depot[0] + 1, depot[1] + 1)]
        for (x, y) in dblk:
            c.cmd(f"feed works 0 {x},{y} val=0,0")
            c.cmd(f"feed works 1 {x},{y}")

        # Nature dams the gorge (ledgered, agent 0).
        dam = [(x, y) for (x, y) in terrain
               if x == DAM_X and terrain[(x, y)] < DAM_CREST]
        for cell in dam:
            old = terrain[cell]
            c.cmd(f"feed water 0 {cell[0]},{cell[1]} val={old} diff=-1")
            c.cmd(f"feed water 0 {cell[0]},{cell[1]} val={DAM_CREST}")
            c.cmd(f"feed ledger 0 {cell[0]},{cell[1]} val=0,{old},{DAM_CREST}")
        c.cmd("tick")

        water = {k: v[0] for k, v in parse_rows(c.cmd("peek water", collect=True)).items()}
        flooded = {k for k, w in water.items() if w > terrain.get(k, 10**9) and k not in dam}
        village = sorted((k for k in flooded if 80 <= k[0] <= 92 and k[1] <= 45),
                         key=lambda k: terrain[k])[:4]
        feas = feasibility(terrain, water, depot)
        road_budget = int(feas[0] * 5 // 4 // 10 * 10)
        briefing = {
            "port": args.port,
            "roles": {"1": "ROADWRIGHT (road/bridge acts only)",
                      "2": "TERRAFORMER (terraform acts only)"},
            "budgets": {"1": road_budget, "2": 1000},
            "village": village,
            "locked": sorted(dam),
            "depot": dblk,
            "grade_limit": GRADE,
            "access_radius": ACCESS_R,
            "costs": {"road": ROAD_COST, "bridge": BRIDGE_COST, "terraform": "|dh|"},
            "lake_level": max(water[k] for k in village),
        }
        json.dump(briefing, open(briefing_path, "w"), indent=1)
        print(f"depot at {dblk}; village {village}; lake {briefing['lake_level']}")
        print(f"feasibility: cheapest road route to the work zone costs ~{feas[0]}"
              f" (+{feas[1]} of grade-assist terraform), reaching {feas[2]}; "
              f"roadwright budget {road_budget}")

    else:  # judge
        briefing = json.load(open(briefing_path))
        c = Client(args.port)
        c.cmd("tick")
        terrain_now = {k: v[0] for k, v in parse_rows(c.cmd("peek terrain", collect=True)).items()}
        water = {k: v[0] for k, v in parse_rows(c.cmd("peek water", collect=True)).items()}
        acts = {k: (v[0], v[1]) for k, v in parse_rows(c.cmd("peek roads", collect=True)).items()}
        net_srv = set(parse_rows(c.cmd("peek road_net", collect=True)))
        acc_srv = set(parse_rows(c.cmd("peek access", collect=True)))
        spend_t = {k[0]: v[0] for k, v in parse_rows(c.cmd("peek spend", collect=True)).items()}
        spend_r = {k[0]: v[0] for k, v in parse_rows(c.cmd("peek road_spend", collect=True)).items()}
        ledger = c.cmd("peek ledger", collect=True)

        base = load_grid(os.path.join(HERE, "engadin_128.txt"))
        declared = dict(base)
        terra_by_agent = {}
        for line in ledger:
            key = line.split("key=Tuple([")[1].split("])")[0]
            val = line.split("val=Tuple([")[1].split("])")[0]
            x, y = (int(p.split("(")[1].rstrip(")")) for p in key.split(", "))
            agent, old, new = (int(p.split("(")[1].rstrip(")")) for p in val.split(", "))
            declared[(x, y)] += new - old
            terra_by_agent.setdefault(agent, []).append((x, y))

        anchors = {tuple(d) for d in briefing["depot"]}
        usable, net_py, acc_py = py_road_views(acts, anchors, terrain_now, water)
        dry = {tuple(v): water[tuple(v)] == terrain_now[tuple(v)]
               for v in briefing["village"]}
        intact = {tuple(v): abs(terrain_now[tuple(v)] - base[tuple(v)]) <= 2
                  for v in briefing["village"]}
        role_ok = (all(a in (0, 1) for a, _ in acts.values())
                   and all(a in (0, 2) for a in terra_by_agent))
        access_ok = all(c in acc_py for c in terra_by_agent.get(2, []))
        checks = {
            "village dry": all(dry.values()),
            "village intact (no burial; +-2 of original)": all(intact.values()),
            "audit terrain==base+ledger": declared == terrain_now,
            "ground truth net==server": net_py == net_srv,
            "ground truth access==server": acc_py == acc_srv,
            "roles respected": role_ok,
            "terraform within access": access_ok,
            "roadwright on budget": spend_r.get(1, 0) <= briefing["budgets"]["1"],
            "terraformer on budget": spend_t.get(2, 0) <= briefing["budgets"]["2"],
        }
        print("== JUDGMENT ==")
        for k, v in checks.items():
            print(f"  {k}: {'OK' if v else 'FAIL'}")
        print(f"  spends: roads {spend_r}, terraform {spend_t}")
        print(f"  village: {dry}")
        print(f"VERDICT: {'SUCCESS' if all(checks.values()) else 'FAILURE'}")
        try:
            os.kill(int(open(pid_path).read()), 15)
        except Exception:
            pass


if __name__ == "__main__":
    main()
