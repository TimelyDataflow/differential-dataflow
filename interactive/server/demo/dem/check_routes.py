#!/usr/bin/env python3
"""Cross-check DDIR's route fixed point against an independent Dijkstra.

`pathways.ddp` encodes shortest paths as a `min` over `(total_cost,
predecessor_x, predecessor_y)`: the leading cost makes `min` Dijkstra's
relaxation, and the trailing coordinates break ties lexicographically, so a
second scope can walk predecessors back from the target. `pathways_rules.py`
settles the same lexicographic pair with a heap, so the two agree cell for
cell — not merely on total cost.

The judge compares route costs; this driver compares the geometry those costs
are supposed to justify, over a small synthetic board with water, a drainage
field, an established path, and a road, across reuse weights 0, 1, and 3.

    python3 check_routes.py [--port 7961]
"""

import argparse
import os
import random
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run_dem import BIN, Client
from logistics_runtime import feed_chunks, load_program, unique_rows
from pathways_game import route_geometry
from pathways_rules import shortest_route

N = 14
STEPS = (26, 37)

# (route_id, start, target, (distance, grade, water, runoff, reuse), max grade)
CASES = [
    (1, (1, 1), (12, 12), (1, 0, 0, 0, 0), None),
    (2, (1, 1), (12, 12), (1, 4, 1000, 8, 0), None),
    (3, (1, 1), (12, 12), (1, 4, 1000, 8, 1), None),
    (4, (2, 10), (11, 2), (1, 4, 1000, 8, 3), None),
    (5, (0, 13), (13, 0), (1, 2, 500, 4, 1), None),
    (6, (1, 1), (12, 12), (1, 4, 1000, 8, 1), 450),
]


def synthetic_terrain():
    """A valley at x=6 and a ridge from x=10, with reproducible noise."""
    random.seed(7)
    return {
        (x, y): 1700 + abs(x - 6) * 9 + (14 if x >= 10 else 0)
                + random.randint(0, 6) + y // 3
        for y in range(N) for x in range(N)
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=7961)
    args = ap.parse_args()

    terrain = synthetic_terrain()
    env = dict(os.environ,
               DDIR_BIND=f"127.0.0.1:{args.port}",
               DDIR_WS_BIND=f"127.0.0.1:{args.port + 1}",
               DDIR_DIAG_PORT=str(args.port + 2),
               DDIR_TICK_MS="0")
    server = subprocess.Popen([BIN], env=env, stdin=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              start_new_session=True)
    failures = 0
    try:
        c = Client(args.port)
        for name in ("water", "flow", "pathways"):
            load_program(c, name, os.path.join(HERE, f"{name}.ddp"))
        feed_chunks(c, [f"feed water 0 {x},{y} val={h}" for (x, y), h in terrain.items()])
        c.cmd(f"feed pathways 5 0 val={STEPS[0]},{STEPS[1]}")
        c.cmd("feed pathways 7 0 val=2,400,17,24,0,50")

        # Two traversals establish a path; a separate column is public road.
        for trip in (1, 2):
            for y in range(2, 9):
                c.cmd(f"feed pathways 2 {trip},3,{y} val=1,900")
        for y in range(3, 10):
            c.cmd(f"feed pathways 3 9,{y} val=1,1")
        for route_id, (sx, sy), (tx, ty), coefficients, cap in CASES:
            c.cmd(f"feed pathways 1 {route_id} val=1,{sx},{sy},{tx},{ty},"
                  + ",".join(str(v) for v in coefficients))
            if cap is not None:
                c.cmd(f"feed pathways 8 {route_id} val={cap}")
        c.cmd("tick")

        def rows(name):
            return unique_rows(c.cmd(f"peek {name}", collect=True), name)

        water = {k: v[0] for k, v in rows("water").items()}
        accum = {k: v[0] for k, v in rows("accum").items()}
        path_use = {k: v[0] for k, v in rows("path_use").items()}
        infrastructure = {k: (v[0], v[1]) for k, v in rows("infrastructure").items()}
        costs = {k[0]: v[0] for k, v in rows("route_cost").items()}
        steps = rows("route_steps")

        for route_id, start, target, coefficients, cap in CASES:
            ddir_path = route_geometry(steps, route_id, start, target)
            expected_path, expected_cost = shortest_route(
                terrain, water, accum, start, target, coefficients,
                path_use, infrastructure, STEPS, cap)
            cost_ok = costs.get(route_id) == expected_cost
            path_ok = ddir_path == expected_path
            failures += not (cost_ok and path_ok)
            ddir_cells = "unresolved" if ddir_path is None else len(ddir_path)
            expected_cells = (
                "unresolved" if expected_path is None else len(expected_path)
            )
            print(("PASS " if cost_ok and path_ok else "FAIL ")
                  + f"route {route_id} reuse={coefficients[4]} cap={cap}: "
                  + f"cost {costs.get(route_id)} vs {expected_cost}, "
                  + f"{ddir_cells} cells vs {expected_cells}")
            if not path_ok:
                print(f"       ddir {ddir_path}")
                print(f"       true {expected_path}")
    finally:
        server.kill()

    print("all routes match the independent Dijkstra" if not failures
          else f"{failures} route(s) diverged")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
