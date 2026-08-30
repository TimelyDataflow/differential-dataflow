#!/usr/bin/env python3
"""Civil-works participant client (see PROTOCOL.md).

  python3 cw_client.py --port P --agent N <command>

Commands:
  sync                 fetch terrain into the local cache (run once first)
  around X Y R         print terrain (and water depth) in a (2R+1)^2 window
  water                lake summary: level histogram of wet cells, count
  edit X Y H           terraform: dual-writes terrain + ledger, updates cache
  tick                 advance the world and let it equilibrate
  spend                per-agent expenditure, from the server's auditor view
"""

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run_dem import Client
from run_physics import parse_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--agent", type=int, required=True)
    ap.add_argument("cmd", nargs="+")
    args = ap.parse_args()
    cache_path = os.path.join(HERE, f"cw_cache_{args.agent}.json")
    c = Client(args.port)
    cmd = args.cmd[0]

    def terrain_cache():
        if not os.path.exists(cache_path):
            rows = parse_rows(c.cmd("peek terrain", collect=True))
            json.dump({f"{x},{y}": v[0] for (x, y), v in rows.items()},
                      open(cache_path, "w"))
        return json.load(open(cache_path))

    if cmd == "sync":
        if os.path.exists(cache_path):
            os.remove(cache_path)
        t = terrain_cache()
        print(f"cached {len(t)} cells")
    elif cmd == "around":
        x0, y0, r = int(args.cmd[1]), int(args.cmd[2]), int(args.cmd[3])
        t = terrain_cache()
        wet = {k: v[0] for k, v in parse_rows(c.cmd("peek water", collect=True)).items()}
        print("terrain (rows are y, cols are x); [depth] where wet:")
        for y in range(y0 - r, y0 + r + 1):
            row = []
            for x in range(x0 - r, x0 + r + 1):
                h = t.get(f"{x},{y}")
                if h is None:
                    row.append("  ----  ")
                else:
                    d = wet.get((x, y), h) - h
                    row.append(f"{h:>5}" + (f"[{d}]" if d > 0 else "   "))
            print(f"y={y:>3} " + "".join(row))
    elif cmd == "water":
        t = terrain_cache()
        wet = {}
        for (x, y), v in parse_rows(c.cmd("peek water", collect=True)).items():
            h = t.get(f"{x},{y}")
            if h is not None and v[0] > h:
                wet[(x, y)] = v[0]
        from collections import Counter
        levels = Counter(wet.values())
        print(f"{len(wet)} wet cells; levels: {sorted(levels.items())}")
    elif cmd == "edit":
        x, y, nh = int(args.cmd[1]), int(args.cmd[2]), int(args.cmd[3])
        t = terrain_cache()
        old = t.get(f"{x},{y}")
        assert old is not None, "no such cell"
        c.cmd(f"feed water 0 {x},{y} val={old} diff=-1")
        c.cmd(f"feed water 0 {x},{y} val={nh}")
        c.cmd(f"feed ledger 0 {x},{y} val={args.agent},{old},{nh}")
        t[f"{x},{y}"] = nh
        json.dump(t, open(cache_path, "w"))
        print(f"terraformed ({x},{y}) {old} -> {nh}; cost {abs(nh - old)}")
    elif cmd == "tick":
        c.cmd("tick")
        print("ticked")
    elif cmd == "spend":
        for k, v in sorted(parse_rows(c.cmd("peek spend", collect=True)).items()):
            print(f"agent {k[0]}: spent {v[0]}")
    else:
        print("unknown command", cmd)
        sys.exit(1)


if __name__ == "__main__":
    main()
