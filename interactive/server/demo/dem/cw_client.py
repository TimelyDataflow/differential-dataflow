#!/usr/bin/env python3
"""Civil-works participant client (see PROTOCOL.md).

  python3 cw_client.py --port P --agent N <command>

Commands:
  sync                 fetch terrain into the local cache (run once first)
  around X Y R         print terrain (and water depth) in a (2R+1)^2 window
  water                lake summary: level histogram of wet cells, count
  edit X Y H           terraform: dual-writes terrain + ledger, updates cache
                       (refused outside road ACCESS when works is loaded)
  road X Y             build a road (cost 10; usable only while dry)
  bridge X Y           build a bridge (cost 50; usable over water)
  net                  road network summary: connectivity, access, road spend
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
        try:
            roads = parse_rows(c.cmd("peek roads", collect=True))
        except RuntimeError:
            roads = {}
        print("terrain (rows are y, cols are x); [depth] wet, R road, B bridge:")
        for y in range(y0 - r, y0 + r + 1):
            row = []
            for x in range(x0 - r, x0 + r + 1):
                h = t.get(f"{x},{y}")
                if h is None:
                    row.append("  ----  ")
                else:
                    d = wet.get((x, y), h) - h
                    mark = ""
                    if (x, y) in roads:
                        mark = "B" if roads[(x, y)][1] == 1 else "R"
                    tag = (f"[{d}]" if d > 0 else "") + mark
                    row.append(f"{h:>5}" + f"{tag:<3}")
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
        try:
            access = parse_rows(c.cmd("peek access", collect=True))
        except RuntimeError:
            access = None  # no works program loaded: access unconstrained
        if access is not None and (x, y) not in access:
            print(f"REFUSED: ({x},{y}) is outside road access "
                  f"(network + 3 cells). Build roads toward it first.")
            sys.exit(2)
        c.cmd(f"feed water 0 {x},{y} val={old} diff=-1")
        c.cmd(f"feed water 0 {x},{y} val={nh}")
        c.cmd(f"feed ledger 0 {x},{y} val={args.agent},{old},{nh}")
        t[f"{x},{y}"] = nh
        json.dump(t, open(cache_path, "w"))
        print(f"terraformed ({x},{y}) {old} -> {nh}; cost {abs(nh - old)}")
    elif cmd == "road" or cmd == "bridge":
        x, y = int(args.cmd[1]), int(args.cmd[2])
        kind = 1 if cmd == "bridge" else 0
        c.cmd(f"feed works 0 {x},{y} val={args.agent},{kind}")
        print(f"{cmd} at ({x},{y}); cost {50 if kind else 10}")
    elif cmd == "net":
        roads = parse_rows(c.cmd("peek roads", collect=True))
        net = parse_rows(c.cmd("peek road_net", collect=True))
        acc = parse_rows(c.cmd("peek access", collect=True))
        print(f"{len(roads)} road/bridge cells; {len(net)} connected to the depot; "
              f"access covers {len(acc)} cells")
        stranded = sorted(set(roads) - set(net))
        if stranded:
            print(f"NOT connected: {stranded[:12]}{'...' if len(stranded) > 12 else ''}")
        for k, v in sorted(parse_rows(c.cmd("peek road_spend", collect=True)).items()):
            print(f"agent {k[0]}: road spend {v[0]}")
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
