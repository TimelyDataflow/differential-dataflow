#!/usr/bin/env python3
"""Stage a DEM board as a live, viewable world: water physics plus the
`meta` self-description clients read instead of hardcoding geography.

    python3 stage_board.py --board engadin_256 --port 7994 --host 0.0.0.0
    python3 stage_board.py --board engadin_128 --port 7996 --meta-only
    python3 stage_board.py --board engadin_512 --port 8051 \
        --ws-host 100.123.78.70 --water-program water_gpri.ddp --with-flow

The default run starts a server (driver-owned time), loads a selectable water
program and meta.ddp, feeds the board, ticks once, and asserts the equilibrium
equals an independent priority flood before leaving the server running.
--with-flow also publishes flow.ddp's exact drainage direction and
accumulation. Use --meta-only to attach `meta` to a world someone else already
staged (for example one from civil_roads.py setup).

Meta values are derived from fetch_dem.PRESETS, so a re-windowed board
cannot drift from what it advertises.
"""

import argparse
import math
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from fetch_dem import PRESETS
from logistics_runtime import unique_rows
from run_dem import BIN, Client, load_grid, parse_water, priority_flood
from run_physics import py_flow_accum

# meta.ddp tag registry.
TAG_CELL_CM, TAG_UNIT_MM, TAG_WIDTH, TAG_HEIGHT, TAG_NW_LAT, TAG_NW_LON = range(6)


def board_meta(name, unit_mm=1000):
    """(tag, value) rows describing a board, from its fetch_dem preset."""
    preset = PRESETS[name]
    zoom, window = preset["zoom"], preset["window"]
    x0, y0, w, h = window
    tx0 = min(tx for tx, _ in preset["tiles"])
    ty0 = min(ty for _, ty in preset["tiles"])
    span = 2 ** zoom * 256

    def lat_of(py):
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty0 * 256 + py) / span))))

    nw_lat, se_lat = lat_of(y0), lat_of(y0 + h)
    nw_lon = (tx0 * 256 + x0) / span * 360 - 180
    cell_m = 156543.03 * math.cos(math.radians((nw_lat + se_lat) / 2)) / 2 ** zoom
    return [
        (TAG_CELL_CM, round(cell_m * 100)),
        (TAG_UNIT_MM, unit_mm),
        (TAG_WIDTH, w),
        (TAG_HEIGHT, h),
        (TAG_NW_LAT, round(nw_lat * 1e6)),
        (TAG_NW_LON, round(nw_lon * 1e6)),
    ]


def load_program(c, name, filename=None):
    src = open(os.path.join(HERE, filename or f"{name}.ddp")).read()
    c.send_lines([f"r{name} load {name} begin"] + src.splitlines() + [f"r{name} end-load"])
    while True:
        toks = c.read_line().split(" ", 2)
        if toks[0] == f"r{name}":
            assert toks[1] == "ok", (name, toks)
            return


def attach_meta(c, board):
    load_program(c, "meta")
    for tag, value in board_meta(board):
        c.cmd(f"feed meta 0 {tag} val={value}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--board", default="engadin_256", choices=sorted(PRESETS))
    ap.add_argument("--port", type=int, default=7994)
    ap.add_argument("--host", default="127.0.0.1", help="TCP listen address")
    ap.add_argument("--ws-host", default=None, help="WebSocket listen address (default: --host)")
    ap.add_argument("--water-program", choices=("water.ddp", "water_gpri.ddp"),
                    default="water.ddp", help="water implementation to install")
    ap.add_argument("--with-flow", action="store_true",
                    help="also install and verify flow/accumulation")
    ap.add_argument("--meta-only", action="store_true",
                    help="attach meta to an already-running world on --port")
    args = ap.parse_args()

    if args.meta_only:
        c = Client(args.port)
        attach_meta(c, args.board)
        c.cmd("tick")
        print(f"meta attached to port {args.port}: {board_meta(args.board)}")
        return

    grid = os.path.join(HERE, PRESETS[args.board]["out"])
    terrain = load_grid(grid)
    env = dict(os.environ,
               DDIR_BIND=f"{args.host}:{args.port}",
               DDIR_WS_BIND=f"{args.ws_host or args.host}:{args.port + 1}",
               DDIR_DIAG_PORT=str(args.port + 2),
               DDIR_TICK_MS="0")
    server = subprocess.Popen([BIN], env=env, stdin=subprocess.PIPE,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              text=True, start_new_session=True)
    pid_path = os.path.join(HERE, f"stage_{args.board}.pid")
    open(pid_path, "w").write(str(server.pid))

    c = Client(args.port)
    load_program(c, "water", args.water_program)
    if args.with_flow:
        load_program(c, "flow")
    lines = [f"feed water 0 {x},{y} val={h}" for (x, y), h in terrain.items()]
    for i in range(0, len(lines), 2000):
        chunk = lines[i:i + 2000]
        c.send_lines(chunk)
        c.drain_oks(len(chunk))
    attach_meta(c, args.board)

    t0 = time.time()
    c.cmd("tick")
    fill_s = time.time() - t0
    water = parse_water(c.cmd("peek water", collect=True))
    truth = priority_flood(terrain)
    assert water == truth, "equilibrium does not match the priority flood"
    wet = sum(1 for k in truth if truth[k] > terrain[k])

    print(f"{args.board}: {len(terrain)} cells, {args.water_program}, "
          f"initial fill {fill_s:.1f}s, "
          f"{wet} wet cells, equilibrium == priority flood")
    if args.with_flow:
        flow_rows = unique_rows(c.cmd("peek flow", collect=True), "flow")
        accum_rows = unique_rows(c.cmd("peek accum", collect=True), "accum")
        flow = dict(flow_rows)
        accum = {key: value[0] for key, value in accum_rows.items()}
        expected_flow, expected_accum = py_flow_accum(water)
        assert flow == expected_flow, "flow does not match the Python oracle"
        assert accum == expected_accum, "accumulation does not match the Python oracle"
        print(f"flow + accumulation == Python oracle; max accumulation {max(accum.values())}")
    print(f"meta: {board_meta(args.board)}")
    print(f"serving TCP {args.host}:{args.port}, WS {args.ws_host or args.host}:{args.port + 1}"
          f" (pid {server.pid} in {os.path.basename(pid_path)}); driver-owned time, so tick to advance")


if __name__ == "__main__":
    main()
