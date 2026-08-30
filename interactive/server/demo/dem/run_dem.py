#!/usr/bin/env python3
"""Equilibrium water over a real Swiss DEM, live on the DDIR server.

Phases, each timed and each cross-checked against an independent Python
priority-flood of the same terrain (the server must agree exactly):

  1. load  — the water program and the terrain grid (St. Moritz window).
  2. init  — first tick: the initial fill. Real DEMs mostly drain; what
             ponds here are the genuine closed depressions.
  3. dam   — raise a wall across the Inn gorge northeast of St. Moritz
             (crest 1775 m): the lake rises until it finds the lowest
             escape the terrain offers. The fixed point discovers the
             spill; nothing scripts it.
  4. notch — cut a 3-cell spillway (1710 m) through the dam: the lake
             drops to the notch. The cheap direction, for comparison
             against the dam's expensive one (the gfp rising replays
             iteration history; falling is delta-driven).

Reported per phase: wall-clock to quiescence, cells whose water level
changed (net, by diffing consolidated peeks), lake cells and volume.

  python3 run_dem.py [--grid engadin_128.txt] [--synthetic] [--port 7991]

Requires target/release/ddir_server (cargo build -p ddir-server --release).
"""

import argparse
import heapq
import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "..", "..", "..", "target", "release", "ddir_server")

DAM_X = 96        # gorge column (local coords), upstream of the top-edge contact
DAM_CREST = 1775  # wall height
NOTCH_H = 1710    # spillway height cut through the dam


class Client:
    def __init__(self, port):
        for _ in range(100):
            try:
                self.sock = socket.create_connection(("127.0.0.1", port), timeout=600)
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("server never came up")
        self.buf = b""
        self.n = 0

    def send_lines(self, lines):
        self.sock.sendall(("\n".join(lines) + "\n").encode())

    def read_line(self):
        while b"\n" not in self.buf:
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise RuntimeError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line.decode()

    def cmd(self, line, collect=False):
        """Send one reqid'd command; drain to its ok/err; return data lines."""
        self.n += 1
        rid = f"r{self.n}"
        self.send_lines([f"{rid} {line}"])
        data = []
        while True:
            toks = self.read_line().split(" ", 2)
            if toks[0] != rid:
                continue
            if toks[1] == "ok":
                return data
            if toks[1] == "err":
                raise RuntimeError(f"{line}: {toks[2] if len(toks) > 2 else ''}")
            if toks[1] == "data" and collect:
                data.append(toks[2])

    def batch(self, feeds):
        """Atomically stage current-epoch feed commands; caller ticks to publish."""
        if not feeds:
            raise ValueError("batch requires at least one feed")
        if any(not line.startswith("feed ") for line in feeds):
            raise ValueError("batch bodies may contain only bare feed commands")
        self.n += 1
        rid = f"r{self.n}"
        self.send_lines(
            [f"{rid} batch begin"] + list(feeds) + [f"{rid} end-batch"]
        )
        while True:
            toks = self.read_line().split(" ", 2)
            if toks[0] != rid:
                continue
            if toks[1] == "ok":
                return toks[2] if len(toks) > 2 else ""
            if toks[1] == "err":
                raise RuntimeError(
                    f"batch: {toks[2] if len(toks) > 2 else ''}"
                )

    def drain_oks(self, count):
        seen = 0
        while seen < count:
            toks = self.read_line().split(" ", 2)
            if toks[1] == "ok":
                seen += 1
            elif toks[1] == "err":
                raise RuntimeError(f"feed failed: {toks[2]}")


def parse_water(data_lines):
    out = {}
    for line in data_lines:
        # diff=1 key=Tuple([Int(x), Int(y)]) val=Tuple([Int(w)])
        key = line.split("key=Tuple([")[1].split("])")[0]
        val = line.split("val=Tuple([")[1].split("])")[0]
        x, y = (int(p.split("(")[1].rstrip(")")) for p in key.split(", "))
        w = int(val.split("(")[1].rstrip(")"))
        out[(x, y)] = w
    return out


def priority_flood(terrain):
    """Ground truth: w(c) = max(t, min over escape paths of the path max)."""
    water = {}
    heap = []
    cells = set(terrain)
    for (x, y), h in terrain.items():
        nbrs = [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]
        if any(n not in cells for n in nbrs):
            heapq.heappush(heap, (h, (x, y)))
    while heap:
        w, c = heapq.heappop(heap)
        if c in water:
            continue
        water[c] = w
        x, y = c
        for n in [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]:
            if n in cells and n not in water:
                heapq.heappush(heap, (max(w, terrain[n]), n))
    return water


def load_grid(path):
    terrain = {}
    with open(path) as f:
        for y, line in enumerate(f):
            for x, h in enumerate(line.split()):
                terrain[(x, y)] = int(h)
    return terrain


def synthetic_grid():
    """8x8 bowl with a rim gap at height 4: the pit fills to the gap."""
    terrain = {}
    for y in range(8):
        for x in range(8):
            edge = x in (0, 7) or y in (0, 7)
            rim = x in (1, 6) or y in (1, 6)
            terrain[(x, y)] = 2 if edge else (6 if rim else 1)
    terrain[(3, 3)] = 0   # pit floor
    terrain[(6, 3)] = 4   # rim gap: the spill point
    return terrain


def lakes(water, terrain):
    wet = {c: water[c] - terrain[c] for c in water if water[c] > terrain[c]}
    return len(wet), sum(wet.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grid", default=os.path.join(HERE, "engadin_128.txt"))
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--port", type=int, default=7991)
    args = ap.parse_args()

    terrain = synthetic_grid() if args.synthetic else load_grid(args.grid)
    print(f"terrain: {len(terrain)} cells, "
          f"heights {min(terrain.values())}..{max(terrain.values())}")

    env = dict(
        os.environ,
        DDIR_BIND=f"127.0.0.1:{args.port}",
        DDIR_WS_BIND=f"127.0.0.1:{args.port + 1}",
        DDIR_DIAG_PORT=str(args.port + 2),
        DDIR_TICK_MS="0",  # the driver owns time
    )
    server = subprocess.Popen(
        [BIN], env=env, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, text=True)
    report = []
    try:
        c = Client(args.port)
        program = open(os.path.join(HERE, "water.ddp")).read()
        c.send_lines(["rload load water begin"] + program.splitlines() + ["rload end-load"])
        while True:
            toks = c.read_line().split(" ", 2)
            if toks[0] == "rload":
                assert toks[1] == "ok", toks
                break

        def feed_all(items):
            lines = [f"feed water 0 {x},{y} val={h} diff={d}" for (x, y), h, d in items]
            for i in range(0, len(lines), 2000):
                chunk = lines[i:i + 2000]
                c.send_lines(chunk)
                c.drain_oks(len(chunk))

        def phase(name, edits):
            """Apply terrain edits (cell -> new height), tick, verify, report."""
            items = []
            for cell, nh in edits.items():
                items.append((cell, terrain[cell], -1))
                items.append((cell, nh, 1))
                terrain[cell] = nh
            t0 = time.time()
            feed_all(items)
            c.cmd("tick")
            dt = time.time() - t0
            water = parse_water(c.cmd("peek water", collect=True))
            truth = priority_flood(terrain)
            assert water == truth, (
                f"{name}: server disagrees with priority flood "
                f"({len(water)} vs {len(truth)} cells; first diffs: "
                f"{[(k, water.get(k), truth.get(k)) for k in list(truth)[:3]]})")
            return dt, water

        t0 = time.time()
        feed_all([(cell, h, 1) for cell, h in terrain.items()])
        load_s = time.time() - t0

        dt, water = phase("init", {})
        n, vol = lakes(water, terrain)
        report.append(("init fill", dt, len(water), n, vol))
        prev = water

        if args.synthetic:
            gap_w = water[(4, 3)]
            print(f"synthetic: pit water level {gap_w} (rim gap is 4)")
            assert gap_w == 4, "pit must fill exactly to the rim gap"
        else:
            # The dam: raise every sub-crest cell in the gorge column.
            dam_cells = {(DAM_X, y): DAM_CREST
                         for (x, y) in terrain if x == DAM_X and terrain[(x, y)] < DAM_CREST}
            dt, water = phase("dam", dam_cells)
            changed = sum(1 for k in water if water[k] != prev[k])
            n, vol = lakes(water, terrain)
            # The dammed lake's level: any WET cell just upstream of the wall
            # (the lake surface is flat, so one is enough).
            lake_lvl = max((water[(DAM_X - 1, y)] for (_, y) in dam_cells
                            if (DAM_X - 1, y) in water
                            and water[(DAM_X - 1, y)] > terrain[(DAM_X - 1, y)]),
                           default=0)
            report.append((f"dam ({len(dam_cells)} cells to {DAM_CREST})",
                           dt, changed, n, vol))
            print(f"dam: the lake behind the wall settled at {lake_lvl} m "
                  f"(crest {DAM_CREST}) — the lowest escape the fixed point "
                  f"found, whether over the crest or around the terrain")
            prev = water

            # The notch: a 3-cell spillway through the middle of the wall.
            ys = sorted(y for (_, y) in dam_cells)
            mid = ys[len(ys) // 2]
            notch = {(DAM_X, y): NOTCH_H for y in (mid - 1, mid, mid + 1)
                     if (DAM_X, y) in dam_cells}
            dt, water = phase("notch", notch)
            changed = sum(1 for k in water if water[k] != prev[k])
            n, vol = lakes(water, terrain)
            report.append((f"notch ({len(notch)} cells to {NOTCH_H})", dt, changed, n, vol))

        print()
        print(f"{'phase':<28}{'seconds':>9}{'cells changed':>15}{'lake cells':>12}{'volume m*cell':>15}")
        print(f"{'load ' + str(len(terrain)) + ' cells':<28}{load_s:>9.2f}{'-':>15}{'-':>12}{'-':>15}")
        for name, dt, changed, n, vol in report:
            print(f"{name:<28}{dt:>9.2f}{changed:>15}{n:>12}{vol:>15}")
        print("\nall phases agree exactly with the independent priority flood")
    finally:
        try:
            server.stdin.write("exit\n")
            server.stdin.flush()
            server.wait(timeout=10)
        except Exception:
            server.kill()


if __name__ == "__main__":
    main()
