#!/usr/bin/env python3
"""GUARD_MB
Bench one water program on one board: initial fill and an incremental dam.

Reports wall-clock, peak RSS, and verifies the equilibrium against the
independent priority flood. Usage:
    bench_water.py PROGRAM.ddp BOARD.txt [--port N] [--skip-dam]
"""
import argparse, os, subprocess, sys, threading, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run_dem import BIN, Client, load_grid, parse_water, priority_flood

ap = argparse.ArgumentParser()
ap.add_argument("program"); ap.add_argument("board")
ap.add_argument("--port", type=int, default=7971)
ap.add_argument("--skip-dam", action="store_true")
ap.add_argument("--dam-x", type=int, default=96)
ap.add_argument("--dam-crest", type=int, default=1775)
ap.add_argument("--guard-mb", type=int, default=6000,
                help="kill the server if its RSS exceeds this (keeps the host off swap)")
a = ap.parse_args()

terrain = load_grid(os.path.join(HERE, a.board))
env = dict(os.environ, DDIR_BIND=f"127.0.0.1:{a.port}", DDIR_WS_BIND=f"127.0.0.1:{a.port+1}",
           DDIR_DIAG_PORT=str(a.port+2), DDIR_TICK_MS="0")
srv = subprocess.Popen([BIN], env=env, stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       start_new_session=True)
peak = [0]
stop = threading.Event()
def sampler():
    while not stop.is_set():
        try:
            rss = int(subprocess.run(["ps","-o","rss=","-p",str(srv.pid)],
                      capture_output=True, text=True).stdout.strip() or 0)
            peak[0] = max(peak[0], rss)
            if rss > a.guard_mb * 1024:
                print(f"GUARD: RSS {rss/1024:.0f} MB exceeded {a.guard_mb} MB — killing")
                srv.kill(); stop.set(); os._exit(2)
        except Exception: pass
        time.sleep(0.05)
threading.Thread(target=sampler, daemon=True).start()

try:
    c = Client(a.port)
    src = open(os.path.join(HERE, a.program)).read()
    c.send_lines(["rl load water begin"] + src.splitlines() + ["rl end-load"])
    while True:
        t = c.read_line().split(" ", 2)
        if t[0] == "rl":
            assert t[1] == "ok", t
            break
    lines = [f"feed water 0 {x},{y} val={h}" for (x, y), h in terrain.items()]
    t0 = time.time()
    for i in range(0, len(lines), 2000):
        ch = lines[i:i+2000]; c.send_lines(ch); c.drain_oks(len(ch))
    feed_s = time.time() - t0
    t0 = time.time(); c.cmd("tick"); fill_s = time.time() - t0
    fill_rss = max(peak[0], int(subprocess.run(["ps","-o","rss=","-p",str(srv.pid)],
                   capture_output=True, text=True).stdout.strip() or 0))
    water = parse_water(c.cmd("peek water", collect=True))
    truth = priority_flood(terrain)
    ok = water == truth
    wet = sum(1 for k in truth if truth[k] > terrain[k])
    print(f"program={a.program} board={a.board} cells={len(terrain)}")
    print(f"  feed {feed_s:.2f}s | initial fill {fill_s:.2f}s | peak RSS {fill_rss/1024:.0f} MB "
          f"| wet {wet} | exact={'YES' if ok else 'NO'}")
    if not ok:
        bad = [k for k in truth if water.get(k) != truth[k]]
        print(f"  MISMATCH on {len(bad)} cells, e.g. {bad[:5]} "
              f"ddir={[water.get(k) for k in bad[:5]]} truth={[truth[k] for k in bad[:5]]}")
    if not a.skip_dam:
        dam = {(x, y): a.dam_crest for (x, y) in terrain
               if x == a.dam_x and terrain[(x, y)] < a.dam_crest}
        feeds = []
        for cell, h in dam.items():
            feeds.append(f"feed water 0 {cell[0]},{cell[1]} val={terrain[cell]} diff=-1")
            feeds.append(f"feed water 0 {cell[0]},{cell[1]} val={h}")
        peak[0] = 0
        c.send_lines(feeds); c.drain_oks(len(feeds))
        t0 = time.time(); c.cmd("tick"); dam_s = time.time() - t0
        dam_rss = max(peak[0], int(subprocess.run(["ps","-o","rss=","-p",str(srv.pid)],
                      capture_output=True, text=True).stdout.strip() or 0))
        w2 = parse_water(c.cmd("peek water", collect=True))
        t2 = dict(terrain); t2.update(dam)
        truth2 = priority_flood(t2)
        changed = sum(1 for k in truth2 if truth2.get(k) != truth.get(k))
        print(f"  dam ({len(dam)} cells) {dam_s:.2f}s | peak RSS {dam_rss/1024:.0f} MB "
              f"| re-derived {changed} | exact={'YES' if w2 == truth2 else 'NO'}")
finally:
    stop.set(); srv.kill()
