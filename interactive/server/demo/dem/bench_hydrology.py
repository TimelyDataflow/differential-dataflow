#!/usr/bin/env python3
"""Benchmark the composed water -> flow -> accumulation pipeline.

Water is filled first and checked against the independent priority flood.
`flow.ddp` is then installed over the settled `water` trace, timed separately,
and checked against the Python flow/accumulation oracle. A dam edit exercises
the incremental composition. The RSS guard applies to the benchmark server,
which is useful when another persistent world is still live on the host.

Usage:
    bench_hydrology.py WATER.ddp BOARD.txt [--dam-x N] [--port N]
"""

import argparse
import os
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_runtime import load_program, unique_rows
from run_dem import BIN, Client, load_grid, priority_flood
from run_physics import py_flow_accum


def rss_kib(pid):
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
    )
    return int(result.stdout.strip() or 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("water_program")
    parser.add_argument("board")
    parser.add_argument("--port", type=int, default=7974)
    parser.add_argument("--dam-x", type=int, default=96)
    parser.add_argument("--dam-crest", type=int, default=1775)
    parser.add_argument("--guard-mb", type=int, default=6000)
    args = parser.parse_args()

    terrain = load_grid(os.path.join(HERE, args.board))
    environment = dict(
        os.environ,
        DDIR_BIND=f"127.0.0.1:{args.port}",
        DDIR_WS_BIND=f"127.0.0.1:{args.port + 1}",
        DDIR_DIAG_PORT=str(args.port + 2),
        DDIR_TICK_MS="0",
    )
    server = subprocess.Popen(
        [BIN],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    peak = [0]
    stop = threading.Event()
    guard_hit = threading.Event()

    def sampler():
        while not stop.is_set():
            try:
                resident = rss_kib(server.pid)
                peak[0] = max(peak[0], resident)
                if resident > args.guard_mb * 1024:
                    print(
                        f"GUARD: RSS {resident / 1024:.0f} MB exceeded "
                        f"{args.guard_mb} MB — killing",
                        flush=True,
                    )
                    guard_hit.set()
                    server.kill()
                    stop.set()
                    return
            except Exception:
                pass
            time.sleep(0.05)

    threading.Thread(target=sampler, daemon=True).start()

    def phase_rss():
        return max(peak[0], rss_kib(server.pid)) / 1024

    def reset_peak():
        peak[0] = rss_kib(server.pid)

    def relation(client, name):
        return unique_rows(client.cmd(f"peek {name}", collect=True), name)

    try:
        client = Client(args.port)
        load_program(
            client,
            "water",
            os.path.join(HERE, args.water_program),
        )
        feeds = [
            f"feed water 0 {x},{y} val={height}"
            for (x, y), height in terrain.items()
        ]
        started = time.time()
        for offset in range(0, len(feeds), 2000):
            chunk = feeds[offset:offset + 2000]
            client.send_lines(chunk)
            client.drain_oks(len(chunk))
        feed_seconds = time.time() - started
        started = time.time()
        client.cmd("tick")
        water_seconds = time.time() - started
        water_rss = phase_rss()
        water = {
            key: value[0]
            for key, value in relation(client, "water").items()
        }
        expected_water = priority_flood(terrain)
        if water != expected_water:
            raise RuntimeError("initial water disagrees with priority flood")

        reset_peak()
        started = time.time()
        load_program(client, "flow", os.path.join(HERE, "flow.ddp"))
        install_seconds = time.time() - started
        started = time.time()
        client.cmd("tick")
        flow_seconds = time.time() - started
        hydrology_rss = phase_rss()
        flow = {
            key: value
            for key, value in relation(client, "flow").items()
        }
        accum = {
            key: value[0]
            for key, value in relation(client, "accum").items()
        }
        expected_flow, expected_accum = py_flow_accum(water)
        if flow != expected_flow or accum != expected_accum:
            raise RuntimeError("initial flow/accumulation disagrees with Python")

        print(
            f"water={args.water_program} board={args.board} "
            f"cells={len(terrain)}",
            flush=True,
        )
        print(
            f"  feed {feed_seconds:.2f}s | water {water_seconds:.2f}s | "
            f"RSS {water_rss:.0f} MB | exact=YES",
            flush=True,
        )
        print(
            f"  install flow {install_seconds:.2f}s + settle "
            f"{flow_seconds:.2f}s | composed RSS {hydrology_rss:.0f} MB | "
            f"max accum {max(accum.values())} | exact=YES",
            flush=True,
        )

        dam = {
            (x, y): args.dam_crest
            for (x, y), height in terrain.items()
            if x == args.dam_x and height < args.dam_crest
        }
        updates = []
        edited = dict(terrain)
        for cell, height in dam.items():
            updates.append(
                f"feed water 0 {cell[0]},{cell[1]} "
                f"val={terrain[cell]} diff=-1"
            )
            updates.append(
                f"feed water 0 {cell[0]},{cell[1]} val={height}"
            )
            edited[cell] = height
        client.send_lines(updates)
        client.drain_oks(len(updates))
        reset_peak()
        started = time.time()
        client.cmd("tick")
        edit_seconds = time.time() - started
        edit_rss = phase_rss()
        edited_water = {
            key: value[0]
            for key, value in relation(client, "water").items()
        }
        edited_flow = {
            key: value
            for key, value in relation(client, "flow").items()
        }
        edited_accum = {
            key: value[0]
            for key, value in relation(client, "accum").items()
        }
        expected_edited_water = priority_flood(edited)
        expected_edited_flow, expected_edited_accum = py_flow_accum(
            expected_edited_water
        )
        if (
            edited_water != expected_edited_water
            or edited_flow != expected_edited_flow
            or edited_accum != expected_edited_accum
        ):
            raise RuntimeError("edited hydrology disagrees with Python")
        changed_accum = sum(
            1
            for cell in edited_accum
            if edited_accum[cell] != accum[cell]
        )
        print(
            f"  dam ({len(dam)} cells) composed edit {edit_seconds:.2f}s | "
            f"RSS {edit_rss:.0f} MB | accum changed {changed_accum} | "
            f"exact=YES",
            flush=True,
        )
    except (OSError, RuntimeError) as error:
        if not guard_hit.is_set():
            raise
        print(f"benchmark stopped by RSS guard: {error}", flush=True)
        return 2
    finally:
        stop.set()
        server.kill()


if __name__ == "__main__":
    sys.exit(main() or 0)
