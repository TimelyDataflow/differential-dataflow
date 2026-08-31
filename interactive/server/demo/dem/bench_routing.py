#!/usr/bin/env python3
"""Benchmark live route fixed points over an exact composed world.

The harness stages water, flow/accumulation, and a selectable pathways program,
then adds up to four route requests one at a time. Every accepted DDIR route is
checked for both cost and predecessor geometry against `shortest_route`.

Usage:
    bench_routing.py WATER.ddp PATHWAYS.ddp BOARD.txt [--routes N]
"""

import argparse
import os
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_runtime import feed_chunks, load_program, unique_rows
from pathways_rules import shortest_route
from run_dem import BIN, Client, load_grid, priority_flood
from run_physics import py_flow_accum


BOARDS = {
    "engadin_128.txt": {
        "steps": (53, 75),
        "sites": {
            1: (99, 36),
            2: (118, 12),
            3: (44, 112),
            4: (44, 120),
            10: (70, 80),
            11: (96, 70),
        },
    },
    "engadin_256.txt": {
        "steps": (26, 37),
        "sites": {
            1: (156, 60),
            2: (182, 174),
            3: (52, 214),
            4: (238, 245),
            10: (110, 120),
            11: (135, 120),
        },
    },
    "engadin_512.txt": {
        "steps": (13, 19),
        "sites": {
            1: (312, 120),
            2: (364, 348),
            3: (104, 428),
            4: (476, 490),
            10: (220, 240),
            11: (270, 240),
        },
    },
}

REQUESTS = (
    # Same endpoints, two attitudes to elevation.
    (1, 11, 3, (1, 1, 1000, 0, 1)),
    (2, 11, 3, (1, 12, 1000, 0, 1)),
    # A drainage-aware cross-valley proposal and a reuse-heavy fourth route.
    (3, 11, 4, (1, 4, 1000, 8, 1)),
    (4, 10, 2, (1, 4, 1000, 0, 3)),
)


def rss_kib(pid):
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
    )
    return int(result.stdout.strip() or 0)


def route_geometry(rows, route_id, start, target):
    predecessors = {
        (key[1], key[2]): (value[1], value[2])
        for key, value in rows.items()
        if key[0] == route_id
    }
    if target not in predecessors:
        return None
    result = [target]
    while result[-1] != start:
        result.append(predecessors[result[-1]])
        if len(result) > len(predecessors) + 1:
            raise RuntimeError(f"route {route_id} predecessor cycle")
    result.reverse()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("water_program")
    parser.add_argument("pathways_program")
    parser.add_argument("board", choices=sorted(BOARDS))
    parser.add_argument("--routes", type=int, default=2, choices=range(1, 5))
    parser.add_argument("--port", type=int, default=7977)
    parser.add_argument("--guard-mb", type=int, default=6000)
    args = parser.parse_args()

    config = BOARDS[args.board]
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

    def reset_peak():
        peak[0] = rss_kib(server.pid)

    def phase_rss():
        return max(peak[0], rss_kib(server.pid)) / 1024

    def relation(client, name):
        return unique_rows(client.cmd(f"peek {name}", collect=True), name)

    try:
        client = Client(args.port)
        load_program(
            client,
            "water",
            os.path.join(HERE, args.water_program),
        )
        load_program(client, "flow", os.path.join(HERE, "flow.ddp"))
        load_program(
            client,
            "pathways",
            os.path.join(HERE, args.pathways_program),
        )
        feed_chunks(
            client,
            [
                f"feed water 0 {x},{y} val={height}"
                for (x, y), height in terrain.items()
            ],
        )
        orthogonal, diagonal = config["steps"]
        client.cmd(
            f"feed pathways 5 0 val={orthogonal},{diagonal}"
        )
        client.cmd("feed pathways 7 0 val=2,0,0,0,0,0")
        reset_peak()
        started = time.time()
        client.cmd("tick")
        base_seconds = time.time() - started
        base_rss = phase_rss()

        water = {
            key: value[0]
            for key, value in relation(client, "water").items()
        }
        accum = {
            key: value[0]
            for key, value in relation(client, "accum").items()
        }
        expected_water = priority_flood(terrain)
        _expected_flow, expected_accum = py_flow_accum(expected_water)
        if water != expected_water or accum != expected_accum:
            raise RuntimeError("base hydrology disagrees with Python")

        print(
            f"water={args.water_program} pathways={args.pathways_program} "
            f"board={args.board} cells={len(terrain)}",
            flush=True,
        )
        print(
            f"  base settle {base_seconds:.2f}s | RSS {base_rss:.0f} MB | "
            f"hydrology exact=YES",
            flush=True,
        )

        active = []
        for route_id, from_id, to_id, coefficients in REQUESTS[:args.routes]:
            start = config["sites"][from_id]
            target = config["sites"][to_id]
            coefficient_text = ",".join(str(value) for value in coefficients)
            client.cmd(
                f"feed pathways 1 {route_id} "
                f"val={route_id},{start[0]},{start[1]},"
                f"{target[0]},{target[1]},{coefficient_text}"
            )
            active.append((route_id, start, target, coefficients))
            reset_peak()
            started = time.time()
            client.cmd("tick")
            route_seconds = time.time() - started
            route_rss = phase_rss()
            costs = {
                key[0]: value[0]
                for key, value in relation(client, "route_cost").items()
            }
            steps = relation(client, "route_steps")
            for active_id, active_start, active_target, active_coefficients in active:
                expected_path, expected_cost = shortest_route(
                    terrain,
                    water,
                    accum,
                    active_start,
                    active_target,
                    active_coefficients,
                    step_lengths=config["steps"],
                )
                actual_path = route_geometry(
                    steps,
                    active_id,
                    active_start,
                    active_target,
                )
                if (
                    costs.get(active_id) != expected_cost
                    or actual_path != expected_path
                ):
                    raise RuntimeError(
                        f"route {active_id} disagrees with Dijkstra"
                    )
            current_path = route_geometry(
                steps,
                route_id,
                start,
                target,
            )
            print(
                f"  add route {route_id}: {route_seconds:.2f}s | "
                f"RSS {route_rss:.0f} MB | cost {costs[route_id]} | "
                f"cells {len(current_path)} | all {len(active)} exact=YES",
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
