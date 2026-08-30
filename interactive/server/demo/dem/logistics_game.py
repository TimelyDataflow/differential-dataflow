#!/usr/bin/env python3
"""Stage or judge the asymmetric civil-logistics Engadine scenario.

  python3 logistics_game.py setup --run-dir runs/trial-01 [--port 8011]
  python3 logistics_game.py judge --run-dir runs/trial-01
"""

import argparse
import json
import os
import signal
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_rules import BRIDGE, ROAD, TERRAFORM, replay_actions, shortest_service_path
from logistics_runtime import (
    actions_from_client,
    ensure_ports_available,
    feed_chunks,
    load_program,
    parse_weighted_rows,
    run_lock,
    unique_rows,
)
from run_dem import BIN, Client, load_grid, priority_flood

DAM_X = 96
DAM_CREST = 1775
DEPOT = (100, 35)
TOWN = (90, 45)
VILLAGE = [(92, 45), (91, 45), (92, 44), (90, 45)]
MAX_SERVICE_HOPS = 20

ROLES = {
    1: ROAD,
    2: BRIDGE,
    3: TERRAFORM,
}
GRANTS = {
    (1, ROAD): 16,
    (2, BRIDGE): 1,
    (3, TERRAFORM): 950,
}


def initial_roads():
    """Two flooded halves of a road plus a connected east-side works spur."""
    east = {(x, 35) for x in range(96, 101)}
    east |= {(99, y) for y in range(27, 36)}
    east |= {(98, y) for y in range(27, 33)}
    east |= {(98, 34), (97, 34)}
    west = {(x, 35) for x in range(90, 95)}
    west |= {(90, y) for y in range(36, 46)}
    return {cell: ROAD for cell in sorted(east | west)}


def scenario_terrain():
    base = load_grid(os.path.join(HERE, "engadin_128.txt"))
    terrain = dict(base)
    dam = {
        cell
        for cell, height in base.items()
        if cell[0] == DAM_X and height < DAM_CREST
    }
    for cell in dam:
        terrain[cell] = DAM_CREST
    return base, terrain, dam


def write_json(path, value):
    with open(path, "w") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


def stop_recorded_server(run_dir):
    try:
        with open(os.path.join(run_dir, "server.pid")) as source:
            pid = int(source.read())
        os.kill(pid, signal.SIGTERM)
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
        pass


def setup(run_dir, port):
    run_dir = os.path.abspath(run_dir)
    if os.path.exists(run_dir):
        raise RuntimeError(f"run directory already exists: {run_dir}")
    if not os.path.exists(BIN):
        raise RuntimeError(f"build the server first: cargo build -p ddir-server --release")
    ensure_ports_available((port, port + 1, port + 2))
    os.makedirs(run_dir)

    server_log = open(os.path.join(run_dir, "server.log"), "w")
    env = dict(
        os.environ,
        DDIR_BIND=f"127.0.0.1:{port}",
        DDIR_WS_BIND=f"127.0.0.1:{port + 1}",
        DDIR_DIAG_PORT=str(port + 2),
        DDIR_TICK_MS="0",
    )
    server = subprocess.Popen(
        [BIN],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=server_log,
        stderr=server_log,
        start_new_session=True,
    )
    with open(os.path.join(run_dir, "server.pid"), "w") as destination:
        destination.write(str(server.pid))

    try:
        client = Client(port)
        load_program(client, "water", os.path.join(HERE, "water.ddp"))
        load_program(client, "logistics", os.path.join(HERE, "logistics.ddp"))

        base, terrain, dam = scenario_terrain()
        feed_chunks(
            client,
            [f"feed water 0 {x},{y} val={height}" for (x, y), height in base.items()],
        )
        nature = []
        for cell in sorted(dam):
            old = base[cell]
            client.cmd(f"feed water 0 {cell[0]},{cell[1]} val={old} diff=-1")
            client.cmd(f"feed water 0 {cell[0]},{cell[1]} val={DAM_CREST}")
            nature.append([cell[0], cell[1], old, DAM_CREST])

        for agent, kind in sorted(ROLES.items()):
            client.cmd(f"feed logistics 1 {agent} val={kind}")
        for (agent, resource), units in sorted(GRANTS.items()):
            client.cmd(f"feed logistics 2 {agent},{resource} val={units}")
        roads = initial_roads()
        for (x, y), kind in sorted(roads.items()):
            client.cmd(f"feed logistics 3 {x},{y} val={kind},0")
        client.cmd(f"feed logistics 4 {DEPOT[0]},{DEPOT[1]}")
        client.cmd("tick")

        actual_water = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek water", collect=True), "water"
            ).items()
        }
        expected_water = priority_flood(terrain)
        if actual_water != expected_water:
            raise RuntimeError("setup water disagrees with independent priority flood")
        if not all(actual_water[cell] > terrain[cell] for cell in VILLAGE):
            raise RuntimeError("scenario village is not initially flooded")

        briefing = {
            "version": 2,
            "run_dir": run_dir,
            "port": port,
            "depot": list(DEPOT),
            "town": list(TOWN),
            "village": [list(cell) for cell in VILLAGE],
            "village_heights": {
                f"{x},{y}": terrain[(x, y)] for x, y in VILLAGE
            },
            "locked": [list(cell) for cell in sorted(dam)],
            "protected": [list(cell) for cell in VILLAGE],
            "nature_edits": nature,
            "roles": {
                "1": {"name": "road engineer", "kind": ROAD, "grant": 16},
                "2": {"name": "structures engineer", "kind": BRIDGE, "grant": 1},
                "3": {"name": "earthworks engineer", "kind": TERRAFORM, "grant": 950},
            },
            "initial_infrastructure": [
                [x, y, kind] for (x, y), kind in sorted(roads.items())
            ],
            "max_service_hops": MAX_SERVICE_HOPS,
            "rules": [
                "Only the role-matching action is legal for each agent.",
                "Surface roads must be dry and extend the depot-connected network.",
                "Bridges extend the network without changing terrain, so water passes beneath.",
                "Terraforming requires a depot-connected road in a neighboring cell; terrain beneath a connected bridge may be cut.",
                "The village and the nature-built dam are locked against all new construction.",
                "Every mutation is one revision and automatically equilibrates the world.",
            ],
            "goal": {
                "village_dry_and_unchanged": True,
                "town_service_distance_at_most": MAX_SERVICE_HOPS,
                "audit_clean": True,
            },
            "playtest_rubric": {
                "too_obvious": "one route is identified immediately and roles only report completion",
                "too_flat": "many arbitrary routes tie and role decisions do not alter one another",
                "productive": "agents negotiate access and bridge placement, revise a plan, and retain scarce-resource tension",
            },
        }
        write_json(os.path.join(run_dir, "briefing.json"), briefing)
        open(os.path.join(run_dir, "events.jsonl"), "w").close()
        with open(os.path.join(run_dir, "site_office.md"), "w") as office:
            office.write(
                "# Civil-logistics site office\n\n"
                "Append surveys, proposals, dependencies, acknowledgments, completed revisions, and corrections.\n"
            )
        open(os.path.join(run_dir, "game.lock"), "a").close()
        print(f"run: {run_dir}")
        print(f"port: {port}; depot {DEPOT}; town {TOWN}")
        print(f"village: {VILLAGE}; lake level {max(actual_water[c] for c in VILLAGE)}")
        print("roles: agent 1 road(16), agent 2 bridge(1), agent 3 terraform(950)")
    except Exception:
        stop_recorded_server(run_dir)
        raise
    finally:
        server_log.close()


def judge(run_dir):
    run_dir = os.path.abspath(run_dir)
    with open(os.path.join(run_dir, "briefing.json")) as source:
        briefing = json.load(source)
    port = int(briefing["port"])
    client = Client(port)

    with run_lock(run_dir):
        client.cmd("tick")
        actions = actions_from_client(client)
        _, nature_terrain, dam = scenario_terrain()
        roads = {
            (x, y): kind
            for x, y, kind in briefing["initial_infrastructure"]
        }
        roles = {
            int(agent): int(role["kind"])
            for agent, role in briefing["roles"].items()
        }
        grants = {
            (int(agent), int(role["kind"])): int(role["grant"])
            for agent, role in briefing["roles"].items()
        }
        protected = {tuple(cell) for cell in briefing["protected"]}
        state = replay_actions(
            terrain=nature_terrain,
            initial_infrastructure=roads,
            depots={tuple(briefing["depot"])},
            roles=roles,
            grants=grants,
            actions=actions,
            priority_flood=priority_flood,
            locked=dam,
            protected=protected,
        )

        actual_terrain = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek terrain", collect=True), "terrain"
            ).items()
        }
        actual_water = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek water", collect=True), "water"
            ).items()
        }
        actual_infrastructure = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek infrastructure", collect=True), "infrastructure"
            ).items()
        }
        actual_connected = set(
            unique_rows(client.cmd("peek connected", collect=True), "connected")
        )
        ddir_spend = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek spend", collect=True), "spend"
            ).items()
        }
        ddir_balance = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek balance", collect=True), "balance"
            ).items()
        }
        expected_balance = {
            key: grant - state["spend"].get(key, 0)
            for key, grant in grants.items()
        }
        empty_audits = {}
        for name in (
            "role_violations",
            "cost_violations",
            "infrastructure_conflicts",
        ):
            empty_audits[name] = parse_weighted_rows(
                client.cmd(f"peek {name}", collect=True)
            )

        village = [tuple(cell) for cell in briefing["village"]]
        dry = {cell: actual_water[cell] == actual_terrain[cell] for cell in village}
        unchanged = {
            cell: actual_terrain[cell] == nature_terrain[cell] for cell in village
        }
        service = shortest_service_path(
            state["passable"], {tuple(briefing["depot"])}, tuple(briefing["town"])
        )
        audit_checks = {
            "terrain_replay": actual_terrain == state["terrain"],
            "water_priority_flood": actual_water == state["water"],
            "infrastructure_replay": actual_infrastructure == state["infrastructure"],
            "connected_replay": actual_connected == state["connected"],
            "spend_replay": ddir_spend == state["spend"],
            "balance_replay": ddir_balance == expected_balance,
            "ddir_violation_views_empty": not any(empty_audits.values()),
            "historical_rules": not state["violations"],
            "locked_terrain": all(actual_terrain[cell] == nature_terrain[cell] for cell in dam),
        }
        verdict = (
            all(dry.values())
            and all(unchanged.values())
            and service is not None
            and service <= int(briefing["max_service_hops"])
            and all(audit_checks.values())
        )

        print("== CIVIL-LOGISTICS JUDGMENT ==")
        print(f"village dry: {dry}")
        print(f"village unchanged: {unchanged}")
        print(f"town service path: {service} hops (limit {briefing['max_service_hops']})")
        print(f"spend: {dict(sorted(state['spend'].items()))}")
        print(f"passable infrastructure: {len(state['passable'])}/{len(state['infrastructure'])}")
        print(f"audit checks: {audit_checks}")
        print(f"historical violations: {state['violations'] or 'none'}")
        print(f"VERDICT: {'SUCCESS' if verdict else 'FAILURE'}")

    stop_recorded_server(run_dir)
    return verdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("setup", "judge"))
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--port", type=int, default=8011)
    args = parser.parse_args()
    if args.mode == "setup":
        setup(args.run_dir, args.port)
    else:
        if not judge(args.run_dir):
            sys.exit(1)


if __name__ == "__main__":
    main()
