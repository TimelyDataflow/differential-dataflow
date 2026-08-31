#!/usr/bin/env python3
"""Stage or judge the persistent paths-and-roads Engadine experiment.

  python3 pathways_game.py setup --run-dir runs/paths-01 --port 8081
  python3 pathways_game.py judge --run-dir runs/paths-01
"""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_runtime import ensure_ports_available, feed_chunks, load_program, unique_rows
from pathways_rules import (
    BRIDGE,
    BRIDGE_ACCUM,
    ENGINEERED_ROAD,
    ROAD,
    TRAIL_USE,
    connected_cells,
    infrastructure_spend,
    required_kind,
    replay_history,
    route_metrics,
    shortest_route,
)
from run_dem import BIN, Client, load_grid, priority_flood
from run_physics import py_flow_accum
from stage_board import board_meta


SITES_256 = [
    # id, label, x, y, kind (0 town / 1 source), demand or supply
    (1, "Samedan", 156, 60, 0, 40),
    (2, "Celerina", 182, 174, 0, 50),
    (3, "St. Moritz", 52, 214, 0, 60),
    (4, "Pontresina", 238, 245, 0, 50),
    (10, "Hillside quarry", 110, 120, 1, 110),
    (11, "Floodplain farm", 135, 120, 1, 110),
]

BOARD_CONFIG = {
    "engadin_128.txt": {
        "orthogonal_metres": 53,
        "diagonal_metres": 75,
        "bridge_accum_threshold": 256,
        "road_grant": 80,
    },
    "engadin_256.txt": {
        "orthogonal_metres": 26,
        "diagonal_metres": 37,
        "bridge_accum_threshold": BRIDGE_ACCUM,
        "road_grant": 160,
    },
}

SITES_128 = [
    (1, "Samedan", 99, 36, 0, 40),
    (2, "Bever", 118, 12, 0, 40),
    (3, "St. Moritz", 44, 112, 0, 60),
    (4, "Champfer", 44, 120, 0, 40),
    (10, "Hillside quarry", 70, 80, 1, 100),
    (11, "Valley farm", 96, 70, 1, 100),
]

SITES_128_V3 = [
    *SITES_128,
    (20, "Muottas quarry bench", 98, 22, 3, 10),
    (21, "St. Moritz watershed works", 36, 106, 4, 30),
]

BOARD_SITES = {
    "engadin_128.txt": SITES_128,
    "engadin_256.txt": SITES_256,
}

AGENTS = {
    1: {
        "name": "direct courier",
        "suggested_coefficients": [1, 1, 1000, 0, 1],
        "road_grant": 160,
        "bridge_grant": 3,
    },
    2: {
        "name": "contour surveyor",
        "suggested_coefficients": [1, 12, 1000, 0, 1],
        "road_grant": 160,
        "bridge_grant": 3,
    },
    3: {
        "name": "watershed surveyor",
        "suggested_coefficients": [1, 4, 1000, 8, 1],
        "road_grant": 160,
        "bridge_grant": 3,
    },
}

PROGRAM_NAMES = ("water", "flow", "pathways", "meta")
HASH_FILES = tuple(f"{name}.ddp" for name in PROGRAM_NAMES) + (
    "pathways_rules.py",
    "pathways_client.py",
)


def program_hashes():
    hashes = {}
    for filename in HASH_FILES:
        path = os.path.join(HERE, filename)
        with open(path, "rb") as source:
            hashes[filename] = hashlib.sha256(source.read()).hexdigest()
    return hashes


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


def setup(
    run_dir,
    port,
    ws_host,
    grid_name,
    briefing_template=None,
    scenario_version=2,
):
    run_dir = os.path.abspath(run_dir)
    if os.path.exists(run_dir):
        raise RuntimeError(f"run directory already exists: {run_dir}")
    if not os.path.exists(BIN):
        raise RuntimeError("build the server first: cargo build -p ddir-server --release")
    ensure_ports_available((port, port + 1, port + 2))
    if briefing_template is None:
        board = BOARD_CONFIG[grid_name]
        if scenario_version == 3 and grid_name != "engadin_128.txt":
            raise RuntimeError("the calibrated V3 hill scenario uses engadin_128.txt")
        scenario_sites = (
            SITES_128_V3
            if scenario_version == 3 else BOARD_SITES[grid_name]
        )
        agents = {
            agent: dict(values, road_grant=board["road_grant"])
            for agent, values in AGENTS.items()
        }
        version = scenario_version
        trail_use_required = TRAIL_USE
        bridge_accum_threshold = board["bridge_accum_threshold"]
        porter_trip_capacity = 5
        porter_town_quota = 10
        max_live_routes = 4
        goal = (
            "activate and road-connect the mountain quarry, then deliver its "
            "bulk rock to the connected watershed worksite without flooding "
            "a protected valley site"
            if version == 3 else
            "fulfil every town's demand without exceeding any construction "
            "grant or flooding a surface road"
        )
        rubric = {
            "too_obvious": "one coefficient profile dominates and agents pave independent routes without revising them",
            "too_flat": "many routes differ visually but road, bridge, and delivery choices are interchangeable",
            "productive": "surveys expose meaningful route tradeoffs and agents reuse, revise, or combine one another's paths",
        }
        rules = None
    else:
        if int(briefing_template.get("version", 1)) < 2:
            raise RuntimeError("pathways recovery requires briefing version 2+")
        if grid_name != briefing_template["grid"]:
            raise RuntimeError("recovery grid disagrees with saved briefing")
        board = {
            "orthogonal_metres": int(
                briefing_template["orthogonal_metres"]
            ),
            "diagonal_metres": int(briefing_template["diagonal_metres"]),
            "bridge_accum_threshold": int(
                briefing_template["bridge_accum_threshold"]
            ),
        }
        scenario_sites = [
            (
                int(site["id"]),
                site["label"],
                int(site["cell"][0]),
                int(site["cell"][1]),
                int(site["kind"]),
                int(site["amount"]),
            )
            for site in briefing_template["sites"]
        ]
        agents = {
            int(agent): dict(values)
            for agent, values in briefing_template["agents"].items()
        }
        version = int(briefing_template["version"])
        trail_use_required = int(briefing_template["trail_use_required"])
        bridge_accum_threshold = int(
            briefing_template["bridge_accum_threshold"]
        )
        porter_trip_capacity = int(
            briefing_template["porter_trip_capacity"]
        )
        porter_town_quota = int(briefing_template["porter_town_quota"])
        max_live_routes = briefing_template.get("max_live_routes")
        goal = briefing_template["goal"]
        rubric = briefing_template["playtest_rubric"]
        rules = briefing_template["rules"]

    if version >= 3:
        road_grade_permille = int(
            (briefing_template or {}).get("road_grade_permille", 400)
        )
        initial_aggregate = int(
            (briefing_template or {}).get("initial_aggregate", 17)
        )
        quarry_aggregate = int(
            (briefing_template or {}).get("quarry_aggregate", 24)
        )
        initial_rock = int(
            (briefing_template or {}).get("initial_rock", 10)
        )
        quarry_rock = int(
            (briefing_template or {}).get("quarry_rock", 50)
        )
        drainage_embankment_fill = int(
            (briefing_template or {}).get("drainage_embankment_fill", 20)
        )
        quarry_activation_agent = int(
            (briefing_template or {}).get("quarry_activation_agent", 1)
        )
        worksite_haul_agent = int(
            (briefing_template or {}).get("worksite_haul_agent", 3)
        )
        agents[1].update(
            name="mountain courier",
            build_kinds=[],
            suggested_coefficients=[1, 12, 1000, 0, 1],
        )
        agents[2].update(
            name="road engineer",
            build_kinds=[ENGINEERED_ROAD],
            suggested_coefficients=[1, 4, 1000, 8, 1],
        )
        agents[3].update(
            name="structures and works crew",
            build_kinds=[BRIDGE],
            suggested_coefficients=[1, 4, 1000, 1, 1],
        )
    else:
        road_grade_permille = 0
        initial_aggregate = 0
        quarry_aggregate = 0
        initial_rock = 0
        quarry_rock = 0
        drainage_embankment_fill = 0
        quarry_activation_agent = 1
        worksite_haul_agent = 3

    os.makedirs(run_dir)

    server_log = open(os.path.join(run_dir, "server.log"), "w")
    env = dict(
        os.environ,
        DDIR_BIND=f"127.0.0.1:{port}",
        DDIR_WS_BIND=f"{ws_host}:{port + 1}",
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
        for name in PROGRAM_NAMES:
            load_program(client, name, os.path.join(HERE, f"{name}.ddp"))

        terrain = load_grid(os.path.join(HERE, grid_name))
        feed_chunks(
            client,
            [f"feed water 0 {x},{y} val={height}" for (x, y), height in terrain.items()],
        )
        for site_id, _label, x, y, kind, amount in scenario_sites:
            client.cmd(f"feed pathways 0 {site_id} val={x},{y},{kind},{amount}")
        for tag, value in board_meta(os.path.splitext(grid_name)[0]):
            client.cmd(f"feed meta 0 {tag} val={value}")
        client.cmd(
            f"feed pathways 5 0 val={board['orthogonal_metres']},"
            f"{board['diagonal_metres']}"
        )
        client.cmd(
            f"feed pathways 7 0 val={version},{road_grade_permille},"
            f"{initial_aggregate},{quarry_aggregate},"
            f"{initial_rock},{quarry_rock}"
        )
        client.cmd("tick")

        actual_water = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek water", collect=True), "water"
            ).items()
        }
        expected_water = priority_flood(terrain)
        if actual_water != expected_water:
            raise RuntimeError("pathways setup water disagrees with priority flood")
        actual_accum = {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek accum", collect=True), "accum"
            ).items()
        }
        _flow, expected_accum = py_flow_accum(actual_water)
        if actual_accum != expected_accum:
            raise RuntimeError("pathways setup accumulation disagrees with Python")

        sites = [
            {
                "id": site_id,
                "label": label,
                "cell": [x, y],
                "kind": kind,
                "amount": amount,
            }
            for site_id, label, x, y, kind, amount in scenario_sites
        ]
        if rules is None:
            rules = [
                "A route survey is computed inside DDIR from integer distance, grade, water-depth, runoff, and surface-reuse coefficients.",
                "Entering raw ground, an established path, or infrastructure adds respectively 2, 1, or 0 times reuse weight times step length.",
                "Each delivery freezes its route cells into history; later changes never reroute old journeys.",
                f"A cell becomes an established path after {trail_use_required} cargo journeys and only established paths may be paved.",
                f"Before road connection, porters carry at most {porter_trip_capacity} units per journey and {porter_town_quota} total units to a town; road freight carries the remainder.",
                "Paving must advance from the live source-connected network; agents have separate road and bridge grants.",
                f"Wet cells and cells with drainage area >= {bridge_accum_threshold} require a bridge so water can pass.",
                "Every delivery route starts at a supply site and ends at its town; total delivery cannot exceed source supply or town demand.",
                f"At most {max_live_routes} route surveys remain live; retire comparisons or completed routes before opening another.",
            ]
            if version >= 3:
                rules.extend([
                    "Only agent 1 may carry the two light five-unit loads that activate quarry 20.",
                    "Agent 1 may also scout a route twice to establish a cheap footpath before any bulk freight can move.",
                    "The quarry unlocks aggregate and rock only after activation and source-connected road access; its cell never seeds the network.",
                    "Agent 2 builds one grade-limited surface-road frontier cell at a time; agent 3 builds required bridges and hauls bulk rock.",
                    f"Engineered road grade is at most {road_grade_permille} permille; deterministic fill raises the least possible road profile and changes live water flow.",
                    "Roads consume one aggregate per cell plus one rock per metre-cell of fill; bridges preserve the terrain and drainage channel.",
                    f"An embankment alignment raises every wet/high-runoff crossing by at least {drainage_embankment_fill} coarse elevation units; a route cannot switch between bridge and embankment mid-build.",
                    "Bulk rock for worksite 21 must start at an online quarry and traverse a fully connected public route.",
                ])
        briefing = {
            "version": version,
            "run_dir": run_dir,
            "port": port,
            "ws_url": f"ws://{ws_host}:{port + 1}",
            "grid": grid_name,
            "cell_metres": board["orthogonal_metres"],
            "orthogonal_metres": board["orthogonal_metres"],
            "diagonal_metres": board["diagonal_metres"],
            "sites": sites,
            "towns": [site["cell"] for site in sites if site["kind"] == 0],
            "sources": [site["cell"] for site in sites if site["kind"] == 1],
            "agents": {str(agent): values for agent, values in agents.items()},
            "trail_use_required": trail_use_required,
            "bridge_accum_threshold": bridge_accum_threshold,
            "porter_trip_capacity": porter_trip_capacity,
            "porter_town_quota": porter_town_quota,
            "max_live_routes": max_live_routes,
            "road_grade_permille": road_grade_permille,
            "initial_aggregate": initial_aggregate,
            "quarry_aggregate": quarry_aggregate,
            "initial_rock": initial_rock,
            "quarry_rock": quarry_rock,
            "drainage_embankment_fill": drainage_embankment_fill,
            "quarry_activation_agent": quarry_activation_agent,
            "worksite_haul_agent": worksite_haul_agent,
            "scout_agent": 1,
            "scout_trip_limit": 2,
            "program_hashes": program_hashes(),
            "rules": rules,
            "goal": goal,
            "playtest_rubric": rubric,
        }
        write_json(os.path.join(run_dir, "briefing.json"), briefing)
        open(os.path.join(run_dir, "events.jsonl"), "w").close()
        open(os.path.join(run_dir, "game.lock"), "a").close()
        with open(os.path.join(run_dir, "site_office.md"), "w") as office:
            office.write(
                "# Persistent pathways site office\n\n"
                "Record route surveys, coefficient choices, intended journeys, shared segments, and build decisions.\n"
            )
        print(f"run: {run_dir}")
        print(f"tcp: 127.0.0.1:{port}; viewer: {briefing['ws_url']}")
        print("sites: " + "; ".join(
            f"{site['id']}={site['label']}@{tuple(site['cell'])}"
            for site in sites
        ))
        print("agents: " + "; ".join(
            f"{agent}={values['name']} coeff={values['suggested_coefficients']}"
            for agent, values in agents.items()
        ))
    except Exception:
        stop_recorded_server(run_dir)
        raise
    finally:
        server_log.close()


def judge(run_dir):
    run_dir = os.path.abspath(run_dir)
    with open(os.path.join(run_dir, "briefing.json")) as source:
        briefing = json.load(source)
    version = int(briefing.get("version", 1))
    client = Client(int(briefing["port"]))
    client.cmd("tick")

    base_terrain = load_grid(os.path.join(HERE, briefing["grid"]))
    terrain = (
        {
            key: value[0]
            for key, value in unique_rows(
                client.cmd("peek terrain", collect=True), "terrain"
            ).items()
        }
        if version >= 3 else base_terrain
    )
    water = {
        key: value[0]
        for key, value in unique_rows(client.cmd("peek water", collect=True), "water").items()
    }
    accum = {
        key: value[0]
        for key, value in unique_rows(client.cmd("peek accum", collect=True), "accum").items()
    }
    routes = unique_rows(client.cmd("peek route_requests", collect=True), "route_requests")
    ddir_costs = {
        key[0]: value[0]
        for key, value in unique_rows(client.cmd("peek route_cost", collect=True), "route_cost").items()
    }
    path_use = {
        key: value[0]
        for key, value in unique_rows(client.cmd("peek path_use", collect=True), "path_use").items()
    }
    infrastructure = {
        key: (value[0], value[1])
        for key, value in unique_rows(
            client.cmd("peek infrastructure", collect=True), "infrastructure"
        ).items()
    }
    step_lengths = (
        briefing.get("orthogonal_metres", 26),
        briefing.get("diagonal_metres", 37),
    )
    route_checks = {}
    for key, request in routes.items():
        route_id = key[0]
        start = (request[1], request[2])
        target = (request[3], request[4])
        coefficients = tuple(request[5:])
        _path, cost = shortest_route(
            terrain,
            water,
            accum,
            start,
            target,
            coefficients,
            path_use if briefing.get("version", 1) >= 2 else None,
            infrastructure if briefing.get("version", 1) >= 2 else None,
            step_lengths,
        )
        route_checks[route_id] = ddir_costs.get(route_id) == cost
    sources = {
        tuple(site["cell"])
        for site in briefing["sites"]
        if site["kind"] in (1, 2)
    }
    expected_connected = connected_cells(
        infrastructure,
        sources,
        terrain,
        water,
        briefing.get("road_grade_permille") if version >= 3 else None,
        step_lengths,
    )
    actual_connected = set(
        unique_rows(client.cmd("peek connected", collect=True), "connected")
    )
    spend = infrastructure_spend(infrastructure)
    grants_ok = all(
        spend.get((int(agent), ROAD), 0) <= values["road_grant"]
        and spend.get((int(agent), BRIDGE), 0) <= values["bridge_grant"]
        for agent, values in briefing["agents"].items()
    )
    trail_ok = all(path_use.get(cell, 0) >= briefing["trail_use_required"] for cell in infrastructure)
    if version >= 3:
        bridge_ok = all(
            kind == BRIDGE
            or required_kind(
                cell,
                terrain,
                water,
                accum,
                briefing["bridge_accum_threshold"],
            ) == ROAD
            for cell, (kind, _owner) in infrastructure.items()
        )
    else:
        bridge_ok = all(
            kind == required_kind(
                cell, terrain, water, accum, briefing["bridge_accum_threshold"]
            )
            for cell, (kind, _owner) in infrastructure.items()
        )

    deliveries = unique_rows(client.cmd("peek deliveries", collect=True), "deliveries")
    traversals = unique_rows(client.cmd("peek traversals", collect=True), "traversals")
    delivered = {}
    porter_delivered = {}
    total_delivered = 0
    configured_agents = {int(agent) for agent in briefing["agents"]}
    delivery_rows_ok = True
    for _delivery, value in deliveries.items():
        if len(value) < 3:
            delivery_rows_ok = False
            continue
        agent, town_id, units = value[:3]
        delivered[town_id] = delivered.get(town_id, 0) + units
        total_delivered += units
        delivery_rows_ok &= agent in configured_agents and units > 0
        if len(value) >= 5 and value[4] == 0:
            porter_delivered[town_id] = porter_delivered.get(town_id, 0) + units
            delivery_rows_ok &= units <= briefing["porter_trip_capacity"]

    history_error = None
    history_checks = {}
    if briefing.get("version", 1) >= 2:
        try:
            with open(os.path.join(run_dir, "events.jsonl")) as source:
                events = [json.loads(line) for line in source if line.strip()]
            replay_water = priority_flood(base_terrain)
            _replay_flow, replay_accum = py_flow_accum(replay_water)
            replayed = replay_history(
                events,
                briefing,
                base_terrain,
                replay_water,
                replay_accum,
            )
            history_checks = {
                "routes": replayed["route_requests"] == routes,
                "infrastructure": replayed["infrastructure"] == infrastructure,
                "deliveries": replayed["deliveries"] == deliveries,
                "traversals": replayed["traversals"] == traversals,
                "path_use": replayed["path_use"] == path_use,
                "porter_totals": (
                    replayed["porter_delivered"] == porter_delivered
                ),
            }
            if version >= 3:
                live_builds = unique_rows(
                    client.cmd("peek build_actions", collect=True),
                    "build_actions",
                )
                history_checks.update({
                    "build_actions": replayed["build_actions"] == live_builds,
                    "terrain": replayed["terrain"] == terrain,
                    "water": replayed["water"] == water,
                    "accum": replayed["accum"] == accum,
                    "connected": replayed["connected"] == actual_connected,
                })
        except (OSError, ValueError, json.JSONDecodeError) as error:
            history_error = str(error)
        delivery_routes_ok = (
            history_error is None
            and bool(history_checks)
            and all(history_checks.values())
        )
    else:
        delivery_routes_ok = True
    towns = {site["id"]: site for site in briefing["sites"] if site["kind"] == 0}
    total_supply = sum(
        site["amount"] for site in briefing["sites"] if site["kind"] == 1
    )
    town_results = {
        town_id: {
            "connected": tuple(site["cell"]) in expected_connected,
            "delivered": delivered.get(town_id, 0),
            "demand": site["amount"],
        }
        for town_id, site in towns.items()
    }
    deliveries_ok = (
        delivery_rows_ok
        and sum(
            units
            for target_id, units in delivered.items()
            if next(
                site["kind"]
                for site in briefing["sites"] if site["id"] == target_id
            ) in (0, 3)
        ) <= total_supply
        and all(result["delivered"] <= result["demand"] for result in town_results.values())
        and (
            briefing.get("version", 1) >= 2
            or all(
                result["connected"] or result["delivered"] == 0
                for result in town_results.values()
            )
        )
        and all(
            units <= briefing.get("porter_town_quota", units)
            for units in porter_delivered.values()
        )
        and delivery_routes_ok
    )
    v3_checks = {}
    if version >= 3:
        quarries = {
            site["id"]: site
            for site in briefing["sites"] if site["kind"] == 3
        }
        worksites = {
            site["id"]: site
            for site in briefing["sites"] if site["kind"] == 4
        }
        online_quarries = {
            site_id
            for site_id, site in quarries.items()
            if delivered.get(site_id, 0) >= site["amount"]
            and tuple(site["cell"]) in expected_connected
        }
        worksite_ok = all(
            delivered.get(site_id, 0) == site["amount"]
            and tuple(site["cell"]) in expected_connected
            for site_id, site in worksites.items()
        )
        build_rows = unique_rows(
            client.cmd("peek build_actions", collect=True), "build_actions"
        )
        role_ok = all(
            value[4] in briefing["agents"][str(value[0])].get(
                "build_kinds", []
            )
            for value in build_rows.values()
        )
        engineered = sum(
            value[4] == ENGINEERED_ROAD for value in build_rows.values()
        )
        fill = sum(
            max(0, value[6] - value[5])
            for value in build_rows.values()
            if value[4] == ENGINEERED_ROAD
        )
        aggregate_capacity = briefing["initial_aggregate"] + (
            briefing["quarry_aggregate"] if online_quarries else 0
        )
        rock_capacity = briefing["initial_rock"] + (
            briefing["quarry_rock"] if online_quarries else 0
        )
        material_rows = unique_rows(
            client.cmd("peek material_balance", collect=True),
            "material_balance",
        )
        material_ok = (
            engineered <= aggregate_capacity
            and fill <= rock_capacity
            and material_rows.get((0,)) == (aggregate_capacity - engineered,)
            and material_rows.get((1,)) == (rock_capacity - fill,)
        )
        protected_dry = all(
            water[tuple(site["cell"])] == terrain[tuple(site["cell"])]
            for site in briefing["sites"] if site["kind"] in (0, 1)
        )
        v3_checks = {
            "quarry_online": bool(quarries) and len(online_quarries) == len(quarries),
            "worksite": worksite_ok,
            "roles": role_ok,
            "materials": material_ok,
            "protected_dry": protected_dry,
        }

    success = (
        all(route_checks.values())
        and expected_connected == actual_connected
        and grants_ok
        and trail_ok
        and bridge_ok
        and deliveries_ok
        and all(result["delivered"] == result["demand"] for result in town_results.values())
        and all(v3_checks.values())
    )

    print("== PERSISTENT PATHWAYS JUDGMENT ==")
    print(f"towns: {town_results}")
    print(f"construction spend: {dict(sorted(spend.items()))}")
    print(f"route costs agree with independent Dijkstra: {route_checks}")
    print(f"network agrees: {expected_connected == actual_connected}")
    print(f"grants/trails/bridges/deliveries: {grants_ok}/{trail_ok}/{bridge_ok}/{deliveries_ok}")
    if briefing.get("version", 1) >= 2:
        print(
            f"porter deliveries: {dict(sorted(porter_delivered.items()))}; "
            f"history replay: {history_checks or history_error}"
        )
    if version >= 3:
        print(f"hill logistics: {v3_checks}")
    print(f"VERDICT: {'SUCCESS' if success else 'INCOMPLETE'}")
    return success


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("setup", "judge"))
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--ws-host", default="127.0.0.1")
    parser.add_argument(
        "--grid",
        choices=tuple(BOARD_CONFIG),
        default="engadin_128.txt",
        help="board resolution; 128 is the lower-memory interactive default",
    )
    parser.add_argument(
        "--version",
        type=int,
        choices=(2, 3),
        default=2,
        help="scenario mechanics version (V3 adds hill logistics and road fill)",
    )
    args = parser.parse_args()
    if args.mode == "setup":
        setup(
            args.run_dir,
            args.port,
            args.ws_host,
            args.grid,
            scenario_version=args.version,
        )
    elif not judge(args.run_dir):
        sys.exit(1)


if __name__ == "__main__":
    main()
