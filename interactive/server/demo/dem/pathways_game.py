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
import time

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
    "engadin_wide.txt": {
        "orthogonal_metres": 53,
        "diagonal_metres": 75,
        "bridge_accum_threshold": 256,
        "road_grant": 200,
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

OBSERVATORY_SITE = (30, "Piz Nair ridge observatory", 55, 110, 5, 20)
TRAIL_SITE = (31, "Muottas ridge shelter", 213, 75, 6, 10)

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
    "pathways_game.py",
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


def stable_hash(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def file_hash(path):
    with open(path, "rb") as source:
        return hashlib.sha256(source.read()).hexdigest()


def migration_snapshot(source_run, shift_x, shift_y):
    """Freeze a live V3 world as translated, sunk-cost V4 genesis state."""
    source_run = os.path.abspath(source_run)
    with open(os.path.join(source_run, "briefing.json")) as source:
        parent = json.load(source)
    if int(parent.get("version", 1)) != 3:
        raise RuntimeError("V4 migration requires a live V3 parent world")
    client = Client(int(parent["port"]))

    def relation(name):
        return unique_rows(
            client.cmd(f"peek {name}", collect=True), name
        )

    infrastructure = relation("infrastructure")
    traversals = relation("traversals")
    deliveries = relation("deliveries")
    live_terrain = relation("terrain")
    base_terrain = load_grid(os.path.join(HERE, parent["grid"]))
    terrain_edits = [
        [x + shift_x, y + shift_y, value[0]]
        for (x, y), value in sorted(live_terrain.items())
        if value[0] != base_terrain[(x, y)]
    ]
    initial_infrastructure = [
        [x + shift_x, y + shift_y, kind, owner]
        for (x, y), (kind, owner) in sorted(infrastructure.items())
    ]
    initial_traversals = [
        [trip, x + shift_x, y + shift_y, agent, route]
        for (trip, x, y), (agent, route) in sorted(traversals.items())
    ]
    initial_deliveries = [
        [delivery, *value]
        for (delivery,), value in sorted(deliveries.items())
    ]
    sites = [
        (
            int(site["id"]),
            site["label"],
            int(site["cell"][0]) + shift_x,
            int(site["cell"][1]) + shift_y,
            int(site["kind"]),
            int(site["amount"]),
        )
        for site in parent["sites"]
    ]
    with open(os.path.join(source_run, "events.jsonl"), "rb") as source:
        parent_events_hash = hashlib.sha256(source.read()).hexdigest()
    state = {
        "infrastructure": initial_infrastructure,
        "traversals": initial_traversals,
        "deliveries": initial_deliveries,
        "terrain_edits": terrain_edits,
    }
    return {
        "sites": sites,
        "initial_infrastructure": initial_infrastructure,
        "initial_traversals": initial_traversals,
        "initial_deliveries": initial_deliveries,
        "terrain_edits": terrain_edits,
        "migration": {
            "parent_run": source_run,
            "parent_grid": parent["grid"],
            "parent_version": int(parent["version"]),
            "parent_program_hashes": parent.get("program_hashes", {}),
            "parent_events_sha256": parent_events_hash,
            "parent_state_sha256": stable_hash(state),
            "coordinate_shift": [shift_x, shift_y],
            "new_events_begin_at": 0,
        },
    }


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
    migration_from=None,
):
    run_dir = os.path.abspath(run_dir)
    if os.path.exists(run_dir):
        raise RuntimeError(f"run directory already exists: {run_dir}")
    if not os.path.exists(BIN):
        raise RuntimeError("build the server first: cargo build -p ddir-server --release")
    genesis = {}
    if briefing_template is None:
        board = BOARD_CONFIG[grid_name]
        if scenario_version == 3 and grid_name != "engadin_128.txt":
            raise RuntimeError("the calibrated V3 hill scenario uses engadin_128.txt")
        if scenario_version == 4:
            if grid_name != "engadin_wide.txt" or migration_from is None:
                raise RuntimeError(
                    "V4 requires engadin_wide.txt and --migrate-from V3_RUN"
                )
            genesis = migration_snapshot(migration_from, 72, 16)
            scenario_sites = [*genesis["sites"], OBSERVATORY_SITE]
        else:
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
        max_live_routes = 1 if version >= 4 else 4
        goal = (
            "survey and road-connect the Piz Nair ridge observatory, then "
            "deliver its rock foundation from the online quarry while "
            "keeping every protected site dry"
            if version == 4 else (
                "activate and road-connect the mountain quarry, then deliver its "
                "bulk rock to the connected watershed worksite without flooding "
                "a protected valley site"
                if version == 3 else
                "fulfil every town's demand without exceeding any construction "
                "grant or flooding a surface road"
            )
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

    initial_infrastructure = list(
        (briefing_template or genesis).get("initial_infrastructure", [])
    )
    initial_traversals = list(
        (briefing_template or genesis).get("initial_traversals", [])
    )
    initial_deliveries = list(
        (briefing_template or genesis).get("initial_deliveries", [])
    )
    terrain_edits = list(
        (briefing_template or genesis).get("terrain_edits", [])
    )
    migration = (briefing_template or genesis).get("migration")
    trail_extension = (briefing_template or genesis).get("trail_extension")
    program_upgrades = list(
        (briefing_template or genesis).get("program_upgrades", [])
    )

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
        if version >= 4:
            initial_aggregate = int(
                (briefing_template or {}).get("initial_aggregate", 0)
            )
            quarry_aggregate = int(
                (briefing_template or {}).get("quarry_aggregate", 65)
            )
            initial_rock = int(
                (briefing_template or {}).get("initial_rock", 0)
            )
            quarry_rock = int(
                (briefing_template or {}).get("quarry_rock", 60)
            )
            agents[3]["bridge_grant"] = int(
                agents[3].get("bridge_grant", 3)
                if briefing_template is not None else 12
            )
        foot_grade_permille = int(
            (briefing_template or {}).get("foot_grade_permille", 800)
        ) if version >= 5 else 0
        trail_outpost_agent = int(
            (briefing_template or {}).get("trail_outpost_agent", 1)
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
        foot_grade_permille = 0
        trail_outpost_agent = 1

    terrain = load_grid(os.path.join(HERE, grid_name))
    for x, y, height in terrain_edits:
        cell = (int(x), int(y))
        if cell not in terrain:
            raise RuntimeError(f"migrated terrain edit is outside V4: {cell}")
        terrain[cell] = int(height)

    if migration_from is not None:
        stop_recorded_server(migration_from)
        for _attempt in range(100):
            try:
                ensure_ports_available((port, port + 1, port + 2))
                break
            except RuntimeError:
                time.sleep(0.1)
        else:
            raise RuntimeError("parent server did not release the V4 ports")
    else:
        ensure_ports_available((port, port + 1, port + 2))

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
        feed_chunks(client, [
            f"feed pathways 2 {trip},{x},{y} val={agent},{route}"
            for trip, x, y, agent, route in initial_traversals
        ])
        feed_chunks(client, [
            f"feed pathways 3 {x},{y} val={kind},{owner}"
            for x, y, kind, owner in initial_infrastructure
        ])
        feed_chunks(client, [
            f"feed pathways 4 {delivery} "
            f"val={agent},{target},{units},{route},{mode}"
            for delivery, agent, target, units, route, mode
            in initial_deliveries
        ])
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
            if version >= 4:
                rules.extend([
                    "The V3 world is a translated, hashed genesis snapshot; its roads, paths, deliveries, and terrain works are sunk history rather than replayed on the wider graph.",
                    "Site 30 is a required high-altitude observatory foundation. It never seeds the road network.",
                    "Agent 1 scouts the observatory approach twice, agent 2 builds its graded roads, and agent 3 builds crossings and delivers all foundation rock from an online quarry over connected infrastructure.",
                    "The current server and viewer deliberately expose the full terrain. Observatory-driven discovery is a future information-boundary mechanic, not cosmetic fog in V4.",
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
            "foot_grade_permille": foot_grade_permille,
            "trail_outpost_agent": trail_outpost_agent,
            "scout_agent": 1,
            "scout_trip_limit": 2,
            "initial_infrastructure": initial_infrastructure,
            "initial_traversals": initial_traversals,
            "initial_deliveries": initial_deliveries,
            "terrain_edits": terrain_edits,
            "migration": migration,
            "trail_extension": trail_extension,
            "program_upgrades": program_upgrades,
            "grid_sha256": file_hash(os.path.join(HERE, grid_name)),
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


def route_geometry(steps, route_id, start, target):
    """Walk DDIR's predecessor chain back from a route's target."""
    chain = {(key[1], key[2]): value for key, value in steps.items() if key[0] == route_id}
    if target not in chain:
        return None
    path = [target]
    while path[-1] != start:
        value = chain[path[-1]]
        prior = (value[1], value[2])
        if prior == path[-1] or len(path) > len(chain) + 1:
            return None
        path.append(prior)
    path.reverse()
    return path


def extend_trail(run_dir):
    """Extend a completed live V4 world with one trail-only ridge outpost."""
    run_dir = os.path.abspath(run_dir)
    briefing_path = os.path.join(run_dir, "briefing.json")
    with open(briefing_path) as source:
        briefing = json.load(source)
    if int(briefing.get("version", 1)) != 4:
        raise RuntimeError("trail extension requires a V4 world")
    if any(int(site["id"]) == TRAIL_SITE[0] for site in briefing["sites"]):
        raise RuntimeError("trail outpost is already present")

    client = Client(int(briefing["port"]))
    client.cmd("tick")
    routes = unique_rows(
        client.cmd("peek route_requests", collect=True), "route_requests"
    )
    if routes:
        raise RuntimeError("retire every live survey before extending V4")
    deliveries = unique_rows(
        client.cmd("peek deliveries", collect=True), "deliveries"
    )
    delivered = sum(
        value[2] for value in deliveries.values() if value[1] == 30
    )
    connected = set(
        unique_rows(client.cmd("peek connected", collect=True), "connected")
    )
    observatory = next(
        site for site in briefing["sites"] if int(site["id"]) == 30
    )
    if (
        delivered != int(observatory["amount"])
        or tuple(observatory["cell"]) not in connected
    ):
        raise RuntimeError("complete the connected observatory before V5")

    old_policy = (
        f"4,{briefing['road_grade_permille']},"
        f"{briefing['initial_aggregate']},{briefing['quarry_aggregate']},"
        f"{briefing['initial_rock']},{briefing['quarry_rock']}"
    )
    new_policy = "5," + old_policy.split(",", 1)[1]
    site_id, label, x, y, kind, amount = TRAIL_SITE
    client.batch([
        f"feed pathways 0 {site_id} val={x},{y},{kind},{amount}",
        f"feed pathways 7 0 val={old_policy} diff=-1",
        f"feed pathways 7 0 val={new_policy}",
    ])
    client.cmd("tick")

    prior_hashes = briefing.get("program_hashes", {})
    briefing["sites"].append({
        "id": site_id,
        "label": label,
        "cell": [x, y],
        "kind": kind,
        "amount": amount,
    })
    briefing.update({
        "version": 5,
        "foot_grade_permille": 800,
        "trail_outpost_agent": 1,
        "goal": (
            "keep the Piz Nair observatory operational and carry the world's "
            "last ten light-supply units to the trail-only Muottas ridge shelter"
        ),
        "program_hashes": program_hashes(),
        "trail_extension": {
            "from_version": 4,
            "prior_program_hashes": prior_hashes,
            "site": [site_id, x, y, kind, amount],
            "foot_grade_permille": 800,
        },
    })
    briefing["rules"] = list(briefing["rules"]) + [
        "Site 31 is a trail-only ridge shelter: it accepts exactly two five-unit courier loads from a lowland source.",
        "Every outpost journey must be dry and remain within an 800-permille foot-grade limit; road freight, paving, and engineered construction to that endpoint are forbidden.",
        "The two real supply journeys establish the public footpath. They consume the final ten units of lowland light-cargo capacity.",
    ]
    write_json(briefing_path, briefing)
    with open(os.path.join(run_dir, "site_office.md"), "a") as office:
        office.write(
            "\n\n## V5 trail extension\n\n"
            "The operational observatory unlocked the Muottas ridge shelter "
            "at `(213,75)`: a deliberately trail-only ten-unit courier job.\n"
        )
    print(
        f"extended live world to V5: site {site_id}={label}@({x},{y}); "
        "foot grade 800 permille, two five-unit courier loads required"
    )


def prepare_route_join_upgrade(run_dir):
    """Seal an exact DDP join-order upgrade and stop the live source world."""
    run_dir = os.path.abspath(run_dir)
    briefing_path = os.path.join(run_dir, "briefing.json")
    with open(briefing_path) as source:
        briefing = json.load(source)
    if int(briefing.get("version", 1)) < 4:
        raise RuntimeError("route join upgrade requires the persistent wide world")
    client = Client(int(briefing["port"]))
    routes = unique_rows(
        client.cmd("peek route_requests", collect=True), "route_requests"
    )
    if routes:
        raise RuntimeError("retire every live route before the DDIR upgrade")
    old_hashes = briefing.get("program_hashes", {})
    new_hashes = program_hashes()
    changed = {
        name for name in set(old_hashes) | set(new_hashes)
        if old_hashes.get(name) != new_hashes.get(name)
    }
    allowed = {"pathways.ddp", "pathways_game.py"}
    if "pathways.ddp" not in changed or not changed <= allowed:
        raise RuntimeError(
            f"unexpected program changes at route-join boundary: {sorted(changed)}"
        )
    briefing.setdefault("program_upgrades", []).append({
        "name": "route-request-before-edge-fanout-v1",
        "changed_files": sorted(changed),
        "old_hashes": {name: old_hashes.get(name) for name in sorted(changed)},
        "new_hashes": {name: new_hashes.get(name) for name in sorted(changed)},
        "claim": "algebraic join reassociation; no input, output, or route-cost change",
    })
    briefing["program_hashes"] = new_hashes
    write_json(briefing_path, briefing)
    stop_recorded_server(run_dir)
    print(
        "sealed route join-order upgrade and stopped source server; "
        f"changed {sorted(changed)}"
    )


def prepare_grade_trials(run_dir):
    """Seal the slope-aware route extension and stop its source world."""
    run_dir = os.path.abspath(run_dir)
    briefing_path = os.path.join(run_dir, "briefing.json")
    with open(briefing_path) as source:
        briefing = json.load(source)
    if int(briefing.get("version", 1)) < 5:
        raise RuntimeError("grade trials require the V5 trail world")
    client = Client(int(briefing["port"]))
    routes = unique_rows(
        client.cmd("peek route_requests", collect=True), "route_requests"
    )
    if routes:
        raise RuntimeError("retire every live route before the grade upgrade")
    old_hashes = briefing.get("program_hashes", {})
    new_hashes = program_hashes()
    changed = {
        name for name in set(old_hashes) | set(new_hashes)
        if old_hashes.get(name) != new_hashes.get(name)
    }
    allowed = {
        "pathways.ddp",
        "pathways_game.py",
        "pathways_rules.py",
        "pathways_client.py",
    }
    if "pathways.ddp" not in changed or not changed <= allowed:
        raise RuntimeError(
            f"unexpected program changes at grade-trial boundary: {sorted(changed)}"
        )
    briefing["max_live_routes"] = 3
    rules = [
        rule for rule in briefing.get("rules", [])
        if not (
            rule.startswith("At most ")
            and "route surveys remain live" in rule
        )
    ]
    rules.extend([
        "A survey may declare a positive maximum edge grade in permille; DDIR excludes steeper edges while finding the route.",
        "At most 3 route surveys remain live during the controlled grade comparison.",
    ])
    briefing["rules"] = rules
    briefing.setdefault("program_upgrades", []).append({
        "name": "route-grade-cap-v1",
        "changed_files": sorted(changed),
        "old_hashes": {
            name: old_hashes.get(name) for name in sorted(changed)
        },
        "new_hashes": {
            name: new_hashes.get(name) for name in sorted(changed)
        },
        "claim": (
            "semantic extension: optional per-route grade caps filter DDIR "
            "edges; historical requests remain effectively unlimited"
        ),
    })
    briefing["program_hashes"] = new_hashes
    write_json(briefing_path, briefing)
    stop_recorded_server(run_dir)
    print(
        "sealed slope-aware routing upgrade and stopped source server; "
        f"changed {sorted(changed)}"
    )


def prepare_water_priority_upgrade(run_dir):
    """Seal the exact boundary-seeded water schedule and stop the old world."""
    run_dir = os.path.abspath(run_dir)
    briefing_path = os.path.join(run_dir, "briefing.json")
    with open(briefing_path) as source:
        briefing = json.load(source)
    if int(briefing.get("version", 1)) < 2:
        raise RuntimeError("water scheduling upgrade requires a replayable world")

    client = Client(int(briefing["port"]))
    terrain = {
        key: value[0]
        for key, value in unique_rows(
            client.cmd("peek terrain", collect=True), "terrain"
        ).items()
    }
    water = {
        key: value[0]
        for key, value in unique_rows(
            client.cmd("peek water", collect=True), "water"
        ).items()
    }
    accum = {
        key: value[0]
        for key, value in unique_rows(
            client.cmd("peek accum", collect=True), "accum"
        ).items()
    }
    expected_water = priority_flood(terrain)
    _expected_flow, expected_accum = py_flow_accum(expected_water)
    if water != expected_water or accum != expected_accum:
        raise RuntimeError(
            "live hydrology must match the independent oracle before upgrade"
        )

    old_hashes = briefing.get("program_hashes", {})
    new_hashes = program_hashes()
    changed = {
        name for name in set(old_hashes) | set(new_hashes)
        if old_hashes.get(name) != new_hashes.get(name)
    }
    allowed = {"water.ddp", "pathways_game.py"}
    if "water.ddp" not in changed or not changed <= allowed:
        raise RuntimeError(
            f"unexpected files at water-priority boundary: {sorted(changed)}"
        )
    briefing.setdefault("program_upgrades", []).append({
        "name": "boundary-seeded-water-v1",
        "changed_files": sorted(changed),
        "old_hashes": {
            name: old_hashes.get(name) for name in sorted(changed)
        },
        "new_hashes": {
            name: new_hashes.get(name) for name in sorted(changed)
        },
        "claim": (
            "exact scheduling change: boundary seeds grow the same priority-"
            "flood fixed point in height classes instead of descending from "
            "a global ceiling"
        ),
        "live_oracle": {
            "terrain_cells": len(terrain),
            "water_exact": True,
            "accum_exact": True,
        },
    })
    briefing["program_hashes"] = new_hashes
    write_json(briefing_path, briefing)
    stop_recorded_server(run_dir)
    print(
        "sealed exact water-scheduling upgrade and stopped source server; "
        f"changed {sorted(changed)}"
    )


def adopt_route_geometry_judge(run_dir):
    """Record a judge-only game-driver upgrade without restarting DDIR."""
    run_dir = os.path.abspath(run_dir)
    briefing_path = os.path.join(run_dir, "briefing.json")
    with open(briefing_path) as source:
        briefing = json.load(source)
    client = Client(int(briefing["port"]))
    routes = unique_rows(
        client.cmd("peek route_requests", collect=True), "route_requests"
    )
    if routes:
        raise RuntimeError("retire every live route before adopting the judge")
    old_hashes = briefing.get("program_hashes", {})
    new_hashes = program_hashes()
    changed = {
        name for name in set(old_hashes) | set(new_hashes)
        if old_hashes.get(name) != new_hashes.get(name)
    }
    if changed != {"pathways_game.py"}:
        raise RuntimeError(
            f"unexpected files at route-geometry judge boundary: {sorted(changed)}"
        )
    briefing.setdefault("program_upgrades", []).append({
        "name": "route-cost-and-geometry-judge-v1",
        "changed_files": ["pathways_game.py"],
        "old_hashes": {"pathways_game.py": old_hashes["pathways_game.py"]},
        "new_hashes": {"pathways_game.py": new_hashes["pathways_game.py"]},
        "claim": "judge validation only; live and replay dataflow semantics unchanged",
    })
    briefing["program_hashes"] = new_hashes
    write_json(briefing_path, briefing)
    print("adopted route cost-and-geometry judge without restarting DDIR")


def judge(run_dir):
    run_dir = os.path.abspath(run_dir)
    with open(os.path.join(run_dir, "briefing.json")) as source:
        briefing = json.load(source)
    version = int(briefing.get("version", 1))
    client = Client(int(briefing["port"]))
    client.cmd("tick")

    base_terrain = load_grid(os.path.join(HERE, briefing["grid"]))
    for x, y, height in briefing.get("terrain_edits", []):
        base_terrain[(int(x), int(y))] = int(height)
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
    route_grade_caps = unique_rows(
        client.cmd("peek route_grade_caps", collect=True),
        "route_grade_caps",
    )
    ddir_costs = {
        key[0]: value[0]
        for key, value in unique_rows(client.cmd("peek route_cost", collect=True), "route_cost").items()
    }
    ddir_steps = unique_rows(client.cmd("peek route_steps", collect=True), "route_steps")
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
        expected_path, cost = shortest_route(
            terrain,
            water,
            accum,
            start,
            target,
            coefficients,
            path_use if briefing.get("version", 1) >= 2 else None,
            infrastructure if briefing.get("version", 1) >= 2 else None,
            step_lengths,
            route_grade_caps.get((route_id,), (None,))[0],
        )
        # Equal cost does not imply equal geometry: a grid carries many
        # equal-cost paths, and the reuse mechanism is a claim about which
        # cells a route takes. Both settle the same lexicographic
        # (cost, predecessor) pair, so the chains must agree exactly.
        route_checks[route_id] = (
            ddir_costs.get(route_id) == cost
            and route_geometry(ddir_steps, route_id, start, target) == expected_path
        )
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
                "route_grade_caps": (
                    replayed["route_grade_caps"] == route_grade_caps
                ),
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
    general_target_kinds = (0, 3, 6) if version >= 5 else (0, 3)
    deliveries_ok = (
        delivery_rows_ok
        and sum(
            units
            for target_id, units in delivered.items()
            if next(
                site["kind"]
                for site in briefing["sites"] if site["id"] == target_id
            ) in general_target_kinds
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
        observatories = {
            site["id"]: site
            for site in briefing["sites"] if site["kind"] == 5
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
        observatory_ok = all(
            delivered.get(site_id, 0) == site["amount"]
            and tuple(site["cell"]) in expected_connected
            and water[tuple(site["cell"])] == terrain[tuple(site["cell"])]
            for site_id, site in observatories.items()
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
            for site in briefing["sites"]
            if site["kind"] in ((0, 1, 5, 6) if version >= 5 else (0, 1))
        )
        v3_checks = {
            "quarry_online": bool(quarries) and len(online_quarries) == len(quarries),
            "worksite": worksite_ok,
            "roles": role_ok,
            "materials": material_ok,
            "protected_dry": protected_dry,
        }
        if version >= 4:
            initial_state = {
                "infrastructure": briefing.get(
                    "initial_infrastructure", []
                ),
                "traversals": briefing.get("initial_traversals", []),
                "deliveries": briefing.get("initial_deliveries", []),
                "terrain_edits": briefing.get("terrain_edits", []),
            }
            migration = briefing.get("migration") or {}
            v3_checks.update({
                "observatory": bool(observatories) and observatory_ok,
                "genesis_hash": (
                    migration.get("parent_state_sha256")
                    == stable_hash(initial_state)
                ),
                "grid_hash": (
                    briefing.get("grid_sha256")
                    == file_hash(os.path.join(HERE, briefing["grid"]))
                ),
            })
        if version >= 5:
            outposts = {
                site["id"]: site
                for site in briefing["sites"] if site["kind"] == 6
            }
            outpost_rows = {
                site_id: [
                    value for value in deliveries.values()
                    if value[1] == site_id
                ]
                for site_id in outposts
            }
            outpost_ok = all(
                delivered.get(site_id, 0) == site["amount"]
                and len(outpost_rows[site_id]) == 2
                and all(
                    value[0] == briefing["trail_outpost_agent"]
                    and value[2] == briefing["porter_trip_capacity"]
                    and value[4] == 0
                    for value in outpost_rows[site_id]
                )
                and tuple(site["cell"]) not in infrastructure
                and path_use.get(tuple(site["cell"]), 0)
                >= briefing["trail_use_required"]
                for site_id, site in outposts.items()
            )
            v3_checks["trail_outpost"] = bool(outposts) and outpost_ok

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
    print(f"route costs and geometry agree with independent Dijkstra: {route_checks}")
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
    parser.add_argument(
        "mode",
        choices=(
            "setup",
            "judge",
            "extend-trail",
            "prepare-route-join-upgrade",
            "prepare-grade-trials",
            "prepare-water-priority-upgrade",
            "adopt-route-geometry-judge",
        ),
    )
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
        choices=(2, 3, 4),
        default=2,
        help="scenario mechanics version (V4 adds the wider observatory project)",
    )
    parser.add_argument(
        "--migrate-from",
        help="live V3 run to freeze and translate into a V4 genesis snapshot",
    )
    args = parser.parse_args()
    if args.mode == "setup":
        setup(
            args.run_dir,
            args.port,
            args.ws_host,
            args.grid,
            scenario_version=args.version,
            migration_from=args.migrate_from,
        )
    elif args.mode == "extend-trail":
        extend_trail(args.run_dir)
    elif args.mode == "prepare-route-join-upgrade":
        prepare_route_join_upgrade(args.run_dir)
    elif args.mode == "prepare-grade-trials":
        prepare_grade_trials(args.run_dir)
    elif args.mode == "prepare-water-priority-upgrade":
        prepare_water_priority_upgrade(args.run_dir)
    elif args.mode == "adopt-route-geometry-judge":
        adopt_route_geometry_judge(args.run_dir)
    elif not judge(args.run_dir):
        sys.exit(1)


if __name__ == "__main__":
    main()
