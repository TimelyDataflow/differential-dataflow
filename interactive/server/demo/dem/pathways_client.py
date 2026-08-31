#!/usr/bin/env python3
"""Participant client for the persistent paths-and-roads experiment.

Examples:
  pathways_client.py --run-dir runs/paths-01 --agent 1 status
  pathways_client.py --run-dir runs/paths-01 --agent 1 survey 101 10 1 1 4 1000 8 1
  pathways_client.py --run-dir runs/paths-01 --agent 1 route 101
  pathways_client.py --run-dir runs/paths-01 --agent 1 deliver 1 5 101
  pathways_client.py --run-dir runs/paths-01 --agent 1 deliver 1 5 101
  pathways_client.py --run-dir runs/paths-01 --agent 1 pave 101 40
  pathways_client.py --run-dir runs/paths-01 --agent 1 deliver 1 30 101
  pathways_client.py --run-dir runs/paths-01 --agent 1 retire 101
"""

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_runtime import append_event, read_briefing, run_lock, unique_rows
from pathways_rules import (
    BRIDGE,
    ENGINEERED_ROAD,
    ROAD,
    connected_cells,
    edge_grade_ok,
    engineered_profile,
    infrastructure_spend,
    neighbors8,
    required_kind,
    route_metrics,
    shortest_route,
)
from run_dem import Client, load_grid, priority_flood
from run_physics import py_flow_accum


def rows(client, name):
    return unique_rows(client.cmd(f"peek {name}", collect=True), name)


def event(run_dir, agent, command, accepted, detail=None):
    append_event(
        run_dir,
        {
            "time": time.time(),
            "agent": agent,
            "command": command,
            "accepted": accepted,
            "detail": detail,
        },
    )


def site_map(briefing):
    return {int(site["id"]): site for site in briefing["sites"]}


def board_step_lengths(briefing):
    return (
        briefing.get("orthogonal_metres", 26),
        briefing.get("diagonal_metres", 37),
    )


def world_metrics(client, briefing):
    if int(briefing.get("version", 1)) >= 3:
        terrain = {key: value[0] for key, value in rows(client, "terrain").items()}
    else:
        terrain = load_grid(os.path.join(HERE, briefing["grid"]))
    water = {key: value[0] for key, value in rows(client, "water").items()}
    accum = {key: value[0] for key, value in rows(client, "accum").items()}
    return terrain, water, accum


def ordered_route(client, route_id):
    requests = rows(client, "route_requests")
    request = requests.get((route_id,))
    if request is None:
        raise ValueError(f"no route {route_id}")
    start = (request[1], request[2])
    target = (request[3], request[4])
    steps = {
        (key[1], key[2]): value
        for key, value in rows(client, "route_steps").items()
        if key[0] == route_id
    }
    if target not in steps:
        raise RuntimeError(f"route {route_id} has not reached its target")
    path = [target]
    while path[-1] != start:
        value = steps[path[-1]]
        prior = (value[1], value[2])
        if prior == path[-1]:
            raise RuntimeError(f"route {route_id} predecessor chain stopped early")
        path.append(prior)
        if len(path) > len(steps) + 1:
            raise RuntimeError(f"route {route_id} predecessor cycle")
    path.reverse()
    return request, path, steps[target][0]


def construction_state(client, briefing):
    terrain, water, accum = world_metrics(client, briefing)
    infrastructure = {
        key: (value[0], value[1])
        for key, value in rows(client, "infrastructure").items()
    }
    path_use = {key: value[0] for key, value in rows(client, "path_use").items()}
    sources = {
        tuple(site["cell"])
        for site in briefing["sites"]
        if int(site["kind"]) in (1, 2)
    }
    expected_connected = connected_cells(
        infrastructure,
        sources,
        terrain,
        water,
        briefing.get("road_grade_permille")
        if int(briefing.get("version", 1)) >= 3 else None,
        board_step_lengths(briefing),
    )
    actual_connected = set(rows(client, "connected"))
    if actual_connected != expected_connected:
        raise RuntimeError("DDIR and Python disagree on the connected road network")
    return terrain, water, accum, infrastructure, path_use, sources, actual_connected


def show_status(client, briefing):
    (
        terrain,
        water,
        accum,
        infrastructure,
        path_use,
        _sources,
        connected,
    ) = construction_state(client, briefing)
    del terrain, water, accum
    spent = infrastructure_spend(infrastructure)
    traversals = rows(client, "traversals")
    established = sum(
        1 for use in path_use.values() if use >= briefing["trail_use_required"]
    )
    print(
        f"{len(traversals)} frozen traversal-cells; {len(path_use)} used cells; "
        f"{established} established path cells"
    )
    print(
        f"infrastructure: {len(infrastructure)} cells; "
        f"network reaches {len(connected)} cells"
    )
    for agent_text, values in sorted(briefing["agents"].items(), key=lambda row: int(row[0])):
        agent = int(agent_text)
        roads = spent.get((agent, ROAD), 0) + spent.get(
            (agent, ENGINEERED_ROAD), 0
        )
        bridges = spent.get((agent, BRIDGE), 0)
        print(
            f"agent {agent} {values['name']}: roads {roads}/{values['road_grant']}, "
            f"bridges {bridges}/{values['bridge_grant']}"
        )

    deliveries = rows(client, "deliveries")
    delivered = {}
    for value in deliveries.values():
        delivered[value[1]] = delivered.get(value[1], 0) + value[2]
    total_supply = sum(
        site["amount"] for site in briefing["sites"] if site["kind"] == 1
    )
    site_kinds = {
        site["id"]: site["kind"] for site in briefing["sites"]
    }
    general_kinds = (
        (0, 3, 6)
        if int(briefing.get("version", 1)) >= 5 else (0, 3)
    )
    general_delivered = sum(
        amount
        for site_id, amount in delivered.items()
        if site_kinds[site_id] in general_kinds
    )
    print(
        f"town/light cargo delivered: {general_delivered}/"
        f"{total_supply} lowland source capacity"
    )
    for site in briefing["sites"]:
        if site["kind"] != 0:
            continue
        cell = tuple(site["cell"])
        amount = delivered.get(site["id"], 0)
        print(
            f"town {site['id']} {site['label']}: "
            f"{'CONNECTED' if cell in connected else 'disconnected'}, "
            f"delivery {amount}/{site['amount']}"
        )

    if int(briefing.get("version", 1)) >= 3:
        online = []
        for site in briefing["sites"]:
            if site["kind"] == 3:
                activated = delivered.get(site["id"], 0) >= site["amount"]
                is_online = activated and tuple(site["cell"]) in connected
                if is_online:
                    online.append(site["id"])
                print(
                    f"quarry {site['id']} {site['label']}: activation "
                    f"{delivered.get(site['id'], 0)}/{site['amount']}, "
                    f"{'ONLINE' if is_online else 'offline'}"
                )
            elif site["kind"] == 4:
                print(
                    f"worksite {site['id']} {site['label']}: "
                    f"{'CONNECTED' if tuple(site['cell']) in connected else 'disconnected'}, "
                    f"rock {delivered.get(site['id'], 0)}/{site['amount']}"
                )
            elif site["kind"] == 5:
                complete = (
                    tuple(site["cell"]) in connected
                    and delivered.get(site["id"], 0) == site["amount"]
                )
                print(
                    f"observatory {site['id']} {site['label']}: "
                    f"{'OPERATIONAL' if complete else 'in construction'}, "
                    f"access {'connected' if tuple(site['cell']) in connected else 'missing'}, "
                    f"foundation rock {delivered.get(site['id'], 0)}/{site['amount']}"
                )
            elif site["kind"] == 6:
                print(
                    f"trail outpost {site['id']} {site['label']}: "
                    f"light supplies {delivered.get(site['id'], 0)}/"
                    f"{site['amount']}, "
                    f"{'trail established' if path_use.get(tuple(site['cell']), 0) >= briefing['trail_use_required'] else 'approach unestablished'}"
                )
        aggregate_capacity = briefing["initial_aggregate"] + (
            briefing["quarry_aggregate"] if online else 0
        )
        rock_capacity = briefing["initial_rock"] + (
            briefing["quarry_rock"] if online else 0
        )
        build_rows = rows(client, "build_actions")
        engineered = (
            sum(value[4] == ENGINEERED_ROAD for value in build_rows.values())
            if int(briefing.get("version", 1)) >= 4 else
            sum(
                kind == ENGINEERED_ROAD
                for kind, _owner in infrastructure.values()
            )
        )
        fill = sum(
            max(0, value[6] - value[5])
            for value in build_rows.values()
            if value[4] == ENGINEERED_ROAD
        )
        print(
            f"construction materials: road aggregate "
            f"{aggregate_capacity - engineered}/"
            f"{aggregate_capacity}, road-fill rock "
            f"{rock_capacity - fill}/{rock_capacity}; "
            f"online quarry ids {online or 'none'}"
        )

    requests = rows(client, "route_requests")
    route_caps = rows(client, "route_grade_caps")
    costs = rows(client, "route_cost")
    if requests:
        print("routes:")
        for key, request in sorted(requests.items()):
            route_id = key[0]
            cost = costs.get((route_id,), (None,))[0]
            try:
                _request, path, _cost = ordered_route(client, route_id)
                length = len(path)
            except (ValueError, RuntimeError):
                length = "unresolved"
            print(
                f"  {route_id}: agent {request[0]} "
                f"({request[1]},{request[2]}) -> ({request[3]},{request[4]}) "
                f"coeff={tuple(request[5:])} "
                f"max_grade={route_caps.get((route_id,), ('unlimited',))[0]} "
                f"cost={cost} cells={length}"
            )


def survey(client, run_dir, briefing, agent, tokens):
    version = briefing.get("version", 1)
    expected = 8 if version >= 2 else 7
    allowed = (expected, expected + 1) if version >= 2 else (expected,)
    if len(tokens) not in allowed:
        suffix = " REUSE" if version >= 2 else ""
        cap_suffix = " [MAX_GRADE_PERMILLE]" if version >= 2 else ""
        raise ValueError(
            "survey requires ROUTE FROM_SITE TO_SITE DIST GRADE WATER RUNOFF"
            + suffix + cap_suffix
        )
    values = list(map(int, tokens))
    route_id, from_id, to_id = values[:3]
    coefficients = values[3:expected]
    max_grade_permille = values[expected] if len(values) > expected else None
    if min(coefficients) < 0 or coefficients[0] < 1:
        raise ValueError("route coefficients must be non-negative and DIST must be positive")
    if max_grade_permille is not None and max_grade_permille <= 0:
        raise ValueError("maximum grade must be positive")
    sites = site_map(briefing)
    if from_id not in sites or to_id not in sites:
        raise ValueError("unknown site id")
    requests = rows(client, "route_requests")
    if (route_id,) in requests:
        raise ValueError(f"route id {route_id} already exists")
    route_limit = briefing.get("max_live_routes")
    if route_limit is not None and len(requests) >= route_limit:
        raise ValueError(
            f"live route limit reached: {len(requests)}/{route_limit}; "
            "retire a comparison or completed survey first"
        )
    start = sites[from_id]["cell"]
    target = sites[to_id]["cell"]
    coefficient_text = ",".join(str(value) for value in coefficients)
    request_text = (
        f"{agent},{start[0]},{start[1]},"
        f"{target[0]},{target[1]},{coefficient_text}"
    )
    feeds = [f"feed pathways 1 {route_id} val={request_text}"]
    if max_grade_permille is not None:
        feeds.append(
            f"feed pathways 8 {route_id} val={max_grade_permille}"
        )
    client.batch(feeds)
    client.cmd("tick")
    try:
        request, path, ddir_cost = ordered_route(client, route_id)
        terrain, water, accum = world_metrics(client, briefing)
        path_use = {
            key: value[0] for key, value in rows(client, "path_use").items()
        }
        infrastructure = {
            key: (value[0], value[1])
            for key, value in rows(client, "infrastructure").items()
        }
        expected_path, expected_cost = shortest_route(
            terrain,
            water,
            accum,
            tuple(start),
            tuple(target),
            tuple(request[5:]),
            path_use if version >= 2 else None,
            infrastructure if version >= 2 else None,
            board_step_lengths(briefing),
            max_grade_permille,
        )
        if expected_path is None:
            raise RuntimeError(
                f"route {route_id} is unresolved under maximum grade "
                f"{max_grade_permille}"
            )
        if path != expected_path or ddir_cost != expected_cost:
            raise RuntimeError("DDIR route disagrees with independent Dijkstra")
        metrics = route_metrics(
            path,
            terrain,
            water,
            accum,
            briefing["bridge_accum_threshold"],
            board_step_lengths(briefing),
        )
    except Exception as error:
        try:
            rollback = [
                f"feed pathways 1 {route_id} val={request_text} diff=-1"
            ]
            if max_grade_permille is not None:
                rollback.append(
                    f"feed pathways 8 {route_id} "
                    f"val={max_grade_permille} diff=-1"
                )
            client.batch(rollback)
            client.cmd("tick")
        except Exception as rollback_error:
            raise RuntimeError(
                f"survey validation failed ({error}); "
                f"route rollback also failed ({rollback_error})"
            ) from rollback_error
        raise
    event(run_dir, agent, "survey " + " ".join(tokens), True, {
        "route": route_id,
        "cost": ddir_cost,
        "max_grade_permille": max_grade_permille,
        "metrics": {key: value if not isinstance(value, list) else len(value)
                    for key, value in metrics.items()},
    })
    print(f"route {route_id} accepted: cost {ddir_cost}; {metrics}")


def retire_route(client, run_dir, agent, route_id):
    requests = rows(client, "route_requests")
    request = requests.get((route_id,))
    if request is None:
        raise ValueError(f"no route {route_id}")
    if request[0] != agent:
        raise ValueError(
            f"route {route_id} belongs to agent {request[0]}, not agent {agent}"
        )
    values = ",".join(str(value) for value in request)
    feeds = [f"feed pathways 1 {route_id} val={values} diff=-1"]
    cap = rows(client, "route_grade_caps").get((route_id,))
    if cap is not None:
        feeds.append(
            f"feed pathways 8 {route_id} val={cap[0]} diff=-1"
        )
    client.batch(feeds)
    client.cmd("tick")
    event(run_dir, agent, f"retire {route_id}", True, {"route": route_id})
    print(f"retired live survey {route_id}")


def show_route(client, briefing, route_id):
    request, path, cost = ordered_route(client, route_id)
    terrain, water, accum = world_metrics(client, briefing)
    metrics = route_metrics(
        path,
        terrain,
        water,
        accum,
        briefing["bridge_accum_threshold"],
        board_step_lengths(briefing),
    )
    print(
        f"route {route_id}, owner agent {request[0]}, coeff={tuple(request[5:])}, "
        f"cost={cost}"
    )
    print({key: value if not isinstance(value, list) else len(value)
           for key, value in metrics.items()})
    print(f"start {path[:6]}")
    print(f"end   {path[-6:]}")
    if metrics["wet_cells"]:
        print(f"wet cells: {metrics['wet_cells'][:20]}")
    if metrics["high_runoff_cells"]:
        print(f"bridge-threshold cells: {metrics['high_runoff_cells'][:20]}")


def walk(client, run_dir, agent, route_id, count):
    if count < 1 or count > 10:
        raise ValueError("walk count must be between 1 and 10")
    _request, path, _cost = ordered_route(client, route_id)
    traversals = rows(client, "traversals")
    next_trip = max((key[0] for key in traversals), default=0) + 1
    feeds = []
    for offset in range(count):
        trip = next_trip + offset
        feeds.extend(
            f"feed pathways 2 {trip},{x},{y} val={agent},{route_id}"
            for x, y in path
        )
    client.batch(feeds)
    client.cmd("tick")
    event(run_dir, agent, f"walk {route_id} {count}", True, {
        "trips": list(range(next_trip, next_trip + count)),
        "cells_each": len(path),
    })
    print(f"recorded {count} journey(s) over route {route_id}, {len(path)} cells each")


def scout(client, run_dir, briefing, agent, route_id):
    if agent != briefing.get("scout_agent", agent):
        raise ValueError("only the mountain courier may scout footpaths")
    _request, path, _cost = ordered_route(client, route_id)
    traversals = rows(client, "traversals")
    used = {
        key[0]
        for key, value in traversals.items()
        if key[0] < 0 and value[1] == route_id
    }
    if len(used) >= briefing.get("scout_trip_limit", 2):
        raise ValueError(
            f"route {route_id} already has {len(used)} scout journeys"
        )
    trip = min((key[0] for key in traversals), default=0) - 1
    client.batch([
        f"feed pathways 2 {trip},{x},{y} val={agent},{route_id}"
        for x, y in path
    ])
    client.cmd("tick")
    detail = {"trip": trip, "route": route_id, "cells": len(path)}
    event(run_dir, agent, f"scout {route_id}", True, detail)
    print(
        f"scouted route {route_id}: journey {trip}, {len(path)} cells; "
        "two journeys establish its footpath"
    )


def pave(client, run_dir, briefing, agent, route_id, count):
    if count < 1:
        raise ValueError("pave count must be positive")
    _request, path, _cost = ordered_route(client, route_id)
    (
        terrain,
        water,
        accum,
        infrastructure,
        path_use,
        sources,
        connected,
    ) = construction_state(client, briefing)
    if path[0] not in connected:
        raise ValueError(
            f"route {route_id} starts at {path[0]}, outside the source-connected network"
        )

    reachable = set(connected)
    proposed = []
    for cell in path:
        if cell in sources:
            reachable.add(cell)
            continue
        if cell in infrastructure:
            kind, _owner = infrastructure[cell]
            passable = kind == BRIDGE or water[cell] == terrain[cell]
            if not passable:
                break
            if cell not in reachable and not any(
                nxt in reachable for nxt in neighbors8(cell)
            ):
                break
            reachable.add(cell)
            continue
        if not any(nxt in reachable for nxt in neighbors8(cell)):
            break
        if path_use.get(cell, 0) < briefing["trail_use_required"]:
            raise ValueError(
                f"{cell} has path use {path_use.get(cell, 0)}; "
                f"needs {briefing['trail_use_required']} before paving"
            )
        kind = required_kind(
            cell, terrain, water, accum, briefing["bridge_accum_threshold"]
        )
        proposed.append((cell, kind))
        reachable.add(cell)
        if len(proposed) >= count:
            break
    if not proposed:
        raise ValueError("route has no reachable unbuilt prefix")

    spent = infrastructure_spend(infrastructure)
    profile = briefing["agents"][str(agent)]
    road_after = spent.get((agent, ROAD), 0) + sum(kind == ROAD for _cell, kind in proposed)
    bridge_after = spent.get((agent, BRIDGE), 0) + sum(kind == BRIDGE for _cell, kind in proposed)
    if road_after > profile["road_grant"]:
        raise ValueError(f"road grant exceeded: {road_after}/{profile['road_grant']}")
    if bridge_after > profile["bridge_grant"]:
        raise ValueError(f"bridge grant exceeded: {bridge_after}/{profile['bridge_grant']}")

    client.batch([
        f"feed pathways 3 {cell[0]},{cell[1]} val={kind},{agent}"
        for cell, kind in proposed
    ])
    client.cmd("tick")
    event(run_dir, agent, f"pave {route_id} {count}", True, {
        "cells": [[cell[0], cell[1], kind] for cell, kind in proposed]
    })
    print(
        f"paved {len(proposed)} cells of route {route_id}: "
        f"{sum(k == ROAD for _c, k in proposed)} road, "
        f"{sum(k == BRIDGE for _c, k in proposed)} bridge"
    )
    print(f"new end: {proposed[-1][0]}")


def build_proposal(client, briefing, route_id, alignment):
    if alignment not in ("bridge", "embankment"):
        raise ValueError("alignment must be bridge or embankment")
    request, path, _cost = ordered_route(client, route_id)
    if int(briefing.get("version", 1)) >= 5:
        target = (request[3], request[4])
        if any(
            site["kind"] == 6 and tuple(site["cell"]) == target
            for site in briefing["sites"]
        ):
            raise ValueError("a trail-outpost route may not be built")
    (
        terrain,
        water,
        accum,
        infrastructure,
        path_use,
        sources,
        connected,
    ) = construction_state(client, briefing)
    if path[0] not in connected:
        raise ValueError(
            f"route {route_id} starts outside the public road network"
        )

    reachable = set(connected)
    frontier = None
    for cell in path:
        if cell in sources:
            reachable.add(cell)
            continue
        if cell in infrastructure:
            if cell not in connected:
                break
            reachable.add(cell)
            continue
        if not any(nxt in reachable for nxt in neighbors8(cell)):
            break
        if path_use.get(cell, 0) < briefing["trail_use_required"]:
            raise ValueError(
                f"{cell} has path use {path_use.get(cell, 0)}; "
                f"needs {briefing['trail_use_required']} before building"
            )
        frontier = cell
        break
    if frontier is None:
        raise ValueError("route has no reachable unbuilt frontier")

    profile = engineered_profile(
        path,
        infrastructure,
        connected | sources,
        terrain,
        water,
        accum,
        briefing["bridge_accum_threshold"],
        briefing["road_grade_permille"],
        board_step_lengths(briefing),
        alignment,
        briefing["drainage_embankment_fill"],
    )
    action = next(
        (candidate for candidate in profile if candidate[0] == frontier),
        None,
    )
    if action is None:
        raise RuntimeError("frontier is absent from the engineered profile")
    return (
        terrain,
        water,
        accum,
        infrastructure,
        connected,
        path,
        profile,
        action,
    )


def quarry_state(briefing, deliveries, connected):
    delivered = {}
    for value in deliveries.values():
        delivered[value[1]] = delivered.get(value[1], 0) + value[2]
    activated = {
        site["id"]
        for site in briefing["sites"]
        if site["kind"] == 3
        and delivered.get(site["id"], 0) >= site["amount"]
    }
    online = {
        site["id"]
        for site in briefing["sites"]
        if site["id"] in activated and tuple(site["cell"]) in connected
    }
    return delivered, activated, online


def preview_build(client, briefing, route_id):
    for alignment in ("bridge", "embankment"):
        try:
            (
                terrain,
                water,
                _accum,
                _infrastructure,
                _connected,
                _path,
                profile,
                action,
            ) = build_proposal(client, briefing, route_id, alignment)
        except ValueError as error:
            print(f"{alignment}: infeasible ({error})")
            continue
        cell, kind, old, new, fill = action
        total_fill = sum(candidate[4] for candidate in profile)
        crossings = sum(
            candidate[1] == BRIDGE
            or candidate[3] - candidate[2]
            >= briefing["drainage_embankment_fill"]
            for candidate in profile
        )
        kind_name = "bridge" if kind == BRIDGE else "engineered road"
        print(
            f"{alignment}: next {cell} is {kind_name}, bed {old}->{new}, "
            f"immediate fill {fill}; remaining profile {len(profile)} cells, "
            f"total planned fill {total_fill}, crossing markers {crossings}"
        )
        if new != old:
            candidate_terrain = dict(terrain)
            candidate_terrain[cell] = new
            candidate_water = priority_flood(candidate_terrain)
            changed = sum(
                candidate_water[key] != water[key] for key in water
            )
            print(
                f"  immediate hydraulic preview: {changed} water levels change"
            )


def build(client, run_dir, briefing, agent, route_id, alignment):
    with open(os.path.join(run_dir, "events.jsonl")) as source:
        prior_modes = {
            tokens[2]
            for record in (json.loads(line) for line in source if line.strip())
            if record.get("accepted")
            and (tokens := record.get("command", "").split())
            and len(tokens) == 3
            and tokens[0] == "build"
            and tokens[1] == str(route_id)
        }
    if prior_modes and prior_modes != {alignment}:
        raise ValueError(
            f"route {route_id} is already committed to {next(iter(prior_modes))}"
        )
    (
        terrain,
        water,
        _accum,
        infrastructure,
        connected,
        _path,
        _profile,
        action,
    ) = build_proposal(client, briefing, route_id, alignment)
    cell, kind, old, new, fill = action
    profile = briefing["agents"][str(agent)]
    if kind not in profile.get("build_kinds", []):
        required = "bridge" if kind == BRIDGE else "surface road"
        raise ValueError(
            f"{profile['name']} cannot build the required {required} at {cell}"
        )

    deliveries = rows(client, "deliveries")
    _delivered, _activated, online = quarry_state(
        briefing, deliveries, connected
    )
    aggregate_capacity = briefing["initial_aggregate"] + (
        briefing["quarry_aggregate"] if online else 0
    )
    rock_capacity = briefing["initial_rock"] + (
        briefing["quarry_rock"] if online else 0
    )
    build_rows = rows(client, "build_actions")
    engineered = (
        sum(value[4] == ENGINEERED_ROAD for value in build_rows.values())
        if int(briefing.get("version", 1)) >= 4 else
        sum(
            built_kind == ENGINEERED_ROAD
            for built_kind, _owner in infrastructure.values()
        )
    )
    fill_spent = sum(
        max(0, value[6] - value[5])
        for value in build_rows.values()
        if value[4] == ENGINEERED_ROAD
    )
    if kind == ENGINEERED_ROAD and engineered + 1 > aggregate_capacity:
        raise ValueError(
            f"aggregate stock exhausted: {engineered + 1}/{aggregate_capacity}"
        )
    if fill_spent + fill > rock_capacity:
        raise ValueError(
            f"road-fill rock exhausted: {fill_spent + fill}/{rock_capacity}"
        )
    if kind == BRIDGE:
        spent = infrastructure_spend(infrastructure)
        after = spent.get((agent, BRIDGE), 0) + 1
        if after > profile["bridge_grant"]:
            raise ValueError(
                f"bridge-kit grant exceeded: {after}/{profile['bridge_grant']}"
            )

    actions = rows(client, "build_actions")
    revision = max((key[0] for key in actions), default=0) + 1
    feeds = []
    if new != old:
        feeds.extend([
            f"feed water 0 {cell[0]},{cell[1]} val={old} diff=-1",
            f"feed water 0 {cell[0]},{cell[1]} val={new}",
        ])
    feeds.append(
        f"feed pathways 6 {revision},0 "
        f"val={agent},{route_id},{cell[0]},{cell[1]},{kind},{old},{new}"
    )
    client.batch(feeds)
    client.cmd("tick")
    detail = {
        "engineering": "fill-envelope-v1",
        "alignment": alignment,
        "revision": revision,
        "cell": [cell[0], cell[1], kind, old, new, fill],
    }
    event(run_dir, agent, f"build {route_id} {alignment}", True, detail)

    changed = 0
    flooded_sites = []
    if new != old:
        candidate = dict(terrain)
        candidate[cell] = new
        candidate_water = priority_flood(candidate)
        changed = sum(candidate_water[key] != water[key] for key in water)
        flooded_sites = [
            site["label"]
            for site in briefing["sites"]
            if site["kind"] in (0, 1)
            and candidate_water[tuple(site["cell"])]
            > candidate[tuple(site["cell"])]
        ]
    print(
        f"built {'bridge' if kind == BRIDGE else 'engineered road'} at {cell}; "
        f"bed {old}->{new}, fill {fill}; water levels changed {changed}"
    )
    if flooded_sites:
        print(f"WARNING: protected sites now flooded: {flooded_sites}")


def build_until_choice(
    client,
    run_dir,
    briefing,
    agent,
    route_id,
    alignment,
    limit,
):
    if limit < 1 or limit > 100:
        raise ValueError("build-until limit must be between 1 and 100")
    completed = 0
    for _ in range(limit):
        try:
            build(client, run_dir, briefing, agent, route_id, alignment)
            completed += 1
        except ValueError as error:
            if completed and (
                "cannot build the required" in str(error)
                or "no reachable unbuilt frontier" in str(error)
            ):
                print(
                    f"build-until stopped after {completed} cells: {error}"
                )
                return
            raise
    print(f"build-until reached its {limit}-cell safety limit")


def deliver(client, run_dir, briefing, agent, town_id, units, route_id=None):
    if units < 1:
        raise ValueError("delivery units must be positive")
    sites = site_map(briefing)
    town = sites.get(town_id)
    version = int(briefing.get("version", 1))
    allowed = (
        (0, 3, 4, 5, 6) if version >= 5 else
        (0, 3, 4, 5) if version >= 4 else
        (0, 3, 4) if version >= 3 else
        (0,)
    )
    if town is None or town["kind"] not in allowed:
        raise ValueError(f"site {town_id} is not a delivery target")
    connected = set(rows(client, "connected"))
    deliveries = rows(client, "deliveries")
    delivered = sum(value[2] for value in deliveries.values() if value[1] == town_id)
    if delivered + units > town["amount"]:
        raise ValueError(f"town demand exceeded: {delivered + units}/{town['amount']}")
    general_kinds = (0, 3, 6) if version >= 5 else (0, 3)
    if town["kind"] in general_kinds:
        used = sum(
            value[2]
            for value in deliveries.values()
            if sites[value[1]]["kind"] in general_kinds
        )
        supply = sum(
            site["amount"]
            for site in briefing["sites"] if site["kind"] == 1
        )
        if used + units > supply:
            raise ValueError(f"source supply exceeded: {used + units}/{supply}")
    delivery_id = max((key[0] for key in deliveries), default=0) + 1
    if briefing.get("version", 1) < 2:
        if tuple(town["cell"]) not in connected:
            raise ValueError(f"{town['label']} is not road-connected to a source")
        client.cmd(f"feed pathways 4 {delivery_id} val={agent},{town_id},{units}")
        client.cmd("tick")
        event(run_dir, agent, f"deliver {town_id} {units}", True, {
            "delivery": delivery_id,
        })
        print(
            f"delivered {units} units to {town['label']} "
            f"({delivered + units}/{town['amount']})"
        )
        return

    if route_id is None:
        raise ValueError("version 2 delivery requires TOWN_ID UNITS ROUTE_ID")
    request, path, _cost = ordered_route(client, route_id)
    source_cells = {
        tuple(site["cell"])
        for site in briefing["sites"] if site["kind"] == 1
    }
    if town["kind"] in (4, 5):
        _totals, _activated, online = quarry_state(
            briefing, deliveries, connected
        )
        source_cells = {
            tuple(sites[site_id]["cell"]) for site_id in online
        }
        if agent != briefing.get("worksite_haul_agent", agent):
            raise ValueError("only the works crew may haul bulk rock")
    elif town["kind"] == 3 and agent != briefing.get(
        "quarry_activation_agent", agent
    ):
        raise ValueError("only the courier may activate the quarry")
    elif town["kind"] == 6:
        if agent != briefing.get("scout_agent", agent):
            raise ValueError("only the mountain courier may supply an outpost")
        if units != briefing["porter_trip_capacity"]:
            raise ValueError("outpost delivery must be exactly porter capacity")
    if path[0] not in source_cells:
        raise ValueError("a delivery route must start at a supply site")
    if path[-1] != tuple(town["cell"]):
        raise ValueError(f"route {route_id} does not end at {town['label']}")

    freight = all(cell in connected for cell in path)
    if town["kind"] == 6:
        terrain, water, _accum = world_metrics(client, briefing)
        if any(water[cell] > terrain[cell] for cell in path):
            raise ValueError("outpost route must be dry")
        if any(
            not edge_grade_ok(
                src,
                dst,
                terrain,
                briefing["foot_grade_permille"],
                board_step_lengths(briefing),
            )
            for src, dst in zip(path, path[1:])
        ):
            raise ValueError("outpost route exceeds the foot-grade limit")
        if freight:
            raise ValueError("outpost delivery may not use road freight")
    if town["kind"] in (4, 5) and not freight:
        raise ValueError("bulk rock requires a fully connected road route")
    mode = 1 if freight else 0
    if not freight:
        if units > briefing["porter_trip_capacity"]:
            raise ValueError(
                f"porter capacity exceeded: {units}/{briefing['porter_trip_capacity']}"
            )
        porter_total = sum(
            value[2]
            for value in deliveries.values()
            if value[1] == town_id and len(value) >= 5 and value[4] == 0
        )
        if porter_total + units > briefing["porter_town_quota"]:
            raise ValueError(
                f"porter quota exceeded: "
                f"{porter_total + units}/{briefing['porter_town_quota']}"
            )

    feeds = [
        f"feed pathways 2 {delivery_id},{x},{y} val={agent},{route_id}"
        for x, y in path
    ]
    feeds.append(
        f"feed pathways 4 {delivery_id} "
        f"val={agent},{town_id},{units},{route_id},{mode}"
    )
    client.batch(feeds)
    client.cmd("tick")
    event(run_dir, agent, f"deliver {town_id} {units} {route_id}", True, {
        "delivery": delivery_id,
        "mode": "freight" if freight else "porter",
        "route": route_id,
        "cells": len(path),
        "path": [[x, y] for x, y in path],
    })
    print(
        f"delivered {units} units to {town['label']} by "
        f"{'road freight' if freight else 'porter'} over route {route_id}; "
        f"{delivered + units}/{town['amount']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--agent", type=int, required=True)
    parser.add_argument(
        "--replay",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("command", nargs="+")
    args = parser.parse_args()
    run_dir = os.path.abspath(args.run_dir)
    briefing = read_briefing(run_dir)
    if briefing.get("version", 1) < 2:
        raise SystemExit(
            "this client supports pathways briefing version 2+; "
            "archived V1 worlds used a different DDIR request schema"
        )
    if str(args.agent) not in briefing["agents"]:
        raise SystemExit(f"agent {args.agent} is not in the briefing")
    command = args.command[0]
    command_text = " ".join(args.command)

    try:
        with run_lock(run_dir):
            client = Client(int(briefing["port"]))
            if command == "status":
                show_status(client, briefing)
            elif command == "survey":
                survey(client, run_dir, briefing, args.agent, args.command[1:])
            elif command == "route":
                if len(args.command) != 2:
                    raise ValueError("route requires ROUTE_ID")
                show_route(client, briefing, int(args.command[1]))
            elif command == "retire":
                if len(args.command) != 2:
                    raise ValueError("retire requires ROUTE_ID")
                retire_route(
                    client,
                    run_dir,
                    args.agent,
                    int(args.command[1]),
                )
            elif command == "overlap":
                if len(args.command) != 3:
                    raise ValueError("overlap requires ROUTE_A ROUTE_B")
                _ra, a, _ca = ordered_route(client, int(args.command[1]))
                _rb, b, _cb = ordered_route(client, int(args.command[2]))
                shared = set(a) & set(b)
                print(
                    f"{len(shared)} shared cells; "
                    f"{len(shared) / max(1, len(a)):.1%} of first route, "
                    f"{len(shared) / max(1, len(b)):.1%} of second"
                )
            elif command == "walk":
                if briefing.get("version", 1) >= 2:
                    raise ValueError(
                        "free walks are disabled; porter and freight deliveries create path use"
                    )
                if len(args.command) not in (2, 3):
                    raise ValueError("walk requires ROUTE_ID [COUNT]")
                walk(
                    client,
                    run_dir,
                    args.agent,
                    int(args.command[1]),
                    int(args.command[2]) if len(args.command) == 3 else 1,
                )
            elif command == "scout":
                if briefing.get("version", 1) < 3 or len(args.command) != 2:
                    raise ValueError("scout requires one ROUTE_ID in V3")
                scout(
                    client,
                    run_dir,
                    briefing,
                    args.agent,
                    int(args.command[1]),
                )
            elif command == "pave":
                if briefing.get("version", 1) >= 3 and not args.replay:
                    raise ValueError(
                        "V3 uses one-cell `build ROUTE_ID`; inspect it first "
                        "with `preview ROUTE_ID`"
                    )
                if briefing.get("version", 1) >= 3:
                    with open(os.path.join(run_dir, "events.jsonl")) as source:
                        accepted = sum(
                            bool(json.loads(line).get("accepted"))
                            for line in source if line.strip()
                        )
                    if accepted >= briefing.get("legacy_event_count", 0):
                        raise ValueError("legacy paving replay window is closed")
                if len(args.command) != 3:
                    raise ValueError("pave requires ROUTE_ID CELL_COUNT")
                pave(
                    client,
                    run_dir,
                    briefing,
                    args.agent,
                    int(args.command[1]),
                    int(args.command[2]),
                )
            elif command == "preview":
                if briefing.get("version", 1) < 3 or len(args.command) != 2:
                    raise ValueError("preview requires one ROUTE_ID in V3")
                preview_build(client, briefing, int(args.command[1]))
            elif command == "build":
                if briefing.get("version", 1) < 3 or len(args.command) != 3:
                    raise ValueError(
                        "build requires ROUTE_ID bridge|embankment in V3"
                    )
                build(
                    client,
                    run_dir,
                    briefing,
                    args.agent,
                    int(args.command[1]),
                    args.command[2],
                )
            elif command == "build-until":
                if briefing.get("version", 1) < 3 or len(args.command) not in (3, 4):
                    raise ValueError(
                        "build-until requires ROUTE_ID bridge|embankment [LIMIT]"
                    )
                build_until_choice(
                    client,
                    run_dir,
                    briefing,
                    args.agent,
                    int(args.command[1]),
                    args.command[2],
                    int(args.command[3]) if len(args.command) == 4 else 100,
                )
            elif command == "deliver":
                expected = 4 if briefing.get("version", 1) >= 2 else 3
                if len(args.command) != expected:
                    suffix = " ROUTE_ID" if expected == 4 else ""
                    raise ValueError("deliver requires TOWN_ID UNITS" + suffix)
                deliver(
                    client,
                    run_dir,
                    briefing,
                    args.agent,
                    int(args.command[1]),
                    int(args.command[2]),
                    int(args.command[3]) if expected == 4 else None,
                )
            else:
                raise ValueError(
                    "commands: status | survey RID FROM TO DIST GRADE WATER RUNOFF "
                    "REUSE [MAX_GRADE_PERMILLE] | route RID | retire RID | overlap RID RID | "
                    "scout RID | preview RID | build RID bridge|embankment | "
                    "build-until RID bridge|embankment [LIMIT] | "
                    "pave RID COUNT (V2) | "
                    "deliver TARGET UNITS ROUTE"
                )
    except (ValueError, RuntimeError) as error:
        event(run_dir, args.agent, command_text, False, str(error))
        raise SystemExit(f"rejected: {error}")


if __name__ == "__main__":
    main()
