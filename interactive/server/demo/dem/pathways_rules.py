"""Independent routing and network oracle for the persistent pathways demo."""

from collections import deque
import heapq
import shlex


ROAD = 1
BRIDGE = 2
TRAIL_USE = 2
BRIDGE_ACCUM = 1024
RAW_SURFACE = 2
PATH_SURFACE = 1
ROAD_SURFACE = 0


def neighbors8(cell):
    x, y = cell
    for dx, dy in (
        (-1, -1), (0, -1), (1, -1),
        (-1, 0),            (1, 0),
        (-1, 1),  (0, 1),  (1, 1),
    ):
        yield (x + dx, y + dy)


def surface_factor(cell, path_use=None, infrastructure=None):
    """Return the generalized-haul surface factor: raw 2, path 1, road 0."""
    if infrastructure and cell in infrastructure:
        return ROAD_SURFACE
    if path_use and path_use.get(cell, 0) >= TRAIL_USE:
        return PATH_SURFACE
    return RAW_SURFACE


def step_cost(
    terrain,
    water,
    accum,
    src,
    dst,
    coefficients,
    path_use=None,
    infrastructure=None,
    step_lengths=(26, 37),
):
    """Match pathways.ddp's strictly-positive destination-entry cost."""
    if len(coefficients) == 4:
        distance_w, grade_w, water_w, runoff_w = coefficients
        reuse_w = 0
    else:
        distance_w, grade_w, water_w, runoff_w, reuse_w = coefficients
    dx = abs(dst[0] - src[0])
    dy = abs(dst[1] - src[1])
    distance = step_lengths[1] if dx and dy else step_lengths[0]
    grade = abs(terrain[dst] - terrain[src])
    depth = water[dst] - terrain[dst]
    runoff = min(accum[dst], 100)
    return (
        distance * distance_w
        + grade * grade_w
        + depth * water_w
        + runoff * runoff_w
        + distance
        * reuse_w
        * surface_factor(dst, path_use, infrastructure)
    )


def shortest_route(
    terrain,
    water,
    accum,
    start,
    target,
    coefficients,
    path_use=None,
    infrastructure=None,
    step_lengths=(26, 37),
):
    """Dijkstra with the same `(cost, predecessor)` lexicographic tie-break."""
    if start not in terrain or target not in terrain:
        raise ValueError("route endpoint is outside the terrain")
    best = {start: (0, start[0], start[1])}
    todo = [(0, start[0], start[1], start[0], start[1])]
    while todo:
        cost, px, py, x, y = heapq.heappop(todo)
        cell = (x, y)
        if best.get(cell) != (cost, px, py):
            continue
        for nxt in neighbors8(cell):
            if nxt not in terrain:
                continue
            candidate = (
                cost
                + step_cost(
                    terrain,
                    water,
                    accum,
                    cell,
                    nxt,
                    coefficients,
                    path_use,
                    infrastructure,
                    step_lengths,
                ),
                x,
                y,
            )
            if candidate < best.get(nxt, (10**30, 10**30, 10**30)):
                best[nxt] = candidate
                heapq.heappush(todo, (candidate[0], x, y, nxt[0], nxt[1]))
    if target not in best:
        return None, None
    path = [target]
    while path[-1] != start:
        _, px, py = best[path[-1]]
        prior = (px, py)
        if prior == path[-1]:
            raise RuntimeError("predecessor chain stopped before the route source")
        path.append(prior)
    path.reverse()
    return path, best[target][0]


def route_metrics(
    path,
    terrain,
    water,
    accum,
    bridge_accum=BRIDGE_ACCUM,
    step_lengths=(26, 37),
):
    distance = 0
    variation = 0
    max_grade = 0
    wet = []
    runoff = []
    for index, cell in enumerate(path):
        if water[cell] > terrain[cell]:
            wet.append(cell)
        if accum[cell] >= bridge_accum:
            runoff.append(cell)
        if index:
            prior = path[index - 1]
            dx = abs(cell[0] - prior[0])
            dy = abs(cell[1] - prior[1])
            distance += step_lengths[1] if dx and dy else step_lengths[0]
            grade = abs(terrain[cell] - terrain[prior])
            variation += grade
            max_grade = max(max_grade, grade)
    return {
        "cells": len(path),
        "distance_m": distance,
        "elevation_variation_m": variation,
        "max_step_m": max_grade,
        "wet_cells": wet,
        "high_runoff_cells": runoff,
    }


def infrastructure_spend(infrastructure):
    spent = {}
    for _cell, (kind, owner) in infrastructure.items():
        key = (owner, kind)
        spent[key] = spent.get(key, 0) + 1
    return spent


def passable_infrastructure(infrastructure, terrain, water):
    return {
        cell
        for cell, (kind, _owner) in infrastructure.items()
        if kind == BRIDGE or water[cell] == terrain[cell]
    }


def connected_cells(infrastructure, sources, terrain, water):
    allowed = passable_infrastructure(infrastructure, terrain, water) | set(sources)
    seen = set(sources)
    todo = deque(sources)
    while todo:
        cell = todo.popleft()
        for nxt in neighbors8(cell):
            if nxt in allowed and nxt not in seen:
                seen.add(nxt)
                todo.append(nxt)
    return seen


def required_kind(cell, terrain, water, accum, bridge_accum=BRIDGE_ACCUM):
    if water[cell] > terrain[cell] or accum[cell] >= bridge_accum:
        return BRIDGE
    return ROAD


def replay_history(events, briefing, terrain, water, accum):
    """Independently replay accepted semantic commands in event order.

    This is deliberately stricter than checking only the final DDIR views:
    routes are recomputed against the path and road state at that revision,
    porter/freight mode is reconstructed, and paving must advance over a path
    that had already been established.  The returned relations can then be
    compared directly with the server's final inputs and derived path counts.
    """
    sites = {int(site["id"]): site for site in briefing["sites"]}
    agents = {int(agent) for agent in briefing["agents"]}
    sources = {
        tuple(site["cell"])
        for site in briefing["sites"]
        if int(site["kind"]) == 1
    }
    network_seeds = {
        tuple(site["cell"])
        for site in briefing["sites"]
        if int(site["kind"]) in (1, 2)
    }
    step_lengths = (
        briefing.get("orthogonal_metres", 26),
        briefing.get("diagonal_metres", 37),
    )
    routes = {}
    deliveries = {}
    traversals = {}
    path_use = {}
    infrastructure = {}
    delivered = {}
    porter_delivered = {}
    total_delivered = 0

    def fail(index, message):
        raise ValueError(f"accepted event {index}: {message}")

    def live_path(route_id, index):
        request = routes.get(route_id)
        if request is None:
            fail(index, f"unknown live route {route_id}")
        path, cost = shortest_route(
            terrain,
            water,
            accum,
            request["start"],
            request["target"],
            request["coefficients"],
            path_use,
            infrastructure,
            step_lengths,
        )
        if path is None or cost is None:
            fail(index, f"route {route_id} is unresolved")
        return path

    for index, event in enumerate(events, 1):
        if not event.get("accepted"):
            continue
        try:
            agent = int(event["agent"])
            tokens = shlex.split(event["command"])
        except (KeyError, TypeError, ValueError) as error:
            fail(index, f"malformed event: {error}")
        if agent not in agents:
            fail(index, f"unknown agent {agent}")
        if not tokens:
            fail(index, "empty command")

        command = tokens[0]
        if command == "survey":
            if len(tokens) != 9:
                fail(index, "survey requires eight integer arguments")
            try:
                route_id, from_id, to_id, *coefficients = map(int, tokens[1:])
            except ValueError as error:
                fail(index, f"invalid survey integer: {error}")
            if route_id in routes:
                fail(index, f"duplicate live route {route_id}")
            if from_id not in sites or to_id not in sites:
                fail(index, "unknown survey endpoint")
            if min(coefficients) < 0 or coefficients[0] < 1:
                fail(index, "invalid survey coefficients")
            route_limit = briefing.get("max_live_routes")
            if route_limit is not None and len(routes) >= route_limit:
                fail(index, "live route limit exceeded")
            request = {
                "agent": agent,
                "start": tuple(sites[from_id]["cell"]),
                "target": tuple(sites[to_id]["cell"]),
                "coefficients": tuple(coefficients),
            }
            routes[route_id] = request
            live_path(route_id, index)

        elif command == "retire":
            if len(tokens) != 2:
                fail(index, "retire requires one route id")
            try:
                route_id = int(tokens[1])
            except ValueError as error:
                fail(index, f"invalid route id: {error}")
            if route_id not in routes:
                fail(index, f"cannot retire absent route {route_id}")
            if routes[route_id]["agent"] != agent:
                fail(index, f"agent {agent} does not own route {route_id}")
            del routes[route_id]

        elif command == "deliver":
            if len(tokens) != 4:
                fail(index, "delivery requires town, units, and route")
            try:
                town_id, units, route_id = map(int, tokens[1:])
            except ValueError as error:
                fail(index, f"invalid delivery integer: {error}")
            town = sites.get(town_id)
            if town is None or int(town["kind"]) != 0:
                fail(index, f"site {town_id} is not a town")
            if units < 1:
                fail(index, "delivery units must be positive")
            path = live_path(route_id, index)
            if path[0] not in sources:
                fail(index, "delivery route does not start at a supply site")
            if path[-1] != tuple(town["cell"]):
                fail(index, "delivery route does not end at its town")
            if delivered.get(town_id, 0) + units > int(town["amount"]):
                fail(index, "town demand exceeded")
            supply = sum(
                int(site["amount"])
                for site in briefing["sites"]
                if int(site["kind"]) == 1
            )
            if total_delivered + units > supply:
                fail(index, "pooled source supply exceeded")
            connected = connected_cells(
                infrastructure, network_seeds, terrain, water
            )
            freight = all(cell in connected for cell in path)
            if not freight:
                if units > int(briefing["porter_trip_capacity"]):
                    fail(index, "porter trip capacity exceeded")
                if (
                    porter_delivered.get(town_id, 0) + units
                    > int(briefing["porter_town_quota"])
                ):
                    fail(index, "porter town quota exceeded")
                porter_delivered[town_id] = (
                    porter_delivered.get(town_id, 0) + units
                )

            delivery_id = len(deliveries) + 1
            mode = 1 if freight else 0
            deliveries[(delivery_id,)] = (
                agent,
                town_id,
                units,
                route_id,
                mode,
            )
            for cell in path:
                traversals[(delivery_id, cell[0], cell[1])] = (
                    agent,
                    route_id,
                )
                path_use[cell] = path_use.get(cell, 0) + 1
            delivered[town_id] = delivered.get(town_id, 0) + units
            total_delivered += units

        elif command == "pave":
            if len(tokens) != 3:
                fail(index, "pave requires route and cell count")
            try:
                route_id, count = map(int, tokens[1:])
            except ValueError as error:
                fail(index, f"invalid paving integer: {error}")
            if count < 1:
                fail(index, "pave count must be positive")
            path = live_path(route_id, index)
            connected = connected_cells(
                infrastructure, network_seeds, terrain, water
            )
            if path[0] not in connected:
                fail(index, "paving route starts outside the public network")
            reachable = set(connected)
            proposed = []
            for cell in path:
                if cell in network_seeds:
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
                if path_use.get(cell, 0) < int(briefing["trail_use_required"]):
                    fail(index, f"paving precedes path establishment at {cell}")
                kind = required_kind(
                    cell,
                    terrain,
                    water,
                    accum,
                    int(briefing["bridge_accum_threshold"]),
                )
                proposed.append((cell, kind))
                reachable.add(cell)
                if len(proposed) >= count:
                    break
            if not proposed:
                fail(index, "pave has no reachable unbuilt prefix")
            detail_cells = event.get("detail", {}).get("cells")
            expected_detail = [
                [cell[0], cell[1], kind] for cell, kind in proposed
            ]
            if detail_cells != expected_detail:
                fail(index, "recorded paving cells disagree with replay")
            spend = infrastructure_spend(infrastructure)
            profile = briefing["agents"][str(agent)]
            road_after = spend.get((agent, ROAD), 0) + sum(
                kind == ROAD for _cell, kind in proposed
            )
            bridge_after = spend.get((agent, BRIDGE), 0) + sum(
                kind == BRIDGE for _cell, kind in proposed
            )
            if road_after > int(profile["road_grant"]):
                fail(index, "road grant exceeded")
            if bridge_after > int(profile["bridge_grant"]):
                fail(index, "bridge grant exceeded")
            for cell, kind in proposed:
                infrastructure[cell] = (kind, agent)

        else:
            fail(index, f"accepted non-semantic command {command!r}")

    request_rows = {}
    for route_id, request in routes.items():
        request_rows[(route_id,)] = (
            request["agent"],
            request["start"][0],
            request["start"][1],
            request["target"][0],
            request["target"][1],
            *request["coefficients"],
        )
    return {
        "route_requests": request_rows,
        "deliveries": deliveries,
        "traversals": traversals,
        "path_use": path_use,
        "infrastructure": infrastructure,
        "porter_delivered": porter_delivered,
    }
