"""Independent routing and network oracle for the persistent pathways demo."""

from collections import deque
import heapq
import shlex


ROAD = 1
BRIDGE = 2
ENGINEERED_ROAD = 3
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


def surface_factor(
    cell,
    path_use=None,
    infrastructure=None,
    terrain=None,
    water=None,
):
    """Return the generalized-haul surface factor: raw 2, path 1, road 0."""
    if infrastructure and cell in infrastructure:
        kind, _owner = infrastructure[cell]
        if kind == BRIDGE or terrain is None or water is None:
            return ROAD_SURFACE
        if water[cell] == terrain[cell]:
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
        * surface_factor(
            dst, path_use, infrastructure, terrain, water
        )
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
    max_grade_permille=None,
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
            if (
                max_grade_permille is not None
                and not edge_grade_ok(
                    cell,
                    nxt,
                    terrain,
                    max_grade_permille,
                    step_lengths,
                )
            ):
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


def grade_limit(src, dst, grade_permille, step_lengths):
    """Maximum integer height step for an orthogonal/diagonal edge."""
    diagonal = src[0] != dst[0] and src[1] != dst[1]
    distance = step_lengths[1] if diagonal else step_lengths[0]
    return grade_permille * distance // 1000


def edge_grade_ok(src, dst, terrain, grade_permille, step_lengths):
    """Whether a terrain edge is within a mode's configured grade limit."""
    if grade_permille is None:
        return True
    return (
        abs(terrain[src] - terrain[dst])
        <= grade_limit(src, dst, grade_permille, step_lengths)
    )


def graded_edge_ok(
    src,
    dst,
    infrastructure,
    terrain,
    grade_permille,
    step_lengths,
):
    """Whether a public-network edge satisfies the V3 road-grade rule.

    V2 roads are retained as legacy infrastructure.  Their mutual edges, and
    every edge touching a bridge, remain exempt.  A new engineered road must
    meet the grade at its source/yard boundary and at legacy intersections.
    """
    if grade_permille is None:
        return True
    src_kind = infrastructure.get(src, (0, 0))[0]
    dst_kind = infrastructure.get(dst, (0, 0))[0]
    if BRIDGE in (src_kind, dst_kind):
        return True
    if ENGINEERED_ROAD not in (src_kind, dst_kind):
        return True
    return edge_grade_ok(
        src,
        dst,
        terrain,
        grade_permille,
        step_lengths,
    )


def connected_cells(
    infrastructure,
    sources,
    terrain,
    water,
    grade_permille=None,
    step_lengths=(26, 37),
):
    allowed = passable_infrastructure(infrastructure, terrain, water) | set(sources)
    seen = set(sources)
    todo = deque(sources)
    while todo:
        cell = todo.popleft()
        for nxt in neighbors8(cell):
            if (
                nxt in allowed
                and nxt not in seen
                and graded_edge_ok(
                    cell,
                    nxt,
                    infrastructure,
                    terrain,
                    grade_permille,
                    step_lengths,
                )
            ):
                seen.add(nxt)
                todo.append(nxt)
    return seen


def engineered_profile(
    path,
    infrastructure,
    fixed_cells,
    terrain,
    water,
    accum,
    bridge_accum,
    grade_permille,
    step_lengths,
    crossing_mode="bridge",
    drainage_fill=0,
):
    """Return the least fill-only profile for all missing cells on a route.

    Bridges split the grade envelope and never alter terrain.  Existing
    connected roads/source pads are fixed boundaries.  New road cells may be
    raised but never cut.  Looking through the whole missing alignment avoids
    committing an early cell too low to meet a later climb.
    """
    proposed = []
    kinds = {}
    for cell in path:
        if cell in infrastructure or cell in fixed_cells:
            continue
        crossing = (
            required_kind(cell, terrain, water, accum, bridge_accum)
            == BRIDGE
        )
        kind = (
            BRIDGE
            if crossing and crossing_mode == "bridge"
            else ENGINEERED_ROAD
        )
        kinds[cell] = kind
        proposed.append(cell)

    heights = {
        cell: max(
            terrain[cell],
            water[cell]
            + (
                drainage_fill
                if required_kind(
                    cell, terrain, water, accum, bridge_accum
                ) == BRIDGE
                and crossing_mode == "embankment"
                else 0
            ),
        )
        for cell in proposed
        if kinds[cell] == ENGINEERED_ROAD
    }
    variables = set(heights)
    fixed = {
        cell
        for cell in fixed_cells
        if cell in terrain
        and infrastructure.get(cell, (0, 0))[0] != BRIDGE
    }
    # Only boundaries adjacent to a proposed road can constrain the envelope.
    fixed = {
        cell
        for cell in fixed
        if any(nxt in variables for nxt in neighbors8(cell))
    }

    changed = True
    while changed:
        changed = False
        for cell in sorted(variables):
            for nxt in neighbors8(cell):
                if nxt not in variables and nxt not in fixed:
                    continue
                limit = grade_limit(
                    cell, nxt, grade_permille, step_lengths
                )
                other = heights[nxt] if nxt in variables else terrain[nxt]
                needed = other - limit
                if heights[cell] < needed:
                    heights[cell] = needed
                    changed = True

    for cell in sorted(variables):
        for nxt in neighbors8(cell):
            if nxt not in fixed:
                continue
            limit = grade_limit(cell, nxt, grade_permille, step_lengths)
            if heights[cell] > terrain[nxt] + limit:
                raise ValueError(
                    f"fill-only profile cannot meet fixed boundary at {nxt}"
                )

    actions = []
    for cell in proposed:
        old = terrain[cell]
        new = heights.get(cell, old)
        actions.append((cell, kinds[cell], old, new, new - old))
    return actions


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
    # V3 owns mutable terrain.  Copies also keep V2 callers from observing
    # accidental mutation while preserving the original replay semantics.
    terrain = dict(terrain)
    water = dict(water)
    accum = dict(accum)
    version = int(briefing.get("version", 1))
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
    deliveries = {
        (int(row[0]),): tuple(map(int, row[1:]))
        for row in briefing.get("initial_deliveries", [])
    }
    traversals = {
        (int(row[0]), int(row[1]), int(row[2])): (
            int(row[3]), int(row[4])
        )
        for row in briefing.get("initial_traversals", [])
    }
    path_use = {}
    for _trip, x, y in traversals:
        path_use[(x, y)] = path_use.get((x, y), 0) + 1
    infrastructure = {
        (int(row[0]), int(row[1])): (int(row[2]), int(row[3]))
        for row in briefing.get("initial_infrastructure", [])
    }
    delivered = {}
    porter_delivered = {}
    total_delivered = 0
    for value in deliveries.values():
        _agent, target_id, units, _route, mode = value
        delivered[target_id] = delivered.get(target_id, 0) + units
        total_delivered += units
        if mode == 0:
            porter_delivered[target_id] = (
                porter_delivered.get(target_id, 0) + units
            )
    build_actions = {}
    build_revision = 0
    fill_spent = 0
    scout_counts = {}
    route_build_modes = {}
    grade_permille = (
        int(briefing["road_grade_permille"])
        if version >= 3 else None
    )

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
            request.get("max_grade_permille"),
        )
        if path is None or cost is None:
            fail(index, f"route {route_id} is unresolved")
        return path

    def public_network():
        return connected_cells(
            infrastructure,
            network_seeds,
            terrain,
            water,
            grade_permille,
            step_lengths,
        )

    def online_quarries():
        connected = public_network()
        return {
            site_id
            for site_id, site in sites.items()
            if int(site["kind"]) == 3
            and delivered.get(site_id, 0) >= int(site["amount"])
            and tuple(site["cell"]) in connected
        }

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
            if len(tokens) not in (9, 10):
                fail(index, "survey requires eight or nine integer arguments")
            try:
                values = list(map(int, tokens[1:]))
            except ValueError as error:
                fail(index, f"invalid survey integer: {error}")
            route_id, from_id, to_id = values[:3]
            coefficients = values[3:8]
            max_grade_permille = values[8] if len(values) == 9 else None
            if route_id in routes:
                fail(index, f"duplicate live route {route_id}")
            if from_id not in sites or to_id not in sites:
                fail(index, "unknown survey endpoint")
            if min(coefficients) < 0 or coefficients[0] < 1:
                fail(index, "invalid survey coefficients")
            if max_grade_permille is not None and max_grade_permille <= 0:
                fail(index, "invalid survey maximum grade")
            route_limit = briefing.get("max_live_routes")
            if route_limit is not None and len(routes) >= route_limit:
                fail(index, "live route limit exceeded")
            request = {
                "agent": agent,
                "target_id": to_id,
                "start": tuple(sites[from_id]["cell"]),
                "target": tuple(sites[to_id]["cell"]),
                "coefficients": tuple(coefficients),
                "max_grade_permille": max_grade_permille,
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

        elif command == "scout":
            if version < 3 or len(tokens) != 2:
                fail(index, "scout requires one live route in V3")
            try:
                route_id = int(tokens[1])
            except ValueError as error:
                fail(index, f"invalid scout route: {error}")
            if agent != int(briefing.get("scout_agent", agent)):
                fail(index, "only the mountain courier may scout")
            if scout_counts.get(route_id, 0) >= int(
                briefing.get("scout_trip_limit", 2)
            ):
                fail(index, "route scout limit exceeded")
            path = live_path(route_id, index)
            trip_id = min((key[0] for key in traversals), default=0) - 1
            expected_detail = {
                "trip": trip_id,
                "route": route_id,
                "cells": len(path),
            }
            if event.get("detail") != expected_detail:
                fail(index, "recorded scout journey disagrees with replay")
            for cell in path:
                traversals[(trip_id, cell[0], cell[1])] = (agent, route_id)
                path_use[cell] = path_use.get(cell, 0) + 1
            scout_counts[route_id] = scout_counts.get(route_id, 0) + 1

        elif command == "deliver":
            if len(tokens) != 4:
                fail(index, "delivery requires town, units, and route")
            try:
                town_id, units, route_id = map(int, tokens[1:])
            except ValueError as error:
                fail(index, f"invalid delivery integer: {error}")
            town = sites.get(town_id)
            target_kind = int(town["kind"]) if town is not None else -1
            allowed_targets = (
                (0, 3, 4, 5, 6) if version >= 5 else
                (0, 3, 4, 5) if version >= 4 else
                (0, 3, 4) if version >= 3 else
                (0,)
            )
            if town is None or target_kind not in allowed_targets:
                fail(index, f"site {town_id} is not a delivery target")
            if units < 1:
                fail(index, "delivery units must be positive")
            path = live_path(route_id, index)
            general_target_kinds = (
                (0, 3, 6) if version >= 5 else (0, 3)
            )
            if target_kind in general_target_kinds:
                if path[0] not in sources:
                    fail(index, "delivery route does not start at a supply site")
            else:
                online_cells = {
                    tuple(sites[site_id]["cell"])
                    for site_id in online_quarries()
                }
                if path[0] not in online_cells:
                    fail(index, "bulk-rock route does not start at an online quarry")
            if path[-1] != tuple(town["cell"]):
                fail(index, "delivery route does not end at its target")
            if delivered.get(town_id, 0) + units > int(town["amount"]):
                fail(index, "delivery target demand exceeded")
            if target_kind in general_target_kinds:
                supply = sum(
                    int(site["amount"])
                    for site in briefing["sites"]
                    if int(site["kind"]) == 1
                )
                general_used = sum(
                    units0
                    for target_id, units0 in delivered.items()
                    if int(sites[target_id]["kind"])
                    in general_target_kinds
                )
                if general_used + units > supply:
                    fail(index, "pooled source supply exceeded")
            if target_kind == 3 and agent != int(
                briefing.get("quarry_activation_agent", agent)
            ):
                fail(index, "only the courier may activate the quarry")
            if target_kind in (4, 5) and agent != int(
                briefing.get("worksite_haul_agent", agent)
            ):
                fail(index, "only the works crew may haul bulk rock")
            if target_kind == 6:
                if agent != int(briefing.get("scout_agent", agent)):
                    fail(index, "only the mountain courier may supply an outpost")
                if units != int(briefing["porter_trip_capacity"]):
                    fail(index, "outpost delivery must be exactly porter capacity")
                if any(water[cell] > terrain[cell] for cell in path):
                    fail(index, "outpost route must be dry")
                foot_grade_permille = int(briefing["foot_grade_permille"])
                if any(
                    not edge_grade_ok(
                        src,
                        dst,
                        terrain,
                        foot_grade_permille,
                        step_lengths,
                    )
                    for src, dst in zip(path, path[1:])
                ):
                    fail(index, "outpost route exceeds the foot-grade limit")
            connected = public_network()
            freight = all(cell in connected for cell in path)
            if target_kind == 6 and freight:
                fail(index, "outpost delivery may not use road freight")
            if target_kind in (4, 5) and not freight:
                fail(index, "bulk rock requires a fully connected road route")
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

            delivery_id = max((key[0] for key in deliveries), default=0) + 1
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
            # Legacy V2 paving remains replayable after a world is upgraded.
            # The V3 client exposes `build` instead, so no new ungraded road can
            # be introduced through this compatibility branch.
            if len(tokens) != 3:
                fail(index, "pave requires route and cell count")
            try:
                route_id, count = map(int, tokens[1:])
            except ValueError as error:
                fail(index, f"invalid paving integer: {error}")
            request = routes.get(route_id)
            if (
                version >= 5
                and request is not None
                and int(sites[request["target_id"]]["kind"]) == 6
            ):
                fail(index, "a trail-outpost route may not be paved")
            if version >= 3 and index > int(
                briefing.get("legacy_event_count", 0)
            ):
                fail(index, "legacy paving occurred after the upgrade boundary")
            if count < 1:
                fail(index, "pave count must be positive")
            path = live_path(route_id, index)
            connected = public_network()
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

        elif command == "build":
            if version < 3:
                fail(index, "build requires a V3 briefing")
            if len(tokens) != 3 or tokens[2] not in ("bridge", "embankment"):
                fail(index, "build requires ROUTE and bridge|embankment")
            try:
                route_id = int(tokens[1])
            except ValueError as error:
                fail(index, f"invalid build route: {error}")
            request = routes.get(route_id)
            if (
                version >= 5
                and request is not None
                and int(sites[request["target_id"]]["kind"]) == 6
            ):
                fail(index, "a trail-outpost route may not be built")
            alignment = tokens[2]
            prior_alignment = route_build_modes.get(route_id)
            if prior_alignment is not None and prior_alignment != alignment:
                fail(index, "a route cannot change its crossing alignment mid-build")
            path = live_path(route_id, index)
            connected = public_network()
            if path[0] not in connected:
                fail(index, "build route starts outside the public network")

            reachable = set(connected)
            frontier = None
            for cell in path:
                if cell in network_seeds:
                    reachable.add(cell)
                    continue
                if cell in infrastructure:
                    if cell not in connected:
                        break
                    reachable.add(cell)
                    continue
                if not any(nxt in reachable for nxt in neighbors8(cell)):
                    break
                if path_use.get(cell, 0) < int(briefing["trail_use_required"]):
                    fail(index, f"building precedes path establishment at {cell}")
                frontier = cell
                break
            if frontier is None:
                fail(index, "route has no reachable unbuilt frontier")

            try:
                profile = engineered_profile(
                    path,
                    infrastructure,
                    connected | network_seeds,
                    terrain,
                    water,
                    accum,
                    int(briefing["bridge_accum_threshold"]),
                    grade_permille,
                    step_lengths,
                    alignment,
                    int(briefing["drainage_embankment_fill"]),
                )
            except ValueError as error:
                fail(index, str(error))
            action = next(
                (candidate for candidate in profile if candidate[0] == frontier),
                None,
            )
            if action is None:
                fail(index, "frontier is absent from engineered profile")
            cell, kind, old, new, fill = action

            allowed_kinds = {
                int(value)
                for value in briefing["agents"][str(agent)].get(
                    "build_kinds", []
                )
            }
            if kind not in allowed_kinds:
                label = "bridge" if kind == BRIDGE else "surface road"
                fail(index, f"agent role may not build the required {label}")

            quarry_online_before = bool(online_quarries())
            aggregate_capacity = int(briefing["initial_aggregate"])
            rock_capacity = int(briefing["initial_rock"])
            if quarry_online_before:
                aggregate_capacity += int(briefing["quarry_aggregate"])
                rock_capacity += int(briefing["quarry_rock"])
            aggregate_spent = (
                sum(
                    value[4] == ENGINEERED_ROAD
                    for value in build_actions.values()
                )
                if version >= 4 else
                sum(
                    built_kind == ENGINEERED_ROAD
                    for built_kind, _owner in infrastructure.values()
                )
            )
            if (
                kind == ENGINEERED_ROAD
                and aggregate_spent + 1 > aggregate_capacity
            ):
                fail(index, "aggregate stock exhausted")
            if fill_spent + fill > rock_capacity:
                fail(index, "road-fill rock stock exhausted")

            build_revision += 1
            expected_detail = {
                "engineering": "fill-envelope-v1",
                "alignment": alignment,
                "revision": build_revision,
                "cell": [cell[0], cell[1], kind, old, new, fill],
            }
            if event.get("detail") != expected_detail:
                fail(index, "recorded build action disagrees with replay")

            infrastructure[cell] = (kind, agent)
            route_build_modes[route_id] = alignment
            build_actions[(build_revision, 0)] = (
                agent,
                route_id,
                cell[0],
                cell[1],
                kind,
                old,
                new,
            )
            if new != old:
                from run_dem import priority_flood
                from run_physics import py_flow_accum

                terrain[cell] = new
                water = priority_flood(terrain)
                _flow, accum = py_flow_accum(water)
            fill_spent += fill

        else:
            fail(index, f"accepted non-semantic command {command!r}")

    request_rows = {}
    route_grade_caps = {}
    for route_id, request in routes.items():
        request_rows[(route_id,)] = (
            request["agent"],
            request["start"][0],
            request["start"][1],
            request["target"][0],
            request["target"][1],
            *request["coefficients"],
        )
        if request.get("max_grade_permille") is not None:
            route_grade_caps[(route_id,)] = (
                request["max_grade_permille"],
            )
    return {
        "route_requests": request_rows,
        "route_grade_caps": route_grade_caps,
        "deliveries": deliveries,
        "traversals": traversals,
        "path_use": path_use,
        "infrastructure": infrastructure,
        "porter_delivered": porter_delivered,
        "terrain": terrain,
        "water": water,
        "accum": accum,
        "build_actions": build_actions,
        "fill_spent": fill_spent,
        "delivered_by_target": delivered,
        "online_quarries": online_quarries(),
        "connected": public_network(),
    }
