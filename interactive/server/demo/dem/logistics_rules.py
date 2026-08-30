"""Pure rules and independent replay oracle for the logistics civil game.

The live DDIR programs maintain the same views incrementally.  This module is
deliberately free of sockets and process state so setup, judgment, and tests
can all use it as an independent authority.
"""

from collections import defaultdict, deque

TERRAFORM = 0
ROAD = 1
BRIDGE = 2

KIND_NAMES = {
    TERRAFORM: "terraform",
    ROAD: "road",
    BRIDGE: "bridge",
}


def neighbors(cell):
    x, y = cell
    return ((x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1))


def action_cost(kind, old_height=0, new_height=0):
    if kind == TERRAFORM:
        return abs(new_height - old_height)
    if kind in (ROAD, BRIDGE):
        return 1
    raise ValueError(f"unknown action kind {kind}")


def passable_infrastructure(infrastructure, terrain, water):
    """Surface roads flood; bridge decks remain passable above the water."""
    return {
        cell
        for cell, kind in infrastructure.items()
        if kind == BRIDGE
        or (kind == ROAD and water.get(cell) == terrain.get(cell))
    }


def connected_cells(passable, depots):
    """Four-neighbor road/bridge reachability, with depots as network seeds."""
    allowed = set(passable) | set(depots)
    seen = set(depots)
    todo = deque(depots)
    while todo:
        cell = todo.popleft()
        for nxt in neighbors(cell):
            if nxt in allowed and nxt not in seen:
                seen.add(nxt)
                todo.append(nxt)
    return seen


def construction_frontier(connected, cells):
    """Cells on which a connected crew can start a new asset or earthwork."""
    domain = set(cells)
    return {
        nxt
        for cell in connected
        for nxt in neighbors(cell)
        if nxt in domain
    }


def shortest_service_path(passable, depots, target):
    """Shortest four-neighbor infrastructure path, in edges, or None."""
    allowed = set(passable) | set(depots)
    todo = deque((cell, 0) for cell in depots)
    seen = set(depots)
    while todo:
        cell, distance = todo.popleft()
        if cell == target:
            return distance
        for nxt in neighbors(cell):
            if nxt in allowed and nxt not in seen:
                seen.add(nxt)
                todo.append((nxt, distance + 1))
    return None


def replay_actions(
    terrain,
    initial_infrastructure,
    depots,
    roles,
    grants,
    actions,
    priority_flood,
    locked=(),
    protected=(),
):
    """Replay revision-grouped actions and return state plus audit violations.

    Each revision is one role-homogeneous batch.  Build batches may extend
    through their own cells, but terraform access is checked against the
    closed state before any cell in the batch changes.
    """
    terrain = dict(terrain)
    infrastructure = dict(initial_infrastructure)
    depots = set(depots)
    roles = {int(agent): int(kind) for agent, kind in roles.items()}
    grants = {(int(a), int(k)): int(v) for (a, k), v in grants.items()}
    locked = set(locked)
    protected = set(protected)
    water = priority_flood(terrain)
    spend = defaultdict(int)
    violations = []

    by_revision = defaultdict(list)
    for action in actions:
        by_revision[int(action["revision"])].append(action)

    expected_revision = 1
    for revision in sorted(by_revision):
        batch = sorted(by_revision[revision], key=lambda a: int(a["item"]))
        if revision != expected_revision:
            violations.append(
                f"revision gap: expected {expected_revision}, saw {revision}"
            )
            expected_revision = revision
        expected_revision += 1

        expected_items = list(range(len(batch)))
        actual_items = [int(a["item"]) for a in batch]
        if actual_items != expected_items:
            violations.append(
                f"revision {revision}: item ids {actual_items}, expected {expected_items}"
            )

        agents = {int(a["agent"]) for a in batch}
        kinds = {int(a["kind"]) for a in batch}
        if len(agents) != 1 or len(kinds) != 1:
            violations.append(
                f"revision {revision}: batch mixes agents {agents} or kinds {kinds}"
            )
            continue
        agent = next(iter(agents))
        kind = next(iter(kinds))
        if roles.get(agent) != kind:
            violations.append(
                f"revision {revision}: agent {agent} cannot {KIND_NAMES.get(kind, kind)}"
            )

        cells = [(int(a["x"]), int(a["y"])) for a in batch]
        if len(cells) != len(set(cells)):
            violations.append(f"revision {revision}: duplicate cells in batch")

        passable = passable_infrastructure(infrastructure, terrain, water)
        connected = connected_cells(passable, depots)
        frontier = construction_frontier(connected, terrain)

        if kind == TERRAFORM:
            bridge_worksites = {
                cell
                for cell in connected
                if infrastructure.get(cell) == BRIDGE
            }
            allowed = frontier | bridge_worksites
            for action, cell in zip(batch, cells):
                old = int(action["old"])
                new = int(action["new"])
                if cell not in terrain:
                    violations.append(f"revision {revision}: no terrain at {cell}")
                    continue
                if cell not in allowed:
                    violations.append(f"revision {revision}: inaccessible earthwork {cell}")
                if cell in locked or cell in protected:
                    violations.append(f"revision {revision}: protected earthwork {cell}")
                if infrastructure.get(cell) == ROAD:
                    violations.append(f"revision {revision}: earthwork under surface road {cell}")
                if terrain[cell] != old:
                    violations.append(
                        f"revision {revision}: stale height at {cell}: {old} != {terrain[cell]}"
                    )
                terrain[cell] = new
            water = priority_flood(terrain)

        elif kind in (ROAD, BRIDGE):
            if any(cell in infrastructure for cell in cells):
                violations.append(f"revision {revision}: infrastructure cell already occupied")
            forbidden = [cell for cell in cells if cell in locked or cell in protected]
            if forbidden:
                violations.append(
                    f"revision {revision}: infrastructure on locked cells {forbidden}"
                )
            if kind == ROAD:
                for cell in cells:
                    if water.get(cell) != terrain.get(cell):
                        violations.append(
                            f"revision {revision}: surface road built on wet cell {cell}"
                        )
            candidate = dict(infrastructure)
            for cell in cells:
                candidate[cell] = kind
            candidate_passable = passable_infrastructure(candidate, terrain, water)
            candidate_connected = connected_cells(candidate_passable, depots)
            disconnected = [cell for cell in cells if cell not in candidate_connected]
            if disconnected:
                violations.append(
                    f"revision {revision}: build is disconnected from depot: {disconnected}"
                )
            infrastructure = candidate
        else:
            violations.append(f"revision {revision}: unknown action kind {kind}")

        for action in batch:
            declared = int(action["cost"])
            actual = action_cost(
                kind, int(action["old"]), int(action["new"])
            )
            if declared != actual:
                violations.append(
                    f"revision {revision}: declared cost {declared} != {actual}"
                )
            spend[(agent, kind)] += actual
        if spend[(agent, kind)] > grants.get((agent, kind), 0):
            violations.append(
                f"revision {revision}: agent {agent} {KIND_NAMES.get(kind, kind)} "
                f"spend {spend[(agent, kind)]} exceeds grant "
                f"{grants.get((agent, kind), 0)}"
            )

    passable = passable_infrastructure(infrastructure, terrain, water)
    connected = connected_cells(passable, depots)
    return {
        "terrain": terrain,
        "water": water,
        "infrastructure": infrastructure,
        "passable": passable,
        "connected": connected,
        "spend": dict(spend),
        "violations": violations,
        "next_revision": expected_revision,
    }
