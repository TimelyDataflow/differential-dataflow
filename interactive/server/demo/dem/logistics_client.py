#!/usr/bin/env python3
"""Participant client for the civil-logistics game.

Examples:
  python3 logistics_client.py --run-dir runs/trial-01 --agent 1 status
  python3 logistics_client.py --run-dir runs/trial-01 --agent 1 road 97 27 96 27
  python3 logistics_client.py --run-dir runs/trial-01 --agent 2 bridge 95 35
  python3 logistics_client.py --run-dir runs/trial-01 --agent 3 terraform 97 28 1708
"""

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from logistics_rules import (
    BRIDGE,
    KIND_NAMES,
    ROAD,
    TERRAFORM,
    action_cost,
    replay_actions,
    shortest_service_path,
)
from logistics_runtime import (
    actions_from_client,
    append_event,
    read_briefing,
    run_lock,
)
from run_dem import Client, load_grid, priority_flood


def inputs_from_briefing(briefing):
    terrain = load_grid(os.path.join(HERE, "engadin_128.txt"))
    for x, y, old, new in briefing["nature_edits"]:
        if terrain[(x, y)] != old:
            raise RuntimeError(f"nature edit base mismatch at {(x, y)}")
        terrain[(x, y)] = new
    infrastructure = {
        (x, y): kind for x, y, kind in briefing["initial_infrastructure"]
    }
    roles = {
        int(agent): int(role["kind"])
        for agent, role in briefing["roles"].items()
    }
    grants = {
        (int(agent), int(role["kind"])): int(role["grant"])
        for agent, role in briefing["roles"].items()
    }
    return terrain, infrastructure, roles, grants


def replay(briefing, actions):
    terrain, infrastructure, roles, grants = inputs_from_briefing(briefing)
    return replay_actions(
        terrain=terrain,
        initial_infrastructure=infrastructure,
        depots={tuple(briefing["depot"])},
        roles=roles,
        grants=grants,
        actions=actions,
        priority_flood=priority_flood,
        locked={tuple(cell) for cell in briefing["locked"]},
        protected={tuple(cell) for cell in briefing["protected"]},
    )


def pairs(tokens):
    if not tokens or len(tokens) % 2:
        raise ValueError("expected one or more X Y pairs")
    values = [int(token) for token in tokens]
    return list(zip(values[0::2], values[1::2]))


def triples(tokens):
    if not tokens or len(tokens) % 3:
        raise ValueError("expected one or more X Y HEIGHT triples")
    values = [int(token) for token in tokens]
    return list(zip(values[0::3], values[1::3], values[2::3]))


def event(run_dir, agent, command, accepted, revision=None, detail=None):
    append_event(run_dir, {
        "time": time.time(),
        "agent": agent,
        "command": command,
        "accepted": accepted,
        "revision": revision,
        "detail": detail,
    })


def show_status(briefing, state, actions):
    village = [tuple(cell) for cell in briefing["village"]]
    dry = {
        cell: state["water"][cell] == state["terrain"][cell]
        for cell in village
    }
    wet_cells = sum(
        1 for cell, height in state["terrain"].items()
        if state["water"][cell] > height
    )
    service = shortest_service_path(
        state["passable"],
        {tuple(briefing["depot"])},
        tuple(briefing["town"]),
    )
    revision = max((action["revision"] for action in actions), default=0)
    print(f"revision {revision}; {wet_cells} wet cells")
    print(f"village dry: {dry}")
    print(
        f"town service: {service if service is not None else 'DISCONNECTED'} "
        f"hops (limit {briefing['max_service_hops']})"
    )
    print(
        f"infrastructure: {len(state['infrastructure'])} total, "
        f"{len(state['passable'])} passable, {len(state['connected'])} depot-connected"
    )
    for agent_text, role in sorted(briefing["roles"].items(), key=lambda row: int(row[0])):
        agent = int(agent_text)
        kind = int(role["kind"])
        spent = state["spend"].get((agent, kind), 0)
        print(
            f"agent {agent} {role['name']}: {KIND_NAMES[kind]} "
            f"{spent}/{role['grant']} (remaining {role['grant'] - spent})"
        )
    bridges = sorted(
        cell for cell, kind in state["infrastructure"].items() if kind == BRIDGE
    )
    print(f"bridges: {bridges or 'none'}")
    if state["violations"]:
        print(f"AUDIT WARNINGS: {state['violations']}")


def show_around(state, x0, y0, radius):
    if not 0 <= radius <= 6:
        raise ValueError("radius must be between 0 and 6")
    print("cell = terrain / marker / water-depth; C=connected road, B=bridge, r=flooded road, +=access")
    print("       " + "".join(f"x={x:^8}" for x in range(x0 - radius, x0 + radius + 1)))
    connected = state["connected"]
    access = {
        (nx, ny)
        for x, y in connected
        for nx, ny in ((x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1))
    }
    for y in range(y0 - radius, y0 + radius + 1):
        cells = []
        for x in range(x0 - radius, x0 + radius + 1):
            cell = (x, y)
            if cell not in state["terrain"]:
                cells.append("   ---- ")
                continue
            height = state["terrain"][cell]
            depth = state["water"][cell] - height
            kind = state["infrastructure"].get(cell)
            if kind == BRIDGE:
                marker = "B"
            elif kind == ROAD and cell in connected:
                marker = "C"
            elif kind == ROAD:
                marker = "r"
            elif cell in access:
                marker = "+"
            else:
                marker = "."
            cells.append(f"{height:4}{marker}{depth:>3}")
        print(f"y={y:>3} " + "".join(cells))


def commit(client, run_dir, briefing, agent, kind, payload, command_text):
    actions = actions_from_client(client)
    before = replay(briefing, actions)
    if before["violations"]:
        raise RuntimeError(f"existing world audit is not clean: {before['violations']}")
    role = briefing["roles"].get(str(agent))
    if role is None:
        raise ValueError(f"agent {agent} is not briefed")
    if int(role["kind"]) != kind:
        raise ValueError(
            f"agent {agent} is the {role['name']} and cannot {KIND_NAMES[kind]}"
        )

    revision = max((action["revision"] for action in actions), default=0) + 1
    proposed = []
    if kind == TERRAFORM:
        for item, (x, y, new) in enumerate(payload):
            cell = (x, y)
            if cell not in before["terrain"]:
                raise ValueError(f"no terrain cell {cell}")
            old = before["terrain"][cell]
            proposed.append({
                "revision": revision,
                "item": item,
                "agent": agent,
                "kind": kind,
                "x": x,
                "y": y,
                "old": old,
                "new": new,
                "cost": action_cost(kind, old, new),
            })
    else:
        for item, (x, y) in enumerate(payload):
            proposed.append({
                "revision": revision,
                "item": item,
                "agent": agent,
                "kind": kind,
                "x": x,
                "y": y,
                "old": 0,
                "new": 0,
                "cost": 1,
            })

    after = replay(briefing, actions + proposed)
    if after["violations"]:
        raise ValueError("; ".join(after["violations"]))

    feeds = []
    for action in proposed:
        if kind == TERRAFORM:
            feeds.extend([
                f"feed water 0 {action['x']},{action['y']} val={action['old']} diff=-1",
                f"feed water 0 {action['x']},{action['y']} val={action['new']}",
            ])
        feeds.append(
            f"feed logistics 0 {revision},{action['item']} "
            f"val={agent},{kind},{action['x']},{action['y']},"
            f"{action['old']},{action['new']},{action['cost']}"
        )
    client.batch(feeds)
    client.cmd("tick")
    event(run_dir, agent, command_text, True, revision, proposed)
    print(
        f"committed revision {revision}: {KIND_NAMES[kind]} "
        f"{len(proposed)} cell(s), cost {sum(a['cost'] for a in proposed)}"
    )
    for action in proposed:
        if kind == TERRAFORM:
            print(
                f"  ({action['x']},{action['y']}) "
                f"{action['old']} -> {action['new']} cost {action['cost']}"
            )
        else:
            print(f"  ({action['x']},{action['y']})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--agent", type=int, required=True)
    parser.add_argument("command", nargs="+")
    args = parser.parse_args()
    run_dir = os.path.abspath(args.run_dir)
    briefing = read_briefing(run_dir)
    if str(args.agent) not in briefing["roles"]:
        raise SystemExit(f"agent {args.agent} is not in the briefing")
    command = args.command[0]

    try:
        with run_lock(run_dir):
            client = Client(int(briefing["port"]))
            actions = actions_from_client(client)
            state = replay(briefing, actions)
            if command == "status":
                show_status(briefing, state, actions)
            elif command == "around":
                if len(args.command) != 4:
                    raise ValueError("around requires X Y R")
                show_around(state, *(int(value) for value in args.command[1:]))
            elif command == "roads":
                for cell, kind in sorted(state["infrastructure"].items()):
                    condition = (
                        "connected" if cell in state["connected"]
                        else "passable" if cell in state["passable"]
                        else "flooded"
                    )
                    print(f"{cell}: {KIND_NAMES[kind]} {condition}")
            elif command == "road":
                commit(
                    client, run_dir, briefing, args.agent, ROAD,
                    pairs(args.command[1:]), " ".join(args.command),
                )
            elif command == "bridge":
                commit(
                    client, run_dir, briefing, args.agent, BRIDGE,
                    pairs(args.command[1:]), " ".join(args.command),
                )
            elif command == "terraform":
                commit(
                    client, run_dir, briefing, args.agent, TERRAFORM,
                    triples(args.command[1:]), " ".join(args.command),
                )
            else:
                raise ValueError(
                    "commands: status | around X Y R | roads | "
                    "road X Y... | bridge X Y... | terraform X Y H..."
                )
    except (ValueError, RuntimeError) as error:
        event(run_dir, args.agent, " ".join(args.command), False, detail=str(error))
        raise SystemExit(f"rejected: {error}")


if __name__ == "__main__":
    main()
