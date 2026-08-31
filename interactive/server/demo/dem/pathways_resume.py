#!/usr/bin/env python3
"""Rebuild one pathways world by replaying its accepted semantic commands.

The DDIR server is intentionally in-memory. This utility is the crash-recovery
boundary: it stages a fresh server from the recorded briefing, then replays
accepted surveys, deliveries, paving, and route retirement in event order.
"""

import argparse
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from pathways_game import (
    SITES_128_V3,
    program_hashes,
    setup,
    stop_recorded_server,
    write_json,
)
from pathways_rules import BRIDGE, ENGINEERED_ROAD
from run_dem import Client


REPLAYABLE = {"survey", "scout", "deliver", "pave", "build", "retire"}


def port_is_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def accepted_commands(events_path):
    commands = []
    with open(events_path) as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            event = json.loads(line)
            if not event.get("accepted"):
                continue
            tokens = shlex.split(event["command"])
            if not tokens or tokens[0] not in REPLAYABLE:
                raise RuntimeError(
                    f"cannot replay accepted event {line_number}: {event['command']}"
                )
            commands.append((int(event["agent"]), tokens))
    return commands


def upgraded_briefing(briefing, legacy_event_count):
    upgraded = json.loads(json.dumps(briefing))
    existing = {int(site["id"]) for site in upgraded["sites"]}
    for site_id, label, x, y, kind, amount in SITES_128_V3:
        if site_id not in existing:
            upgraded["sites"].append({
                "id": site_id,
                "label": label,
                "cell": [x, y],
                "kind": kind,
                "amount": amount,
            })
    upgraded.update({
        "version": 3,
        "road_grade_permille": 400,
        "initial_aggregate": 17,
        "quarry_aggregate": 24,
        "initial_rock": 10,
        "quarry_rock": 50,
        "drainage_embankment_fill": 20,
        "quarry_activation_agent": 1,
        "worksite_haul_agent": 3,
        "scout_agent": 1,
        "scout_trip_limit": 2,
        "legacy_event_count": legacy_event_count,
        "program_hashes": program_hashes(),
        "goal": (
            "activate and road-connect the mountain quarry, then deliver its "
            "bulk rock to the connected watershed worksite without flooding "
            "a protected valley site"
        ),
    })
    upgraded["agents"]["1"].update({
        "name": "mountain courier",
        "build_kinds": [],
        "suggested_coefficients": [1, 12, 1000, 0, 1],
    })
    upgraded["agents"]["2"].update({
        "name": "road engineer",
        "build_kinds": [ENGINEERED_ROAD],
        "suggested_coefficients": [1, 4, 1000, 8, 1],
    })
    upgraded["agents"]["3"].update({
        "name": "structures and works crew",
        "build_kinds": [BRIDGE],
        "suggested_coefficients": [1, 4, 1000, 1, 1],
    })
    upgraded["rules"] = list(upgraded["rules"]) + [
        "Two light five-unit courier deliveries activate quarry 20 and establish its approach path.",
        "The quarry unlocks aggregate and rock only after source-connected road access; it never seeds the network.",
        "Agent 1 may scout a route twice to establish a cheap footpath before bulk freight can move.",
        "Agent 2 builds grade-limited surface roads; agent 3 builds required bridges and hauls bulk rock.",
        "Deterministic road fill consumes rock and edits live terrain/water; bridges preserve the drainage channel.",
        "Each route commits to a bridge or embankment alignment; the latter raises runoff crossings by 20 coarse elevation units.",
    ]
    return upgraded


def resume(source_run, destination_run, port, ws_host, upgrade_v3=False):
    source_run = os.path.abspath(source_run)
    destination_run = os.path.abspath(destination_run)
    with open(os.path.join(source_run, "briefing.json")) as source:
        briefing = json.load(source)
    if briefing.get("version", 1) < 2:
        raise RuntimeError("semantic replay supports pathways briefing version 2+")
    expected_hashes = briefing.get("program_hashes")
    if (
        not upgrade_v3
        and expected_hashes is not None
        and expected_hashes != program_hashes()
    ):
        raise RuntimeError(
            "current DDIR programs differ from the recorded world; "
            "recover with the matching revision"
        )
    if port_is_open(int(briefing["port"])):
        raise RuntimeError(
            "source world is still reachable; stop it before creating a replacement"
        )

    if os.path.exists(destination_run):
        raise RuntimeError(f"destination already exists: {destination_run}")
    commands = accepted_commands(os.path.join(source_run, "events.jsonl"))
    if upgrade_v3:
        if int(briefing.get("version", 1)) != 2:
            raise RuntimeError("--upgrade-v3 requires a V2 source world")
        if briefing["grid"] != "engadin_128.txt":
            raise RuntimeError("the V3 hill sites are calibrated for engadin_128")
    with open(os.path.join(source_run, "site_office.md")) as source:
        prior_office = source.read()

    staging_run = destination_run + ".recovering-" + uuid.uuid4().hex[:8]
    client_path = os.path.join(HERE, "pathways_client.py")
    try:
        setup(
            staging_run,
            port,
            ws_host,
            briefing["grid"],
            briefing_template=briefing,
        )
        for agent, tokens in commands:
            subprocess.run(
                [
                    sys.executable,
                    client_path,
                    "--run-dir",
                    staging_run,
                    "--agent",
                    str(agent),
                    "--replay",
                    *tokens,
                ],
                check=True,
            )

        if upgrade_v3:
            upgraded = upgraded_briefing(briefing, len(commands))
            client = Client(port)
            old_policy = "2,0,0,0,0,0"
            new_policy = (
                f"3,{upgraded['road_grade_permille']},"
                f"{upgraded['initial_aggregate']},"
                f"{upgraded['quarry_aggregate']},"
                f"{upgraded['initial_rock']},{upgraded['quarry_rock']}"
            )
            feeds = [
                f"feed pathways 7 0 val={old_policy} diff=-1",
                f"feed pathways 7 0 val={new_policy}",
            ]
            prior_sites = {int(site["id"]) for site in briefing["sites"]}
            feeds.extend(
                f"feed pathways 0 {site['id']} "
                f"val={site['cell'][0]},{site['cell'][1]},"
                f"{site['kind']},{site['amount']}"
                for site in upgraded["sites"]
                if int(site["id"]) not in prior_sites
            )
            client.batch(feeds)
            client.cmd("tick")
            with open(os.path.join(staging_run, "briefing.json")) as source:
                staged = json.load(source)
            upgraded["run_dir"] = staged["run_dir"]
            upgraded["port"] = staged["port"]
            upgraded["ws_url"] = staged["ws_url"]
            write_json(os.path.join(staging_run, "briefing.json"), upgraded)

        office_path = os.path.join(staging_run, "site_office.md")
        with open(office_path, "w") as destination:
            destination.write(prior_office.rstrip())
            destination.write(
                "\n\n## Recovery\n\n"
                f"Replayed {len(commands)} accepted commands from "
                f"`{source_run}`.\n"
            )
        with open(os.path.join(staging_run, "briefing.json")) as source:
            recovered_briefing = json.load(source)
        recovered_briefing["run_dir"] = destination_run
        write_json(
            os.path.join(staging_run, "briefing.json"),
            recovered_briefing,
        )
        os.rename(staging_run, destination_run)
    except Exception:
        stop_recorded_server(staging_run)
        shutil.rmtree(staging_run, ignore_errors=True)
        raise
    print(f"recovered {len(commands)} commands into {destination_run}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-run", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ws-host", default="127.0.0.1")
    parser.add_argument("--upgrade-v3", action="store_true")
    args = parser.parse_args()
    resume(
        args.from_run,
        args.run_dir,
        args.port,
        args.ws_host,
        upgrade_v3=args.upgrade_v3,
    )


if __name__ == "__main__":
    main()
